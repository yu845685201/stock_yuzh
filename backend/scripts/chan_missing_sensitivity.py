"""P0-7 缠论缺日敏感性：窗口覆盖门控对一买/二买先验统计的影响（V2.0 §3.4）。

目的：确认 HIST_NOTE 先验（一买 60 日费后 +6.8%/胜率 53.1%、二买 +5.1%/51.6%）
在 P0-7 覆盖门控（coverage < min_coverage 剔除）下量级不变——V2.0 方案的
结论性风险项。若任一统计变动 >1pp → 升级 HIST_NOTE 修订流程（同步
chan_candidates.HIST_NOTE / SKILL.md / 方案状态）。

方法（不重跑结构引擎，直接复用 base 验证产物做样本切分）：
1. base/signals.csv（b1/b2）× base/trades.csv（fixed 60）→ 每信号 60 日费后收益；
2. 对每个信号计算其 **520 交易日窗口**（门控口径，与复盘两入口一致）与
   **全历史口径**（验证引擎实际加载跨度，20140601 起）的K线覆盖率
   （expected 按股票 list_date 起算，防次新误判，与 scan_stock_anchor 同规则）；
3. 分「全样本 vs 门控保留样本（coverage ≥ 0.98）」输出均值/胜率对比表
   → ``data/chan_validation/missing_sensitivity_<ts>.md``。

用法：
    python scripts/chan_missing_sensitivity.py [--window 520] [--min-coverage 0.98]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from bisect import bisect_left, bisect_right
from pathlib import Path

import pandas as pd
import psycopg2

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.chan.data import DB, LOAD_FROM, fetch_df  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chan_missing_sens")

KINDS = ("b1", "b2")
KIND_CN = {"b1": "一买", "b2": "二买"}
EXIT_MODE, EXIT_PARAM = "fixed", 60
REPORT_DIR = BACKEND.parent / "data" / "chan_validation"


def load_calendar_list(conn, start: str, end: str) -> list[str]:
    df = fetch_df(
        conn,
        "SELECT calendar_date FROM base_trade_calendar "
        "WHERE is_trading_day=1 AND calendar_date>=%s AND calendar_date<=%s "
        "ORDER BY calendar_date",
        (f"{start[:4]}-{start[4:6]}-{start[6:]}",
         f"{end[:4]}-{end[4:6]}-{end[6:]}"),
    )
    return [d.replace("-", "") for d in df["calendar_date"]]


def load_list_dates(conn, codes: list[str]) -> dict[str, str]:
    df = fetch_df(
        conn,
        "SELECT DISTINCT ts_code, list_date FROM base_stock_info "
        "WHERE ts_code = ANY(%s) AND list_date IS NOT NULL",
        (codes,),
    )
    return {str(r.ts_code): str(r.list_date).replace("-", "")
            for r in df.itertuples()}


def load_bar_dates(conn, codes: list[str]) -> dict[str, list[str]]:
    """逐股K线日期列表（升序；仅日期，不取价格——覆盖率只需行数）。"""
    out: dict[str, list[str]] = {}
    B = 500
    for i in range(0, len(codes), B):
        chunk = codes[i:i + B]
        df = fetch_df(
            conn,
            "SELECT ts_code, trade_date FROM his_kline_day "
            "WHERE ts_code = ANY(%s) AND trade_date>=%s ORDER BY ts_code, trade_date",
            (chunk, LOAD_FROM),
        )
        df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "")
        for code, g in df.groupby("ts_code", sort=True):
            out[str(code)] = g["trade_date"].tolist()
        log.info("bar dates %d/%d 股", min(i + B, len(codes)), len(codes))
    return out


def _span_stats(cal: list[str], dates: list[str], fire_idx: int, window: int,
                list_date: str, full_span: bool) -> tuple[int, int]:
    """返回 (expected, actual)：应含交易日数 vs 实际K线数。

    窗口起点 = max(窗口首日, list_date)（次新按上市日起算，与
    scan_stock_anchor 的 expected_bars 口径一致）。full_span=True 时窗口为
    验证引擎的实际加载跨度（LOAD_FROM 起），False 时为末尾 ``window`` 个交易日。
    """
    if full_span:
        start_idx = 0
    else:
        start_idx = max(0, fire_idx - window + 1)
    eff = cal[start_idx]
    if list_date and list_date > eff:
        eff = list_date
    e_idx = bisect_left(cal, eff)
    expected = fire_idx - e_idx + 1
    a_idx = bisect_left(dates, eff)
    actual = bisect_right(dates, cal[fire_idx]) - a_idx
    return expected, actual


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--window", type=int, default=520)
    ap.add_argument("--min-coverage", type=float, default=0.98)
    args = ap.parse_args()
    window, min_cov = args.window, args.min_coverage

    t0 = time.time()
    sig_dir = REPORT_DIR / "base"
    signals = pd.read_csv(sig_dir / "signals.csv", dtype=str)
    signals = signals[signals["kind"].isin(KINDS)].copy()
    trades = pd.read_csv(sig_dir / "trades.csv", dtype=str)
    t60 = trades[(trades["mode"] == EXIT_MODE)
                 & (trades["param"] == str(EXIT_PARAM))][
        ["signal_kind", "ts_code", "fire_date", "ret_net"]].copy()
    t60["ret_net"] = t60["ret_net"].astype(float)
    # 同信号多行（涨停顺延重入场变体，实测 87 行）→ 按信号键去重取首行
    t60 = t60.drop_duplicates(subset=["signal_kind", "ts_code", "fire_date"],
                              keep="first")
    sig = signals.merge(t60, left_on=["kind", "ts_code", "fire_date"],
                        right_on=["signal_kind", "ts_code", "fire_date"],
                        how="left")
    n_no_trade = int(sig["ret_net"].isna().sum())
    sig = sig.dropna(subset=["ret_net"])
    log.info("信号 %d 条（b1/b2，缺 60 日成交 %d 条）", len(sig), n_no_trade)

    conn = psycopg2.connect(**DB)
    try:
        cal = load_calendar_list(conn, LOAD_FROM, "20260930")
        cal_idx = {d: i for i, d in enumerate(cal)}
        codes = sorted(sig["ts_code"].unique())
        log.info("涉及股票 %d 只，日历 %d 天（%s~%s）", len(codes), len(cal),
                 cal[0], cal[-1])
        list_dates = load_list_dates(conn, codes)
        bar_dates = load_bar_dates(conn, codes)
    finally:
        conn.close()

    def _cov(row, full_span: bool) -> float:
        dates = bar_dates.get(row.ts_code)
        if not dates:
            return float("nan")
        fidx = cal_idx.get(row.fire_date)
        if fidx is None:
            return float("nan")
        expected, actual = _span_stats(cal, dates, fidx, window,
                                       list_dates.get(row.ts_code, ""),
                                       full_span)
        return actual / expected if expected > 0 else float("nan")

    sig["cov_520"] = sig.apply(lambda r: _cov(r, False), axis=1)
    sig["cov_full"] = sig.apply(lambda r: _cov(r, True), axis=1)
    sig = sig.dropna(subset=["cov_520", "cov_full"])

    lines: list[str] = [
        f"# 缠论缺日敏感性（P0-7，{pd.Timestamp.now():%Y-%m-%d %H:%M}）",
        "",
        f"- 门控口径：520 交易日窗口覆盖率 < {min_cov:.0%} 剔除"
        f"（与复盘两入口 scan_stock_anchor expected_bars 同规则，"
        f"expected 按股票 list_date 起算）",
        f"- 样本：base 验证信号（2015-01~2026-09 全A），b1/b2，"
        f"60 日 fixed 费后收益；共 {len(sig)} 条 / "
        f"{sig['ts_code'].nunique()} 只",
        "",
    ]
    overall_change = 0.0
    for label, col in (("520 交易日窗口（门控口径）", "cov_520"),
                       ("全历史（验证引擎加载跨度）", "cov_full")):
        lines += [f"## 口径：{label}", "",
                  "| 种类 | 全样本 n | 全样本均值 | 全样本胜率 |"
                  " 保留样本 n | 保留均值 | 保留胜率 | Δ均值 | Δ胜率 |"
                  " 剔除信号占比 | 缺日股占比 |",
                  "|---|---|---|---|---|---|---|---|---|---|---|"]
        for kind in KINDS:
            k = sig[sig["kind"] == kind]
            keep = k[k[col] >= min_cov]
            drop_rate = 1 - len(keep) / len(k) if len(k) else 0.0
            stocks = k.groupby("ts_code")[col].min()
            stock_drop = (stocks < min_cov).mean()
            m_all, w_all = k["ret_net"].mean(), (k["ret_net"] > 0).mean()
            m_ke = keep["ret_net"].mean() if len(keep) else float("nan")
            w_ke = (keep["ret_net"] > 0).mean() if len(keep) else float("nan")
            dm = (m_ke - m_all) * 100
            dw = (w_ke - w_all) * 100
            overall_change = max(overall_change, abs(dm), abs(dw))
            lines.append(
                f"| {KIND_CN[kind]} | {len(k)} | {m_all:+.2%} | {w_all:.1%} |"
                f" {len(keep)} | {m_ke:+.2%} | {w_ke:.1%} |"
                f" {dm:+.2f}pp | {dw:+.2f}pp | {drop_rate:.1%} | {stock_drop:.1%} |")
        lines.append("")

    verdict = ("✅ 量级不变：门控剔除对先验统计的扰动 ≤1pp，HIST_NOTE 维持现状"
               if overall_change <= 1.0 else
               "⚠️ 变动 >1pp：触发 HIST_NOTE 修订流程（同步 chan_candidates."
               "HIST_NOTE / SKILL.md / 方案状态，先于其他缠论增强）")
    lines += ["## 结论", "", verdict, "",
              f"（最大变动 {overall_change:.2f}pp；耗时 {time.time() - t0:.0f}s）", ""]

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / f"missing_sensitivity_{time.strftime('%Y%m%d_%H%M%S')}.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("报告已产出: %s", out)
    print(verdict)
    return 0


if __name__ == "__main__":
    sys.exit(main())
