"""P0-7 缺日门控保留样本的随机基线重放（HIST_NOTE 修订配套，2026-10-25）。

背景：HIST_NOTE 修订（用户批准）把先验均值/胜率切换为门控保留样本统计
（missing_sensitivity_20261025_000524 定稿口径）。基线是"同股票配对随机
重抽"（12 轮），缺日股被门控剔除后其配对基线抽取须同批剔除，否则超额
口径错配——故对保留样本池重放随机基线。

方法（方法零漂移）：直接复用 analyze_chan_results.run_baseline（REPS=12、
种子 20261024、valid_fire_mask 可执行约束、fixed 出场规则全部同原引擎），
仅把输入 trades 换成 520 日窗口保留样本（覆盖率 ≥ min_coverage，与复盘
两入口 scan_stock_anchor 同规则），s3 传空不参与。覆盖率计算复用
chan_missing_sensitivity 的日历/list_date/bar_dates 与 _span_stats。

产出：data/chan_validation/missing_sensitivity_gated_baseline_<ts>.md
（保留信号 vs 保留基线的均值/胜率/超额），供 chan_candidates.HIST_NOTE
与 report.md §3.6 引用。

用法：
    python scripts/chan_gated_baseline.py [--window 520] [--min-coverage 0.98]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
sys.path.insert(0, str(BACKEND / "scripts"))

import analyze_chan_results as acr  # noqa: E402
import chan_missing_sensitivity as cms  # noqa: E402
from src.chan.data import DB  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chan_gated_baseline")

KINDS = ("b1", "b2")
KIND_CN = {"b1": "一买", "b2": "二买"}
REPORT_DIR = cms.REPORT_DIR


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
    t60 = trades[(trades["mode"] == "fixed") & (trades["param"] == "60")][
        ["signal_kind", "ts_code", "fire_date", "ret_net"]].copy()
    t60["ret_net"] = t60["ret_net"].astype(float)
    t60 = t60.drop_duplicates(subset=["signal_kind", "ts_code", "fire_date"],
                              keep="first")
    sig = signals.merge(t60, left_on=["kind", "ts_code", "fire_date"],
                        right_on=["signal_kind", "ts_code", "fire_date"],
                        how="left").dropna(subset=["ret_net"])

    conn = psycopg2.connect(**DB)
    try:
        cal = cms.load_calendar_list(conn, cms.LOAD_FROM, "20260930")
        cal_idx = {d: i for i, d in enumerate(cal)}
        codes = sorted(sig["ts_code"].unique())
        list_dates = cms.load_list_dates(conn, codes)
        bar_dates = cms.load_bar_dates(conn, codes)
    finally:
        conn.close()

    def _cov(row) -> float:
        dates = bar_dates.get(row.ts_code)
        fidx = cal_idx.get(row.fire_date)
        if not dates or fidx is None:
            return float("nan")
        expected, actual = cms._span_stats(cal, dates, fidx, window,
                                           list_dates.get(row.ts_code, ""), False)
        return actual / expected if expected > 0 else float("nan")

    sig["cov"] = sig.apply(_cov, axis=1)
    keep = sig.dropna(subset=["cov"])
    keep = keep[keep["cov"] >= min_cov].copy()
    log.info("保留样本 b1/b2：%d / %d 条（与 000524 定稿 4366/2254 对齐校验）",
             int((keep["kind"] == "b1").sum()), int((keep["kind"] == "b2").sum()))

    # 保留池重放随机基线：run_baseline 原样复用（s3 传空）。
    # 注意先选列再改名：keep 同时含 kind 与 signal_kind（merge 双列），直接
    # rename 会产生重名列 → g["signal_kind"] 退化成 DataFrame、kind 变行数组。
    fake_trades = keep[["ts_code", "kind", "fire_date"]].rename(
        columns={"kind": "signal_kind"}).copy()
    fake_trades["mode"] = "fixed"
    empty_signals = pd.DataFrame(columns=["kind", "ts_code", "fire_date"])
    out_dir = REPORT_DIR / "base_gated"
    out_dir.mkdir(parents=True, exist_ok=True)
    rand = acr.run_baseline(fake_trades, empty_signals, out_dir)

    r60 = rand[(rand["kind"].isin(KINDS)) & (rand["param"] == 60)].copy()
    if r60.empty:
        raise RuntimeError("重放基线为空（b1/b2 fixed60 无记录），检查 fake_trades 构造")
    base_by_rep = r60.groupby(["kind", "rep"])["ret"].mean()  # 每轮均值（同 bootstrap_diff 口径）

    lines = [
        f"# 门控保留样本随机基线重放（HIST_NOTE 修订配套，"
        f"{pd.Timestamp.now():%Y-%m-%d %H:%M}）",
        "",
        f"- 方法：复用 run_baseline（REPS={acr.REPS}、种子 20261024、"
        f"valid_fire_mask 同口径），信号池 = 520 日窗口保留样本"
        f"（coverage ≥ {min_cov:.0%}，与 missing_sensitivity_20261025_000524 同批）",
        "",
        "| 种类 | 保留信号 n | 保留均值/胜率 | 基线抽取 n | 基线均值 | 超额 |",
        "|---|---|---|---|---|---|",
    ]
    excess_out: dict[str, float] = {}
    for kind in KINDS:
        k = keep[keep["kind"] == kind]
        m_ke, w_ke = k["ret_net"].mean(), (k["ret_net"] > 0).mean()
        m_r = float(base_by_rep.loc[kind].mean())
        exc = (m_ke - m_r) * 100
        excess_out[kind] = exc
        n_base = int((r60["kind"] == kind).sum())
        lines.append(
            f"| {KIND_CN[kind]} | {len(k)} | {m_ke:+.2%} / {w_ke:.1%} |"
            f" {n_base} | {m_r:+.2%} | **{exc:+.2f}pp** |")
    lines += [
        "",
        f"结论：HIST_NOTE 修订采用保留口径超额 "
        f"{' / '.join(f'{excess_out[k]:+.2f}pp' for k in KINDS)}。",
        f"（耗时 {time.time() - t0:.0f}s）", "",
    ]
    out = REPORT_DIR / f"missing_sensitivity_gated_baseline_{time.strftime('%Y%m%d_%H%M%S')}.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("报告已产出: %s", out)
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
