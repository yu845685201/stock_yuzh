"""P4-7 缠论互补性分段检验：按情绪周期四态分段重算一买/二买（+三买评估）。

目的（V2.0 P4-7）：联动分级规则（回暖/震荡→弱化观察；冰点/退潮→升主关注；
高潮→仅记录）当前无分段数据支撑——本脚本按**复盘系统同一情绪判据口径**
（config review.sentiment_rules，命中顺序 高潮→冰点→退潮→回暖/震荡）对
验证样本（2015-01~2026-09 全A）逐信号日定态，输出各态 60 日 fixed 费后
均值/胜率分段表，回答"冰点/退潮期升主关注是否有数据支撑"；附带评估三买
负超额是否在特定态翻转（当前仍不引入，仅报告）。

方法（口径零漂移）：
1. 情绪四态：按 SentimentMetricsManager._backfill_chunk 的**同一实现链**
   （annotate_panel / suppress_first_day / cross_section_at / sentiment_metrics，
   单一出处）按年块计算 2015-2026 逐日 max_streak/burst_rate/premium_mean，
   只读不写库（区别于回填）；逐日缓存 data/chan_validation/sentiment_states_2015_2026.csv；
2. 信号样本：base/signals.csv × trades（fixed 60）——一买/二买套用 P0-7
   520 日窗口覆盖率门控（与 missing_sensitivity_20261025_000524 保留样本
   同批），三买同样门控仅作附带评估；
3. 分段统计 + 与全期保留样本对比 → 联动分级定稿结论。

用法：
    python scripts/chan_sentiment_segment.py [--min-coverage 0.98] [--refresh-states]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from bisect import bisect_left, bisect_right
from pathlib import Path

import pandas as pd
import yaml

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
sys.path.insert(0, str(BACKEND / "scripts"))

import chan_missing_sensitivity as cms  # noqa: E402  （覆盖率门控同源）
from src.analysis.data_reader import DataReader  # noqa: E402
from src.config.config_manager import ConfigManager  # noqa: E402
from src.review.cross_section import (annotate_panel, cross_section_at,  # noqa: E402
                                      sentiment_metrics, suppress_first_day)
from src.sync.sentiment_metrics_manager import PANEL_COLS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chan_sentiment_segment")

KIND_CN = {"b1": "一买", "b2": "二买", "b3": "三买（附带评估，未引入）"}
STATES = ("高潮", "冰点", "退潮", "回暖/震荡")
REPORT_DIR = cms.REPORT_DIR
STATES_CACHE = REPORT_DIR / "sentiment_states_2015_2026.csv"
START, END = "20150105", "20260930"


def sentiment_rules() -> dict:
    """情绪判据阈值（config 单一出处）。"""
    cfg = yaml.safe_load((BACKEND.parent / "config" / "config.yaml").read_text("utf-8"))
    return ((cfg.get("review") or {}).get("sentiment_rules") or {})


def state_of(t: str, trio: dict, rules: dict) -> str:
    """判据定态（与 aggregator._sentiment_cycle 同一命中顺序：高潮→冰点→退潮→回暖/震荡）。

    trio 值缺失（数据不足）→ '数据不足'（不计入分段）。
    """
    m = trio.get(t)
    if not m or m.get("max_streak") is None:
        return "数据不足"
    climax, ice, ebb = rules.get("climax") or {}, rules.get("ice") or {}, rules.get("ebb") or {}
    ms, br, pm = m["max_streak"], m.get("burst_rate"), m.get("premium_mean")
    if (ms >= int(climax.get("max_streak_gte", 99))
            and pm is not None and pm >= float(climax.get("premium_mean_gte", 99))):
        return "高潮"
    if (ms <= int(ice.get("max_streak_lte", -1))
            or (br is not None and br >= float(ice.get("burst_rate_gte", 99)))):
        return "冰点"
    if pm is not None and pm <= float(ebb.get("premium_mean_lte", 999)):
        return "退潮"
    return "回暖/震荡"


def compute_states(reader: DataReader, refresh: bool) -> dict:
    """逐日情绪三量（2015-2026，同 manager 年块口径；缓存 CSV 供复用）。"""
    if STATES_CACHE.exists() and not refresh:
        df = pd.read_csv(STATES_CACHE, dtype={"trade_date": str})
        log.info("情绪三量缓存命中：%d 天（%s）", len(df), STATES_CACHE.name)
        return {str(r.trade_date): {"max_streak": r.max_streak,
                                    "burst_rate": None if pd.isna(r.burst_rate) else float(r.burst_rate),
                                    "premium_mean": None if pd.isna(r.premium_mean) else float(r.premium_mean)}
                for r in df.itertuples()}

    from datetime import date, timedelta
    trio: dict = {}
    y = int(START[:4])
    while y <= int(END[:4]):
        chunk_start, chunk_end = date(y, 1, 1), date(y, 12, 31)
        rows = reader._query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE is_trading_day = 1 AND calendar_date >= %s AND calendar_date <= %s "
            "ORDER BY calendar_date",
            ((chunk_start - timedelta(days=400)).isoformat(), chunk_end.isoformat()))
        cal_all = [str(r[0]).replace("-", "") for r in rows]
        dates = [d for d in cal_all if START <= d <= END]
        if not dates:
            y += 1
            continue
        # 与 manager 同款：块内逐日，块首前推 62 交易日做面板 warmup
        anchor_idx = next((i for i, d in enumerate(cal_all) if d >= dates[0]), 0)
        panel_start = cal_all[max(0, anchor_idx - 62)]
        panel = reader.load_kline(PANEL_COLS, dates[-1], start_date=panel_start)
        if panel.empty:
            log.warning("块 %d 面板为空", y)
            y += 1
            continue
        panel = annotate_panel(panel)
        list_dates = {s.ts_code: s.list_date for s in reader.load_stock_universe()}
        panel = suppress_first_day(panel, list_dates)
        n_done = 0
        for i, t in enumerate(dates):
            prev_t = cal_all[anchor_idx + i - 1] if anchor_idx + i >= 1 else None
            cs_today = cross_section_at(panel, t)
            if cs_today.empty:
                continue
            cs_prev = cross_section_at(panel, prev_t) if prev_t else cs_today.iloc[0:0]
            m = sentiment_metrics(cs_today, cs_prev)
            trio[t] = {"max_streak": int(m["max_streak"] or 0),
                       "burst_rate": m.get("burst_rate"),
                       "premium_mean": m.get("premium_mean")}
            n_done += 1
        log.info("块 %d：%d 天三量完成", y, n_done)
        y += 1
    pd.DataFrame([{"trade_date": t, **v} for t, v in sorted(trio.items())]).to_csv(
        STATES_CACHE, index=False)
    return trio


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-coverage", type=float, default=0.98)
    ap.add_argument("--refresh-states", action="store_true", help="忽略三量缓存重算")
    args = ap.parse_args()
    t0 = time.time()

    # 信号 × 60 日 fixed 费后（与敏感性/基线脚本同批构造）
    sig_dir = REPORT_DIR / "base"
    signals = pd.read_csv(sig_dir / "signals.csv", dtype=str)
    trades = pd.read_csv(sig_dir / "trades.csv", dtype=str)
    t60 = trades[(trades["mode"] == "fixed") & (trades["param"] == "60")][
        ["signal_kind", "ts_code", "fire_date", "ret_net"]].copy()
    t60["ret_net"] = t60["ret_net"].astype(float)
    t60 = t60.drop_duplicates(subset=["signal_kind", "ts_code", "fire_date"], keep="first")
    sig = signals.merge(t60, left_on=["kind", "ts_code", "fire_date"],
                        right_on=["signal_kind", "ts_code", "fire_date"],
                        how="left").dropna(subset=["ret_net"])

    # 覆盖率门控（P0-7 同口径；与 000524 保留样本对齐校验）
    conn_args = None
    import psycopg2
    from src.chan.data import DB
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
        expected, actual = cms._span_stats(cal, dates, fidx, 520,
                                           list_dates.get(row.ts_code, ""), False)
        return actual / expected if expected > 0 else float("nan")

    sig["cov"] = sig.apply(_cov, axis=1)
    keep = sig.dropna(subset=["cov"])
    keep = keep[keep["cov"] >= args.min_coverage].copy()
    n_b1 = int((keep["kind"] == "b1").sum())
    n_b2 = int((keep["kind"] == "b2").sum())
    log.info("保留样本 b1/b2/b3：%d / %d / %d（b1/b2 与 000524 对齐校验 4366/2254）",
             n_b1, n_b2, int((keep["kind"] == "b3").sum()))

    # 情绪四态
    reader = DataReader(ConfigManager())
    try:
        trio = compute_states(reader, args.refresh_states)
    finally:
        reader.close()
    rules = sentiment_rules()
    keep["state"] = keep["fire_date"].map(lambda d: state_of(d, trio, rules))

    lines = [
        f"# 缠论互补性分段检验（P4-7，{pd.Timestamp.now():%Y-%m-%d %H:%M}）",
        "",
        f"- 分段口径：复盘情绪判据（config sentiment_rules，命中顺序 高潮→冰点→"
        f"退潮→回暖/震荡），逐信号日定态；逐日三量与回填 manager 同一实现链"
        f"（annotate_panel/cross_section_at/sentiment_metrics）",
        f"- 样本：base 验证信号 × 60 日 fixed 费后，P0-7 门控保留样本"
        f"（b1 {n_b1} / b2 {n_b2} / b3 {int((keep['kind'] == 'b3').sum())}）；"
        f"定态'数据不足'的信号不计入",
        "",
    ]
    overall = {}
    for kind in ("b1", "b2", "b3"):
        k = keep[keep["kind"] == kind]
        overall[kind] = (k["ret_net"].mean(), (k["ret_net"] > 0).mean(), len(k))
    for kind in ("b1", "b2", "b3"):
        k = keep[keep["kind"] == kind]
        m_all, w_all, n_all = overall[kind]
        lines += [f"## {KIND_CN[kind]}（保留样本 n={n_all}，全期均值 {m_all:+.2%} / 胜率 {w_all:.1%}）",
                  "", "| 情绪态 | n | 均值 | 中位 | 胜率 | vs 全期均值 |", "|---|---|---|---|---|---|"]
        for st in STATES + ("数据不足",):
            g = k[k["state"] == st]
            if g.empty:
                lines.append(f"| {st} | 0 | — | — | — | — |")
                continue
            m, w = g["ret_net"].mean(), (g["ret_net"] > 0).mean()
            lines.append(f"| {st} | {len(g)} | {m:+.2%} | {g['ret_net'].median():+.2%} | "
                         f"{w:.1%} | {(m - m_all) * 100:+.2f}pp |")
        lines.append("")

    # 联动分级定稿评估（一买/二买：冰点/退潮 vs 回暖/震荡）
    lines += ["## 联动分级评估", ""]
    for kind in ("b1", "b2"):
        k = keep[keep["kind"] == kind]
        left = k[k["state"].isin(("冰点", "退潮"))]["ret_net"]
        warm = k[k["state"] == "回暖/震荡"]["ret_net"]
        if len(left) and len(warm):
            d = (left.mean() - warm.mean()) * 100
            verdict = "支撑" if d > 0 else "不支撑"
            lines.append(f"- {KIND_CN[kind]}：冰点/退潮期均值 {left.mean():+.2%}（n={len(left)}）"
                         f"vs 回暖/震荡 {warm.mean():+.2%}（n={len(warm)}）→ Δ{d:+.2f}pp，"
                         f"**{verdict}『冰点/退潮期升主关注』**")
    b3 = keep[keep["kind"] == "b3"]
    for st in ("冰点", "退潮"):
        g = b3[b3["state"] == st]
        if len(g):
            note = ("绝对均值为负，维持不引入"
                    if g["ret_net"].mean() < 0
                    else "绝对均值为正（超额未分段计算——分段随机基线不可得），"
                         "属样本外观察，不据此引入")
            lines.append(f"- 三买（附带评估）：{st}期绝对均值 {g['ret_net'].mean():+.2%}"
                         f"（n={len(g)}，胜率 {(g['ret_net'] > 0).mean():.0%}）——{note}")
    lines += ["", f"（耗时 {time.time() - t0:.0f}s）", ""]

    out = REPORT_DIR / f"sentiment_segment_{time.strftime('%Y%m%d_%H%M%S')}.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("报告已产出: %s", out)
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
