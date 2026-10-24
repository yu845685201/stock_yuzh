"""涨停判定 A/B 对比（P0-1/P0-2 回补闸门交付物，V2.0 §6.4）。

对 4 个历史日期双跑**旧实现**（固定下浮 0.10 + 首日清零）与**新实现**
（P0-1 动态阈值 + P0-2 次新无限制窗口），产出：
1. 翻转清单 CSV：code/date/列/old→new/翻转原因（低价补判|次新剔除）；
2. 汇总对比：涨停/跌停/炸板家数、炸板率、连板梯队分布、溢价均值、
   情绪判据输入与定位（config review.sentiment_rules 判据）；
3. 报告 ``doc/reports/limit_up_ab_diff_<ts>.md`` → **停，交用户审阅**
   （全量回补须用户授权后执行，见 V2.0 §8.2）。

用法：
    python scripts/limit_up_ab_diff.py [--dates 20261023,20220801,20150901,20080610]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.analysis.data_reader import DataReader  # noqa: E402
from src.config.config_manager import ConfigManager  # noqa: E402
from src.review import cross_section as cs  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("limit_up_ab")

#: 默认对比日期：10-23（现行）+ 护栏实测四日期中的 2015/2022 + 2008（低价股密集）
DEFAULT_DATES = ["20261023", "20220801", "20150901", "20080610"]
REPO = BACKEND.parent
REPORT_DIR = REPO / "doc" / "reports"
CSV_DIR = REPO / "data" / "reports"

_COLS = ['stock_name', 'open', 'high', 'low', 'close', 'preclose',
         'change_rate', 'volume', 'amount', 'is_st', 'raw_close']


def _fixed_thresholds(limit_pct, change_rate, raw_close=None, signed: str = "up"):
    """旧实现桩：固定下浮 0.10、无 raw_close 动态项（复刻 P0-1 前口径）。"""
    lp = pd.to_numeric(pd.Series(limit_pct), errors="coerce")
    th = lp - 0.10
    ex = pd.Series(False, index=lp.index)
    if signed == "down":
        th = -th
    return th, ex


def _run_panel(reader, as_of: str, mode: str) -> pd.DataFrame:
    """加载 62 交易日窗口面板并按 mode（old/new）追加判定列。"""
    start = reader.nth_trading_day_back(as_of, 62)
    df = reader.load_kline(_COLS, as_of, start_date=start)
    if df.empty:
        raise ValueError(f"{as_of} 窗口内无日K数据")
    import src.review.cross_section as mod
    import src.analysis.limit_up_rules as lur
    if mode == "old":
        # 旧实现桩须同时补丁两处引用（limit_up_rules.annotate_* 与
        # cross_section 炸板阈值各自持有 dynamic_thresholds 名字）
        origs = [(mod, mod.dynamic_thresholds), (lur, lur.dynamic_thresholds)]
        for _, cur in origs:
            pass
        mod.dynamic_thresholds = _fixed_thresholds
        lur.dynamic_thresholds = _fixed_thresholds
        try:
            panel = mod.annotate_panel(df)
        finally:
            mod.dynamic_thresholds = origs[0][1]
            lur.dynamic_thresholds = origs[1][1]
        list_dates = {s.ts_code: s.list_date for s in reader.load_stock_universe()}
        panel = cs.suppress_first_day(panel, list_dates)
    else:
        panel = mod.annotate_panel(df)
        list_dates = {s.ts_code: s.list_date for s in reader.load_stock_universe()}
        panel = cs.suppress_no_limit_window(panel, list_dates)
    return panel


def _sentiment_inputs(panel: pd.DataFrame, as_of: str) -> dict:
    """当日截面情绪判据输入（高度/溢价均值/炸板率）。"""
    today = panel[panel["trade_date"] == as_of]
    limit_up = today["_limit_up"].fillna(False)
    limit_down = today["_limit_down"].fillna(False)
    burst = today["_burst"].fillna(False)
    n_up, n_burst = int(limit_up.sum()), int(burst.sum())
    denom = n_up + n_burst
    burst_rate = round(n_burst / denom, 3) if denom else None
    streaks = today.loc[limit_up, "_streak"]
    max_streak = int(streaks.max()) if len(streaks) else 0
    ladder = today.loc[limit_up & (today["_streak"] >= 2), "_streak"] \
        .value_counts().sort_index(ascending=False).to_dict()
    # 溢价均值：昨日涨停股今日涨幅均值（与报告层 _add_prev_limit_up 同源列）
    prev = panel[panel["trade_date"] != as_of]
    prev_today = panel[panel["trade_date"] == as_of][["ts_code", "change_rate"]]
    prev_up = prev[prev["_limit_up"].fillna(False)][["ts_code"]]
    merged = prev_up.merge(prev_today, on="ts_code", how="left")
    premium = round(pd.to_numeric(merged["change_rate"], errors="coerce").mean(), 2) \
        if len(merged) else None
    return {"limit_up": n_up, "limit_down": int(limit_down.sum()),
            "burst": n_burst, "burst_rate": burst_rate,
            "max_streak": max_streak, "ladder": {int(k): int(v) for k, v in ladder.items()},
            "premium_mean": premium,
            "prev_limit_n": len(merged)}


def _position(metrics: dict, rules: dict) -> str:
    """按 config 情绪判据给定位（与报告层 _sentiment_cycle 同判据，无分位列）。"""
    climax = rules.get("climax") or {}
    ice = rules.get("ice") or {}
    ebb = rules.get("ebb") or {}
    hs, pm, br = metrics["max_streak"], metrics["premium_mean"], metrics["burst_rate"]
    if hs >= climax.get("max_streak_gte", 99) and pm is not None \
            and pm >= climax.get("premium_mean_gte", 99):
        return "高潮"
    if hs <= ice.get("max_streak_lte", -1) or (br is not None and br >= ice.get("burst_rate_gte", 99)):
        return "冰点"
    if pm is not None and pm <= ebb.get("premium_mean_lte", 999):
        return "退潮"
    return "回暖/震荡"


def compare_date(reader, as_of: str, rules: dict) -> dict:
    log.info("A/B %s：双跑新旧实现…", as_of)
    old_panel = _run_panel(reader, as_of, "old")
    new_panel = _run_panel(reader, as_of, "new")
    mo, mn = _sentiment_inputs(old_panel, as_of), _sentiment_inputs(new_panel, as_of)

    o = old_panel[old_panel["trade_date"] == as_of][
        ["ts_code", "stock_name", "_limit_up", "_limit_down", "_burst"]].copy()
    n = new_panel[new_panel["trade_date"] == as_of][
        ["ts_code", "_limit_up", "_limit_down", "_burst"]].copy()
    o.columns = ["ts_code", "stock_name", "up_old", "down_old", "burst_old"]
    n.columns = ["ts_code", "up_new", "down_new", "burst_new"]
    m = o.merge(n, on="ts_code", how="outer").fillna(False)
    # 次新无限制窗口内的新面板行（_limit_known=False）→ 次新剔除归因
    nl = new_panel[(new_panel["trade_date"] == as_of)
                   & (~new_panel["_limit_known"].fillna(False).astype(bool))]
    no_limit_codes = set(nl["ts_code"])
    flips = []
    for col, label in (("up", "涨停"), ("down", "跌停"), ("burst", "炸板")):
        diff = m[f"{col}_old"].astype(bool) != m[f"{col}_new"].astype(bool)
        for _, r in m[diff].iterrows():
            in_nl = r["ts_code"] in no_limit_codes
            old_v, new_v = bool(r[f"{col}_old"]), bool(r[f"{col}_new"])
            if new_v and not old_v:
                reason = "低价补判(动态阈值)"
            elif old_v and not new_v and in_nl:
                reason = "次新剔除(无限制窗口)"
            elif old_v and not new_v:
                reason = "动态阈值收紧(触板/涨幅精确化)"
            else:
                reason = "其他"
            flips.append({
                "code": r["ts_code"], "name": r["stock_name"], "date": as_of,
                "列": label, "old": old_v, "new": new_v,
                "no_limit": in_nl, "翻转原因": reason,
            })
    return {"as_of": as_of, "old": mo, "new": mn,
            "pos_old": _position(mo, rules), "pos_new": _position(mn, rules),
            "flips": flips}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dates", default=",".join(DEFAULT_DATES))
    args = ap.parse_args()
    dates = [d.strip() for d in args.dates.split(",") if d.strip()]

    cm = ConfigManager()
    cfg = cm.load_config()
    rules = ((cfg.get("review") or {}).get("sentiment_rules") or {})
    started = time.time()
    results, all_flips = [], []
    with DataReader(cm) as reader:
        for d in dates:
            r = compare_date(reader, d, rules)
            results.append(r)
            all_flips.extend(r["flips"])
            log.info("%s 完成：翻转 %d 处", d, len(r["flips"]))

    ts = time.strftime("%Y%m%d_%H%M%S")
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = CSV_DIR / f"limit_up_flips_{ts}.csv"
    pd.DataFrame(all_flips).to_csv(csv_path, index=False, encoding="utf-8-sig")

    lines = [
        "# 涨停判定 A/B 对比（P0-1/P0-2 回补闸门）",
        "",
        f"- 生成：{time.strftime('%Y-%m-%d %H:%M')}｜对比日期：{', '.join(dates)}",
        "- 旧实现 = 固定下浮 0.10 + 首日清零；新实现 = P0-1 动态阈值"
        "（0.005/估计昨收，raw_close 缺失回退）+ P0-2 注册制次新前 5 交易日"
        "（北交所首日）无限制窗口",
        f"- 翻转清单全量：`{csv_path.relative_to(REPO)}`（{len(all_flips)} 处）",
        "",
        "## 汇总对比（锚定日截面）",
        "",
        "| 日期 | 实现 | 涨停 | 跌停 | 炸板 | 炸板率 | 高度 | 梯队(≥2板) | "
        "溢价均值 | 昨涨停基数 | 判据定位 |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        for mode, m, pos in (("旧", r["old"], r["pos_old"]), ("新", r["new"], r["pos_new"])):
            ladder = "、".join(f"{k}板×{v}" for k, v in sorted(m["ladder"].items(),
                                                               reverse=True)) or "—"
            lines.append(
                f"| {r['as_of']} | {mode} | {m['limit_up']} | {m['limit_down']} | "
                f"{m['burst']} | {m['burst_rate'] if m['burst_rate'] is not None else '—'} | "
                f"{m['max_streak']} | {ladder} | "
                f"{m['premium_mean'] if m['premium_mean'] is not None else '—'}% | "
                f"{m['prev_limit_n']} | {pos} |")
        lines.append("")
    lines += ["## 翻转原因分布", ""]
    from collections import Counter
    cnt = Counter(f["翻转原因"] for f in all_flips)
    for k, v in cnt.most_common():
        lines.append(f"- {k}：{v} 处")
    lines += ["", "## 结论与审阅要点", "",
              "- 翻转方向应集中于「低价补判」（新多判低价真实涨停）与「次新剔除」"
              "（注册制新股上市前 5 日不再误判涨停/炸板）；",
              "- 若出现其他原因的翻转（旧判新否），须逐条解释后方可进入回补授权；",
              "- 本报告**仅为闸门交付物**：全量回补（sentiment_metrics_daily 重算 + "
              "链 CSV 重出 + 历史报告重生成）须用户审阅本报告并授权后执行（V2.0 §8.2）。",
              ""]
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / f"limit_up_ab_diff_{ts}.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("A/B 报告已产出: %s（耗时 %.0fs）", out, time.time() - started)
    print(f"✓ A/B 报告: {out}")
    print("⏸ 停：请审阅翻转清单与汇总对比后，再决定是否授权全量回补（步骤 6）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
