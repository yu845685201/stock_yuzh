"""P4-8 离线同构回测：R3 全历史 / R1 近一年（数据可行性勘探定稿，2026-10-25）。

勘探结论（分层可行）：R3 纯 K 线驱动 → 全历史全保真；R1 依赖
limit_seal_stats_daily（20251009 起 253 日）→ 近一年，更早日期封板缺失按
谓词语义"视为通过"（与报告层同行为）；R2 概念成分 in/out 记录仅 20260930
起（用当前快照近似历史成分有前视偏差，违反 C3 红线）、R4 lhb_detail 仅
22 个交易日——两者**离线回测不可行**，维持核验体系前向积累（先验注记如实
降级，见 tomorrow_plan._r_pool_prior）。

方法（口径零漂移）：复用 candidate_rules.is_r1/is_r3 谓词（两入口同实现）
与 review.cross_section 同一实现链（annotate_panel/suppress_no_limit_window/
cross_section_at），按**核验体系同口径**测 T+1 中位超额 = 池中位涨跌幅 −
全市场中位涨跌幅（vs 全市场中位为逐日核验的同一基准；池为原始池、不套
情绪开放度/ADX 闸门——与落库预测载荷同语义）。产出
``data/priors/r_pool_offline_prior.json``（tomorrow_plan._r_pool_prior 消费）
+ ``doc/reports/r_pool_offline_backtest_<ts>.md``。

用法：
    python scripts/r_pool_offline_backtest.py [--r1-start 20251009]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.analysis.candidate_rules import BUY_RULES, is_r1, is_r3  # noqa: E402
from src.analysis.data_reader import DataReader  # noqa: E402
from src.config.config_manager import ConfigManager  # noqa: E402
from src.review.cross_section import (annotate_panel, cross_section_at,  # noqa: E402
                                      suppress_no_limit_window)
from src.sync.sentiment_metrics_manager import PANEL_COLS  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("r_pool_backtest")

START, END = "20150105", "20260930"
REPORT_DIR = BACKEND.parent / "doc" / "reports"
PRIOR_PATH = BACKEND.parent / "data" / "priors" / "r_pool_offline_prior.json"


def load_rules() -> dict:
    cfg = yaml.safe_load((BACKEND.parent / "config" / "config.yaml").read_text("utf-8"))
    rules = dict(BUY_RULES)
    rules.update((((cfg.get("review") or {}).get("tomorrow_plan") or {})
                  .get("buy_rules")) or {})
    return rules


def _open_gain_close_lt_open(r) -> tuple:
    try:
        o, pc, c = float(r["open"]), float(r["preclose"]), float(r["close"])
        if pc > 0:
            return (o - pc) / pc * 100.0, c < o
    except (TypeError, ValueError):
        pass
    return None, False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--r1-start", default="20251009", help="R1 起算日（封板数据起点）")
    args = ap.parse_args()
    t0 = time.time()
    rules = load_rules()
    log.info("谓词阈值：r1_min_streak=%s r1_high_streak=%s r3_volume_min=%s r3_change_min=%s",
             rules.get("r1_min_streak"), rules.get("r1_high_streak"),
             rules.get("r3_volume_min"), rules.get("r3_change_min"))

    reader = DataReader(ConfigManager())
    try:
        # R1 封板数据一次载入（20251009 起）
        seal_rows = reader._query(
            "SELECT trade_date, ts_code, open_count FROM limit_seal_stats_daily "
            "WHERE trade_date >= %s", (args.r1_start,))
        seal = {}
        for d, ts, oc in seal_rows:
            seal.setdefault(str(d), {})[str(ts)] = oc
        log.info("封板数据 %d 日（%s 起）", len(seal), args.r1_start)

        acc = {"R1": [], "R3": []}   # 每日 {'date','excess','pool_n'}
        y = int(START[:4])
        while y <= int(END[:4]):
            rows = reader._query(
                "SELECT calendar_date FROM base_trade_calendar "
                "WHERE is_trading_day = 1 AND calendar_date >= %s AND calendar_date <= %s "
                "ORDER BY calendar_date",
                ((date(y, 1, 1) - timedelta(days=400)).isoformat(),
                 date(y, 12, 31).isoformat()))
            cal_all = [str(r[0]).replace("-", "") for r in rows]
            dates = [d for d in cal_all if START <= d <= END]
            if not dates:
                y += 1
                continue
            anchor_idx = next((i for i, d in enumerate(cal_all) if d >= dates[0]), 0)
            panel_start = cal_all[max(0, anchor_idx - 62)]
            panel = reader.load_kline(PANEL_COLS, dates[-1], start_date=panel_start)
            if panel.empty:
                log.warning("块 %d 面板为空", y)
                y += 1
                continue
            panel = annotate_panel(panel)
            list_dates = {s.ts_code: s.list_date for s in reader.load_stock_universe()}
            panel = suppress_no_limit_window(panel, list_dates)

            # 块内相邻日对（D 池 → D+1 计量；跨块边界跳过，损失 ~11/2800 样本）
            for i in range(len(dates) - 1):
                d, d1 = dates[i], dates[i + 1]
                if d1 != _next_in(cal_all, d):
                    continue
                cs_d = cross_section_at(panel, d)
                cs_d1 = cross_section_at(panel, d1)
                if cs_d.empty or cs_d1.empty:
                    continue
                chg1 = pd.to_numeric(cs_d1["change_rate"], errors="coerce")
                market_med = float(chg1.median())
                chg1_map = dict(zip(cs_d1["ts_code"], chg1))

                # R3 趋势新高（全历史）
                nh = cs_d[cs_d["_new_high"].fillna(False)]
                pool3 = []
                for _, r in nh.iterrows():
                    og, clo = _open_gain_close_lt_open(r)
                    vs = r["_volume_surge"]
                    cr = r["change_rate"]
                    if is_r3(True, bool(r["is_st"]),
                             None if pd.isna(vs) else float(vs),
                             None if pd.isna(cr) else float(cr),
                             rules, big_net=None, open_gain=og, close_lt_open=clo):
                        pool3.append(r["ts_code"])
                _record(acc, "R3", d, pool3, chg1_map, market_med)

                # R1 梯队接力（封板数据起；缺失日 seal.get(d, {}) → open_count None 视为通过）
                if d >= args.r1_start:
                    lu = cs_d[cs_d["_limit_up"].fillna(False)]
                    seal_d = seal.get(d, {})
                    pool1 = []
                    for _, r in lu.iterrows():
                        streak = int(r["_streak"]) if pd.notna(r["_streak"]) else 0
                        if is_r1(streak, bool(r["is_st"]), seal_d.get(r["ts_code"]), rules,
                                 market_weak=False):
                            pool1.append(r["ts_code"])
                    _record(acc, "R1", d, pool1, chg1_map, market_med)
            log.info("块 %d 完成（累计 R1 %d 日 / R3 %d 日）", y,
                     len(acc["R1"]), len(acc["R3"]))
            y += 1
    finally:
        reader.close()

    out = {}
    lines = ["# R 系列候选离线同构回测（P4-8，2026-10-25）", "",
             "- 口径：T+1 池中位涨跌幅 − 全市场中位（与核验体系逐日核验同口径）；"
             "池为原始池（不套情绪开放度/ADX 闸门，与落库预测载荷同语义）；"
             "谓词 = candidate_rules.is_r1/is_r3 两入口同实现", ""]
    for key in ("R3", "R1"):
        days = acc[key]
        if not days:
            continue
        excesses = [x["excess"] for x in days]
        wins = sum(1 for e in excesses if e > 0)
        entry = {
            "window": f"{days[0]['date']}~{days[-1]['date']}",
            "days": len(days),
            "signals": int(sum(x["pool_n"] for x in days)),
            "avg_pool": round(float(np.mean([x["pool_n"] for x in days])), 1),
            "avg_excess_median": round(float(np.mean(excesses)), 2),
            "win_rate": round(wins / len(excesses), 2),
        }
        out[key] = entry
        lines += [f"## {key}（{entry['window']}，{entry['days']} 个核验日）", "",
                  f"- 信号总数 {entry['signals']}（日均池 {entry['avg_pool']} 只）",
                  f"- 平均中位超额 **{entry['avg_excess_median']:+.2f}pct**，"
                  f"胜率（超额>0 的日子）**{entry['win_rate']:.0%}**", ""]
    out["meta"] = {
        "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "metric": "T+1 池中位涨跌幅 − 全市场中位（与核验体系同口径）",
        "r2_r4_note": "R2/R4 离线回测不可行：概念成分时点记录 20260930 起（快照近似有前视偏差）、"
                      "龙虎榜明细仅 22 个交易日——维持核验体系前向积累",
        "r1_note": f"R1 自 {args.r1_start}（封板数据起点），更早日期不测；open_count 缺失按谓词语义视为通过",
    }
    lines += ["## R2/R4（不可行说明）", "", f"- {out['meta']['r2_r4_note']}", ""]

    PRIOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    PRIOR_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    md = REPORT_DIR / f"r_pool_offline_backtest_{time.strftime('%Y%m%d_%H%M%S')}.md"
    md.write_text("\n".join(lines), encoding="utf-8")
    log.info("先验 JSON: %s；报告: %s", PRIOR_PATH, md)
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


def _next_in(cal_all: list, d: str):
    i = cal_all.index(d) if d in cal_all else None
    return cal_all[i + 1] if i is not None and i + 1 < len(cal_all) else None


def _record(acc: dict, key: str, d: str, pool: list, chg1_map: dict, market_med: float):
    if not pool:
        return
    vals = [chg1_map[ts] for ts in pool if ts in chg1_map
            and chg1_map[ts] is not None and not pd.isna(chg1_map[ts])]
    if not vals:
        return
    acc[key].append({"date": d, "pool_n": len(vals),
                     "excess": float(np.median(vals)) - market_med})


if __name__ == "__main__":
    sys.exit(main())
