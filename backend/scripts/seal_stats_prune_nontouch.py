"""seal_stats 非触板行清洗（附录 C.6，2026-10-25，一次性）。

背景：全市场分笔模式（20260917 起）此前把**所有股票**的分笔 max 落
limit_seal_stats_daily.limit_price——未触板股实为日内最高价（收在最高时即
收盘价），伪装成触板价污染表语义（预存问题 #2：封板对拍仅第一向有效的根因）。

动作：按触板口径（与 money_flow_manager._seal_row_for 同款：阈值法
pct−0.10 浮点余量 + high/low 触及，前复权比值；ST/板块经 limit_pct）逐日
计算触板集合，DELETE 表中非触板行。幂等可重跑；不改动触板行。

用法：python scripts/seal_stats_prune_nontouch.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.analysis.data_reader import DataReader  # noqa: E402
from src.analysis.limit_up_rules import board_of, limit_pct  # noqa: E402
from src.config.config_manager import ConfigManager  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("seal_prune")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    reader = DataReader(ConfigManager())
    # DELETE 须自备连接：DataReader 为只读包装（不 commit，写操作随池回滚——
    # 第二版实跑踩坑：RETURNING 取到了行数但未提交，重查全在）
    import psycopg2
    from src.chan.data import DB
    conn = psycopg2.connect(**DB)
    conn.autocommit = True
    deleted = kept = 0
    try:
        days = [str(r[0]).replace("-", "") for r in reader._query(
            "SELECT DISTINCT trade_date FROM limit_seal_stats_daily ORDER BY trade_date")]
        log.info("seal 表 %d 个交易日（%s~%s）", len(days), days[0], days[-1])
        for d in days:
            rows = reader._query(
                "SELECT ts_code, stock_code, high, low, preclose, is_st "
                "FROM his_kline_day WHERE trade_date = %s", (d,))
            if not rows:
                continue
            touched = set()
            for ts, code6, high, low, preclose, is_st in rows:
                pct = limit_pct(board_of(str(code6)), d, bool(is_st))
                if pct is None or not preclose or float(preclose) <= 0:
                    continue
                high, low, preclose = float(high), float(low), float(preclose)
                thr = pct - 0.10
                if (high - preclose) / preclose * 100.0 >= thr \
                        or (low - preclose) / preclose * 100.0 <= -thr:
                    touched.add(str(ts))
            seal = [str(r[0]) for r in reader._query(
                "SELECT ts_code FROM limit_seal_stats_daily WHERE trade_date = %s", (d,))]
            drop = [ts for ts in seal if ts not in touched]
            keep = len(seal) - len(drop)
            deleted += len(drop)
            kept += keep
            if drop and not args.dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM limit_seal_stats_daily WHERE trade_date = %s "
                        "AND ts_code = ANY(%s)", (d, drop))
                log.info("%s：已删 %d 行", d, len(drop))
            log.info("%s：seal %d 行，触板保留 %d，删 %d", d, len(seal), keep, len(drop))
    finally:
        reader.close()
        conn.close()
    log.info("合计：%s 删 %d 行 / 保留 %d 行（触板股）",
             "[dry-run] " if args.dry_run else "", deleted, kept)
    return 0


if __name__ == "__main__":
    sys.exit(main())
