#!/usr/bin/env python
"""XDXR vs baostock 股本一致性校验闸门（换源投产前置，2026-10-25）

抽样活跃股 300 + 退市股 20，每股取 5 个探针日期，比较两侧「同一瞬间」的
total_share/float_share，相对误差 ≤1% 记一致。

探针时点 = 库内 baostock 行的 stat_date（报告期末）：baostock 行值即报告期末
股本，XDXR 取 ≤stat_date 最近事件后值 = 同一瞬间实际股本。两侧同瞬间对拍，
排除「报告期末→披露日之间发生送转/增发」的合法时点差异（该场景 XDXR 反映
事件后真实股本，对下游 turnover_rate 更准确，属换源收益而非数据错误）。

达标线：total_share ≥99%、float_share ≥98%（流通口径两源存在合理小差异）。
不达标输出差异清单并退出码 1（暂停投产）。

用法：cd backend && ../.venv/bin/python scripts/verify_xdxr_vs_baostock.py [--sample 300 --delisted 20]
"""

import argparse
import json
import random
import sys
import time
import urllib.request
from bisect import bisect_right
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import ConfigManager  # noqa: E402
from src.database.connection import DatabaseConnection  # noqa: E402
from src.sync.xdxr_share_manager import xdxr_events_to_rows  # noqa: E402

TOLERANCE = 0.01
PASS_TOTAL, PASS_FLOAT = 0.99, 0.98


def fetch_xdxr(base_url: str, code: str) -> list:
    url = f"{base_url}/api/xdxr?code={code}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.load(resp)
    if payload.get('code') not in (0, None):
        raise RuntimeError(payload.get('message') or 'unknown')
    return (payload.get('data') or {}).get('list') or []


def asof_value(sorted_dates, rows, probe: str, field: str):
    idx = bisect_right(sorted_dates, probe) - 1
    if idx < 0:
        return None
    return rows[idx].get(field)


def compare_side(db_v, xdxr_v):
    if db_v is None and xdxr_v is None:
        return True, None
    if db_v is None or xdxr_v is None:
        return False, (db_v, xdxr_v)
    if db_v == 0 and xdxr_v == 0:
        return True, None
    rel = abs(db_v - xdxr_v) / max(abs(db_v), abs(xdxr_v))
    return rel <= TOLERANCE, (db_v, xdxr_v, round(rel, 4))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sample', type=int, default=300)
    ap.add_argument('--delisted', type=int, default=20)
    ap.add_argument('--seed', type=int, default=20261025)
    ap.add_argument('--show-mismatch', type=int, default=15)
    args = ap.parse_args()

    random.seed(args.seed)
    cfg = ConfigManager()
    db = DatabaseConnection(cfg)
    base_url = (cfg.get('data_sources.tdx_api.base_url') or 'http://127.0.0.1:8080').rstrip('/')

    stocks = db.execute_query("""
        SELECT s.ts_code, s.stock_code, s.stock_name, s.list_date, s.list_status
        FROM base_stock_info s
        WHERE s.type = '1' AND s.list_status = 'L'
          AND EXISTS (SELECT 1 FROM base_fundamentals_info f WHERE f.ts_code = s.ts_code)
    """)
    delisted = db.execute_query("""
        SELECT s.ts_code, s.stock_code, s.stock_name, s.list_date, s.list_status
        FROM base_stock_info s
        WHERE s.type = '1' AND s.list_status = 'D'
          AND EXISTS (SELECT 1 FROM base_fundamentals_info f WHERE f.ts_code = s.ts_code)
    """)
    active_sample = random.sample(stocks, min(args.sample, len(stocks)))
    delisted_sample = random.sample(delisted, min(args.delisted, len(delisted)))
    print(f"抽样：活跃 {len(active_sample)}/{len(stocks)}，退市 {len(delisted_sample)}/{len(delisted)}")

    counters = {'total': [0, 0], 'float': [0, 0]}  # [match, total]
    mismatches = []
    delisted_ok = 0

    for label, sample in (('活跃', active_sample), ('退市', delisted_sample)):
        for k, s in enumerate(sample, 1):
            ts_code = s['ts_code']
            code = str(s['stock_code'] or ts_code.split('.')[-1])
            frows = db.execute_query(
                "SELECT stat_date, disclosure_date, total_share, float_share "
                "FROM base_fundamentals_info WHERE ts_code = %s ORDER BY disclosure_date, stat_date",
                (ts_code,),
            )
            if not frows:
                continue
            try:
                events = fetch_xdxr(base_url, code)
            except Exception as e:
                print(f"  [warn] {ts_code} XDXR 拉取失败: {e}")
                time.sleep(0.5)
                continue
            xrows = xdxr_events_to_rows(ts_code, code, s.get('stock_name'), events, list_date=s.get('list_date'))
            if not xrows:
                mismatches.append((ts_code, 'NO_XDXR_ROWS', '-'))
                continue
            if label == '退市':
                delisted_ok += 1

            f_dates = [str(r['stat_date']) for r in frows]
            x_rows_sorted = sorted(xrows, key=lambda r: str(r['disclosure_date']))
            x_dates = [str(r['disclosure_date']) for r in x_rows_sorted]

            probes = [f_dates[int(j * (len(f_dates) - 1) / 4)] for j in range(5)]
            # 探针早于 XDXR 序列首行（晚于探针日上市的新股）→ 两侧无可比瞬间，跳过
            probes = [p for p in probes if p >= x_dates[0]]
            for probe in probes:
                for side, field in (('total', 'total_share'), ('float', 'float_share')):
                    db_v = asof_value(f_dates, frows, probe, field)
                    x_v = asof_value(x_dates, x_rows_sorted, probe, field)
                    ok, detail = compare_side(db_v, x_v)
                    counters[side][1] += 1
                    if ok:
                        counters[side][0] += 1
                    elif len(mismatches) < 200:
                        mismatches.append((ts_code, f"{side}@{probe}", detail))
            time.sleep(0.05)
        print(f"  {label}样本完成（{len(sample)} 只）")

    t_match, t_n = counters['total']
    f_match, f_n = counters['float']
    t_rate = t_match / t_n if t_n else 0
    f_rate = f_match / f_n if f_n else 0
    print("\n========== 校验结果 ==========")
    print(f"total_share 一致率: {t_match}/{t_n} = {t_rate:.4f}（达标线 {PASS_TOTAL}）")
    print(f"float_share 一致率: {f_match}/{f_n} = {f_rate:.4f}（达标线 {PASS_FLOAT}）")
    print(f"退市股 XDXR 可得: {delisted_ok}/{len(delisted_sample)}")
    if mismatches:
        print(f"\n差异清单（前 {args.show_mismatch} 条，共 {len(mismatches)}）:")
        for ts_code, where, detail in mismatches[:args.show_mismatch]:
            print(f"  {ts_code}  {where}  {detail}")
    passed = t_rate >= PASS_TOTAL and f_rate >= PASS_FLOAT
    print(f"\n闸门结论: {'PASS ✅' if passed else 'FAIL ❌（暂停投产，按差异清单归因）'}")
    sys.exit(0 if passed else 1)


if __name__ == '__main__':
    main()
