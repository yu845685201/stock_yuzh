"""G3 验证：日K全量重建后的自动对账（对基线快照）

用法: python3 scripts/verify_g3.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config.config_manager import ConfigManager
from src.database.connection import DatabaseConnection


def main():
    config_manager = ConfigManager()
    db = DatabaseConnection(config_manager)
    meta = json.loads((ROOT / 'tmp' / 'baseline_meta.json').read_text(encoding='utf-8'))
    checks = []

    # 1. 全局统计
    kline = db.fetch_one("""
        SELECT COUNT(*) AS rows_total, COUNT(DISTINCT ts_code) AS stocks,
               MIN(trade_date) AS min_date, MAX(trade_date) AS max_date,
               COUNT(*) FILTER (WHERE amount IS NULL) AS amount_null,
               COUNT(*) FILTER (WHERE amount = 0) AS amount_zero,
               COUNT(*) FILTER (WHERE amount > 0) AS amount_pos,
               COUNT(*) FILTER (WHERE raw_close IS NOT NULL) AS raw_close_filled,
               COUNT(*) FILTER (WHERE adjust_flag = 2) AS adjust_qfq,
               COUNT(*) FILTER (WHERE fundamentals_disclosure_date IS NOT NULL
                                  AND fundamentals_disclosure_date > trade_date) AS lookahead
        FROM his_kline_day""")
    print(f"[重建后] {kline}")
    base_rows = int(meta['kline']['rows_total'])
    print(f"[基线]   行数={base_rows}, 股票={meta['kline']['stocks']}")

    # 2. 检查项
    rows_ratio = kline['rows_total'] / base_rows
    checks.append(('行数在基线 0.9~1.3 倍内（含退市股新增覆盖）', 0.9 <= rows_ratio <= 1.3))
    checks.append(('amount 零值≈0', kline['amount_zero'] < kline['amount_pos'] * 0.001))
    checks.append(('raw_close 覆盖率 > 99%', kline['raw_close_filled'] > kline['rows_total'] * 0.99))
    checks.append(('adjust_flag 全部=2', kline['adjust_qfq'] == kline['rows_total']))
    checks.append(('前视消除（披露日≤交易日）', kline['lookahead'] == 0))

    # 3. 与基线逐股行数对比（抽样 30 只）
    stats_file = ROOT / 'tmp' / 'baseline_stock_stats.json'
    base_stats = {r['ts_code']: r for r in json.loads(stats_file.read_text(encoding='utf-8'))}
    sample_codes = list(base_stats.keys())[:30]
    placeholders = ','.join(['%s'] * len(sample_codes))
    new_stats = db.execute_query(f"""
        SELECT ts_code, COUNT(*) AS rows, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date
        FROM his_kline_day WHERE ts_code IN ({placeholders})
        GROUP BY ts_code ORDER BY ts_code""", tuple(sample_codes))
    worse = [r['ts_code'] for r in new_stats
             if r['rows'] < int(base_stats[r['ts_code']]['rows']) * 0.98]
    checks.append((f'抽样 30 只行数不低于基线 98%（实际 {len(new_stats)} 只）', not worse))
    if worse:
        print(f"    行数变少的股票: {worse}")

    # 4. 除权衔接抽验（000001 / 600519 除权日前后 preclose 连续）
    for code, ex_div in [('sz.000001', '20260612'), ('sh.600519', '20260626')]:
        rows = db.execute_query("""
            SELECT trade_date, close, preclose FROM his_kline_day
            WHERE ts_code = %s AND trade_date IN (
                (SELECT MAX(trade_date) FROM his_kline_day WHERE ts_code=%s AND trade_date < %s), %s)
            ORDER BY trade_date""", (code, code, ex_div, ex_div))
        ok = len(rows) == 2 and rows[0]['close'] is not None and rows[1]['preclose'] is not None \
             and abs(float(rows[0]['close']) - float(rows[1]['preclose'])) <= 0.011
        print(f"    {code} 除权衔接: "
              f"{[(r['trade_date'], str(r['close']), str(r['preclose'])) for r in rows]}")
        checks.append((f'{code} 除权日 preclose 衔接', ok))

    print("\n=== G3 验证结论 ===")
    all_ok = True
    for name, ok in checks:
        print(f"{'✓' if ok else '✗'} {name}")
        all_ok = all_ok and ok
    print("G3:", "全部通过" if all_ok else "存在未通过项")
    return 0 if all_ok else 1


if __name__ == '__main__':
    sys.exit(main())
