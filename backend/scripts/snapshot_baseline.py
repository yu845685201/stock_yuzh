"""基线快照：记录跑数前数据库状态，供重建后 G3 对账

输出：
- doc/reports/baseline_snapshot_{ts}.md        人读报告
- backend/tmp/baseline_meta.json               全局数字（G3 自动对比用）
- backend/tmp/baseline_stock_stats.json        测试集 100 股逐股统计
- backend/tmp/baseline_stock_dump_{code}.json  3 只样本股全量行
"""
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config.config_manager import ConfigManager
from src.database.connection import DatabaseConnection

DUMP_CODES = ['sz.000001', 'sh.600519', 'sz.000858']


def j(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    print(f"  写入 {path}")


def main():
    config_manager = ConfigManager()
    db = DatabaseConnection(config_manager)

    set_file = ROOT / 'test' / 'test_stock_set.json'
    test_codes = [s['ts_code'] for s in json.loads(set_file.read_text(encoding='utf-8'))['stocks']]

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    print(f"== 基线快照 {ts}")

    # 1. 日K全局统计
    kline = db.fetch_one("""
        SELECT COUNT(*) AS rows_total,
               COUNT(DISTINCT ts_code) AS stocks,
               MIN(trade_date) AS min_date, MAX(trade_date) AS max_date,
               COUNT(*) FILTER (WHERE amount IS NULL) AS amount_null,
               COUNT(*) FILTER (WHERE amount = 0) AS amount_zero,
               COUNT(*) FILTER (WHERE amount > 0) AS amount_pos,
               COUNT(*) FILTER (WHERE raw_close IS NOT NULL) AS raw_close_filled
        FROM his_kline_day""")
    print(f"  日K: {kline}")

    # 2. 基本面统计
    fund = db.fetch_one("""
        SELECT COUNT(*) AS rows_total, COUNT(DISTINCT ts_code) AS stocks,
               MIN(disclosure_date) AS min_date, MAX(disclosure_date) AS max_date
        FROM base_fundamentals_info""")
    print(f"  基本面: {fund}")

    # 3. 测试集逐股统计
    placeholders = ','.join(['%s'] * len(test_codes))
    per_stock = db.execute_query(f"""
        SELECT ts_code, COUNT(*) AS rows, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date
        FROM his_kline_day WHERE ts_code IN ({placeholders})
        GROUP BY ts_code ORDER BY ts_code""", tuple(test_codes))

    # 4. 样本股全量行
    dumps = {}
    for code in DUMP_CODES:
        rows = db.execute_query("""
            SELECT trade_date, open, high, low, close, preclose, volume, amount,
                   change_rate, turnover_rate, raw_close, adjust_flag
            FROM his_kline_day WHERE ts_code = %s ORDER BY trade_date""", (code,))
        dumps[code] = rows
        print(f"  样本 {code}: {len(rows)} 行")

    meta = {
        'snapshot_time': ts,
        'kline': {k: str(v) for k, v in kline.items()},
        'fundamentals': {k: str(v) for k, v in fund.items()},
        'test_stock_count': len(test_codes),
        'per_stock_covered': len(per_stock),
        'dump_codes': {c: len(rows) for c, rows in dumps.items()},
    }
    j(ROOT / 'tmp' / 'baseline_meta.json', meta)
    j(ROOT / 'tmp' / 'baseline_stock_stats.json', per_stock)
    for code, rows in dumps.items():
        j(ROOT / 'tmp' / f'baseline_stock_dump_{code}.json', rows)

    report = ROOT.parent / 'doc' / 'reports' / f'baseline_snapshot_{ts}.md'
    report.parent.mkdir(parents=True, exist_ok=True)
    stats_lines = '\n'.join(
        f"| {r['ts_code']} | {r['rows']} | {r['min_date']} | {r['max_date']} |"
        for r in per_stock[:20])
    report.write_text(f"""# 基线快照 {ts}

## 日K（his_kline_day）
- 总行数：{kline['rows_total']}，股票数：{kline['stocks']}
- 日期范围：{kline['min_date']} ~ {kline['max_date']}
- amount：NULL={kline['amount_null']}，=0：{kline['amount_zero']}，>0：{kline['amount_pos']}
- raw_close 非空：{kline['raw_close_filled']}

## 基本面（base_fundamentals_info）
- 总行数：{fund['rows_total']}，股票数：{fund['stocks']}
- 披露日范围：{fund['min_date']} ~ {fund['max_date']}

## 测试集覆盖：{len(per_stock)}/{len(test_codes)}（前 20 只）

| ts_code | 行数 | 最早 | 最晚 |
|---------|------|------|------|
{stats_lines}

样本股全量行已存 tmp/baseline_stock_dump_*.json（{', '.join(DUMP_CODES)}）
""", encoding='utf-8')
    print(f"  报告 {report}")


if __name__ == '__main__':
    main()
