"""生成 100 股测试股票集（大盘蓝筹/ST/次新/随机混合），写入 test/test_stock_set.json"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.config_manager import ConfigManager
from src.database.connection import DatabaseConnection

BLUECHIP = ['000001', '600519', '000858', '601398', '600036',
            '601328', '601939', '601288', '601988', '000002']

SQL = f"""
WITH candidates AS (
    SELECT ts_code, stock_code, stock_name, list_date,
        CASE
            WHEN stock_code IN ({','.join(f"'{c}'" for c in BLUECHIP)}) THEN 'bluechip'
            WHEN stock_name LIKE '%%ST%%' THEN 'st'
            WHEN list_date >= '20250101' THEN 'new'
            ELSE 'random'
        END AS category,
        row_number() OVER (PARTITION BY
            CASE
                WHEN stock_code IN ({','.join(f"'{c}'" for c in BLUECHIP)}) THEN 'bluechip'
                WHEN stock_name LIKE '%%ST%%' THEN 'st'
                WHEN list_date >= '20250101' THEN 'new'
                ELSE 'random'
            END
            ORDER BY random()) AS rn
    FROM base_stock_info
    WHERE type='1' AND list_status='L'
)
SELECT ts_code, stock_code, stock_name, COALESCE(list_date,'') AS list_date, category
FROM candidates
WHERE (category='bluechip' AND rn<=10)
   OR (category='st' AND rn<=10)
   OR (category='new' AND rn<=10)
   OR (category='random' AND rn<=70)
ORDER BY category, ts_code
"""


def main():
    config_manager = ConfigManager()
    db = DatabaseConnection(config_manager)
    rows = db.execute_query(SQL)
    stocks = [
        {
            'ts_code': r['ts_code'],
            'stock_code': r['stock_code'],
            'stock_name': r['stock_name'],
            'list_date': r['list_date'],
            'category': r['category'],
        }
        for r in rows
    ]
    out = Path(__file__).resolve().parent.parent / 'test' / 'test_stock_set.json'
    out.write_text(json.dumps({'generated_at': str(__import__('datetime').datetime.now()),
                               'count': len(stocks), 'stocks': stocks}, ensure_ascii=False, indent=2),
                   encoding='utf-8')
    print(f"测试股票集已写入 {out}，共 {len(stocks)} 只")
    from collections import Counter
    print("分类分布:", dict(Counter(s['category'] for s in stocks)))


if __name__ == '__main__':
    main()
