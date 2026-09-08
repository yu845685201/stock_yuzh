"""G1 验证：基本面全量重刷后的自动检查（方案 §5.5 清单）

用法: python3 scripts/verify_g1.py
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
    checks = []

    # 1. 总量与覆盖
    total = db.fetch_one("""
        SELECT COUNT(*) AS rows_total, COUNT(DISTINCT ts_code) AS stocks,
               COUNT(*) FILTER (WHERE disclosure_date IS NOT NULL) AS pub_filled,
               COUNT(*) FILTER (WHERE disclosure_date IS NULL) AS pub_null,
               COUNT(*) FILTER (WHERE stat_date IS NULL) AS stat_null
        FROM base_fundamentals_info""")
    print(f"[1] 总量: {total}")
    checks.append(('stat_date 全部非空', total['stat_null'] == 0))
    checks.append(('覆盖股票数 ≥ 5000', total['stocks'] >= 5000))

    # 2. 披露日不可早于报告期（数据错误直接暴露）
    bad = db.fetch_one("""
        SELECT COUNT(*) AS n FROM base_fundamentals_info
        WHERE disclosure_date IS NOT NULL AND disclosure_date < stat_date""")
    print(f"[2] 披露日早于报告期的行: {bad['n']}（应为 0）")
    checks.append(('披露日 ≥ 报告期', bad['n'] == 0))

    # 3. 前视消除全表检查（依赖 his_kline_day 的 M2 重建，此处先查存量）
    lookahead = db.fetch_one("""
        SELECT COUNT(*) AS n FROM his_kline_day
        WHERE fundamentals_disclosure_date IS NOT NULL
          AND fundamentals_disclosure_date > trade_date""")
    print(f"[3] his_kline_day 前视行数（M2 重建后应为 0）: {lookahead['n']}")
    checks.append(('his_kline_day 前视=0（M2 后生效）', lookahead['n'] == 0))

    # 4. 抽样比对：平安银行
    p = db.fetch_one("""
        SELECT stat_date, disclosure_date FROM base_fundamentals_info
        WHERE ts_code='sz.000001' AND stat_date='20250930'""")
    ok = p and p['disclosure_date'] == '20251025'
    print(f"[4] 平安银行 20250930 → {p['disclosure_date'] if p else '缺失'}（期望 20251025）")
    checks.append(('平安银行 pubDate', ok))

    # 5. 同日双报告共存
    dual = db.execute_query("""
        SELECT ts_code, disclosure_date, COUNT(*) AS n
        FROM base_fundamentals_info WHERE disclosure_date IS NOT NULL
        GROUP BY ts_code, disclosure_date HAVING COUNT(*) > 1
        ORDER BY n DESC LIMIT 5""")
    print(f"[5] 同日多报告样例（共存即通过）: "
          f"{[(r['ts_code'], r['disclosure_date'], r['n']) for r in dual] or '无'}")

    # 6. turnover 抽样重算（100 股灰度行）
    sample = db.execute_query("""
        SELECT k.ts_code, k.volume, k.turnover_rate, f.float_share
        FROM his_kline_day k
        JOIN base_fundamentals_info f ON f.ts_code = k.ts_code
        WHERE k.ts_code IN (
            SELECT ts_code FROM base_stock_info WHERE type='1' AND list_status='L' LIMIT 5)
          AND k.fundamentals_disclosure_date = f.disclosure_date
          AND k.volume > 0 AND f.float_share > 0
        ORDER BY random() LIMIT 20""")
    bad_turnover = 0
    for r in sample:
        expect = float(r['volume']) / float(r['float_share']) * 100
        if r['turnover_rate'] is None or abs(float(r['turnover_rate']) - expect) / expect > 0.01:
            bad_turnover += 1
    print(f"[6] turnover 抽样重算不一致: {bad_turnover}/{len(sample)}")
    checks.append(('turnover 抽样一致', bad_turnover == 0 and len(sample) > 0))

    print("\n=== G1 验证结论 ===")
    all_ok = True
    for name, ok in checks:
        print(f"{'✓' if ok else '✗'} {name}")
        all_ok = all_ok and ok
    print("G1:", "全部通过" if all_ok else "存在未通过项")


if __name__ == '__main__':
    main()
