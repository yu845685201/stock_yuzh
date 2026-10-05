"""P0-1 存量回填：his_kline_day.is_st 死列一次性回填（幂等，可重跑）

回填口径与 DAO 派生一致（复盘报告层与 P0 修复技术方案 §1.2）：
    is_st = (stock_name LIKE '%ST%')
即「该行同步时点的名称含 ST」，覆盖 *ST/ST/S*ST/SST 形态。

用法（仓库根 stock_yuzh/ 下，NO_PROXY 必带，UAT 库）:
    NO_PROXY=127.0.0.1,localhost .venv/bin/python backend/scripts/backfill_is_st.py [--dry-run]

步骤：
    1. 统计回填前口径不一致行数（is_st IS DISTINCT FROM (stock_name LIKE '%ST%')）；
    2. 整表单条 UPDATE 回填（~200 只 ST × 各自历史行数，量级百万行内，事务内完成；
       若锁表顾虑可按 ts_code 首字母分批，本脚本默认整表，实测耗时后自行调整）；
    3. 验收断言：回填后 is_st=true 行数 == stock_name LIKE '%ST%' 行数，且不一致行数为 0；
    4. 抽验：随机抽 5 只 ST 股、5 只正常股，逐股核对 is_st 与名称。
"""
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config.config_manager import ConfigManager
from src.database.connection import DatabaseConnection

BACKFILL_SQL = """
UPDATE his_kline_day
SET is_st = (stock_name LIKE '%ST%')
WHERE is_st IS DISTINCT FROM (stock_name LIKE '%ST%')
"""


def main():
    dry_run = '--dry-run' in sys.argv
    db = DatabaseConnection(ConfigManager())

    def _stats():
        return db.fetch_one("""
            SELECT COUNT(*) FILTER (WHERE is_st IS DISTINCT FROM (stock_name LIKE '%ST%')) AS mismatch,
                   COUNT(*) FILTER (WHERE stock_name LIKE '%ST%') AS st_rows,
                   COUNT(*) FILTER (WHERE is_st) AS true_rows
            FROM his_kline_day""")

    before = _stats()
    print(f"[1] 回填前：口径不一致行 {before['mismatch']}，"
          f"名称含 ST 行 {before['st_rows']}，is_st=true 行 {before['true_rows']}")

    if before['mismatch'] > 0:
        if dry_run:
            print("    --dry-run：不执行回填")
            return
        db.execute_update(BACKFILL_SQL)
        print("[2] 回填 UPDATE 已执行")
    else:
        print("[2] 口径已一致，无需回填（幂等出口）")

    after = _stats()
    print(f"[3] 回填后：口径不一致行 {after['mismatch']}，"
          f"名称含 ST 行 {after['st_rows']}，is_st=true 行 {after['true_rows']}")
    assert after['mismatch'] == 0, f"仍存在口径不一致行 {after['mismatch']}（验收不过）"
    assert after['true_rows'] == after['st_rows'], \
        f"is_st=true 行数 {after['true_rows']} != 名称含 ST 行数 {after['st_rows']}"
    print("    ✓ 验收断言通过")

    # 抽验：5 只 ST + 5 只正常股，逐股核对全部行 is_st 与名称口径一致
    for label, expect, where in (
            ('ST 股', True, "stock_name LIKE '%ST%'"),
            ('正常股', False, "stock_name NOT LIKE '%ST%'")):
        codes = [r['ts_code'] for r in db.fetch_all(
            f"""SELECT ts_code FROM (
                    SELECT DISTINCT ts_code FROM his_kline_day
                    WHERE {where} AND (is_st IS NOT NULL) IS TRUE
                ) t ORDER BY random() LIMIT 5""")]
        if not codes:
            print(f"[4] {label}: 库内无样本，跳过抽验")
            continue
        rows = db.fetch_all(
            "SELECT ts_code, is_st FROM his_kline_day WHERE ts_code = ANY(%s)", (codes,))
        by_code: dict = {}
        for r in rows:
            by_code.setdefault(r['ts_code'], set()).add(bool(r['is_st']))
        bad = [c for c, flags in by_code.items() if flags != {expect}]
        ok = not bad and len(by_code) == len(codes)
        print(f"[4] {label}: 抽 {len(by_code)} 只逐股核对 -> {'✓ 一致' if ok else f'✗ 不一致 {bad}'}")
        assert ok, f"{label} 抽验不过: {bad}"


if __name__ == '__main__':
    main()
