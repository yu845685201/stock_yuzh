"""P0 回补：rt_hkHSI 快照 close 误存开盘价的既有行（技术方案 V1.0 §2.2/§7.3）

背景：sina_source.py rt_hk* 分支把 fields[2]（今开）当 close，2026-09-27 上线起
overseas_market_daily 的恒生"收盘"一直是开盘价（涨跌幅 [8] 正确，仅 close 失真）。
解析分支已修（close 取 fields[6]），本脚本回补既有行。

口径：库内行 close 实为某市场日的今开——用腾讯 ifzq hkHSI 日线按 open 精确匹配
定位该市场日，取该日真实收盘回写（数据真实性：逐行源数据定位，不硬编码数值；
唯一匹配才写，多匹配/无匹配保持原值并列入失败清单）。

用法（backend/ 下，幂等可重跑——已修的行 open 匹配不到会如实报"无匹配"）:
    NO_PROXY=127.0.0.1,localhost ../.venv/bin/python scripts/fix_rthk_close_backfill.py [--dry-run]
"""
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config.config_manager import ConfigManager
from src.database.connection import DatabaseConnection
from src.sync.review_common import write_review_report

IFZQ_URL = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=hkHSI,day,,,60,qfq'
TOL = 0.005  # open 精确匹配容差（最小报价单位 0.01 的一半）


def _to_float(v):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def fetch_hsi_days() -> dict:
    """ifzq hkHSI 日线：{iso_date: {open, close}}（day 行布局 [date, open, close, …]）"""
    import requests
    resp = requests.get(IFZQ_URL, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    rows = ((payload.get('data') or {}).get('hkHSI') or {}).get('day') or []
    days = {}
    for r in rows:
        open_, close = _to_float(r[1]), _to_float(r[2])
        if open_ is not None and close is not None:
            days[r[0].replace('-', '')] = {'open': open_, 'close': close}
    return days


def main():
    dry_run = '--dry-run' in sys.argv
    started = datetime.now()
    lines = []

    days = fetch_hsi_days()
    lines.append(f'- ifzq hkHSI 日线取到 {len(days)} 个市场日（{min(days)}~{max(days)}）')

    db = DatabaseConnection(ConfigManager())
    rows = db.execute_query(
        "SELECT id, trade_date, close FROM overseas_market_daily "
        "WHERE market_code = 'rt_hkHSI' AND close IS NOT NULL ORDER BY trade_date")
    lines.append(f'- 库内 rt_hkHSI 行 {len(rows)} 条，逐行按 close==市场日 open 定位：')

    fixed, skipped, failed = 0, 0, []
    for r in rows:
        stored = float(r['close'])
        matches = [(d, v) for d, v in days.items() if abs(v['open'] - stored) <= TOL]
        if len(matches) != 1:
            failed.append(f"{r['trade_date']}(close={stored}, 匹配 {len(matches)} 个市场日)")
            continue
        mkt_day, v = matches[0]
        if dry_run:
            lines.append(f"  - {r['trade_date']}: {stored} -> {v['close']}（市场日 {mkt_day}）[dry-run]")
            fixed += 1
            continue
        db.execute_update(
            "UPDATE overseas_market_daily SET close = %s, update_time = NOW() "
            "WHERE id = %s", (v['close'], r['id']))
        lines.append(f"  - {r['trade_date']}: {stored} -> {v['close']}（市场日 {mkt_day}）")
        fixed += 1
        time.sleep(0.2)
    skipped = len(rows) - fixed - len(failed)
    if failed:
        lines.append(f'- 无匹配/多匹配（保持原值待人工核对）: {failed}')

    status = '成功' if not failed else '部分成功（含无匹配行）'
    lines.append(f'- 状态：{status}（修复 {fixed} 行，已正确 {skipped} 行，'
                 f'失败 {len(failed)} 行{"，dry-run 未动库" if dry_run else ""}）')
    duration = (datetime.now() - started).total_seconds()
    report = write_review_report('P0 恒生快照 close 位次回补', 'rthk_close_backfill_report',
                                 status, lines, duration)
    print(report or '\n'.join(lines))
    sys.exit(0 if not failed else 2)


if __name__ == '__main__':
    main()
