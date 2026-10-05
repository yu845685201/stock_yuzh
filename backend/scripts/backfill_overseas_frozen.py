"""O4 冻结区间回补：美股三大指数换源 gb_* 并重采历史（幂等，可重跑）

背景（doc/每日复盘skill与复盘链优化方案-V1.0-20261004.md O4）：
sina int_dji/int_nasdaq/int_sp500 接口数值冻结，库内 20260927~0930 四个交易日
close 与涨跌幅完全相同（46247.29/0.65、22484.07/0.44、6643.70/0.59），
create_time 逐日新增——接口返回冻结值但采集状态"成功"，报告照常展示。
2026-10-04 实测接口仍返回冻结值，已换 sina gb_ 前缀代码（sina_source 增加
gb_ 解析分支，config review_sync.sina_markets 已切换），与腾讯 qt/usfqkline
双源对拍一致。

本脚本步骤：
1. 对拍前置：腾讯 ifzq 美股日线尾部两日 vs 腾讯同响应内嵌 qt 实时（昨收/现值）
   vs sina gb_ 实时报价（[1]最新价、[26]昨收、[2]涨跌幅）三方交叉核对，
   不一致即中止、不落库（数据真实性原则：先证源可靠再写数）；
2. 删除 int_* 旧代码全部行（均为冻结垃圾值，库内仅 0927~0930 四日）；
3. 腾讯 ifzq 日线按「A 股交易日 D → D 前最近一个美股交易日收盘」口径回补
   gb_* 行（该口径即 D 日晚间取到的"昨夜收盘"，与盘后采集语义一致；
   涨跌幅按相邻美股交易日收盘自算）；
4. 幂等：重跑先删同区间 gb_* 行再插；
5. 报告落 data/reports/overseas_backfill_report_*.md。

用法（backend/ 下）:
    NO_PROXY=127.0.0.1,localhost ../.venv/bin/python scripts/backfill_overseas_frozen.py [--dry-run]
"""
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.config.config_manager import ConfigManager
from src.data_sources.sina_source import SinaSource
from src.database.connection import DatabaseConnection
from src.sync.review_common import write_review_report

# 冻结区间（即库内 int_* 现存全部 A 股交易日）
FROZEN_DATES = ['20260927', '20260928', '20260929', '20260930']
# 冻结垃圾值（tripwire：回补值若命中说明历史源同样不可信，必须中止）
FROZEN_VALUES = {46247.29, 22484.07, 6643.70}
# (sina gb_ 代码, 腾讯 ifzq 代码, 中文名)
INDICES = [
    ('gb_dji', 'usDJI', '道琼斯'),
    ('gb_ixic', 'usIXIC', '纳斯达克'),
    ('gb_$inx', 'usINX', '标普500'),
]
# 回补最近美股交易日数（冻结区间需 4 日 + 前一日算涨跌幅，取 10 留裕量）
KLINE_DAYS = 10
IFZQ_URL = ('https://web.ifzq.gtimg.cn/appstock/app/usfqkline/get'
            '?param={symbol},day,,,{days},qfq')
TOL = 0.02  # 指数点位对拍容差（0.01 最小报价单位留裕量）


def _to_float(v):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def fetch_ifzq_kline(symbol: str) -> dict:
    """腾讯 ifzq 美股日线：{iso_date: close}，升序。含响应内嵌 qt 实时块。"""
    resp = requests.get(IFZQ_URL.format(symbol=symbol, days=KLINE_DAYS), timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    node = (payload.get('data') or {}).get(symbol) or {}
    days = node.get('day') or []
    if len(days) < 3:
        raise RuntimeError(f'ifzq {symbol} 日线不足: {len(days)} 行')
    closes = {}
    for row in days:
        # day 行格式：[date, open, close, high, low, volume, ...]（与 qt 字段对拍验证）
        close = _to_float(row[2])
        if close:
            closes[row[0].replace('-', '')] = close
    return {'closes': closes, 'qt': node.get('qt', {}).get(symbol)}


def cross_check(klines: dict, sina_quotes: dict) -> list:
    """三方对拍：ifzq 尾日/前日收盘 vs 内嵌 qt 实时 vs sina gb_ 实时。返回核对行。"""
    checks = []
    for _, tencent_symbol, name in INDICES:
        k = klines[tencent_symbol]
        dates = sorted(k['closes'])
        last_dt, prev_dt = dates[-1], dates[-2]
        last_close, prev_close = k['closes'][last_dt], k['closes'][prev_dt]
        qt = k['qt'] or []
        # qt 实时：[3]现值 [4]昨收（字段布局已与日线对拍验证）
        qt_now, qt_prev = _to_float(qt[3]) if len(qt) > 4 else None, _to_float(qt[4]) if len(qt) > 4 else None
        sq = sina_quotes.get(_sina_code(tencent_symbol)) or {}
        checks.append({
            'name': name, 'last_date': last_dt, 'prev_date': prev_dt,
            'ifzq_last': last_close, 'ifzq_prev': prev_close,
            'qt_now': qt_now, 'qt_prev': qt_prev,
            'sina_now': sq.get('close'), 'sina_prev': sq.get('prev_close'),
        })
    return checks


def _sina_code(tencent_symbol: str) -> str:
    return dict((t, s) for s, t, _ in INDICES)[tencent_symbol]


def validate_checks(checks: list) -> list:
    """对拍断言：三方两两一致；回补值不命中冻结垃圾值。返回问题清单（空=通过）。"""
    problems = []
    for c in checks:
        pairs = [('ifzq_last', 'qt_now'), ('ifzq_last', 'sina_now'),
                 ('ifzq_prev', 'qt_prev'), ('ifzq_prev', 'sina_prev')]
        for a, b in pairs:
            va, vb = c[a], c[b]
            if va is None or vb is None:
                problems.append(f"{c['name']}: {a}/{b} 缺值（{va}/{vb}）")
            elif abs(va - vb) > TOL:
                problems.append(f"{c['name']}: {a}={va} vs {b}={vb} 超容差 {TOL}")
        if round(c['ifzq_last'], 2) in FROZEN_VALUES or round(c['ifzq_prev'], 2) in FROZEN_VALUES:
            problems.append(f"{c['name']}: 历史源命中冻结垃圾值，源不可信")
    return problems


def build_backfill_rows(klines: dict) -> list:
    """A 股交易日 D → D 前最近美股交易日收盘；涨跌幅按相邻美股交易日自算。"""
    rows = []
    for sina_code, tencent_symbol, name in INDICES:
        closes = klines[tencent_symbol]['closes']
        dates = sorted(closes)
        for d in FROZEN_DATES:
            prior = [x for x in dates if x < d]
            if not prior:
                raise RuntimeError(f'{name}: {d} 前无历史日线可回补')
            us_date = prior[-1]
            close = closes[us_date]
            prev = closes[prior[-2]] if len(prior) >= 2 else None
            change_pct = round((close - prev) / prev * 100, 4) if prev else None
            rows.append({
                'trade_date': d, 'market_code': sina_code, 'market_name': name,
                'close': close, 'change_pct': change_pct,
            })
    return rows


def main():
    dry_run = '--dry-run' in sys.argv
    started = datetime.now()
    lines = []

    # 1. 取数：腾讯 ifzq 日线 + sina gb_ 实时（走项目 SinaSource，验证新解析分支）
    config = ConfigManager().load_config()
    sina = SinaSource(config)
    markets = [{'code': s, 'name': n} for s, _, n in INDICES]
    time.sleep(1.0)
    sina_quotes = {}
    for q in sina.fetch_markets(markets):
        sina_quotes[q['market_code']] = q
    # sina gb_ 昨收在解析分支未取（第 26 字段），对拍需要 → 原始文本补取
    text = sina.get_text(
        'https://hq.sinajs.cn/list=' + ','.join(s for s, _, _ in INDICES))
    raw = {}
    for chunk in text.split(';'):
        if 'hq_str_' not in chunk or '=' not in chunk:
            continue
        var, _, body = chunk.partition('=')
        fields = [f.strip() for f in body.strip().strip('"').split(',')]
        raw[var.replace('var hq_str_', '').strip()] = fields
    for code in sina_quotes:
        f = raw.get(code) or []
        sina_quotes[code]['prev_close'] = _to_float(f[26]) if len(f) > 26 else None

    klines = {}
    for _, tencent_symbol, _ in INDICES:
        klines[tencent_symbol] = fetch_ifzq_kline(tencent_symbol)
        time.sleep(1.5)

    # 2. 对拍
    checks = cross_check(klines, sina_quotes)
    problems = validate_checks(checks)
    lines.append('- 对拍（腾讯 ifzq 日线 / 内嵌 qt 实时 / sina gb_ 实时）：')
    for c in checks:
        lines.append(
            f"  - {c['name']}：{c['last_date']} 收盘 {c['ifzq_last']:.2f} | "
            f"qt {c['qt_now']} | sina {c['sina_now']}；{c['prev_date']} 收盘 "
            f"{c['ifzq_prev']:.2f} | qt {c['qt_prev']} | sina {c['sina_prev']}")
    if problems:
        for p in problems:
            lines.append(f'  - ❌ {p}')
        lines.append('- 状态：失败（对拍不一致，未动库）')
        print('\n'.join(lines))
        sys.exit(2)
    lines.append('  - ✅ 三方一致，历史源可信')

    rows = build_backfill_rows(klines)
    lines.append('- 回补口径：A 股交易日 D → D 前最近美股交易日收盘（盘后采集的"昨夜收盘"）')
    for r in rows:
        pct = f"{r['change_pct']:+.4f}%" if r['change_pct'] is not None else 'None'
        lines.append(f"  - {r['trade_date']} {r['market_name']}({r['market_code']}）："
                     f"close {r['close']:.2f}  change_pct {pct}")

    db = DatabaseConnection(ConfigManager())
    if dry_run:
        lines.append('- 状态：dry-run（未动库）')
    else:
        gb_codes = [s for s, _, _ in INDICES]
        deleted_frozen = db.execute_update(
            "DELETE FROM overseas_market_daily WHERE market_code = ANY(%s)",
            (['int_dji', 'int_nasdaq', 'int_sp500'],))
        deleted_rerun = db.execute_update(
            "DELETE FROM overseas_market_daily WHERE trade_date = ANY(%s) "
            "AND market_code = ANY(%s)", (FROZEN_DATES, gb_codes))
        db.upsert_overseas_markets(rows)
        lines.append(f'- 删除冻结旧代码行 {deleted_frozen}（int_*，重跑幂等删除 gb_* {deleted_rerun} 行）')
        lines.append(f'- 回补写入 {len(rows)} 行（{len(INDICES)} 指数 × {len(FROZEN_DATES)} 日）')
        lines.append('- 状态：成功')
    lines.append('- 遗留口径：美股为昨夜收盘（T-1）；0927/0928 均映射 09-25（周五）收盘，'
                 '系周末无美股交易日的真实分布，非冻结')

    duration = (datetime.now() - started).total_seconds()
    report = write_review_report('美股三大指数冻结区间回补（O4）', 'overseas_backfill_report',
                                 '成功' if not dry_run else 'dry-run', lines, duration)
    print(report or '\n'.join(lines))


if __name__ == '__main__':
    main()
