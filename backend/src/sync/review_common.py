"""
每日复盘数据采集共享工具（各 manager 公共件）

- code6_to_ts_code：6 位代码 -> ts_code（sh./sz./bj. 前缀，与 limit_up_rules 板块口径一致）
- load_manifest / save_manifest：全量任务断点（tmp/*.manifest.json，原子写 .tmp+replace，
  禁止手工改动——沿用 fundamentals_rebuild 惯例）
- write_review_report：统一 sync report 骨架（data/reports/，沿用 report_writer）
- ReviewSyncError：manager 通用失败（区别于风控急停 ReviewBlockedError）
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .report_writer import resolve_repo_root, write_markdown_report

logger = logging.getLogger(__name__)


class ReviewSyncError(RuntimeError):
    """review 采集通用失败（网络耗尽/校验不过/结构变更等，不触发冷却）"""


def code6_to_ts_code(code: str) -> str:
    """6 位股票/指数代码 -> ts_code 前缀。

    码段表：60/68（沪 A 股/科创）、5（沪基金/ETF）、10/11（沪转债）→ sh.；
    0/3（深 A 股/创业）、12（深转债）、15/16/18（深基金）→ sz.（后三段走兜底）；
    4/8/92（北交所）→ bj.；其余兜底 sz.。
    5 开头=沪市基金/ETF（探路 X-15：原兜底分支错标 sz.，两融表曾污染 2135 行）；
    10/11 开头=沪市可转债（两融标的含转债，防御性）。
    """
    c = str(code).strip()
    if c.startswith(('60', '68', '5', '10', '11')):
        return f'sh.{c}'
    if c.startswith(('0', '3')):
        return f'sz.{c}'
    if c.startswith(('4', '8', '92')):
        return f'bj.{c}'
    return f'sz.{c}'


_HTML_NAME_CLEAN = __import__('re').compile(r'<[^>]+>|&[a-z]+;')


def clean_stock_name(name: Any) -> Optional[str]:
    """股票名称清洗：剥除来源页残留的 HTML 标签与实体（探路 X-17：巨潮 <em> 高亮、
    深交所 &nbsp;）。空白压为单空格后去首尾；清洗后为空返回 None。"""
    if name is None:
        return None
    s = _HTML_NAME_CLEAN.sub('', str(name)).replace('\u00a0', ' ')
    s = ' '.join(s.split())
    return s or None


def ts_code_to_code6(ts_code: str) -> str:
    """sz.000001 / 000001.SZ / 000001 -> 6 位代码"""
    c = str(ts_code).strip().lower()
    for prefix in ('sh.', 'sz.', 'bj.'):
        if c.startswith(prefix):
            return c[len(prefix):].upper()
    for suffix in ('.sh', '.sz', '.bj'):
        if c.endswith(suffix):
            return c[:-3].upper()
    return c.upper()


# 北交所 6 位代码白名单前缀（P0-4：43x/83x/87x 老段 + 920 新段；920 尚未全启用、921 未启用）
_BJ_ALLOWED_PREFIXES = ('43', '83', '87', '920')


def validate_bj_stock_code(code6: str) -> str:
    """北交所 6 位代码白名单校验（P0-4，复盘报告层与 P0 修复技术方案 §四）。

    老号段（43/83/87 段）已批量迁移 920xxx，且在外部接口普遍返回**僵尸数据且
    HTTP 200**——静默入库会污染日K，故命中 921+ 或其他形态一律抛 ValueError
    （宁抛错不入库）。返回归一化后的 6 位代码。
    """
    c = str(code6).strip()
    if not (len(c) == 6 and c.isdigit()):
        raise ValueError(f'北交所代码非 6 位数字: {code6!r}')
    if not c.startswith(_BJ_ALLOWED_PREFIXES):
        raise ValueError(f'北交所代码号段越界（白名单 43x/83x/87x/920x）: {c}')
    return c


def detect_zombie_bj_codes(rows: List[Dict[str, Any]],
                           min_consecutive_days: int = 5) -> List[str]:
    """北交所僵尸码检测（P0-4 数据审计规则，纯函数）。

    判据：某代码**连续 ≥ ``min_consecutive_days`` 个交易日**的行满足
    ``volume == 0 且 close == preclose``（全市场有行情而该股无成交、价格定格）
    → 疑似僵尸码（号段迁移后旧码定格数据的典型形态）。连续性按该股自己的
    交易日序列走查：恢复成交或自然日断裂（>12 天，长假容差）均打断累计。

    Args:
        rows: 该批北交所日K行（至少含 stock_code/trade_date/volume/close/preclose）
        min_consecutive_days: 连续交易日数阈值

    Returns:
        疑似僵尸码的 6 位代码清单（升序）；无则为空表。
    """
    # {code: {date: 是否定格}}——定格 = 无成交且价格未动（疑似停牌的 close != preclose 不算）
    zeroed: Dict[str, Dict[str, bool]] = {}
    for r in rows:
        code = str(r.get('stock_code') or '')
        d = str(r.get('trade_date') or '')
        if not code or not d:
            continue
        try:
            vol = float(r.get('volume') or 0)
            close = float(r.get('close'))
            preclose = float(r.get('preclose'))
        except (TypeError, ValueError):
            continue
        zeroed.setdefault(code, {})[d] = (vol == 0 and close == preclose)

    zombies = []
    for code, flags in zeroed.items():
        ordered = sorted(flags)
        run = 0
        max_run = 0
        prev = None
        for d in ordered:
            if prev is None or not _is_next_trading_day(prev, d):
                run = 0
            run = run + 1 if flags[d] else 0
            max_run = max(max_run, run)
            prev = d
        if max_run >= min_consecutive_days:
            zombies.append(code)
    return sorted(zombies)


def _is_next_trading_day(a: str, b: str) -> bool:
    """b 是否紧邻 a 的下一交易日（自然日差 1~12 天内视为连续，覆盖国庆+中秋
    最长连休的自然日跨度；更长断裂不计连续——审计规则宁可少报不误报）。"""
    from datetime import datetime
    try:
        da = datetime.strptime(a, '%Y%m%d')
        db = datetime.strptime(b, '%Y%m%d')
    except ValueError:
        return False
    return 1 <= (db - da).days <= 12


def load_manifest(path: Path) -> Dict[str, Any]:
    """读 manifest（不存在/损坏时重建；每完成一只保存，急停可续）"""
    if path.exists():
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception as e:
            logger.warning(f'manifest 读取失败，重建: {e}')
    return {'stocks': {}, 'updated_at': None}


def save_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    """原子写 manifest（.tmp + replace）"""
    manifest['updated_at'] = datetime.now().isoformat(timespec='seconds')
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding='utf-8')
    tmp.replace(path)


def write_review_report(title: str, file_prefix: str, status: str, lines: List[str],
                        duration_seconds: Optional[float] = None) -> Optional[str]:
    """统一报告骨架：标题 + 状态 + 自定义行 + 耗时，落 data/reports/"""
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_lines = [f'# {title} {ts}', '', f'- 状态：{status}']
    report_lines.extend(lines)
    if duration_seconds is not None:
        report_lines.append(f'- 耗时：{duration_seconds / 60:.1f} 分钟')
    report_lines.append('')
    return write_markdown_report(report_lines, file_prefix, log_label=title, ts=ts)


def tmp_dir() -> Path:
    """仓库根 tmp/（manifest 与触发清单所在）"""
    return resolve_repo_root() / 'tmp'


def latest_closed_trade_date(db: Any) -> str:
    """增量目标基准日：交易日历中 ≤ 今天的最近交易日；若为"今天"且未收盘（15:30 前）再退一日。

    F1（数据采集链路风险修复方案 V1.0）从 SyncManager._latest_closed_trade_date 抽出为
    公共函数，供新鲜度巡检（freshness_check_manager）与编排层 F2 数据日校验复用，
    保证"最近已收盘的数据交易日"口径全链路一致（问题记录 P-11）。
    db 须有 execute_query(query, params)（DatabaseConnection）。
    """
    from datetime import date as _date
    today_str = _date.today().strftime('%Y-%m-%d')
    rows = db.execute_query(
        "SELECT calendar_date FROM base_trade_calendar "
        "WHERE calendar_date <= %s AND is_trading_day = 1 "
        "ORDER BY calendar_date DESC LIMIT 1", (today_str,))
    if not rows:
        return today_str
    latest = rows[0]['calendar_date']
    latest_str = latest.strftime('%Y-%m-%d') if hasattr(latest, 'strftime') else str(latest)
    if latest_str == today_str and datetime.now().time() < datetime.strptime('15:30', '%H:%M').time():
        prev = db.execute_query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE calendar_date < %s AND is_trading_day = 1 "
            "ORDER BY calendar_date DESC LIMIT 1", (latest_str,))
        if prev:
            p = prev[0]['calendar_date']
            return p.strftime('%Y-%m-%d') if hasattr(p, 'strftime') else str(p)
    return latest_str


def expand_trade_days(db: Any, start_date: str, end_date: str) -> List[str]:
    """按 base_trade_calendar 展开回填区间交易日（is_trading_day=1，yyyyMMdd 升序）。

    开发项 C：回填循环的统一入口——非交易日自动跳过（区别于 shell 逐日循环）。
    db 须有 fetch_trade_calendar(start_iso, end_iso)（DaoFundamentals）。
    """
    start = _parse_date_arg(start_date)
    end = _parse_date_arg(end_date)
    if start is None or end is None or start > end:
        raise ValueError(f'回填区间非法: {start_date!r} ~ {end_date!r}')
    rows = db.fetch_trade_calendar(start.isoformat(), end.isoformat())
    out = []
    for r in rows:
        s = str(r.get('calendar_date') or '').replace('-', '')
        if len(s) == 8 and s.isdigit():
            out.append(s)
    return out


def _parse_date_arg(value: str):
    from datetime import datetime as _dt
    s = str(value).replace('-', '')[:8]
    try:
        return _dt.strptime(s, '%Y%m%d').date()
    except ValueError:
        return None
