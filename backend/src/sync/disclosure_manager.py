"""
财报披露日历（方案 4.4，M2；沪深交易所官网预约披露页）

接口事实（2026-09-27 实抓锁定，详见 sse_szse_source 文件头）：
- 上交所：sqlId=SSE_SZSGG_DQBGYYQK_CAST_NEW，bulletintype L011年报/L012半年报/
  L013一季报/L014三季报 + publishYear；行含 companyCode/companyAbbr/
  publishDate0 预约日/actualDate 实际披露日 → 预约与实际同表不同列一次拿全
- 深交所：官网改版后预约披露目录未定位（探路结论"待核实"）→ 标"待补"；
  实际披露日不受影响：由巨潮公告 pubDate 回填（base_financial_report COALESCE 规则）

频率：每日回填（SSE 4 类型全量查询 ~4-8 请求，幂等 upsert）；schema 上预约/实际同表不同列。
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.sse_szse_source import SzseSseSource
from ..database.connection import DatabaseConnection
from .review_common import code6_to_ts_code, write_review_report

logger = logging.getLogger(__name__)

BULLETIN_TYPES = ('L011', 'L012', 'L013', 'L014')
BULLETIN_NAMES = {'L011': '年报', 'L012': '半年报', 'L013': '一季报', 'L014': '三季报'}


class DisclosureManager:
    """披露日历：上交所预约+实际（深交所待补）-> base_disclosure_calendar"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = SzseSseSource(self.config)

    def execute(self, year: Optional[int] = None, dry_run: bool = False) -> Dict[str, Any]:
        """执行采集：当前年份（默认）全部报告类型，预约/实际一并 upsert"""
        started = datetime.now()
        year = year or date.today().year
        # Q1 时上一年年报预约仍有效：多抓上一年年报类型
        years = [year, year - 1] if date.today().month <= 4 else [year]
        stats: Dict[str, Any] = {
            'years': years, 'sse_rows': 0, 'plan_rows': 0, 'actual_rows': 0,
            'szse_status': '待补（目录待核实）', 'errors': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        rows: List[Dict[str, Any]] = []
        try:
            for y in years:
                for bt in BULLETIN_TYPES:
                    try:
                        raw = self.source.fetch_sse_plan(y, bt)
                    except NotImplementedError as e:
                        stats['errors'].append(str(e))
                        continue
                    period_md, kind = _bulletin_period(bt)
                    for item in raw:
                        code = str(item.get('companyCode') or '')
                        if not code:
                            continue
                        plan_date = _norm_date(item.get('publishDate0'))
                        actual_date = _norm_date(item.get('actualDate'))
                        rows.append({
                            'ts_code': code6_to_ts_code(code),
                            'report_period': f'{y}{period_md}',
                            'plan_disclosure_date': plan_date,
                            'actual_disclosure_date': actual_date,
                            'exchange': 'sse',
                        })
                        if plan_date:
                            stats['plan_rows'] += 1
                        if actual_date:
                            stats['actual_rows'] += 1
            stats['sse_rows'] = len(rows)
            stats['rows'] = stats['sse_rows']  # 编排器摘要行数约定
            if not dry_run and rows:
                self.db.upsert_disclosure_calendar(rows)
            result['success'] = len(rows) > 0
            if not rows:
                result['errors'].append('上交所预约披露无数据（深交所待补）')
        except Exception as e:
            logger.exception(f'披露日历采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    def fetch_disclosed_on(self, ann_date: str) -> List[Dict[str, Any]]:
        """查询某日实际披露清单（THS 财报增量的当日披露范围）"""
        return self.db.fetch_disclosure_actual_between(ann_date, ann_date)

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 年份：{stats['years']}；上交所行数：{stats['sse_rows']}"
            f"（预约 {stats['plan_rows']}，实际 {stats['actual_rows']}）",
            f"- 深交所预约披露：{stats['szse_status']}（实际披露日由巨潮公告 pubDate 回填）",
        ]
        if stats['errors']:
            lines.append(f"- 错误：{stats['errors']}")
        status = '成功' if result['success'] else '部分待补'
        return write_review_report('财报披露日历同步报告', 'disclosure_calendar_sync_report',
                                   status, lines, result.get('duration'))


def _bulletin_period(bulletin_type: str) -> tuple:
    return {
        'L011': ('1231', '年报'), 'L012': ('0630', '半年报'),
        'L013': ('0331', '一季报'), 'L014': ('0930', '三季报'),
    }[bulletin_type]


def _norm_date(value: Any) -> Optional[str]:
    s = str(value or '').strip()
    if not s:
        return None
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 3:
            return ''.join(p.zfill(2) for p in parts)
    if len(s) == 8 and s.isdigit():
        return s
    return None
