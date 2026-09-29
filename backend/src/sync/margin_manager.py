"""
融资融券同步（方案 4.11，M3；沪深交易所官网每日披露，T-1 日数据）

接口事实（2026-09-27 实抓锁定，详见 sse_szse_source 文件头）：
- 深交所：1837_xxpl tab2（txtDate=当日；融资买入额/融资余额亿元、融券卖出量/余量万股、
  融券余额万元；无融资偿还额列 → 留空）
- 上交所：RZRQ_MX_INFO（全市场单日 ~2000 条；rzye/rzmre/rzche 元、rqyl 融券余量；
  无融券余额金额列 → 留空）
- 官网保留历史，次日并抓；复盘报告两融章节必须标注"数据日期 T-1"
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.sse_szse_source import SzseSseSource
from ..database.connection import DatabaseConnection
from .review_common import code6_to_ts_code, write_review_report

logger = logging.getLogger(__name__)


class MarginManager:
    """两融：官网分页明细 -> margin_trade_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = SzseSseSource(self.config)
        review = self.config.get('review_sync', {}) or {}
        self.lookback_days = int(review.get('margin_lookback_days', 5))

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        """拉取 trade_date（缺省 T-1）全市场两融明细。

        交易所发布滞后时逐日回看（默认最多 3 天），沪深独立回补到各自最近有数据日，
        报告如实标注每所数据日期（复盘两融章节标注"数据日期 T-N"）。
        """
        started = datetime.now()
        if trade_date:
            target = datetime.strptime(trade_date, '%Y%m%d').date()
        else:
            target = datetime.now().date() - timedelta(days=1)
        date_str = target.strftime('%Y%m%d')
        stats: Dict[str, Any] = {
            'data_date': date_str, 'szse_date': None, 'sse_date': None,
            'szse_rows': 0, 'sse_rows': 0, 'rows': 0, 'missing': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        rows: List[Dict[str, Any]] = []
        try:
            # 深交所（单位换算：亿元×1e8；融券余额万元×1e4）——发布滞后时逐日回看
            szse_count = 0
            for offset in range(self.lookback_days):
                day = (target - timedelta(days=offset)).strftime('%Y%m%d')
                batch = self._szse_rows(day)
                if batch:
                    rows.extend(batch)
                    szse_count = len(batch)
                    stats['szse_date'] = day
                    break
            stats['szse_rows'] = szse_count

            # 上交所（元原样；无融券余额金额列 → rqye 留空）
            sse_count = 0
            for offset in range(self.lookback_days):
                day = (target - timedelta(days=offset)).strftime('%Y%m%d')
                batch = self._sse_rows(day)
                if batch:
                    rows.extend(batch)
                    sse_count = len(batch)
                    stats['sse_date'] = day
                    break
            stats['sse_rows'] = sse_count
            stats['rows'] = len(rows)

            # 行数下限校验（两融全市场 >3000 标的，方案 5.4）
            if len(rows) < 3000 and len(rows) > 0:
                logger.warning(f'两融仅 {len(rows)} 行，低于历史日均 3000，'
                               f'疑似数据不全（仍入库，报告标注）')
                stats['missing'].append(f'行数 {len(rows)} < 3000')

            if not rows:
                result['errors'].append(
                    f'{date_str} 起回看 {self.lookback_days} 日沪深两融均无数据（非交易日或未发布）')
            if not dry_run and rows:
                self.db.upsert_margin_trades(rows)
            result['success'] = bool(rows)
        except Exception as e:
            logger.exception(f'两融采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 每所行构造（回看复用） ----------

    def _szse_rows(self, day: str) -> List[Dict[str, Any]]:
        rows = []
        for item in self.source.fetch_szse_margin(day):
            code = str(item.get('zqdm') or '')
            if not code:
                continue
            rzmre = _num(item.get('jrrzmr'), 1e8)
            rzye = _num(item.get('jrrzye'), 1e8)
            rqye = _num(item.get('jrrjye'), 1e4)
            if rzye is None and rzmre is None:
                continue  # 脏行剔除
            rows.append({
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': rzye,
                'rzmre': rzmre,
                'rzche': None,   # 深交所明细无融资偿还额列
                'rqye': rqye,
            })
        return rows

    def _sse_rows(self, day: str) -> List[Dict[str, Any]]:
        rows = []
        for item in self.source.fetch_sse_margin(day):
            code = str(item.get('stockCode') or '')
            if not code:
                continue
            rzye = _num(item.get('rzye'))
            rzmre = _num(item.get('rzmre'))
            if rzye is None and rzmre is None:
                continue
            rows.append({
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': rzye,
                'rzmre': rzmre,
                'rzche': _num(item.get('rzche')),
                'rqye': None,
            })
        return rows

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期：深交所 {stats.get('szse_date') or '无'} / "
            f"上交所 {stats.get('sse_date') or '无'}（T-{stats.get('lookback', '')} 口径按实际标注；"
            f"交易所次日盘前发布）",
            f"- 深交所 {stats['szse_rows']} 行 / 上交所 {stats['sse_rows']} 行，合计 {stats['rows']}",
            f"- 单位：元（深交所原始亿元、融券余额万元已换算；偿还额深交所无该列）",
        ]
        if stats['missing']:
            lines.append(f"- 校验提示：{stats['missing']}")
        if result['errors']:
            lines.append(f"- 提示：{result['errors']}")
        status = '成功' if result['success'] else '失败（待补）'
        return write_review_report('融资融券同步报告', 'margin_trade_sync_report',
                                   status, lines, result.get('duration'))


def _num(value: Any, scale: float = 1.0) -> Optional[float]:
    if value is None:
        return None
    s = str(value).replace(',', '').strip()
    if s in ('', '--', '-'):
        return None
    try:
        return float(s) * scale
    except ValueError:
        return None
