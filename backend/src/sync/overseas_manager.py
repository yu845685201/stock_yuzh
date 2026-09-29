"""
外围市场同步（方案 4.14，M1；新浪 hq.sinajs.cn 唯一来源，无兜底）

- 批量逗号拼接 1-2 请求拿全部市场；代码清单配置化（review_sync.sina_markets）
- 口径标注：北京时间盘后取到的美股为昨夜收盘（T-1），汇率为最新值——报告按此标注
- 失败处理：重试 1 次（基类 3 次网络重试已覆盖）后标"待补"（轻量数据，不做复杂恢复）
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.sina_source import SinaSource
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

logger = logging.getLogger(__name__)


class OverseasManager:
    """外围市场：新浪批量报价 -> overseas_market_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = SinaSource(self.config)
        review = self.config.get('review_sync', {}) or {}
        self.markets: List[Dict[str, str]] = list(review.get('sina_markets', []) or [])

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        """执行采集（trade_date 为 A 股交易日 yyyyMMdd，作为落库键）"""
        started = datetime.now()
        trade_date = trade_date or datetime.now().strftime('%Y%m%d')
        stats: Dict[str, Any] = {
            'trade_date': trade_date, 'markets_total': len(self.markets),
            'markets_ok': 0, 'rows': 0, 'missing': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            if not self.markets:
                result['errors'].append('外围市场清单为空（config review_sync.sina_markets）')
                return result
            quotes = self.source.fetch_markets(self.markets)
            rows = []
            for q in quotes:
                if q.get('close') is None:
                    stats['missing'].append(q['market_code'])
                    continue
                rows.append({
                    'trade_date': trade_date,
                    'market_code': q['market_code'],
                    'market_name': q.get('market_name'),
                    'close': q.get('close'),
                    'change_pct': q.get('change_pct'),
                })
            stats['rows'] = len(rows)
            stats['markets_ok'] = len(rows)
            if not dry_run and rows:
                self.db.upsert_overseas_markets(rows)
            result['success'] = len(rows) > 0
            if stats['missing']:
                result['errors'].append(f'无报价代码: {stats["missing"]}（标待补）')
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期（A 股交易日）：{stats['trade_date']}",
            f"- 口径：美股为昨夜收盘（T-1），汇率/美元指数为最新值",
            f"- 市场：{stats['markets_total']} 个（成功 {stats['markets_ok']}，缺失 {len(stats['missing'])}）",
        ]
        if stats['missing']:
            lines.append(f"- 缺失清单：{stats['missing']}（标待补）")
        status = '成功' if result['success'] else '失败（待补）'
        return write_review_report('外围市场同步报告', 'overseas_market_sync_report',
                                   status, lines, result.get('duration'))
