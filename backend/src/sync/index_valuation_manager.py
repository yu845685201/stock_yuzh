"""
指数估值同步（方案 4.5，M2；中证指数公司官网每日 indicator 文件，唯一来源）

- URL 与列结构见 csindex_source 文件头（2026-09-27 实测锁定）
- 只采官网发布口径，不自算指数 PE；官方文件无 TTM 口径 → pe_ttm 留空
- 官网保留历史文件（文件内含近 20 个交易日），失败次日自动回溯补齐缺口日期
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.csindex_source import CsindexFileNotPublished, CsindexSource
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

logger = logging.getLogger(__name__)


class IndexValuationManager:
    """指数估值：中证 indicator 文件 -> index_valuation_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = CsindexSource(self.config)
        review = self.config.get('review_sync', {}) or {}
        self.indices: List[Dict[str, str]] = list(review.get('csindex_indices', []) or [])
        self.backfill_days = int(review.get('csindex_backfill_days', 5))

    def execute(self, dry_run: bool = False) -> Dict[str, Any]:
        """每日盘后抓取（文件含近 20 日滚动窗口，入库校验后 upsert）"""
        started = datetime.now()
        stats: Dict[str, Any] = {
            'indices_total': len(self.indices), 'indices_ok': 0, 'rows': 0,
            'missing': [], 'failed': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            import datetime as _dt
            min_date = (datetime.now() - _dt.timedelta(days=self.backfill_days + 7)) \
                .strftime('%Y%m%d')
            rows: List[Dict[str, Any]] = []
            for idx in self.indices:
                code = idx['code']
                name = idx.get('name') or code
                try:
                    parsed = self.source.fetch_indicator_file(code)
                except CsindexFileNotPublished as e:
                    stats['missing'].append(f'{name}({code})')
                    logger.info(f'中证 {code} 文件未发布: {e}')
                    continue
                except Exception as e:
                    # 单指数失败不影响其余（中证未覆盖的深交所指数等会稳定 404）
                    stats['failed'].append(f'{name}({code}): {e}')
                    logger.warning(f'中证 {code} 抓取失败: {e}')
                    continue
                recent = [p for p in parsed if p['trade_date'] >= min_date]
                if not recent:
                    stats['missing'].append(f'{name}({code}) 窗口内无新数据')
                    continue
                rows.extend(self.source.to_row(p, code, name) for p in recent)
                stats['indices_ok'] += 1
            stats['rows'] = len(rows)
            if not dry_run and rows:
                self.db.upsert_index_valuations(rows)
            result['success'] = stats['indices_ok'] > 0
        except Exception as e:
            logger.exception(f'指数估值采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 指数：{stats['indices_total']} 个（成功 {stats['indices_ok']}，"
            f"未发布 {len(stats['missing'])}）",
            f"- 落库行数：{stats['rows']}（近 {self.backfill_days} 个交易日回溯窗口）",
            f"- 口径：中证官方 P/E2（计算用股本）+ D/P2；官方文件无 TTM → pe_ttm 留空",
        ]
        if stats['missing']:
            lines.append(f"- 未发布清单：{stats['missing']}（次日回溯补齐）")
        status = '成功' if result['success'] else '失败（待补）'
        return write_review_report('指数估值同步报告', 'index_valuation_sync_report',
                                   status, lines, result.get('duration'))
