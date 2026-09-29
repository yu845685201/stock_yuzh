"""
THS 财报指标同步（方案 4.3，M2；同花顺个股财务页=首选来源，Tushare 已整体排除）

- 接口与结构见 ths_source.py 文件头（2026-09-27 实测锁定：无 hexin-v 强制）
- 披露日口径（项目约定"披露日以真实 pubDate 为准"）：THS 无披露日列，disclosure_date
  留空，由 4.4（官网 actual）与 4.13（公告 ann_date）经 backfill SQL 回填
- 全量初始化：manifest 断点（tmp/ths_finance_manifest.json）跨天
- 日常增量：只抓"当日有披露"的股——当日披露清单来自 4.4 actual 回填 + 4.13 公告标题
  匹配（定期报告/业绩快报）；普通日几十只，披露高峰日数百只（可 --sources 跳过后台补）
- 风控：ThsBlockedError 急停+冷却，绝不重试轰炸；连续失败报告标"待补"
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.ths_source import ThsSource
from ..database.connection import DatabaseConnection
from .review_common import (
    code6_to_ts_code,
    load_manifest,
    save_manifest,
    ts_code_to_code6,
    write_review_report,
)

logger = logging.getLogger(__name__)

MANIFEST_NAME = 'ths_finance_manifest.json'


class ThsFinanceManager:
    """THS 财报指标：全历史期数（每股 1 请求）-> base_financial_report"""

    def __init__(self, config_manager: ConfigManager, manifest_path: Optional[Any] = None):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = ThsSource(self.config)
        review = self.config.get('review_sync', {}) or {}
        ths_cfg = review.get('ths_finance', {}) or {}
        self.enabled = bool(ths_cfg.get('enabled', True))
        self.backfill_years = int(ths_cfg.get('backfill_years', 5))
        from pathlib import Path
        from .review_common import tmp_dir
        self.manifest_path = Path(manifest_path) if manifest_path else tmp_dir() / MANIFEST_NAME

    def execute(self, init_mode: bool = False, codes: Optional[List[str]] = None,
                as_of: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        """执行采集。

        Args:
            init_mode: 全量初始化（全市场，manifest 断点，backfill_years 截断期数）
            codes: 指定股票（兼容 ts_code/6 位）
            as_of: 增量模式的"当日披露"日期（yyyyMMdd，缺省当日）
            dry_run: 只列目标不请求
        """
        started = datetime.now()
        stats: Dict[str, Any] = {
            'mode': '', 'targets': 0, 'rows': 0, 'stocks_ok': 0, 'stocks_empty': 0,
            'failed': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'skipped_disabled': False}
        if not self.enabled:
            # 探路结论为"不可行"时的开关（本项探路结论：可用，默认开启；概念/解禁为 false）
            result['skipped_disabled'] = True
            result['errors'].append('review_sync.ths_finance.enabled=false，本项标待补')
            result['report_path'] = self._write_report(result, '跳过（配置关闭，待补）')
            return result

        try:
            if codes:
                targets = sorted({ts_code_to_code6(c) for c in codes})
                stats['mode'] = f'指定 {len(targets)} 只'
            elif init_mode:
                targets = self._all_codes()
                stats['mode'] = '全量初始化'
            else:
                as_of_date = self._parse_date(as_of) or date.today()
                targets = self._disclosed_codes(as_of_date)
                stats['mode'] = f'增量（{as_of_date.strftime("%Y%m%d")} 当日披露 {len(targets)} 只）'
            stats['targets'] = len(targets)
            if dry_run:
                result['success'] = True
                return result

            total_share_map = self.db.fetch_latest_total_shares()
            manifest = load_manifest(self.manifest_path)
            for i, code in enumerate(targets, 1):
                if init_mode:
                    done = manifest.get('stocks', {}).get(code, {}).get('done')
                    if done:
                        stats['stocks_ok'] += 1
                        continue
                try:
                    share = total_share_map.get(code6_to_ts_code(code))
                    n = self._sync_one(code, share)
                    stats['rows'] += n
                    if n:
                        stats['stocks_ok'] += 1
                        if init_mode:
                            manifest.setdefault('stocks', {})[code] = {
                                'done': True, 'rows': n,
                                'updated_at': datetime.now().isoformat(timespec='seconds')}
                            save_manifest(self.manifest_path, manifest)
                    else:
                        # 空结果可能是 THS 瞬时拦截：不记 done，标记 error 供末轮/重跑找回
                        stats['stocks_empty'] += 1
                        stats['failed'].append(code)
                        if init_mode:
                            manifest.setdefault('stocks', {})[code] = {
                                'done': False, 'error': 'ths_empty',
                                'updated_at': datetime.now().isoformat(timespec='seconds')}
                            save_manifest(self.manifest_path, manifest)
                except Exception as e:
                    logger.exception(f'{code} THS 财报采集失败: {e}')
                    stats['failed'].append(code)
                    if init_mode:
                        manifest.setdefault('stocks', {}).setdefault(code, {})['error'] = str(e)
                        save_manifest(self.manifest_path, manifest)
                if init_mode and i % 25 == 0:
                    elapsed = (datetime.now() - started).total_seconds()
                    eta = elapsed / i * (len(targets) - i) / 60
                    logger.info(f'THS 财报进度 {i}/{len(targets)}，ETA {eta:.0f} 分钟')
            result['success'] = not stats['failed'] and stats['targets'] > 0
            if not stats['targets']:
                result['success'] = True  # 当日无披露：空跑即成功
            # 全量模式末轮：对空结果/失败股票自动重试一轮（THS 拦截多为瞬时）
            if init_mode and stats['failed']:
                retry_list = list(dict.fromkeys(stats['failed']))
                logger.info(f'末轮重试 {len(retry_list)} 只（THS 拦截/异常找回）')
                recovered = 0
                for code in retry_list:
                    try:
                        n = self._sync_one(code, total_share_map.get(code6_to_ts_code(code)))
                        if n > 0:
                            recovered += 1
                            stats['rows'] += n
                            manifest.setdefault('stocks', {})[code] = {
                                'done': True, 'rows': n,
                                'updated_at': datetime.now().isoformat(timespec='seconds')}
                            save_manifest(self.manifest_path, manifest)
                        if code in stats['failed']:
                            stats['failed'] = [c for c in stats['failed'] if c != code]
                    except Exception as e:
                        logger.warning(f'重试 {code} 仍失败: {e}')
                logger.info(f'末轮重试找回 {recovered}/{len(retry_list)} 只')
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            if not result.get('report_path'):
                status = '成功' if result['success'] else '存在失败'
                result['report_path'] = self._write_report(result, status)
        return result

    def backfill_disclosure_dates(self) -> int:
        """披露日回填：disclosure_date = COALESCE(公告 ann_date, 官网 actual_date)（独立步骤）"""
        return self.db.backfill_financial_disclosure_dates()

    # ---------- 单股 ----------

    def _sync_one(self, code: str, total_share: Optional[float]) -> int:
        flash = self.source.fetch_finance_main(code)
        rows = self.source.parse_finance_flash(flash, total_share=total_share,
                                               backfill_years=None if self._full_history() else self.backfill_years)
        if not rows:
            return 0
        for r in rows:
            r['ts_code'] = _ts(code)
        self.db.upsert_financial_reports(rows)
        return len(rows)

    @staticmethod
    def _full_history() -> bool:
        """全历史保留（122 期单股仅 ~30KB 落库，回填口径不设年限截断）"""
        return True

    # ---------- 目标清单 ----------

    def _all_codes(self) -> List[str]:
        stocks = self.db.fetch_stock_basic()
        return sorted({ts_code_to_code6(s['ts_code']) for s in stocks if s.get('ts_code')})

    def _disclosed_codes(self, as_of: date) -> List[str]:
        """当日披露清单：官网 actual 回填 + 公告标题匹配（定期报告/业绩快报）"""
        codes: Dict[str, None] = {}
        end = as_of.strftime('%Y%m%d')
        start = (as_of - timedelta(days=1)).strftime('%Y%m%d')
        for row in self.db.fetch_disclosure_actual_between(start, end):
            codes[ts_code_to_code6(row['ts_code'])] = None
        ann_codes = self.db.fetch_announcement_codes_on(end)
        for ts in ann_codes:
            codes[ts_code_to_code6(ts)] = None
        return sorted(codes)

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        s = str(value).replace('-', '')[:8]
        try:
            return datetime.strptime(s, '%Y%m%d').date()
        except ValueError:
            return None

    def _write_report(self, result: Dict[str, Any], status: str) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 模式：{stats.get('mode', '')}",
            f"- 目标 {stats.get('targets', 0)} 只，成功 {stats.get('stocks_ok', 0)}"
            f"（空 {stats.get('stocks_empty', 0)}），失败 {len(stats.get('failed', []))}",
            f"- 落库期数行：{stats.get('rows', 0)}（THS 全历史期数，披露日由 4.4/4.13 回填）",
        ]
        if stats.get('failed'):
            lines.append(f"- 失败清单：{stats['failed'][:50]}")
        return write_review_report('THS财报指标同步报告', 'ths_finance_sync_report',
                                   status, lines, result.get('duration'))


def _ts(code6: str) -> str:
    from .review_common import code6_to_ts_code
    return code6_to_ts_code(code6)
