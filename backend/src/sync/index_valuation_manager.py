"""
指数估值同步（方案 4.5，M2；中证指数公司官网每日 indicator 文件，唯一来源）

- URL 与列结构见 csindex_source 文件头（2026-09-27 实测锁定）
- 只采官网发布口径，不自算指数 PE；官方文件无 TTM 口径 → pe_ttm 留空
- 官网保留历史文件（文件内含近 20 个交易日），失败次日自动回溯补齐缺口日期
- 指数历史日K（延后项补齐方案 N2，2026-10-04）：同站 index-perf API 附加增量，
  日常拉最近 10 自然日窗口（--init-perf 按年回补全历史），落 trade_index_info
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.csindex_source import CsindexFileNotPublished, CsindexSource
from ..database.connection import DatabaseConnection
from .review_common import load_manifest, save_manifest, tmp_dir, write_review_report

logger = logging.getLogger(__name__)

PERF_INIT_DEFAULT_START_YEAR = 2005   # 单指数缺省起始年（000985 实测自 2005 有全历史）
PERF_INCREMENTAL_DAYS = 10            # 日常增量自然日窗口
# WAF 实测（2026-10-04）：www.csindex.com.cn 连续请求（约 10 次/小时内）会触发临时封禁
# （403 拦截页，curl/requests 同样中招，约数十分钟级解除）——回补按年 22+ 请求必须降速，
# 且断点续跑（manifest），封禁即退避、稍后续跑
PERF_INIT_INTERVAL = 10.0             # --init-perf 请求间隔（秒，csindex_perf_interval 可覆盖）
PERF_MANIFEST_NAME = 'index_perf_backfill_manifest.json'


class IndexValuationManager:
    """指数估值：中证 indicator 文件 -> index_valuation_daily；附带 index-perf 日K增量"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = CsindexSource(self.config)
        review = self.config.get('review_sync', {}) or {}
        self.indices: List[Dict[str, str]] = list(review.get('csindex_indices', []) or [])
        self.backfill_days = int(review.get('csindex_backfill_days', 5))
        # N2：指数历史日K清单（code/ts_code/name/init_start_year）
        self.perf_indices: List[Dict[str, Any]] = list(
            review.get('csindex_perf_indices', []) or [])

    def execute(self, dry_run: bool = False,
                init_perf: bool = False) -> Dict[str, Any]:
        """每日盘后抓取（文件含近 20 日滚动窗口，入库校验后 upsert）

        init_perf=True（延后项补齐方案 N2）：跳过估值文件，仅做 index-perf 历史
        回补——按年切窗逐年 1 请求 + upsert（幂等，重跑安全）。
        """
        started = datetime.now()
        stats: Dict[str, Any] = {
            'indices_total': len(self.indices), 'indices_ok': 0, 'rows': 0,
            'missing': [], 'failed': [],
            'perf_indices_total': len(self.perf_indices), 'perf_ok': 0,
            'perf_rows': 0, 'perf_failed': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        if init_perf:
            return self._execute_init_perf(started, dry_run, result)
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
            # N2：指数历史日K日常增量（最近 10 自然日窗口，每指数 1 请求；
            # 同 source 串行天然共享 csindex_interval 限速，单指数失败不阻塞）
            perf_rows = self._fetch_perf_incremental(stats)
            stats['perf_rows'] = len(perf_rows)
            if not dry_run and perf_rows:
                self.db.upsert_index_klines(perf_rows)
            result['success'] = stats['indices_ok'] > 0
            # F2（方案 V1.0 §F2）：自报数据日——本次入库行的最大 trade_date（无新行
            # 则报库内 max），供编排层"成功 ≠ 有当日数据"校验降级
            candidate_dates = [p.get('trade_date') for p in rows
                               if isinstance(p, dict) and p.get('trade_date')]
            perf_dates = [p.get('trade_date') for p in perf_rows
                          if isinstance(p, dict) and p.get('trade_date')]
            all_dates = [str(d) for d in candidate_dates + perf_dates if d]
            if all_dates:
                stats['data_date_max'] = max(all_dates)
            else:
                try:
                    r = self.db.execute_query(
                        'SELECT MAX(trade_date) AS mx FROM index_valuation_daily')
                    stats['data_date_max'] = str(r[0]['mx']) if r and r[0].get('mx') else None
                except Exception:
                    stats['data_date_max'] = None
        except Exception as e:
            logger.exception(f'指数估值采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- N2：index-perf 历史日K ----------

    def _perf_row(self, item: Dict[str, Any], idx: Dict[str, Any]) -> Dict[str, Any]:
        """API 行补齐 trade_index_info 行构造所需的键（up_count/down_count 中证无）"""
        return {**item, 'ts_code': idx['ts_code'], 'index_code': idx['code'],
                'index_name': idx.get('name') or idx['code'],
                'up_count': None, 'down_count': None}

    def _fetch_perf_incremental(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """日常增量：每指数最近 PERF_INCREMENTAL_DAYS 自然日窗口（当日文件未发布时
        窗口自然回看，不依赖游标——单请求全窗口 upsert 幂等）"""
        import datetime as _dt
        end = datetime.now().strftime('%Y%m%d')
        start = (datetime.now() - _dt.timedelta(days=PERF_INCREMENTAL_DAYS)) \
            .strftime('%Y%m%d')
        rows: List[Dict[str, Any]] = []
        for idx in self.perf_indices:
            try:
                got = self.source.fetch_perf_history(idx['code'], start, end)
                rows.extend(self._perf_row(r, idx) for r in got)
                stats['perf_ok'] += 1
            except Exception as e:
                stats['perf_failed'].append(f"{idx.get('name') or idx['code']}({idx['code']}): {e}")
                logger.warning(f'index-perf {idx["code"]} 增量失败: {e}')
        return rows

    def _perf_year_windows(self, start_year: int) -> List[tuple]:
        """--init-perf 年度切窗：start_year 起每年 1 窗（当年截至今天，历史年整年）"""
        import datetime as _dt
        today = datetime.now()
        windows = []
        for year in range(int(start_year), today.year + 1):
            s = f'{year}0101'
            e = f'{year}1231' if year < today.year else today.strftime('%Y%m%d')
            windows.append((s, e))
        return windows

    def _execute_init_perf(self, started: datetime, dry_run: bool,
                           result: Dict[str, Any]) -> Dict[str, Any]:
        """历史回补模式：每指数按年切窗逐请求 upsert（降速 PERF_INIT_INTERVAL）。

        manifest 断点续跑：每完成一个年度窗记录（幂等），WAF 封禁/中断后重跑
        自动跳过已完成的窗（403 → 退避，稍后重跑本命令即可续）。
        """
        stats = result['stats']
        manifest_path = tmp_dir() / PERF_MANIFEST_NAME
        manifest = load_manifest(manifest_path)
        done: Dict[str, Any] = manifest.setdefault('done_windows', {})
        # 回补专用降速（覆盖实例间隔，不影响日常增量的 csindex_interval）
        self.source.request_interval = float(
            (self.config.get('review_sync') or {}).get('csindex_perf_interval',
                                                       PERF_INIT_INTERVAL))
        try:
            total = 0
            for idx in self.perf_indices:
                start_year = int(idx.get('init_start_year') or PERF_INIT_DEFAULT_START_YEAR)
                windows = self._perf_year_windows(start_year)
                for (s, e) in windows:
                    key = f'{idx["code"]}|{s[:4]}'
                    if key in done:
                        total += int(done[key])
                        continue
                    got = self.source.fetch_perf_history(idx['code'], s, e)
                    rows = [self._perf_row(r, idx) for r in got]
                    if not dry_run and rows:
                        self.db.upsert_index_klines(rows)
                    done[key] = len(rows)
                    total += len(rows)
                    save_manifest(manifest_path, manifest)
                    logger.info(f'index-perf {idx["code"]} {s[:4]} 窗完成（{len(rows)} 行）')
                stats['perf_ok'] += 1
                logger.info(f'index-perf {idx["code"]} 回补完成 '
                            f'（{start_year} 起 {len(windows)} 窗）')
            stats['perf_rows'] = total
            result['success'] = stats['perf_ok'] > 0
        except Exception as e:
            logger.exception(f'index-perf 历史回补失败（已完成窗已落库，重跑自动续）: {e}')
            result['errors'].append(f'{e}（已完成窗已落库并记录 manifest，稍后重跑自动续）')
            result['blocked'] = isinstance(e, Exception) and '403' in str(e)
        finally:
            stats['perf_done_windows'] = len(done)
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result, init_perf=True)
        return result

    def _write_report(self, result: Dict[str, Any], init_perf: bool = False) -> Optional[str]:
        stats = result['stats']
        if init_perf:
            lines = [
                f"- 回补指数：{stats['perf_indices_total']} 个（成功 {stats['perf_ok']}）",
                f"- 落库行数：{stats['perf_rows']}；已完成年度窗 {stats.get('perf_done_windows', 0)} 个"
                f"（manifest 断点，重跑自动续）",
                f"- 限速：{self.source.request_interval}s/请求（csindex WAF 频控实测，封禁即退避）",
                f"- 口径：tradingValue 亿元×1e8 对齐元口径；preclose=close−change；"
                f"中证 API 无涨跌家数 → up_count/down_count 留空",
            ]
            if result['errors']:
                lines.append(f"- 错误：{result['errors']}")
            status = '风控急停' if result.get('blocked') else ('成功' if result['success'] else '失败')
            return write_review_report('指数历史日K回补报告', 'index_perf_backfill_report',
                                       status, lines, result.get('duration'))
        lines = [
            f"- 指数：{stats['indices_total']} 个（成功 {stats['indices_ok']}，"
            f"未发布 {len(stats['missing'])}）",
            f"- 落库行数：{stats['rows']}（近 {self.backfill_days} 个交易日回溯窗口）",
            f"- 口径：中证官方 P/E2（计算用股本）+ D/P2；官方文件无 TTM → pe_ttm 留空",
        ]
        if stats['missing']:
            lines.append(f"- 未发布清单：{stats['missing']}（次日回溯补齐）")
        if self.perf_indices:
            lines.append(f"- 指数日K增量（N2）：{stats['perf_indices_total']} 个"
                         f"（成功 {stats['perf_ok']}），行数 {stats['perf_rows']}"
                         f"（近 {PERF_INCREMENTAL_DAYS} 自然日窗口）")
        if stats['perf_failed']:
            lines.append(f"- 指数日K失败：{stats['perf_failed']}")
        status = '成功' if result['success'] else '失败（待补）'
        return write_review_report('指数估值同步报告', 'index_valuation_sync_report',
                                   status, lines, result.get('duration'))
