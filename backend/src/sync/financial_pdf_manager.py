"""
财报PDF采集管理器（巨潮资讯网，文件落地 + 进度表 + 增量采集）

设计要点（对应方案）：
- PDF 只落 data/financial_pdf/{stock_code}_{stock_name}/{yyyy}_{NN}.pdf，不落数据库
- 数据库仅维护 sync_financial_pdf_progress（股票编码 + 最后财报期），支撑增量
- 默认=增量：有进度记录只采报告期 > last_stat_date 的新财报；无记录回补近 N 年（默认5）
- 幂等三层：进度表（增量下界）、文件存在校验（%PDF头，跳过重复下载）、manifest 失败清单（次轮重试）
- 同报告期多版本：manifest.files 记录每份文件对应的 announcementId，巨潮出现更新/修订版
  时 announcementId 变化 -> 覆盖重下，保留最新披露版本（manifest.replaced 审计）
- 巨潮串行限速；封禁（CninfoBlockedError）整轮急停，manifest/进度已保存可续
"""
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.cninfo_source import CninfoBlockedError, CninfoSource
from ..database.connection import DatabaseConnection
from .report_writer import resolve_repo_root, write_markdown_report

logger = logging.getLogger(__name__)

DEFAULT_MANIFEST = resolve_repo_root() / 'tmp' / 'financial_pdf_manifest.json'


class FinancialPdfManager:
    """财报PDF采集：逐股流水线（读进度 -> 检索 -> 解析过滤 -> 存在检查 -> 下载 -> 推进进度）"""

    def __init__(self, config_manager: ConfigManager, manifest_path: Optional[Path] = None):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.source = CninfoSource(self.config.get('cninfo', {}))
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_financial_pdf_progress_table()
        self.pdf_dir = self._resolve_pdf_dir()
        self.backfill_years = int(self.config.get('cninfo.backfill_years', 5))
        self.manifest_path = Path(manifest_path) if manifest_path else DEFAULT_MANIFEST

    def _resolve_pdf_dir(self) -> Path:
        raw = self.config.get('cninfo.pdf_dir', 'data/financial_pdf')
        path = Path(raw)
        if not path.is_absolute():
            path = resolve_repo_root() / path
        return path

    # ---------- manifest ----------

    def _load_manifest(self) -> Dict[str, Any]:
        if self.manifest_path.exists():
            try:
                return json.loads(self.manifest_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.warning(f"manifest 读取失败，重建: {e}")
        return {'stocks': {}, 'updated_at': None}

    def _save_manifest(self, manifest: Dict[str, Any]) -> None:
        manifest['updated_at'] = datetime.now().isoformat(timespec='seconds')
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.manifest_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding='utf-8')
        tmp.replace(self.manifest_path)

    # ---------- 主流程 ----------

    def execute(self, init_mode: bool = False,
                year_from: Optional[int] = None, year_to: Optional[int] = None,
                codes: Optional[List[str]] = None, dry_run: bool = False) -> Dict[str, Any]:
        """执行采集。

        Args:
            init_mode: 全历史初始化（报告期无下界）
            year_from/year_to: 报告期年份窗口（含端点；--year 单年即两者相同）
            codes: 指定股票代码（6位数字，兼容 sh./sz. 前缀）
            dry_run: 只建清单不下载、不写进度
        """
        started = datetime.now()
        stats: Dict[str, Any] = {
            'mode': self._describe_mode(init_mode, year_from, year_to),
            'stocks_total': 0, 'stocks_done': 0,
            'docs_planned': 0, 'docs_downloaded': 0, 'docs_skipped_existing': 0,
            'docs_skipped_non_pdf': 0,
            'docs_retried_ok': 0, 'docs_failed': 0, 'docs_replaced': 0,
            'progress_advanced': 0, 'estimated_mb': 0.0,
            'failed_stocks': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'manifest_path': str(self.manifest_path),
                                  'report_path': None}
        manifest = self._load_manifest()

        try:
            stock_map = self.source.get_stock_map()
            wanted = self._select_stocks(stock_map, codes)
            if not wanted:
                result['errors'].append('没有可采集的股票（代码不在巨潮A股映射中）')
                return result
            stats['stocks_total'] = len(wanted)
            progress_map = {} if dry_run else self.db.fetch_financial_pdf_progress()
            logger.info(f"采集模式: {stats['mode']}；股票 {len(wanted)} 只；dry_run={dry_run}")

            for code, info in wanted.items():
                try:
                    self._process_stock(code, info, init_mode, year_from, year_to,
                                        progress_map, manifest, stats, dry_run)
                except CninfoBlockedError as e:
                    logger.error(f'巨潮风控触发，整轮急停（进度/断点已保存）: {e}')
                    result['errors'].append(f'巨潮风控急停: {e}')
                    result['blocked'] = True
                    break
                except Exception as e:
                    logger.exception(f'{code} 处理异常: {e}')
                    stats['failed_stocks'].append(code)
                    manifest.setdefault('stocks', {}).setdefault(code, {})['error'] = str(e)

                if not dry_run:
                    self._save_manifest(manifest)
                stats['stocks_done'] += 1
                if stats['stocks_done'] % 25 == 0:
                    self._log_progress(stats, started)

            result['success'] = not result['blocked'] and not stats['failed_stocks']
        finally:
            if not dry_run:
                self._save_manifest(manifest)
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 单股处理 ----------

    def _process_stock(self, code: str, info: Dict[str, str], init_mode: bool,
                       year_from: Optional[int], year_to: Optional[int],
                       progress_map: Dict[str, date], manifest: Dict[str, Any],
                       stats: Dict[str, Any], dry_run: bool) -> None:
        """单股流水线：窗口 -> 检索 -> 解析过滤 -> 选版 -> 下载 -> 推进进度"""
        last_stat = progress_map.get(code)
        se_start = self._resolve_se_start(init_mode, year_from, last_stat)
        se_end = datetime.now().strftime('%Y-%m-%d')
        raw_anns = self.source.query_periodic_reports(code, info['org_id'], se_start, se_end)
        planned, non_pdf_keys = self._build_plan(raw_anns, init_mode, year_from, year_to, last_stat)
        stats['docs_planned'] += len(planned)
        if non_pdf_keys:
            stats['docs_skipped_non_pdf'] += len(non_pdf_keys)

        if dry_run:
            for item in planned:
                stats['estimated_mb'] += self.source.fetch_pdf_size_kb(item['announcement']) / 1024
            return

        stock_entry = manifest.setdefault('stocks', {}).setdefault(code, {})
        files_entry = stock_entry.setdefault('files', {})
        succeeded_periods: List[date] = []
        for item in planned:
            key = f"{item['year']}_{item['nn']}"
            announcement_id = str(item['announcement'].get('announcementId') or '')
            target = self._target_path(code, info['name'], item['year'], item['nn'])
            recorded = (files_entry.get(key) or {}).get('announcement_id')
            already_valid = self.source.validate_pdf_file(target)

            if already_valid and recorded == announcement_id:
                # 文件已在且版本一致：跳过
                stats['docs_skipped_existing'] += 1
                succeeded_periods.append(item['period_end'])
                continue

            # 两种下载路径：文件缺失；或文件在但巨潮出现更新/修订版（announcementId 变化）-> 覆盖
            overwrite = bool(already_valid and recorded and recorded != announcement_id)
            dl = self.source.download_pdf(item['announcement']['adjunctUrl'], target,
                                          overwrite=overwrite)
            if dl['status'] == 'failed':
                stats['docs_failed'] += 1
                failed_list = stock_entry.setdefault('failed', [])
                stock_entry['failed'] = [f for f in failed_list if f.get('key') != key]
                stock_entry['failed'].append({
                    'key': key, 'title': item['announcement'].get('announcementTitle'),
                    'adjunct_url': item['announcement']['adjunctUrl'],
                    'target': str(target), 'reason': dl['error'],
                })
                continue

            if overwrite:
                stats['docs_replaced'] += 1
                stock_entry.setdefault('replaced', []).append(
                    f"{key}.pdf（{recorded} -> {announcement_id}）")
            elif dl['status'] == 'downloaded':
                stats['docs_downloaded'] += 1
            else:  # exists_valid：竞态下文件刚被补齐，按跳过计
                stats['docs_skipped_existing'] += 1
            files_entry[key] = {'announcement_id': announcement_id,
                                'title': item['announcement'].get('announcementTitle'),
                                'updated_at': datetime.now().isoformat(timespec='seconds')}
            succeeded_periods.append(item['period_end'])

        stats['docs_retried_ok'] += self._retry_manifest_failures(code, manifest, stats)

        # 非 PDF 公告对应的失败记录永不可能成功（附件是 HTML），直接清理
        stale = [f for f in stock_entry.get('failed', []) if f.get('key') in set(non_pdf_keys)]
        if stale:
            stock_entry['failed'] = [f for f in stock_entry['failed']
                                     if f.get('key') not in set(non_pdf_keys)]
            logger.info(f'{code} 清理非PDF失败记录 {len(stale)} 条（早年 HTML 公告，无 PDF 可采）')

        new_last = max(succeeded_periods) if succeeded_periods else None
        old_last = progress_map.get(code)
        if new_last and (old_last is None or new_last > old_last):
            self.db.upsert_financial_pdf_progress([(code, new_last)])
            progress_map[code] = new_last
            stats['progress_advanced'] += 1
        if not stock_entry.get('failed'):
            stock_entry.pop('failed', None)

    def _build_plan(self, raw_anns: List[Dict[str, Any]], init_mode: bool,
                    year_from: Optional[int], year_to: Optional[int],
                    last_stat: Optional[date]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """解析公告标题 -> 应用报告期过滤 -> 同期多版本选最新披露版

        Returns:
            (计划列表, 非PDF公告的 (yyyy_NN) 键列表——早年公告有 HTML 形态，无 PDF 可采，
            调用方据此清理 manifest 中对应的陈旧失败记录)
        """
        candidates: Dict[Tuple[int, str], Dict[str, Any]] = {}
        non_pdf_keys: List[str] = []
        for ann in raw_anns:
            title = ann.get('announcementTitle') or ''
            adjunct = ann.get('adjunctUrl') or ''
            parsed = self.source.parse_report_title(title)
            if not parsed:
                continue
            if not adjunct.lower().endswith('.pdf'):
                # 早年公告存在 HTML 形态（如 000003 2001年中期报告）：无 PDF 可采，如实跳过
                non_pdf_keys.append(f'{parsed[0]}_{parsed[1]}')
                continue
            year, nn, _revision = parsed
            period_end = self.source.period_end_date(year, nn)
            if not init_mode:
                if year_from is not None:
                    # 显式年份窗口优先于增量/回补逻辑
                    if year < year_from or (year_to and year > year_to):
                        continue
                elif last_stat is not None:
                    # 增量：只采报告期晚于进度的新财报
                    if period_end <= last_stat:
                        continue
                else:
                    # 无进度回补：报告期年份 >= 当前年 - backfill_years
                    if year < datetime.now().year - self.backfill_years:
                        continue
            key = (year, nn)
            prev = candidates.get(key)
            if prev is None or int(ann.get('announcementTime') or 0) > int(prev.get('announcementTime') or 0):
                candidates[key] = ann
        plan = [{'year': year, 'nn': nn,
                 'period_end': self.source.period_end_date(year, nn),
                 'announcement': ann}
                for (year, nn), ann in candidates.items()]
        plan.sort(key=lambda x: (x['year'], x['nn']))
        return plan, non_pdf_keys

    def _retry_manifest_failures(self, code: str, manifest: Dict[str, Any], stats: Dict[str, Any]) -> int:
        """重试 manifest 中该股历史失败下载；成功即从失败清单移除"""
        entry = manifest.get('stocks', {}).get(code) or {}
        failed = entry.get('failed') or []
        if not failed:
            return 0
        still_failed = []
        recovered = 0
        for f in failed:
            dl = self.source.download_pdf(f['adjunct_url'], Path(f['target']))
            if dl['status'] == 'failed':
                still_failed.append(f)
            else:
                recovered += 1
        if recovered:
            logger.info(f'{code} 失败重试找回 {recovered} 份')
        if still_failed:
            entry['failed'] = still_failed
        else:
            entry.pop('failed', None)
        return recovered

    # ---------- 窗口/路径辅助 ----------

    @staticmethod
    def _normalize_code(raw: str) -> str:
        """兼容 600519 / sh.600519 / 600519.SH 等写法，统一为 6 位数字"""
        c = raw.strip().lower()
        for prefix in ('sh.', 'sz.', 'bj.'):
            if c.startswith(prefix):
                c = c[len(prefix):]
        for suffix in ('.sh', '.sz', '.bj'):
            if c.endswith(suffix):
                c = c[:-3]
        return c

    def _select_stocks(self, stock_map: Dict[str, Dict[str, str]],
                       codes: Optional[List[str]]) -> Dict[str, Dict[str, str]]:
        if not codes:
            return {c: stock_map[c] for c in sorted(stock_map)}
        wanted: Dict[str, Dict[str, str]] = {}
        missing = []
        for raw in codes:
            code = self._normalize_code(raw)
            if code in stock_map:
                wanted[code] = stock_map[code]
            else:
                missing.append(raw)
        if missing:
            logger.warning(f'以下代码不在巨潮A股映射中，跳过: {missing}')
        return wanted

    def _resolve_se_start(self, init_mode: bool, year_from: Optional[int],
                          last_stat: Optional[date]) -> str:
        """披露时间窗起点（seDate 左端）：报告期不会早于其披露，取各模式最保守值"""
        if init_mode:
            return '1990-01-01'
        if year_from is not None:
            return f'{year_from}-01-01'
        if last_stat is not None:
            return last_stat.isoformat()
        # 无进度回补：报告期最早可能在上一年末后披露，多留一年缓冲
        return f'{datetime.now().year - self.backfill_years}-01-01'

    def _target_path(self, code: str, name: str, year: int, nn: str) -> Path:
        dir_name = f'{code}_{self.source.sanitize_dir_name(name)}'
        return self.pdf_dir / dir_name / f'{year}_{nn}.pdf'

    def _describe_mode(self, init_mode: bool, year_from: Optional[int],
                       year_to: Optional[int]) -> str:
        if init_mode:
            return '全历史初始化'
        if year_from and year_to:
            return f'指定报告期年份（{year_from}~{year_to}）'
        if year_from:
            return f'指定报告期年份（{year_from} 起）'
        return f'增量（无进度股票回补近{self.backfill_years}年）'

    @staticmethod
    def _log_progress(stats: Dict[str, Any], started: datetime) -> None:
        done = stats['stocks_done']
        elapsed = (datetime.now() - started).total_seconds()
        eta_min = elapsed / done * (stats['stocks_total'] - done) / 60 if done else 0
        logger.info(f"进度 {done}/{stats['stocks_total']}，已下载 {stats['docs_downloaded']}，"
                    f"跳过已有 {stats['docs_skipped_existing']}，失败 {stats['docs_failed']}，"
                    f"ETA {eta_min:.0f} 分钟")

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        lines = [
            f"# 财报PDF采集报告 {ts}",
            "",
            f"- 状态：{'成功' if result['success'] else ('风控急停' if result['blocked'] else '存在失败')}",
            f"- 模式：{stats['mode']}",
            f"- 股票：{stats['stocks_total']} 只（完成 {stats['stocks_done']}）",
            f"- 计划 {stats['docs_planned']} 份，下载 {stats['docs_downloaded']}，"
            f"跳过已有 {stats['docs_skipped_existing']}，非PDF跳过 {stats['docs_skipped_non_pdf']}，"
            f"重试找回 {stats['docs_retried_ok']}，版本覆盖 {stats['docs_replaced']}，"
            f"失败 {stats['docs_failed']}",
            f"- 进度推进股票数：{stats['progress_advanced']}",
            f"- dry-run 容量估算：{stats['estimated_mb']:.0f} MB（adjunctSize 口径）",
            f"- 失败股票：{stats['failed_stocks'] or '无'}",
            f"- manifest：{self.manifest_path}",
            f"- 耗时：{result.get('duration', 0) / 60:.1f} 分钟",
            "",
        ]
        return write_markdown_report(lines, 'financial_pdf_sync_report',
                                     log_label='财报PDF采集报告', ts=ts)
