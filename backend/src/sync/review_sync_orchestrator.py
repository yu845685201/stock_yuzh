"""
盘后编排器 sync-review-data（方案 2.5，每日复盘数据采集总入口）

- 执行顺序固定（快源在前、依赖在后），单源 try/except 隔离，失败不阻塞后续源；
- **源间并发**（用户决策 2026-09-27）：独立源按波次并行采集（各源内部仍串行限速，
  不违反每源风控约束）；依赖源（ths_finance 依赖公告/披露、valuation_calc 依赖财报）
  顺序在后。parallel_workers 控制波内并发上限（review_sync.parallel_workers，默认 4）
- 前置校验：日K依赖源（factor/money_flow/valuation_calc）要求当日 his_kline_day
  已同步（行数 vs base_stock_info 股数），未同步则中止并提示先跑 sync-kline-day
  （分笔子集、涨跌幅筛选都依赖日K；不自动触发，与现有流程正交）
- **一键模式**（execute(include_kline=True)，供 sync-review-all 使用）：全市场日K当日
  增量作为"波1伪源"与不依赖日K的源并发执行；日K依赖源顺延到波2/波3，在日K完成后
  按其结果+行数阈值门控（未通过标"跳过"）；缺省源集在 weekly_refresh_weekday（0=周一）
  当天自动并入周频源 concept/unlock
- 风控急停：ReviewBlockedError → 该源冷却 sleep 后重试 1 次（可配置）→ 仍失败标"待补"
- 幂等重跑：upsert + 游标设计，晚间子集补抓（如 --sources=lhb,announcement）安全
- 汇总报告：源名/状态(成功|失败|待补|跳过)/行数/耗时/失败摘要 → data/reports/
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

logger = logging.getLogger(__name__)

# 默认每日盘后全流程（方案 2.5 顺序 + 行业分类日更——全量仅 ~150 请求、2s 限速约 4 分钟，
# 日更顺带覆盖当日新股的行业归属，避免新股最多缺席一周行业榜）
DEFAULT_SOURCES = [
    'index', 'factor', 'overseas', 'overseas_kline', 'valuation_idx', 'announcement',
    'disclosure', 'lhb', 'margin', 'ths_finance', 'money_flow', 'valuation_calc',
    'sentiment_metrics', 'industry', 'bj_kline', 'news',
    # 资金锚两源（B7）： runners/波次/labels 早已就绪，仅默认集漏配导致
    # sync-review-all 永不执行、--sources 显式指定报"未知源名"
    'hkex_connect', 'etf_scale',
]
# 周频任务（周末执行，--sources 显式指定；ths_l2=申万二级增量，N3 2026-10-04 挂载）
WEEKLY_SOURCES = ['concept', 'unlock', 'ths_l2']
ALL_SOURCES = DEFAULT_SOURCES + WEEKLY_SOURCES
# 依赖当日全市场日K的源
KLINE_DEPENDENT_SOURCES = {'factor', 'money_flow', 'valuation_calc', 'sentiment_metrics'}
# 一键模式内置的全市场日K伪源（仅 include_kline=True 时编入波次，不接受 --sources 指定）
KLINE_SOURCE = 'kline'
# 波次划分：波内并行，波间顺序（后者依赖前者产出）
SOURCE_WAVES = [
    ['index', 'factor', 'overseas', 'overseas_kline', 'valuation_idx', 'announcement',
     'disclosure', 'lhb', 'margin', 'money_flow', 'industry', 'concept', 'unlock',
     'hkex_connect', 'etf_scale', 'news', 'shares_xdxr',
     'ths_l2', 'bj_kline'],
    ['ths_finance'],          # 依赖 announcement/disclosure 的当日披露清单
    ['valuation_calc', 'sentiment_metrics'],   # 依赖 ths_finance 财报 + 日K
]

SOURCE_LABELS = {
    'kline': '全市场日K',
    'index': '指数日K', 'factor': '复权因子', 'overseas': '外围市场',
    'overseas_kline': '外围日K',
    'valuation_idx': '指数估值', 'announcement': '公告清单+业绩预告',
    'disclosure': '披露日历', 'lhb': '龙虎榜', 'margin': '两融',
    'ths_finance': 'THS财报', 'money_flow': '分笔资金流', 'valuation_calc': 'PE/PB自算',
    'hkex_connect': '互联互通(北向+南向)', 'etf_scale': 'ETF份额',
    'sentiment_metrics': '情绪指标日频',
    'industry': '行业分类', 'concept': '概念板块', 'unlock': '解禁日历',
    'news': '市场要闻(新浪7x24)',
    'shares_xdxr': '股本(XDXR)',
    'ths_l2': '申万二级行业',
    'bj_kline': '北交所日K',
}


class ReviewSyncOrchestrator:
    """每日复盘数据采集编排器（波次并行，单源隔离）"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        review = self.config.get('review_sync', {}) or {}
        self.cooldown_seconds = int(review.get('blocked_cooldown_seconds', 600))
        self.retry_once = bool(review.get('blocked_retry_once', True))
        self.parallel_workers = max(1, int(review.get('parallel_workers', 4)))
        self.parallel_enabled = bool(review.get('parallel_sources', True))

    def execute(self, sources: Optional[str] = None, as_of: Optional[str] = None,
                dry_run: bool = False, include_kline: bool = False) -> Dict[str, Any]:
        started = datetime.now()
        requested = self._parse_sources(sources)
        as_of_date = self._parse_as_of(as_of)
        as_of_str = as_of_date.strftime('%Y%m%d')

        result: Dict[str, Any] = {
            'success': False, 'as_of': as_of_str, 'sources': requested, 'summary': [],
            'errors': [], 'kline_check': None, 'duration': 0.0, 'report_path': None,
        }

        if include_kline:
            # 一键模式：缺省源集在周频日自动并入 concept/unlock（显式 --sources 以显式为准）
            explicit_sources = bool(sources and sources.strip() != 'all')
            if not explicit_sources:
                weekly_weekday = int(
                    (self.config.get('review_sync', {}) or {}).get('weekly_refresh_weekday', 6))
                if as_of_date.weekday() == weekly_weekday:
                    requested = requested + [s for s in WEEKLY_SOURCES if s not in requested]
                    result['sources'] = requested
        else:
            kline_needed = bool(KLINE_DEPENDENT_SOURCES & set(requested))
            if kline_needed:
                ok, detail = self._check_kline_ready(as_of_str)
                result['kline_check'] = detail
                if not ok:
                    result['errors'].append(
                        f'当日全市场日K未同步（{detail}）。请先执行 sync-kline-day：'
                        f'分笔子集、涨跌幅筛选与估值自算都依赖日K。'
                        f'或使用 --sources 排除日K依赖源（如 index,overseas,announcement,lhb,margin）。')
                    result['report_path'] = self._write_report(result, started)
                    return result

        runners = self._build_runners(as_of_str)
        wave_requested = list(requested) + ([KLINE_SOURCE] if include_kline else [])
        summary_by_source, kline_gate_detail = self._run_waves(
            wave_requested, runners, dry_run,
            waves=self._build_waves(include_kline),
            include_kline=include_kline, as_of_str=as_of_str)
        if include_kline:
            result['kline_check'] = kline_gate_detail
        # 汇总按方案 2.5 固定顺序呈现（一键模式日K在最前）
        summary: List[Dict[str, Any]] = []
        if include_kline and KLINE_SOURCE in summary_by_source:
            summary.append(summary_by_source[KLINE_SOURCE])
        summary += [summary_by_source[s] for s in DEFAULT_SOURCES if s in summary_by_source]
        summary += [summary_by_source[s] for s in WEEKLY_SOURCES if s in summary_by_source]
        result['summary'] = summary

        result['success'] = any(e['status'] == '成功' for e in result['summary']) \
            and not all(e['status'] in ('失败', '跳过') for e in result['summary'])
        result['duration'] = (datetime.now() - started).total_seconds()
        result['report_path'] = self._write_report(result, started)
        return result

    # ---------- 波次执行 ----------

    def _run_source(self, name: str, runner: Callable[[bool], Dict[str, Any]],
                    dry_run: bool) -> Dict[str, Any]:
        """单源执行（含风控急停冷却重试一次）；异常在 entry 内收敛，不外抛"""
        label = SOURCE_LABELS.get(name, name)
        entry: Dict[str, Any] = {'source': name, 'label': label, 'status': '跳过',
                                 'rows': 0, 'duration': 0.0, 'detail': ''}
        src_start = time.time()
        try:
            src_result = runner(dry_run)
            if self._is_blocked(src_result) and self.retry_once:
                logger.warning(f'{label} 风控急停，冷却 {self.cooldown_seconds}s 后重试一次')
                time.sleep(self.cooldown_seconds)
                src_result = runner(dry_run)
            if self._is_blocked(src_result):
                entry['status'] = '待补'
                entry['detail'] = '风控急停（重试后仍失败）'
            else:
                self._fill_entry(entry, src_result)
        except Exception as e:
            logger.exception(f'{label} 执行异常: {e}')
            entry['status'] = '失败'
            entry['detail'] = str(e)
        entry['duration'] = round(time.time() - src_start, 1)
        logger.info(f'{label} -> {entry["status"]}（{entry["duration"]}s）')
        return entry

    def _run_waves(self, requested: List[str], runners: Dict[str, Callable[[bool], Dict[str, Any]]],
                   dry_run: bool, waves: Optional[List[List[str]]] = None,
                   include_kline: bool = False,
                   as_of_str: Optional[str] = None) -> tuple:
        """按波次执行：波内并行（parallel_workers 上限），波间顺序。

        一键模式（include_kline=True）：kline 所在波结束后评估日K门控，未通过则
        后续波中的日K依赖源标"跳过"（dry_run 不门控，预检清单全展开）。
        返回 (summary, kline_gate_detail)。
        """
        summary: Dict[str, Dict[str, Any]] = {}
        kline_gate_detail: Optional[str] = None
        kline_ok: Optional[bool] = None
        for wave in (waves or SOURCE_WAVES):
            names = [s for s in wave if s in requested]
            if not names:
                continue
            if kline_ok is False:
                gated = [s for s in names if s in KLINE_DEPENDENT_SOURCES]
                names = [s for s in names if s not in gated]
                for s in gated:
                    summary[s] = {'source': s, 'label': SOURCE_LABELS.get(s, s), 'status': '跳过',
                                  'rows': 0, 'duration': 0.0, 'detail': kline_gate_detail}
                if not names:
                    continue
            if self.parallel_enabled and len(names) > 1:
                logger.info(f'波次并行开始: {names}（并发上限 {self.parallel_workers}）')
                with ThreadPoolExecutor(max_workers=min(self.parallel_workers, len(names))) as executor:
                    futures = {executor.submit(self._run_source, s, runners[s], dry_run): s
                               for s in names if s in runners}
                    for future in as_completed(futures):
                        entry = future.result()
                        summary[entry['source']] = entry
                logger.info(f'波次并行完成: {names}')
            else:
                for s in names:
                    if s in runners:
                        entry = self._run_source(s, runners[s], dry_run)
                        summary[entry['source']] = entry
            if include_kline and KLINE_SOURCE in wave:
                kline_ok, kline_gate_detail = self._evaluate_kline_gate(
                    as_of_str, summary.get(KLINE_SOURCE), dry_run)
        # runner 未注册的源补"跳过"
        for s in requested:
            if s not in summary:
                summary[s] = {'source': s, 'label': SOURCE_LABELS.get(s, s), 'status': '跳过',
                              'rows': 0, 'duration': 0.0, 'detail': 'runner 未注册'}
        return summary, kline_gate_detail

    @staticmethod
    def _build_waves(include_kline: bool) -> List[List[str]]:
        """一键模式波次重排：日K编入波1与独立源并发，日K依赖源（factor/money_flow）
        顺延到波2（与依赖公告/披露清单的 ths_finance 同波），valuation_calc 仍在波3"""
        if not include_kline:
            return SOURCE_WAVES
        head = [s for s in SOURCE_WAVES[0] if s not in KLINE_DEPENDENT_SOURCES]
        tail = [s for s in SOURCE_WAVES[0] if s in KLINE_DEPENDENT_SOURCES]
        return [[KLINE_SOURCE] + head, tail + SOURCE_WAVES[1]] + list(SOURCE_WAVES[2:])

    # ---------- 各源 runner ----------

    def _build_runners(self, as_of: str) -> Dict[str, Callable[[bool], Dict[str, Any]]]:
        from .adjust_factor_manager import AdjustFactorManager
        from .announcement_manager import AnnouncementManager
        from .bj_kline_manager import BjKlineManager
        from .concept_manager import ConceptManager
        from .etf_scale_manager import EtfScaleManager
        from .hkex_connect_manager import HkexConnectManager
        from .news_telegraph_manager import NewsTelegraphManager
        from .disclosure_manager import DisclosureManager
        from .index_kline_manager import IndexKlineManager
        from .index_valuation_manager import IndexValuationManager
        from .industry_manager import IndustryManager
        from .lhb_manager import LhbManager
        from .margin_manager import MarginManager
        from .money_flow_manager import MoneyFlowManager
        from .overseas_kline_manager import OverseasKlineManager
        from .overseas_manager import OverseasManager
        from .sentiment_metrics_manager import SentimentMetricsManager
        from .ths_finance_manager import ThsFinanceManager
        from .ths_industry_l2_manager import ThsIndustryL2Manager
        from .unlock_manager import UnlockManager
        from .valuation_calc_manager import ValuationCalcManager
        from .xdxr_share_manager import XdxrShareManager

        cm = self.config_manager
        return {
            'kline': lambda dry: self._run_kline_sync(dry, as_of),
            'index': lambda dry: IndexKlineManager(cm).execute(dry_run=dry),
            'factor': lambda dry: AdjustFactorManager(cm).execute(trade_date=as_of, dry_run=dry),
            'overseas': lambda dry: OverseasManager(cm).execute(trade_date=as_of, dry_run=dry),
            # 外围日K（ifzq，技术方案 20261004）：对外围快照无依赖，波 1 并发，互拍在采集器内部
            'overseas_kline': lambda dry: OverseasKlineManager(cm).execute(dry_run=dry),
            'valuation_idx': lambda dry: IndexValuationManager(cm).execute(dry_run=dry),
            'announcement': lambda dry: AnnouncementManager(cm).execute(as_of=as_of, dry_run=dry),
            'disclosure': lambda dry: DisclosureManager(cm).execute(dry_run=dry),
            'lhb': lambda dry: LhbManager(cm).execute(trade_date=as_of, dry_run=dry),
            'margin': lambda dry: MarginManager(cm).execute(dry_run=dry),
            'ths_finance': lambda dry: ThsFinanceManager(cm).execute(as_of=as_of, dry_run=dry),
            'money_flow': lambda dry: MoneyFlowManager(cm).execute(trade_date=as_of, dry_run=dry),
            'valuation_calc': lambda dry: ValuationCalcManager(cm).execute(
                trade_date=as_of, dry_run=dry),
            # 开发项 B：缺省增量模式（最近 10 个交易日补齐，~40s；历史用 review backfill-sentiment）
            'sentiment_metrics': lambda dry: SentimentMetricsManager(cm).execute(dry_run=dry),
            'concept': lambda dry: ConceptManager(cm).execute(dry_run=dry),
            'industry': lambda dry: IndustryManager(cm).execute(dry_run=dry),
            'unlock': lambda dry: UnlockManager(cm).execute(dry_run=dry),
            'ths_l2': lambda dry: ThsIndustryL2Manager(cm).execute(dry_run=dry),
            'bj_kline': lambda dry: BjKlineManager(cm).execute(trade_date=as_of, dry_run=dry),
            # P3 互联互通与 ETF 份额（量价资金方案 V1.1；独立源非日K依赖，失败不阻塞）
            'hkex_connect': lambda dry: HkexConnectManager(cm).execute(),
            'etf_scale': lambda dry: EtfScaleManager(cm).execute(),
            # 市场要闻（四期方案 §4；独立源非日K依赖，失败不阻塞）
            'news': lambda dry: NewsTelegraphManager(cm).execute(dry_run=dry),
            # 股本刷新（baostock 下线换源 20261025：XDXR 全刷约 15 分钟，
            # resume=False 每次全量；先于 wave3 估值自算（fetch_total_shares_asof））
            'shares_xdxr': lambda dry: XdxrShareManager(cm).execute(resume=False, dry_run=dry),
        }

    # ---------- 一键模式：内置日K ----------

    def _run_kline_sync(self, dry_run: bool,
                        trade_date: Optional[str] = None) -> Dict[str, Any]:
        """全市场日K增量（sync-review-all 内置）：缺省当日，指定 trade_date 时回补该
        交易日；把 SyncManager 顶层返回（db_rows/records/failed_stocks）重塑为编排器
        源契约（stats.rows）"""
        if dry_run:
            return {'success': False, 'skipped_dry_run': True, 'stats': {'rows': 0},
                    'errors': [], 'blocked': False}
        from .sync_manager import SyncManager
        raw = SyncManager(self.config_manager).sync_kline_day(
            init_mode=False, start_date=trade_date, end_date=trade_date,
            save_to_csv=True, save_to_db=True)
        stats = {'rows': int(raw.get('db_rows') or raw.get('records') or 0)}
        errors = list(raw.get('errors') or [])
        failed = int(raw.get('failed_stocks') or 0)
        if failed:
            errors.append(f'{failed} 只个股同步失败（明细见日K报告）')
        return {'success': bool(raw.get('success')), 'stats': stats, 'errors': errors,
                'blocked': False, 'report_path': raw.get('report_path')}

    def _evaluate_kline_gate(self, as_of_str: Optional[str],
                             kline_entry: Optional[Dict[str, Any]],
                             dry_run: bool) -> tuple:
        """一键模式日K门控：kline 结果 + 行数阈值复检（防节假日/部分同步误放行）。
        dry_run 只记录不门控。返回 (ok, detail)。"""
        if dry_run:
            return True, 'dry-run：日K门控不生效'
        if kline_entry and kline_entry.get('status') != '成功':
            return False, (f'日K同步未成功（{kline_entry.get("detail") or kline_entry.get("status")}），'
                           f'日K依赖源跳过，可重跑本命令或先执行 sync-kline-day')
        ok, detail = self._check_kline_ready(as_of_str)
        if not ok:
            return False, f'日K行数校验未过（{detail}），日K依赖源跳过'
        return True, f'内置日K已同步：{detail}'

    # ---------- 工具 ----------

    @staticmethod
    def _parse_sources(sources: Optional[str]) -> List[str]:
        if not sources or sources.strip() == 'all':
            return list(DEFAULT_SOURCES)
        names = [s.strip() for s in sources.split(',') if s.strip()]
        unknown = [n for n in names if n not in ALL_SOURCES]
        if unknown:
            raise ValueError(f'未知源名: {unknown}（可用: {ALL_SOURCES}）')
        return names

    @staticmethod
    def _parse_as_of(as_of: Optional[str]) -> date:
        if not as_of:
            return date.today()
        return datetime.strptime(str(as_of).replace('-', '')[:8], '%Y%m%d').date()

    def _check_kline_ready(self, as_of: str) -> tuple:
        """当日 his_kline_day 是否已同步：与上一已同步交易日行数动态对比。

        基准取 base_stock_info 全量表会误判——全量表含长期停牌股（实测 5559 基数中
        347 只无当日数据，94% 触发误报，见问题记录 P-06）。现口径：当日行数 >3000
        且 ≥ 上一交易日实际行数的 80%。
        """
        try:
            kline_rows = self.db.execute_query(
                "SELECT COUNT(*) AS n FROM his_kline_day WHERE trade_date = %s", (as_of,))[0]['n']
            prev_rows = self.db.execute_query("""
                SELECT COUNT(*) AS n FROM his_kline_day
                WHERE trade_date = (SELECT MAX(trade_date) FROM his_kline_day WHERE trade_date < %s)
            """, (as_of,))[0]['n']
            detail = f'当日K行数 {kline_rows}，上一已同步交易日 {prev_rows}'
            ok = kline_rows > 3000 and prev_rows > 0 and kline_rows >= prev_rows * 0.8
            return ok, detail
        except Exception as e:
            return False, f'日K前置校验失败: {e}'

    @staticmethod
    def _is_blocked(src_result: Any) -> bool:
        return isinstance(src_result, dict) and bool(src_result.get('blocked'))

    @staticmethod
    def _fill_entry(entry: Dict[str, Any], src_result: Any) -> None:
        if not isinstance(src_result, dict):
            entry['status'] = '失败'
            entry['detail'] = '无效返回值'
            return
        if src_result.get('skipped_dry_run'):
            entry['status'] = '跳过'
            entry['detail'] = 'dry-run 跳过日K同步'
            return
        if src_result.get('skipped_disabled'):
            entry['status'] = '待补'
            entry['detail'] = '; '.join(src_result.get('errors') or []) or '配置停用'
            return
        if src_result.get('success'):
            entry['status'] = '成功'
        elif src_result.get('errors'):
            entry['status'] = '失败'
        else:
            # 无 errors 但不成功：区分"真无数据"与"个股级失败被清空"（见问题记录 P-09）
            failed_n = len((src_result.get('stats') or {}).get('failed') or [])
            entry['status'] = '失败' if failed_n else '待补'
        stats = src_result.get('stats') or {}
        entry['rows'] = stats.get('rows') or stats.get('records') or 0
        detail_errors = '; '.join((src_result.get('errors') or [])[:2])
        failed_n = len((stats.get('failed') or []))
        if failed_n and not detail_errors:
            entry['detail'] = f'{failed_n} 只失败（明细见该源报告/manifest）'
        else:
            entry['detail'] = detail_errors
        if src_result.get('report_path'):
            entry['report_path'] = src_result['report_path']

    def _write_report(self, result: Dict[str, Any], started: datetime) -> Optional[str]:
        lines = [
            f"- 数据日期：{result['as_of']}；全程耗时 {result.get('duration', 0) / 60:.1f} 分钟",
        ]
        if result.get('kline_check'):
            lines.append(f"- 日K前置校验：{result['kline_check']}")
        lines.append('')
        lines.append('| 源 | 状态 | 行数 | 耗时(s) | 摘要 |')
        lines.append('|---|---|---|---|---|')
        for e in result['summary']:
            lines.append(f"| {e['label']} | {e['status']} | {e['rows']} | "
                         f"{e['duration']} | {e['detail'] or '-'} |")
        if result['errors']:
            lines.append('')
            for err in result['errors']:
                lines.append(f"- 错误：{err}")
        return write_review_report('每日复盘数据采集编排报告', 'review_orchestrator_report',
                                   '完成', lines, result.get('duration'))
