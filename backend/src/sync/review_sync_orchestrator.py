"""
盘后编排器 sync-review-data（方案 2.5，每日复盘数据采集总入口）

- **依赖驱动调度**（2026-11-01，替代波次 barrier）：每源声明依赖（SOURCE_DEPENDENCIES），
  依赖完成后立即提交线程池（parallel_workers 槽位）——消除"长尾源阻塞无依赖源"的
  波级等待。典型：ths_finance 只依赖公告/披露（~4 分钟），不必等全市场日K（~15 分钟），
  两者并行；factor/money_flow 等 kline 完成即启动，不与 THS 串行。各源内部仍按各自
  限速执行，不违反每源风控约束（用户决策 2026-09-27 语义保留）。
- 单源 try/except 隔离，失败不阻塞后续源；
- 前置校验：日K依赖源（factor/money_flow/valuation_calc）要求当日 his_kline_day
  已同步（行数 vs base_stock_info 股数），未同步则中止并提示先跑 sync-kline-day
  （分笔子集、涨跌幅筛选都依赖日K；不自动触发，与现有流程正交）
- **一键模式**（execute(include_kline=True)，供 sync-review-all 使用）：全市场日K当日
  增量作为内置伪源参与依赖调度；kline 完成后按其结果+行数阈值门控（未通过则日K
  依赖源标"跳过"）；缺省源集在 weekly_refresh_weekday（0=周一）当天自动并入周频源
  concept/unlock
- 风控急停：ReviewBlockedError → 该源冷却 sleep 后重试 1 次（可配置）→ 仍失败标"待补"
- 幂等重跑：upsert + 游标设计，晚间子集补抓（如 --sources=lhb,announcement）安全
- 汇总报告：源名/状态(成功|失败|待补|跳过)/行数/耗时/失败摘要 → data/reports/
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait as wait_futures
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

# D01（每日复盘采集与分析闭环技术方案 V1.0 §3/§6.1）：作业/分区记账与源期望
from .collection_contracts import (
    PartitionStatus,
    RunContext,
    RunMode,
    entry_to_partition_result,
    new_id,
)
from .source_expectations import DATASET_REGISTRY, expected_partition

logger = logging.getLogger(__name__)

# 默认每日盘后全流程（方案 2.5 顺序 + 行业分类日更——全量仅 ~150 请求、2s 限速约 4 分钟，
# 日更顺带覆盖当日新股的行业归属，避免新股最多缺席一周行业榜）。列表序同时是依赖
# 调度的提交优先序（就绪扫描按此序，先提交快源腾槽）。
DEFAULT_SOURCES = [
    'index', 'factor', 'overseas', 'overseas_kline', 'valuation_idx', 'announcement',
    'disclosure', 'lhb', 'margin', 'ths_finance', 'money_flow', 'valuation_calc',
    'sentiment_metrics', 'industry', 'bj_kline',     'news',
    # 资金锚两源（B7）： runners/波次/labels 早已就绪，仅默认集漏配导致
    # sync-review-all 永不执行、--sources 显式指定报"未知源名"
    'hkex_connect', 'etf_scale',
    # F15（方案 V1.0 §F15，2026-11-18 用户拍板）：macro（~20 请求/日）与 option
    # （日频，T 日盘后）入盘后编排，消除 26 天/2 日静默停更；north_flow_detail
    # 源侧 2024-08 停披露，不入编排（F1 巡检豁免 + CLI 标注退役）
    'macro', 'option',
]
# 周频任务（周末执行，--sources 显式指定；ths_l2=申万二级增量，N3 2026-10-04 挂载）
WEEKLY_SOURCES = ['concept', 'unlock', 'ths_l2']
ALL_SOURCES = DEFAULT_SOURCES + WEEKLY_SOURCES
# 依赖当日全市场日K的源（kline gate 未通过时整类标"跳过"）
KLINE_DEPENDENT_SOURCES = {'factor', 'money_flow', 'valuation_calc', 'sentiment_metrics'}
# F4（数据采集链路风险修复方案 V1.0）：依赖 tdx-api 中间件的源——请求集命中任一
# 即在调度前做中间件健康预检（/api/health），失败直接返回并明示 Docker 容器
TDX_DEPENDENT_SOURCES = {'kline', 'index', 'factor', 'money_flow', 'sentiment_metrics',
                         'valuation_calc', 'shares_xdxr'}
# 一键模式内置的全市场日K伪源（仅 include_kline=True 时参与调度，不接受 --sources 指定）
KLINE_SOURCE = 'kline'

# 源依赖声明（2026-11-01 依赖驱动调度）：
# - ths_finance 只等 announcement/disclosure（其当日披露清单的来源），与日K并行；
# - kline 可选等待 shares_xdxr：一键模式显式编入股本时维持"股本先于日K"约束
#   （除权/增发当日生效的股本须在日K入库读股本映射前落库，否则事件股当日
#   turnover_rate 用前一日股本）；股本不在请求集时不等待；
# - 不在本次请求集内的依赖视为外部已满足（单源补跑语义，日K前置校验兜底）。
# - 交易所系源串行化（2026-11-17）：disclosure→lhb→margin 链式依赖，同一时刻
#   仅一个流请求交易所官网域（三源共用同一出口 IP，同波并发曾触发 403 风控
#   急停：2026-11-17 19:02 披露日历翻页突发打热 IP，龙虎榜/两融首请求即 403）。
#   依赖语义"完成即放行（无论成败）"（见 _run_parallel 调度说明），故披露日历
#   失败不阻塞龙虎榜/两融，仅保证不与它们并发打同一域。
SOURCE_DEPENDENCIES = {
    'ths_finance': {'announcement', 'disclosure'},
    'kline': {'shares_xdxr'},
    'factor': {'kline'},
    'money_flow': {'kline'},
    'valuation_calc': {'kline', 'ths_finance'},
    'sentiment_metrics': {'kline', 'ths_finance'},
    'lhb': {'disclosure'},
    'margin': {'lhb'},
    # F13（方案 V1.0 §F13）：etf_scale 走 query.sse.com.cn+szse.cn，与交易所域三源
    # 同域串行（排在 margin 之后，避免并发触发风控）
    'etf_scale': {'margin'},
}
# 兼容保留（历史波次结构，仅作文档与旧测试参照，不再参与调度）
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
    'macro': '宏观指标（F15 入编排）',
    'option': 'ETF期权（F15 入编排）',
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
        started = datetime.now(timezone.utc)
        requested = self._parse_sources(sources)
        as_of_date = self._parse_as_of(as_of)
        as_of_str = as_of_date.strftime('%Y%m%d')

        result: Dict[str, Any] = {
            'success': False, 'as_of': as_of_str, 'sources': requested, 'summary': [],
            'errors': [], 'kline_check': None, 'duration': 0.0, 'report_path': None,
            # D01 作业记账（技术方案 §6.1：编排结果输出 job_id 与分层状态）
            'job_id': None, 'execution_status': None, 'data_readiness': None,
        }

        # T10（技术方案 §6.1）：调度启动前检查依赖图循环，删除死锁时强行提交
        # 任务绕过依赖的做法（原"防意外死锁强提"分支随本次改造移除）
        cycles = self._check_dependency_cycles(SOURCE_DEPENDENCIES)
        if cycles:
            result['errors'].append(
                f'源依赖声明存在循环依赖，调度拒绝启动: {cycles}（请修正 SOURCE_DEPENDENCIES）')
            result['report_path'] = self._write_report(result, started)
            return result

        # D01：运行上下文（目标业务日与运行模式解耦的第一步——显式 as_of 记为
        # BACKFILL，缺省 DAILY；runner 侧行为解耦属 D04/D05 范畴）
        ctx = RunContext(
            job_id=new_id(),
            target_date=as_of_str,
            run_mode=RunMode.BACKFILL if as_of else RunMode.DAILY,
            cutoff_at=started,
            dry_run=dry_run,
        )
        result['job_id'] = ctx.job_id

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

        # F4（方案 V1.0 §F4）：中间件健康预检——请求集含 tdx 依赖源时统一预检一次，
        # 失败直接返回（11-17 教训：中间件宕机时各任务空跑全程，697 只静默漏采）。
        # base_url 未配置时跳过（未配置环境下由各 manager 既有 connect 行为兜底）。
        if not dry_run and bool((self.config.get('review_sync', {}) or {})
                                .get('middleware_health_check_enabled', True)):
            tdx_needed = include_kline or bool(set(requested) & TDX_DEPENDENT_SOURCES)
            if tdx_needed:
                from ..data_sources.tdx_api_source import middleware_health_check
                base_url = ((self.config_manager.get('data_sources.tdx_api', {}) or {})
                            .get('base_url', ''))
                if base_url:
                    ok, msg = middleware_health_check(base_url)
                    if not ok:
                        result['errors'].append(
                            f'中间件健康预检失败: {msg}（请确认 Docker 容器 tdx-stock-web 已启动，'
                            f'Docker Desktop 已运行）')
                        result['report_path'] = self._write_report(result, started)
                        return result
                    logger.info(f'中间件健康预检通过: {base_url}')

        wave_requested = list(requested) + ([KLINE_SOURCE] if include_kline else [])
        # D01：作业记账开始（调度前；dry-run 不写任何控制表——T11 零业务写入）
        if not dry_run:
            self._account_job_start(ctx, requested)
        summary_by_source, kline_gate_detail = self._run_waves(
            wave_requested, runners, dry_run,
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
        result['duration'] = (datetime.now(timezone.utc) - started).total_seconds()

        # D01：作业记账收尾——逐源 PartitionResult 写 sync_partition + finish_job
        # （§6.1：NOT_DUE 不算执行失败；execution_status 完整=0/失败=1/部分=3，
        # CLI 退出码接线属 D05）
        execution_status, data_readiness = self._derive_execution_status(summary)
        result['execution_status'] = execution_status
        result['data_readiness'] = data_readiness
        if not dry_run:
            self._account_job_finish(ctx, summary, execution_status, data_readiness,
                                     result)

        # F1（方案 V1.0 §F1）：数据新鲜度巡检挂载——编排末尾自动跑，红灯进
        # result['freshness_red'] 并入汇总报告（不改变各源自身状态）
        result['freshness_red'] = []
        if not dry_run and bool((self.config.get('review_sync', {}) or {})
                                .get('freshness_check_enabled', True)):
            try:
                from .freshness_check_manager import FreshnessCheckManager
                fr = FreshnessCheckManager(self.config_manager).execute()
                result['freshness_red'] = [r['table'] for r in fr.get('red', [])]
                result['freshness_red_detail'] = fr.get('red', [])
                result['freshness_report_path'] = fr.get('report_path')
                if fr.get('red'):
                    logger.warning(
                        "新鲜度巡检红灯: "
                        + ', '.join(f"{r['label']}({r['table']}) max={r.get('max_date')}"
                                    for r in fr['red']))
            except Exception as e:
                logger.warning(f"数据新鲜度巡检异常（不影响编排结果）: {e}")

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
        """依赖驱动调度（2026-11-01，替代波次 barrier）：每源声明依赖（SOURCE_DEPENDENCIES），
        依赖源完成后立即提交线程池（parallel_workers 槽位）——长尾源（日K/THS）不阻塞
        无依赖的快源，也不被无关长尾阻塞。

        契约（与波次模型逐项对齐）：
        - kline gate：kline 完成后立即评估（结果+行数阈值），未通过则日K依赖源标"跳过"
          （dry_run 不门控）；
        - 依赖源"完成即放行"（无论成败）：announcement 失败不阻塞 ths_finance——与原
          波次行为一致（披露清单查库为空时 THS 空跑成功，公告缺口由晚间补跑覆盖）；
        - 不在本次请求集内的依赖视为外部已满足（单源补跑语义）；
        - 单源隔离/风控重试（_run_source）、runner 未注册补"跳过"，均不变。
        waves 参数保留以兼容旧调用签名，调度不再使用。
        返回 (summary, kline_gate_detail)。
        """
        summary: Dict[str, Dict[str, Any]] = {}
        kline_gate_detail: Optional[str] = None
        scope = set(requested) | ({KLINE_SOURCE} if include_kline else set())

        def effective_deps(name: str) -> set:
            if dry_run:
                return set()
            return SOURCE_DEPENDENCIES.get(name, set()) & scope

        pending_order = [s for s in requested if s in runners]
        pending_set = set(pending_order)
        handled: set = set()
        running: Dict[Any, str] = {}

        # parallel_sources=false：单 worker 跑同一依赖调度——执行序退化为拓扑序
        # （依赖仍被尊重，factor 不会先于 kline）
        max_workers = self.parallel_workers if self.parallel_enabled else 1

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while pending_set or running:
                progressed = False
                if running:
                    done, _ = wait_futures(set(running), timeout=0.5,
                                           return_when=FIRST_COMPLETED)
                    for future in done:
                        name = running.pop(future)
                        entry = future.result()
                        summary[name] = entry
                        handled.add(name)
                        progressed = True
                        if include_kline and name == KLINE_SOURCE:
                            kline_ok, kline_gate_detail = self._evaluate_kline_gate(
                                as_of_str, entry, dry_run)
                            if kline_ok is False:
                                gated = [s for s in pending_set
                                         if s in KLINE_DEPENDENT_SOURCES]
                                for s in gated:
                                    pending_set.discard(s)
                                    handled.add(s)
                                    summary[s] = {
                                        'source': s, 'label': SOURCE_LABELS.get(s, s),
                                        'status': '跳过', 'rows': 0, 'duration': 0.0,
                                        'detail': kline_gate_detail}
                ready = [s for s in pending_order if s in pending_set
                         and effective_deps(s) <= handled]
                # 受槽位约束提交（提交数-完成数 ≤ workers）：保证提交序=执行序，
                # 后续才就绪的高优任务（如公告完成后立即就绪的 ths_finance）不会被
                # 早前批量提交的队列压到队尾
                for s in ready:
                    if len(running) >= max_workers:
                        break
                    pending_set.discard(s)
                    deps_txt = '、'.join(sorted(effective_deps(s))) or '无'
                    logger.info(f'源提交: {SOURCE_LABELS.get(s, s)}（依赖: {deps_txt}）')
                    running[executor.submit(self._run_source, s, runners[s], dry_run)] = s
                    progressed = True
                if not pending_set and not running:
                    break
                if not progressed and not running:
                    # T10（技术方案 §6.1）：依赖无法满足（循环依赖或依赖源未注册）时
                    # 显式失败，不再强行提交绕过依赖（原"防意外死锁强提"分支已删除，
                    # 循环依赖在 execute() 启动前已拦截；此处兜底依赖源 runner 未注册）
                    stuck = [s for s in pending_order if s in pending_set]
                    for s in stuck:
                        pending_set.discard(s)
                        dep_txt = '、'.join(sorted(effective_deps(s))) or '无'
                        summary[s] = {
                            'source': s, 'label': SOURCE_LABELS.get(s, s),
                            'status': '失败', 'rows': 0, 'duration': 0.0,
                            'detail': f'依赖无法满足: {dep_txt}（循环依赖或依赖源未注册）'}
                    logger.error(f'依赖无法满足，以下源显式失败（不绕过依赖）: {stuck}')
        # runner 未注册的源补"跳过"
        for s in requested:
            if s not in summary:
                summary[s] = {'source': s, 'label': SOURCE_LABELS.get(s, s), 'status': '跳过',
                              'rows': 0, 'duration': 0.0, 'detail': 'runner 未注册'}
        return summary, kline_gate_detail

    # ---------- 各源 runner ----------

    def _build_runners(self, as_of: str) -> Dict[str, Callable[[bool], Dict[str, Any]]]:
        from .adjust_factor_manager import AdjustFactorManager
        from .announcement_manager import AnnouncementManager
        from .bj_kline_manager import BjKlineManager
        from .concept_manager import ConceptManager
        from .etf_scale_manager import EtfScaleManager
        from .hkex_connect_manager import HkexConnectManager
        from .macro_manager import MacroManager
        from .news_telegraph_manager import NewsTelegraphManager
        from .disclosure_manager import DisclosureManager
        from .index_kline_manager import IndexKlineManager
        from .index_valuation_manager import IndexValuationManager
        from .industry_manager import IndustryManager
        from .lhb_manager import LhbManager
        from .margin_manager import MarginManager
        from .money_flow_manager import MoneyFlowManager
        from .option_manager import OptionManager
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
            # F15（方案 V1.0 §F15，2026-11-18 用户拍板）：macro/option 入盘后编排，
            # 消除静默停更（macro 曾停 26 天、option 停 2 个交易日无人知）
            'macro': lambda dry: MacroManager(cm).execute(dry_run=dry),
            'option': lambda dry: OptionManager(cm).execute(dry_run=dry),
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

    # ---------- D01 作业记账与依赖检查 ----------

    @staticmethod
    def _check_dependency_cycles(deps: Dict[str, set]) -> List[str]:
        """源依赖图循环检测（T10）：DFS 三色标记，返回环描述（无环返回空）"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in deps}
        stack: List[str] = []
        cycles: List[str] = []

        def dfs(node: str) -> None:
            color[node] = GRAY
            stack.append(node)
            for nxt in sorted(deps.get(node, set())):
                if nxt not in deps:
                    continue  # 依赖声明外的源视为外部已满足（单源补跑语义）
                if color.get(nxt, WHITE) == GRAY:
                    ring = stack[stack.index(nxt):] + [nxt]
                    cycles.append(' → '.join(ring))
                elif color.get(nxt, WHITE) == WHITE:
                    dfs(nxt)
            stack.pop()
            color[node] = BLACK

        for n in sorted(deps):
            if color.get(n) == WHITE:
                dfs(n)
        return cycles

    def _account_job_start(self, ctx: RunContext, requested: List[str]) -> None:
        """创建 sync_job（RUNNING）。记账失败只告警，不影响采集主流程"""
        try:
            self.db.ensure_collection_tables()
            with self.db.get_connection() as conn:
                self.db.create_job_tx(
                    conn, ctx.job_id, ctx.target_date, ctx.run_mode.value,
                    cutoff_at=ctx.cutoff_at, requested_sources=requested)
                conn.commit()
        except Exception as e:
            logger.warning(f'采集作业记账启动失败（不影响采集）: {e}')

    def _derive_execution_status(self, summary: List[Dict[str, Any]]) -> tuple:
        """分层状态推导（技术方案 §6.1：完整执行 0 / 失败 1 / 部分完成 3）。

        NOT_DUE 类"跳过"不算失败；NOT_APPLICABLE（未注册等）不进应到分母。
        返回 (execution_status: '0'|'1'|'3', data_readiness: {source: status})。"""
        data_readiness: Dict[str, str] = {}
        hard_bad = 0
        effective = 0
        for e in summary:
            name = e['source']
            status_raw = e['status']
            if status_raw == '成功':
                pr_status = PartitionStatus.COMPLETE if (e.get('rows') or 0) > 0 \
                    else PartitionStatus.CONFIRMED_EMPTY
            elif status_raw == '失败':
                pr_status = PartitionStatus.FAILED
            elif status_raw == '待补':
                pr_status = PartitionStatus.PARTIAL
            else:  # 跳过
                detail_l = e.get('detail') or ''
                pr_status = PartitionStatus.BLOCKED if ('日K' in detail_l or '门控' in detail_l
                                                        or '依赖' in detail_l) \
                    else PartitionStatus.NOT_APPLICABLE
            data_readiness[name] = pr_status.value
            if pr_status in (PartitionStatus.COMPLETE, PartitionStatus.CONFIRMED_EMPTY):
                effective += 1
            if pr_status in (PartitionStatus.FAILED, PartitionStatus.BLOCKED):
                hard_bad += 1
        # §6.1 退出码语义：完整执行 0（无失败且无部分）、失败 1（无完整成功源）、
        # 部分完成 3（有完整成功源但也存在失败/部分）
        if effective == 0:
            execution_status = '1'
        elif hard_bad or any(v == PartitionStatus.PARTIAL.value for v in data_readiness.values()):
            execution_status = '3'
        else:
            execution_status = '0'
        return execution_status, data_readiness

    def _account_job_finish(self, ctx: RunContext, summary: List[Dict[str, Any]],
                            execution_status: str,
                            data_readiness: Dict[str, str],
                            result: Dict[str, Any]) -> None:
        """逐源写 sync_partition + finish_job（单事务；失败只告警）"""
        if not ctx.job_id:
            return
        try:
            partition_rows = []
            for e in summary:
                name = e['source']
                spec = DATASET_REGISTRY.get(name)
                market = spec.market if spec else 'CN'
                exchange = spec.exchanges[0] if (spec and len(spec.exchanges) == 1) else None
                expected_date = ctx.target_date
                try:  # 应到日按源期望计算（两融 T-1 等）；查日历失败回退目标日
                    exp = expected_partition(ctx, name, db=self.db, config=self.config)
                    if exp.get('expected_date'):
                        expected_date = exp['expected_date']
                except Exception:
                    pass
                pr = entry_to_partition_result(
                    e, dataset=name, market=market, target_date=expected_date,
                    source=(spec.source if spec else name), exchange=exchange)
                pr.job_id = ctx.job_id
                partition_rows.append(pr.to_row(ctx.job_id))
            job_status = {'0': 'SUCCEEDED', '3': 'PARTIAL', '1': 'FAILED'} \
                .get(execution_status, 'FAILED')
            with self.db.get_connection() as conn:
                for row in partition_rows:
                    self.db.upsert_partition_tx(conn, row)
                self.db.finish_job_tx(
                    conn, ctx.job_id, job_status, execution_status=execution_status,
                    data_readiness=data_readiness,
                    result_summary={
                        'duration': result.get('duration'),
                        'success': bool(result.get('success')),
                        'freshness_red': result.get('freshness_red') or [],
                        'kline_check': result.get('kline_check'),
                    })
                conn.commit()
            logger.info(f'采集作业记账完成: job_id={ctx.job_id} status={job_status} '
                        f'partitions={len(partition_rows)}')
        except Exception as e:
            logger.warning(f'采集作业记账收尾失败（不影响采集结果）: {e}')

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

        F9（数据采集链路风险修复方案 V1.0）：阈值参数化并增加绝对差约束——11-17 实测
        4873/5571=87.5% 通过旧 80% 阈值，697 只静默漏采向下游扩散（资金流子集缩小）。
        新口径：rows > min_rows 且 rows ≥ prev×min_ratio 且 |rows-prev| ≤ max_abs_diff。
        大面积真实停牌日可能误拦——门控失败文案附处置提示（--sources 排除或临时调阈值）。
        配置：review_sync.kline_gate_min_rows（默认 3000）/ kline_gate_min_ratio（默认
        0.95）/ kline_gate_max_abs_diff（默认 300）。
        """
        try:
            review_cfg = (self.config.get('review_sync', {}) or {})
            min_rows = int(review_cfg.get('kline_gate_min_rows', 3000) or 3000)
            min_ratio = float(review_cfg.get('kline_gate_min_ratio', 0.95) or 0.95)
            max_abs_diff = int(review_cfg.get('kline_gate_max_abs_diff', 300) or 300)
            kline_rows = self.db.execute_query(
                "SELECT COUNT(*) AS n FROM his_kline_day WHERE trade_date = %s", (as_of,))[0]['n']
            prev_rows = self.db.execute_query("""
                SELECT COUNT(*) AS n FROM his_kline_day
                WHERE trade_date = (SELECT MAX(trade_date) FROM his_kline_day WHERE trade_date < %s)
            """, (as_of,))[0]['n']
            detail = f'当日K行数 {kline_rows}，上一已同步交易日 {prev_rows}（阈值: >{min_rows} 且 ≥{min_ratio:.0%} 且 |差|≤{max_abs_diff}）'
            ok = (kline_rows > min_rows and prev_rows > 0
                  and kline_rows >= prev_rows * min_ratio
                  and abs(kline_rows - prev_rows) <= max_abs_diff)
            if not ok:
                detail += ('（门控未过：若为大面积真实停牌/缩量日，可用 --sources 排除日K依赖源'
                           '或临时调高 review_sync.kline_gate_* 阈值）')
            return ok, detail
        except Exception as e:
            return False, f'日K前置校验失败: {e}'

    @staticmethod
    def _is_blocked(src_result: Any) -> bool:
        return isinstance(src_result, dict) and bool(src_result.get('blocked'))

    def _fill_entry(self, entry: Dict[str, Any], src_result: Any) -> None:
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

        # F2（数据采集链路风险修复方案 V1.0 §F2）："成功 ≠ 有当日数据"——源自报
        # stats.data_date_max（本次入库最大数据日），落后于最近已收盘交易日（基准日，
        # P-11 口径）则由"成功"降为"待补（数据日滞后 N 天）"。
        # 开关 review_sync.data_date_check_enabled（默认 true）。覆盖源：index_valuation/
        # adjust_factor/etf_scale/hkex_connect/option（其余源可渐进补 stats.data_date_max）。
        data_date_max = stats.get('data_date_max')
        if entry['status'] == '成功' and data_date_max:
            try:
                if bool((self.config.get('review_sync', {}) or {})
                        .get('data_date_check_enabled', True)):
                    from .review_common import latest_closed_trade_date
                    base_compact = latest_closed_trade_date(self.db).replace('-', '')
                    if str(data_date_max) < base_compact:
                        lag = (int(base_compact[:4]) - int(str(data_date_max)[:4])) * 371 \
                            + (int(base_compact[4:6]) - int(str(data_date_max)[4:6])) * 31 \
                            + (int(base_compact[6:8]) - int(str(data_date_max)[6:8]))
                        entry['status'] = '待补'
                        entry['detail'] = (f"数据日滞后：入库最大 {data_date_max} < "
                                           f"基准交易日 {base_compact}（源成功但无当日数据）")
                        entry['data_lag_days'] = lag
            except Exception as e:
                logger.warning(f"F2 数据日校验异常（按成功处理）: {e}")

        detail_errors = '; '.join((src_result.get('errors') or [])[:2])
        failed_n = len((stats.get('failed') or []))
        # D01（技术方案 §6.1）：警告、错误等 detail 为累积列表语义——此前末段赋值
        # 会覆盖 F2 已写入的"数据日滞后"原因（问题确认：detail_errors 为空且无
        # 个股失败时 detail 被清空），改为追加不覆盖
        extra: List[str] = []
        if failed_n and not detail_errors:
            extra.append(f'{failed_n} 只失败（明细见该源报告/manifest）')
        elif detail_errors:
            extra.append(detail_errors)
        if extra:
            existing = entry['detail'] or ''
            entry['detail'] = '；'.join([d for d in (existing, *extra) if d])
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
        if result.get('freshness_red'):
            lines.append('')
            lines.append(f"**数据新鲜度红灯（{len(result['freshness_red'])} 项）**："
                         + ', '.join(result['freshness_red'])
                         + f"——详见 {result.get('freshness_report_path', 'freshness_check_report')}")
        if result['errors']:
            lines.append('')
            for err in result['errors']:
                lines.append(f"- 错误：{err}")
        return write_review_report('每日复盘数据采集编排报告', 'review_orchestrator_report',
                                   '完成', lines, result.get('duration'))
