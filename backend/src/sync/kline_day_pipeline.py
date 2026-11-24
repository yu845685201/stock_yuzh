"""
日K线同步单股处理管线（R-07c 自 sync_manager.sync_kline_day 逐字移动拆出）

- KlineDaySharedState：原闭包 nonlocal/共享可变状态（计时、span、检测统计、异常样本、result 引用）
- KlineDayDeps：原闭包捕获的外部依赖（数据源/写者/DB/回调，显式注入）
- process_one_stock / safe_process_one_stock / merge_stock_result：原 process_stock /
  _safe_process_stock / merge_stock_result 三个闭包的模块级版本

"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import threading
import time

from ..data_sources.tdx_api_source import TdxApiUnavailableError


class RawFallbackBudget:
    """单次同步 raw 兜底股票数上限（线程安全，一股一额度；方案 V1.0 §3.5）"""

    def __init__(self, max_stocks: int):
        self._remaining = max(0, int(max_stocks))
        self._lock = threading.Lock()

    def try_acquire(self) -> bool:
        with self._lock:
            if self._remaining <= 0:
                return False
            self._remaining -= 1
            return True

    @property
    def exhausted(self) -> bool:
        with self._lock:
            return self._remaining <= 0


@dataclass
class KlineDaySharedState:
    """sync_kline_day 跨单股共享的可累加状态（原 nonlocal 变量组）"""

    result: Dict[str, Any]
    detection: Dict[str, int] = field(default_factory=lambda: {'checked': 0, 'hits': 0, 'refetched': 0})
    detection_details: List[Dict[str, Any]] = field(default_factory=list)
    source_missing: Dict[str, int] = field(default_factory=lambda: {'qfq_empty': 0, 'raw_empty': 0})
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    # ---- 批次1（数据采集链路耗时优化技术方案 V1.1 §3.1）：单股失败分类 ----
    # 失败明细（ts_code/请求阶段/错误类别/原始错误摘要），上限保护防异常日放大
    stock_failures: List[Dict[str, Any]] = field(default_factory=list)
    # 失败分类计数：'biz'=业务错误（中间件可达但查询失败）、'conn'=连接重试耗尽、
    # 'other'=其余异常（NameError 类历史 bug 兜底，修复后应恒为 0）
    failure_counts: Dict[str, int] = field(default_factory=lambda: {'biz': 0, 'conn': 0, 'other': 0})
    # 已提交处理（含失败）的股票集合——缺口对账用：目标范围 - submitted = 取消/未提交
    submitted_stocks: set = field(default_factory=set)
    # ---- 批次4（§3.2）：分组熔断观察模式 ----
    # key = f"{qfq|raw}|{sh|sz|bj}"，value = {'ok': n, 'biz': n, 'conn': n, 'other': n}
    # 观察模式只记录不暂停：报告输出"本应暂停的分组、后续实际成功数"，用于校准阈值
    group_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)
    # 当日检出除权（qfq_rebase 命中并整股重拉）的 ts_code 清单：
    # sync 完成后由调用方落 tmp/adjust_trigger_YYYYMMDD.json，供复权因子增量同步使用（方案 4.2）
    xdxr_hit_codes: List[str] = field(default_factory=list)
    # raw 兜底计数（方案 V1.0）：merge 层兜底写入/除权跳过；恢复阶段计数由 _recover_raw_fallback 直写 result
    fallback_written: int = 0
    fallback_skipped_exrights: int = 0
    # F5 整批熔断（数据采集链路风险修复方案 V1.0）：连续 conn 类失败计数，
    # 达阈值置 circuit_tripped；处理成功归零。业务类（biz）错误不累计（ chronic BJ qfq）。
    circuit_lock: Any = field(default_factory=threading.Lock)
    consecutive_conn_errors: int = 0
    circuit_tripped: bool = False
    circuit_error: str = ''
    api_time: float = 0.0
    csv_time: float = 0.0
    db_time: float = 0.0
    api_span: Optional[Tuple[float, float]] = None
    csv_span: Optional[Tuple[float, float]] = None
    db_span: Optional[Tuple[float, float]] = None


@dataclass
class KlineDayDeps:
    """单股处理所需的外部依赖（原闭包捕获值，显式注入）"""

    tdx_api_source: Any
    csv_writer: Any
    db_conn: Any
    logger: Any
    fundamentals_map: Dict[str, Dict[str, Any]]
    trade_dates: Optional[List[str]]
    init_mode: bool
    is_explicit_range: bool
    save_to_csv: bool
    save_to_db: bool
    fetch_full: bool
    tail_limit: int
    total_stocks: int
    log_progress: Callable
    merge_sources: Callable
    detect_refetch: Callable
    filter_raw: Callable
    normalize_records: Callable
    raw_fallback_budget: Any = None
    circuit_breaker_threshold: int = 20


def _bump_group(group_stats: Dict[str, Dict[str, int]], key: str, outcome: str) -> None:
    """观察模式组统计累加（§3.2：同接口、同市场错误集中；只记录不暂停）"""
    bucket = group_stats.setdefault(key, {'ok': 0, 'biz': 0, 'conn': 0, 'other': 0})
    bucket[outcome] = bucket.get(outcome, 0) + 1


def _tdx_unavailable_result(ts_code: str, e: 'TdxApiUnavailableError', stage: str,
                            ctx: 'KlineDaySharedState', deps: 'KlineDayDeps',
                            local_source_missing: Dict[str, int], fetch_start: float,
                            market_seg: str) -> Dict[str, Any]:
    """TdxApiUnavailableError 统一失败出口（批次1，方案 V1.1 §3.1）：

    - 错误类别（biz/conn）分类计数 + 明细留痕（ts_code/请求阶段/类别/原始摘要）
    - conn 类喂熔断计数（F5 语义不变）
    - 观察模式组统计（qfq/raw × 市场）
    返回单股失败结果（含 ts_code/error_stage/error_kind）。"""
    err_kind = e.err_type if e.err_type in ('biz', 'conn') else 'other'
    with ctx.circuit_lock:
        ctx.failure_counts[err_kind] = ctx.failure_counts.get(err_kind, 0) + 1
        if len(ctx.stock_failures) < 700:
            ctx.stock_failures.append({
                'ts_code': ts_code,
                'stage': stage,
                'kind': err_kind,
                'error': str(e),
            })
        _bump_group(ctx.group_stats, f'{stage.split("_")[0]}|{market_seg}', err_kind)
        if e.err_type == 'conn':
            ctx.consecutive_conn_errors += 1
            if (not ctx.circuit_tripped
                    and ctx.consecutive_conn_errors >= deps.circuit_breaker_threshold):
                ctx.circuit_tripped = True
                ctx.circuit_error = (f"连续 {ctx.consecutive_conn_errors} 只连接失败，"
                                     f"整批熔断（中间件疑似不可用）: {e}")
    if ctx.circuit_tripped:
        deps.logger.error(ctx.circuit_error)
    return {
        'ts_code': ts_code,
        'records': 0, 'db_rows': 0,
        'fallback_written': 0, 'fallback_skipped_exrights': 0,
        'api_time': time.time() - fetch_start, 'csv_time': 0.0, 'db_time': 0.0,
        'api_span': (fetch_start, time.time()),
        'csv_span': None, 'db_span': None,
        'anomalies': [], 'detection': {'checked': 0, 'hits': 0, 'refetched': 0},
        'detection_details': [], 'source_missing': local_source_missing,
        'error': f'{stage} {err_kind}: {e}',
        'error_stage': stage, 'error_kind': err_kind,
        'circuit_tripped': ctx.circuit_tripped,
    }


def process_one_stock(
    stock_item: Dict[str, Any],
    stock_index: int,
    *,
    ctx: KlineDaySharedState,
    deps: KlineDayDeps
) -> Dict[str, Any]:
    ts_code = stock_item.get('ts_code')
    stock_code = stock_item.get('stock_code')
    stock_name = stock_item.get('stock_name') or ''
    # API 代码用带市场前缀格式（SH600000/SZ000001）：裸 6 位代码在 tdx-api 服务端
    # 走另一路服务器解析，节后首两日数据可能不全（2026-10-24 实测 6409 vs 6411 条）
    api_code = ts_code.replace('.', '').upper() if ts_code else stock_code
    # 批次1（方案 V1.1 §3.1）：local_source_missing 初始化移到两路 API 请求之前
    # ——修复历史 bug：原在请求后才初始化，异常分支引用未定义变量触发 NameError，
    # 单股真实错误被掩盖为"单股日K采集失败: name 'local_source_missing' is not defined"
    local_source_missing = {'qfq_empty': 0, 'raw_empty': 0}
    # 观察模式分组市场段（§3.2：同接口、同市场错误集中观察）
    market_seg = ts_code.split('.')[0].lower() if ts_code and '.' in ts_code else 'unknown'
    if not ts_code or not stock_code:
        return {
            'records': 0,
            'db_rows': 0,
            'api_time': 0.0,
            'csv_time': 0.0,
            'db_time': 0.0,
            'api_span': None,
            'csv_span': None,
            'db_span': None,
            'anomalies': []
        }

    per_stock_records: List[Dict[str, Any]] = []
    raw_for_csv: List[Dict[str, Any]] = []
    fallback_ledger: List[Dict[str, Any]] = []
    skipped_ledger: List[Dict[str, Any]] = []
    local_api_time = 0.0
    local_csv_time = 0.0
    local_db_time = 0.0
    local_anomalies: List[Dict[str, Any]] = []
    api_start_wall = None
    api_end_wall = None
    csv_start_wall = None
    csv_end_wall = None
    db_start_wall = None
    db_end_wall = None

    # ---- 双源拉取与合并（init/区间=双全量；日常=双尾部）----
    merged_records: List[Dict[str, Any]] = []
    raw_skips: List[Dict[str, Any]] = []
    refetched = False
    stock_detection_details: List[Dict[str, Any]] = []
    fetch_start = time.time()
    if api_start_wall is None:
        api_start_wall = fetch_start

    # 批次1（方案 V1.1 §3.1）：qfq/raw 两路请求分别标注请求阶段（qfq/raw、tail/full），
    # 抛错时单股失败结果携带 ts_code/请求阶段/错误类别/原始错误摘要
    qfq_stage = 'qfq_full' if deps.fetch_full else 'qfq_tail'
    raw_stage = 'raw_full' if deps.fetch_full else 'raw_tail'
    try:
        qfq_list = (deps.tdx_api_source.get_kline_qfq_full(api_code)
                    if deps.fetch_full
                    else deps.tdx_api_source.get_kline_qfq_tail(api_code, deps.tail_limit))
    except TdxApiUnavailableError as e:
        return _tdx_unavailable_result(ts_code, e, qfq_stage, ctx, deps,
                                       local_source_missing, fetch_start, market_seg)
    try:
        raw_list = (deps.tdx_api_source.get_kline_raw_full(api_code)
                    if deps.fetch_full
                    else deps.tdx_api_source.get_kline_raw_tail(api_code, deps.tail_limit))
    except TdxApiUnavailableError as e:
        return _tdx_unavailable_result(ts_code, e, raw_stage, ctx, deps,
                                       local_source_missing, fetch_start, market_seg)
    api_end_wall = time.time()
    local_api_time += api_end_wall - fetch_start
    with ctx.circuit_lock:
        _bump_group(ctx.group_stats, f'qfq|{market_seg}', 'ok')
        _bump_group(ctx.group_stats, f'raw|{market_seg}', 'ok')
    deps.log_progress('数据采集', stock_index, deps.total_stocks, stock_code, stock_name, api_end_wall - fetch_start)

    if not qfq_list:
        local_source_missing['qfq_empty'] = 1
    if not raw_list:
        local_source_missing['raw_empty'] = 1

    merged_records, raw_skips = deps.merge_sources(qfq_list, raw_list, deps.trade_dates, ts_code, deps.raw_fallback_budget)
    # F5：本股拉取成功 → 连接失败计数归零
    with ctx.circuit_lock:
        ctx.consecutive_conn_errors = 0

    # ---- 日常增量的跨快照除权检测（命中→整股重拉自愈）----
    # 仅日常增量执行：显式区间补齐不做整股重拉，避免触发全量回源
    stock_detection = {'checked': 0, 'hits': 0, 'refetched': 0}
    if not deps.fetch_full and not deps.is_explicit_range and merged_records:
        hit, detail = deps.detect_refetch(ts_code, merged_records)
        stock_detection['checked'] = 1
        if hit:
            stock_detection['hits'] = 1
            stock_detection_details.append({'ts_code': ts_code, **detail})
            deps.logger.warning(f"除权/修订检测命中，整股重拉: {ts_code} {detail}")
            refetch_start = time.time()
            # 增强路径下必须 refresh=True 绕过缓存回源，否则从同一份失效缓存取回同样错位的数据
            qfq_list = deps.tdx_api_source.get_kline_qfq_full(api_code, refresh=True)
            raw_list = deps.tdx_api_source.get_kline_raw_full(api_code)
            local_api_time += time.time() - refetch_start
            merged_records, raw_skips = deps.merge_sources(qfq_list, raw_list, deps.trade_dates, ts_code, deps.raw_fallback_budget)
            stock_detection['refetched'] = 1
            refetched = True
    elif deps.is_explicit_range and merged_records and not deps.fetch_full:
        # F11④（数据采集链路风险修复方案 V1.0）：显式区间补齐跨除权日不整股重拉
        # （避免全量回源），但对重叠窗口执行同款接缝检测——命中仅告警，提示可人工
        # `sync-kline-day --init --codes <该股>` 彻底重拉
        try:
            hit, detail = deps.detect_refetch(ts_code, merged_records)
            stock_detection['checked'] = 1
            if hit:
                stock_detection['hits'] = 1
                detail['explicit_range_warning'] = True
                stock_detection_details.append({'ts_code': ts_code, **detail})
                deps.logger.warning(
                    f"显式区间接缝告警（F11④，不整股重拉）: {ts_code} {detail}"
                    f"——如需彻底修复请对该股执行 --init 重拉")
        except Exception as e:
            deps.logger.warning(f"显式区间接缝检测异常: {ts_code} {e}")

    if not merged_records:
        # F11②③（数据采集链路风险修复方案 V1.0）：双源同空不再纯静默——
        # 库内近 10 自然日有数据的"突兀空"与 raw 源空各记单股异常样本（仅报告可见，
        # 不触发重拉；strict 模式下中间件不可用已在上方抛错，此处空=源真无数据）
        empty_anomalies: List[Dict[str, Any]] = []
        try:
            recent = deps.db_conn.execute_query(
                "SELECT MAX(trade_date) AS d FROM his_kline_day WHERE ts_code = %s", (ts_code,))
            last_d = (recent[0].get('d') if recent else None)
            if last_d:
                from datetime import date as _date, datetime as _dt
                s = str(last_d).replace('-', '')
                if len(s) == 8:
                    age = (_dt.now().date()
                           - _date(int(s[:4]), int(s[4:6]), int(s[6:8]))).days
                    if age <= 10:
                        empty_anomalies.append({
                            'ts_code': ts_code, 'type': 'merged_empty_with_history',
                            'trade_date': s,
                            'note': f'双源同空但库内 {age} 天前有数据（疑源端异常）'})
        except Exception:
            pass
        if local_source_missing.get('raw_empty') and len(empty_anomalies) < 200:
            empty_anomalies.append({
                'ts_code': ts_code, 'type': 'raw_source_empty',
                'note': 'raw 源空（F11③：raw_revision 检测本轮失效，仅报告可见）'})
        return {
            'ts_code': ts_code,
            'records': 0,
            'db_rows': 0,
            'api_time': local_api_time,
            'csv_time': local_csv_time,
            'db_time': local_db_time,
            'api_span': (api_start_wall, api_end_wall) if api_start_wall and api_end_wall else None,
            'csv_span': None,
            'db_span': None,
            'anomalies': empty_anomalies,
            'detection': stock_detection,
            'detection_details': stock_detection_details,
            'source_missing': local_source_missing,
            'error': None
        }

    # ---- CSV（合并后的原始口径，按允许日期过滤）----
    if deps.save_to_csv:
        for item in merged_records:
            if isinstance(item, dict):
                item_with_code = {'ts_code': ts_code}
                item_with_code.update(item)
                raw_for_csv.append(item_with_code)
        csv_start = time.time()
        if csv_start_wall is None:
            csv_start_wall = csv_start
        filtered_raw = deps.filter_raw(raw_for_csv, deps.trade_dates)
        if filtered_raw:
            deps.csv_writer.write_his_kline_day_raw(ts_code, filtered_raw)
        csv_end_wall = time.time()
        local_csv_time += csv_end_wall - csv_start
        deps.log_progress('csv 生成', stock_index, deps.total_stocks, stock_code, stock_name, csv_end_wall - csv_start)

    # ---- normalize（preclose 链/change_rate/基本面匹配/turnover）----
    allowed_dates = set(deps.trade_dates) if (deps.trade_dates and not deps.init_mode) else None
    if deps.init_mode or refetched:
        # 全量口径：整股替换写库，不做日期过滤
        records, record_anomalies = deps.normalize_records(
            merged_records, stock_item, deps.fundamentals_map
        )
    else:
        records, record_anomalies = deps.normalize_records(
            merged_records, stock_item, deps.fundamentals_map, allowed_dates=allowed_dates
        )
    for record in records:
        record['adjust_flag'] = 2  # 前复权
    per_stock_records.extend(records)
    local_anomalies.extend(record_anomalies)

    # ---- 写库：init/重拉=整股原子替换；区间/日常=upsert ----
    if per_stock_records and deps.save_to_db:
        db_start = time.time()
        if db_start_wall is None:
            db_start_wall = db_start
        if deps.init_mode or refetched:
            db_rows = deps.db_conn.replace_his_kline_day(ts_code, per_stock_records)
        else:
            db_rows = deps.db_conn.upsert_his_kline_day(per_stock_records)
        db_end_wall = time.time()
        local_db_time += db_end_wall - db_start
        deps.log_progress('数据入库-', stock_index, deps.total_stocks, stock_code, stock_name, db_end_wall - db_start)

        # ---- raw 兜底台账（方案 V1.0 §3.3）：与数据写库同批落账；失败不影响已写数据 ----
        fallback_ledger = [
            {'ts_code': r['ts_code'], 'trade_date': r['trade_date'], 'status': 'fallback',
             'reason': 'qfq_missing', 'raw_close': r.get('raw_close')}
            for r in per_stock_records if r.get('source') == 'TDXAPI_RAWFB'
        ]
        skipped_ledger = [
            {'ts_code': ts_code, 'trade_date': s.get('trade_date'), 'status': 'skip_exrights',
             'reason': s.get('reason'), 'raw_close': s.get('raw_close')}
            for s in raw_skips
        ]
        if fallback_ledger or skipped_ledger:
            try:
                deps.db_conn.upsert_raw_fallback_records(fallback_ledger + skipped_ledger)
            except Exception as e:
                # 台账写失败只降级追踪能力（source 列标记仍在），不影响已入库数据
                deps.logger.warning(f"raw 兜底台账写入失败（不影响日K数据）: {ts_code} {e}")
    else:
        db_rows = 0

    return {
        'ts_code': ts_code,
        'records': len(per_stock_records),
        'db_rows': db_rows,
        'fallback_written': len(fallback_ledger),
        'fallback_skipped_exrights': len(skipped_ledger),
        'api_time': local_api_time,
        'csv_time': local_csv_time,
        'db_time': local_db_time,
        'api_span': (api_start_wall, api_end_wall) if api_start_wall and api_end_wall else None,
        'csv_span': (csv_start_wall, csv_end_wall) if csv_start_wall and csv_end_wall else None,
        'db_span': (db_start_wall, db_end_wall) if db_start_wall and db_end_wall else None,
        'anomalies': local_anomalies,
        'detection': stock_detection,
        'detection_details': stock_detection_details,
        'source_missing': local_source_missing,
        'error': None
    }


def safe_process_one_stock(
    stock_item: Dict[str, Any],
    stock_index: int,
    *,
    ctx: KlineDaySharedState,
    deps: KlineDayDeps
) -> Dict[str, Any]:
    """单股异常隔离：一只股票失败不影响整场同步"""
    try:
        return process_one_stock(stock_item, stock_index, ctx=ctx, deps=deps)
    except Exception as e:
        deps.logger.exception(f"单股日K采集失败: {stock_item.get('ts_code')} {e}")
        # 批次1（方案 V1.1 §3.1）：非 TdxApiUnavailableError 的异常归入 'other' 类别——
        # 修复 local_source_missing 未定义 bug 后本类别应恒为 0，非零即代码回归信号
        with ctx.circuit_lock:
            ctx.failure_counts['other'] = ctx.failure_counts.get('other', 0) + 1
            if len(ctx.stock_failures) < 700:
                ctx.stock_failures.append({
                    'ts_code': stock_item.get('ts_code'),
                    'stage': 'pipeline',
                    'kind': 'other',
                    'error': f'{type(e).__name__}: {e}',
                })
        return {
            'ts_code': stock_item.get('ts_code'),
            'records': 0, 'db_rows': 0, 'api_time': 0.0, 'csv_time': 0.0, 'db_time': 0.0,
            'api_span': None, 'csv_span': None, 'db_span': None,
            'anomalies': [], 'detection': {'checked': 0, 'hits': 0, 'refetched': 0},
            'detection_details': [], 'source_missing': {},
            'error': f"{stock_item.get('ts_code')}: {e}",
            'error_stage': 'pipeline', 'error_kind': 'other'
        }


def merge_stock_result(res: Dict[str, Any], ctx: KlineDaySharedState) -> None:
    result = ctx.result

    # 批次1：本股已提交处理（无论成败）——缺口对账：目标范围 - submitted = 取消/未提交
    ts_code = res.get('ts_code')
    if ts_code:
        with ctx.circuit_lock:
            ctx.submitted_stocks.add(ts_code)

    if res.get('error'):
        result['errors'].append(res['error'])
        result['failed_stocks'] += 1

    det = res.get('detection') or {}
    ctx.detection['checked'] += det.get('checked', 0)
    ctx.detection['hits'] += det.get('hits', 0)
    ctx.detection['refetched'] += det.get('refetched', 0)
    details = res.get('detection_details') or []
    if details:
        ctx.detection_details.extend(details[:200 - len(ctx.detection_details)])
    for d in details:
        # 仅 qfq_rebase（前复权重基=除权）记入触发清单；raw_revision 是源修订，与除权无关
        if d.get('type') == 'qfq_rebase' and d.get('ts_code') and d['ts_code'] not in ctx.xdxr_hit_codes:
            ctx.xdxr_hit_codes.append(d['ts_code'])
    for k, v in (res.get('source_missing') or {}).items():
        ctx.source_missing[k] = ctx.source_missing.get(k, 0) + v

    ctx.fallback_written += res.get('fallback_written', 0) or 0
    ctx.fallback_skipped_exrights += res.get('fallback_skipped_exrights', 0) or 0

    result['records'] += res['records']
    result['db_rows'] += res['db_rows']
    ctx.api_time += res['api_time']
    ctx.csv_time += res['csv_time']
    ctx.db_time += res['db_time']
    ctx.anomalies.extend(res['anomalies'])

    if res['api_span']:
        ctx.api_span = (
            min(ctx.api_span[0], res['api_span'][0]) if ctx.api_span else res['api_span'][0],
            max(ctx.api_span[1], res['api_span'][1]) if ctx.api_span else res['api_span'][1]
        )
    if res['csv_span']:
        ctx.csv_span = (
            min(ctx.csv_span[0], res['csv_span'][0]) if ctx.csv_span else res['csv_span'][0],
            max(ctx.csv_span[1], res['csv_span'][1]) if ctx.csv_span else res['csv_span'][1]
        )
    if res['db_span']:
        ctx.db_span = (
            min(ctx.db_span[0], res['db_span'][0]) if ctx.db_span else res['db_span'][0],
            max(ctx.db_span[1], res['db_span'][1]) if ctx.db_span else res['db_span'][1]
        )
