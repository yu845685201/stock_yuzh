"""
日K线同步单股处理管线（R-07c 自 sync_manager.sync_kline_day 逐字移动拆出）

- KlineDaySharedState：原闭包 nonlocal/共享可变状态（计时、span、检测统计、异常样本、result 引用）
- KlineDayDeps：原闭包捕获的外部依赖（数据源/写者/DB/回调，显式注入）
- process_one_stock / safe_process_one_stock / merge_stock_result：原 process_stock /
  _safe_process_stock / merge_stock_result 三个闭包的模块级版本

注意：本模块刻意不定义模块级 logger——process_one_stock 中除权检测命中分支的裸名
logger.warning 是单列 bug B-1（命中路径 NameError 后被 safe 包装吞掉），在其单独修复前
必须原样保留该行为。
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import time


@dataclass
class KlineDaySharedState:
    """sync_kline_day 跨单股共享的可累加状态（原 nonlocal 变量组）"""

    result: Dict[str, Any]
    detection: Dict[str, int] = field(default_factory=lambda: {'checked': 0, 'hits': 0, 'refetched': 0})
    detection_details: List[Dict[str, Any]] = field(default_factory=list)
    source_missing: Dict[str, int] = field(default_factory=lambda: {'qfq_empty': 0, 'raw_empty': 0})
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
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
    refetched = False
    stock_detection_details: List[Dict[str, Any]] = []
    fetch_start = time.time()
    if api_start_wall is None:
        api_start_wall = fetch_start

    if deps.fetch_full:
        qfq_list = deps.tdx_api_source.get_kline_qfq_full(stock_code)
        raw_list = deps.tdx_api_source.get_kline_raw_full(stock_code)
    else:
        qfq_list = deps.tdx_api_source.get_kline_qfq_tail(stock_code, deps.tail_limit)
        raw_list = deps.tdx_api_source.get_kline_raw_tail(stock_code, deps.tail_limit)
    api_end_wall = time.time()
    local_api_time += api_end_wall - fetch_start
    deps.log_progress('数据采集', stock_index, deps.total_stocks, stock_code, stock_name, api_end_wall - fetch_start)

    local_source_missing = {'qfq_empty': 0, 'raw_empty': 0}
    if not qfq_list:
        local_source_missing['qfq_empty'] = 1
    if not raw_list:
        local_source_missing['raw_empty'] = 1

    merged_records = deps.merge_sources(qfq_list, raw_list)

    # ---- 日常增量的跨快照除权检测（命中→整股重拉自愈）----
    # 仅日常增量执行：显式区间补齐不做整股重拉，避免触发全量回源
    stock_detection = {'checked': 0, 'hits': 0, 'refetched': 0}
    if not deps.fetch_full and not deps.is_explicit_range and merged_records:
        hit, detail = deps.detect_refetch(ts_code, merged_records)
        stock_detection['checked'] = 1
        if hit:
            stock_detection['hits'] = 1
            stock_detection_details.append({'ts_code': ts_code, **detail})
            logger.warning(f"除权/修订检测命中，整股重拉: {ts_code} {detail}")
            refetch_start = time.time()
            # 增强路径下必须 refresh=True 绕过缓存回源，否则从同一份失效缓存取回同样错位的数据
            qfq_list = deps.tdx_api_source.get_kline_qfq_full(stock_code, refresh=True)
            raw_list = deps.tdx_api_source.get_kline_raw_full(stock_code)
            local_api_time += time.time() - refetch_start
            merged_records = deps.merge_sources(qfq_list, raw_list)
            stock_detection['refetched'] = 1
            refetched = True

    if not merged_records:
        return {
            'records': 0,
            'db_rows': 0,
            'api_time': local_api_time,
            'csv_time': local_csv_time,
            'db_time': local_db_time,
            'api_span': (api_start_wall, api_end_wall) if api_start_wall and api_end_wall else None,
            'csv_span': None,
            'db_span': None,
            'anomalies': [],
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
    else:
        db_rows = 0

    return {
        'records': len(per_stock_records),
        'db_rows': db_rows,
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
        return {
            'records': 0, 'db_rows': 0, 'api_time': 0.0, 'csv_time': 0.0, 'db_time': 0.0,
            'api_span': None, 'csv_span': None, 'db_span': None,
            'anomalies': [], 'detection': {'checked': 0, 'hits': 0, 'refetched': 0},
            'detection_details': [], 'source_missing': {},
            'error': f"{stock_item.get('ts_code')}: {e}"
        }


def merge_stock_result(res: Dict[str, Any], ctx: KlineDaySharedState) -> None:
    result = ctx.result

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
    for k, v in (res.get('source_missing') or {}).items():
        ctx.source_missing[k] = ctx.source_missing.get(k, 0) + v

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
