"""
1分钟K线同步管线（R-12 自 usecase.py 拆出）

- Kline1MinSharedState：原 execute 闭包 nonlocal/共享可变状态
- Kline1MinDeps：原闭包捕获的外部依赖（ports/写者/回调，显式注入）
- process_one_stock / merge_stock_result：原 process_stock / merge_stock_result 闭包的模块级版本
- run_pipeline：原 Kline1MinSyncUseCase._run_pipeline 整体移动（write_queue 定义已提到 worker 闭包之前）

注意：本项不修 pipeline 路径 errors 丢失问题（merge_local 不合并 errors，bug B-3 单列），
原行为逐字保留。
"""

import threading
import time
from dataclasses import dataclass, field
from queue import Queue
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class Kline1MinSharedState:
    """sync_kline_1min 跨单股共享的可累加状态（原 nonlocal 变量组）"""

    result: Dict[str, Any]
    anomaly_limit: int
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    anomaly_omitted: int = 0
    api_time: float = 0.0
    csv_time: float = 0.0
    db_time: float = 0.0
    api_span: Optional[Tuple[float, float]] = None
    csv_span: Optional[Tuple[float, float]] = None
    db_span: Optional[Tuple[float, float]] = None


@dataclass
class Kline1MinDeps:
    """单股处理与管线所需的外部依赖（原闭包捕获值，显式注入）"""

    source: Any
    repository: Any
    partition_writer: Any
    csv_writer: Any
    normalizer: Any
    logger: Any
    config: Dict[str, Any]
    fundamentals_map: Dict[str, Dict[str, Any]]
    trade_dates: Optional[List[str]]
    init_mode: bool
    save_to_csv: bool
    save_to_db: bool
    total_stocks: int
    db_write_semaphore: threading.BoundedSemaphore
    log_progress: Callable
    filter_raw: Callable
    chunk_list: Callable
    record_failed_dates: Callable
    run_with_failure_capture: Callable


def process_one_stock(
    stock_item: Dict[str, Any],
    stock_index: int,
    *,
    ctx: Kline1MinSharedState,
    deps: Kline1MinDeps
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

    record_count = 0
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
    prev_preclose = None
    if not deps.init_mode:
        prev_preclose = deps.repository.fetch_last_close(ts_code)
    db_rows = 0

    def write_records_to_db(records: List[Dict[str, Any]]) -> None:
        nonlocal db_rows, local_db_time, db_start_wall, db_end_wall
        if not records or not deps.save_to_db:
            return

        with deps.db_write_semaphore:
            db_start = time.time()
            if db_start_wall is None:
                db_start_wall = db_start
            try:
                db_rows += deps.partition_writer.write_records(records)
            except Exception:
                deps.record_failed_dates([record.get('trade_date') for record in records])
                raise
            finally:
                db_end = time.time()

        local_db_time += db_end - db_start
        db_end_wall = db_end

    if deps.init_mode:
        api_start = time.time()
        if api_start_wall is None:
            api_start_wall = api_start
        raw = deps.run_with_failure_capture(
            lambda: deps.source.fetch_kline_all(stock_code, 'minute1'),
            deps.trade_dates or []
        )
        api_end_wall = time.time()
        local_api_time += api_end_wall - api_start
        deps.log_progress('数据采集', stock_index, deps.total_stocks, stock_code, stock_name, api_end_wall - api_start)

        if raw:
            if deps.save_to_csv:
                for item in raw:
                    if isinstance(item, dict):
                        item['ts_code'] = ts_code
                        raw_for_csv.append(item)
            prev_preclose = None
            records, record_anomalies = deps.normalizer.normalize_records(
                raw, stock_item, deps.fundamentals_map, prev_preclose
            )
            record_count += len(records)
            write_records_to_db(records)
            local_anomalies.extend(record_anomalies)
    else:
        for trade_date_chunk in deps.chunk_list(deps.trade_dates, 3):
            start_trade_date = trade_date_chunk[0]
            end_trade_date = trade_date_chunk[-1]
            api_start = time.time()
            if api_start_wall is None:
                api_start_wall = api_start
            raw = deps.run_with_failure_capture(
                lambda: deps.source.fetch_kline_range(
                    stock_code,
                    'minute1',
                    start_date=start_trade_date,
                    end_date=end_trade_date,
                    limit=800
                ),
                trade_date_chunk
            )
            api_end_wall = time.time()
            local_api_time += api_end_wall - api_start
            deps.log_progress('数据采集', stock_index, deps.total_stocks, stock_code, stock_name, api_end_wall - api_start)

            if not raw:
                continue

            if deps.save_to_csv:
                for item in raw:
                    if isinstance(item, dict):
                        item['ts_code'] = ts_code
                        raw_for_csv.append(item)

            allowed_dates = set(trade_date_chunk)
            records, record_anomalies = deps.normalizer.normalize_records(
                raw, stock_item, deps.fundamentals_map, prev_preclose, allowed_dates=allowed_dates
            )
            if records:
                prev_preclose = records[-1].get('close')
                record_count += len(records)
                write_records_to_db(records)
            local_anomalies.extend(record_anomalies)

    if record_count and deps.save_to_csv and raw_for_csv:
        csv_start = time.time()
        if csv_start_wall is None:
            csv_start_wall = csv_start
        filtered_raw = deps.filter_raw(raw_for_csv, deps.trade_dates)
        deps.csv_writer.enqueue_raw(ts_code, filtered_raw)
        csv_end_wall = time.time()
        local_csv_time += csv_end_wall - csv_start
        deps.log_progress('csv 生成', stock_index, deps.total_stocks, stock_code, stock_name, csv_end_wall - csv_start)

    if db_rows:
        deps.log_progress('数据入库-', stock_index, deps.total_stocks, stock_code, stock_name, local_db_time)

    return {
        'records': record_count,
        'db_rows': db_rows,
        'api_time': local_api_time,
        'csv_time': local_csv_time,
        'db_time': local_db_time,
        'api_span': (api_start_wall, api_end_wall) if api_start_wall and api_end_wall else None,
        'csv_span': (csv_start_wall, csv_end_wall) if csv_start_wall and csv_end_wall else None,
        'db_span': (db_start_wall, db_end_wall) if db_start_wall and db_end_wall else None,
        'anomalies': local_anomalies
    }


def merge_stock_result(res: Dict[str, Any], ctx: Kline1MinSharedState) -> None:
    result = ctx.result

    result['records'] += res.get('records', 0)
    result['db_rows'] += res.get('db_rows', 0)
    ctx.api_time += res.get('api_time', 0.0)
    ctx.csv_time += res.get('csv_time', 0.0)
    ctx.db_time += res.get('db_time', 0.0)

    if res.get('errors'):
        result['errors'].extend(res['errors'])

    if 'anomaly_omitted' in res:
        if ctx.anomaly_limit > 0:
            remain = ctx.anomaly_limit - len(ctx.anomalies)
            if remain > 0:
                ctx.anomalies.extend(res['anomalies'][:remain])
        else:
            ctx.anomalies.extend(res['anomalies'])
        ctx.anomaly_omitted += res.get('anomaly_omitted', 0)
    else:
        if ctx.anomaly_limit > 0:
            remain = ctx.anomaly_limit - len(ctx.anomalies)
            if remain > 0:
                ctx.anomalies.extend(res['anomalies'][:remain])
            omitted = len(res['anomalies']) - max(remain, 0)
            if omitted > 0:
                ctx.anomaly_omitted += omitted
        else:
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


def run_pipeline(
    deps: Kline1MinDeps,
    stocks: List[Dict[str, Any]],
    total_stocks: int,
    fundamentals_map: Dict[str, Dict[str, Any]],
    trade_dates: Optional[List[str]],
    save_to_csv: bool,
    save_to_db: bool,
    db_write_semaphore: threading.BoundedSemaphore,
    pipeline_queue_size: int,
    pipeline_fetch_workers: int,
    pipeline_normalize_workers: int,
    pipeline_write_workers: int,
    init_mode: bool,
    record_failed_dates: Callable[[List[Any]], None],
    run_with_failure_capture: Callable[[Callable[[], Optional[List[Dict[str, Any]]]], List[str]], List[Dict[str, Any]]]
) -> Dict[str, Any]:
    result = {
        'records': 0,
        'db_rows': 0,
        'api_time': 0.0,
        'csv_time': 0.0,
        'db_time': 0.0,
        'api_span': None,
        'csv_span': None,
        'db_span': None,
        'anomalies': [],
        'errors': []
    }

    fetch_queue: "Queue[Optional[Dict[str, Any]]]" = Queue(maxsize=pipeline_queue_size)
    normalize_queue: "Queue[Optional[Dict[str, Any]]]" = Queue(maxsize=pipeline_queue_size)
    write_queue: "Queue[Optional[Dict[str, Any]]]" = Queue(maxsize=pipeline_queue_size)

    anomalies: List[Dict[str, Any]] = []
    anomaly_limit = max(0, int(deps.config.get('sync.kline_anomaly_report_limit', 5000) or 0))
    anomaly_omitted = 0
    lock = threading.Lock()

    def merge_local(local: Dict[str, Any]) -> None:
        nonlocal anomaly_omitted
        with lock:
            result['records'] += local.get('records', 0)
            result['db_rows'] += local.get('db_rows', 0)
            result['api_time'] += local.get('api_time', 0.0)
            result['csv_time'] += local.get('csv_time', 0.0)
            result['db_time'] += local.get('db_time', 0.0)

            # B-3 修复：write_worker 捕获的 DB 异常原先只停留在 local['errors']，
            # merge_local 从不合并 → result['errors'] 为空，写库失败无感知
            local_errors = local.get('errors')
            if local_errors:
                result['errors'].extend(local_errors)

            if anomaly_limit > 0:
                remain = anomaly_limit - len(anomalies)
                local_anomalies = local.get('anomalies', [])
                if remain > 0:
                    anomalies.extend(local_anomalies[:remain])
                omitted = len(local_anomalies) - max(remain, 0)
                if omitted > 0:
                    anomaly_omitted += omitted
            else:
                anomalies.extend(local.get('anomalies', []))

            api_span = local.get('api_span')
            if api_span:
                if result['api_span']:
                    result['api_span'] = (min(result['api_span'][0], api_span[0]), max(result['api_span'][1], api_span[1]))
                else:
                    result['api_span'] = api_span
            csv_span = local.get('csv_span')
            if csv_span:
                if result['csv_span']:
                    result['csv_span'] = (min(result['csv_span'][0], csv_span[0]), max(result['csv_span'][1], csv_span[1]))
                else:
                    result['csv_span'] = csv_span
            db_span = local.get('db_span')
            if db_span:
                if result['db_span']:
                    result['db_span'] = (min(result['db_span'][0], db_span[0]), max(result['db_span'][1], db_span[1]))
                else:
                    result['db_span'] = db_span

    def fetch_worker(worker_id: int) -> None:
        while True:
            item = fetch_queue.get()
            if item is None:
                fetch_queue.task_done()
                break
            stock_item = item['stock']
            stock_index = item['index']
            ts_code = stock_item.get('ts_code')
            stock_code = stock_item.get('stock_code')
            stock_name = stock_item.get('stock_name') or ''
            local_api_start = time.time()
            local_api_end = local_api_start
            raw = None
            if ts_code and stock_code:
                raw = run_with_failure_capture(
                    lambda: deps.source.fetch_kline_all(stock_code, 'minute1'),
                    item.get('fallback_dates', [])
                )
            local_api_end = time.time()

            local = {
                'stock': stock_item,
                'index': stock_index,
                'stock_code': stock_code,
                'stock_name': stock_name,
                'raw': raw or [],
                'raw_for_csv': [],
                'api_time': local_api_end - local_api_start,
                'api_span': (local_api_start, local_api_end)
            }

            if raw and save_to_csv:
                raw_for_csv = []
                for item_raw in raw:
                    if isinstance(item_raw, dict):
                        item_raw['ts_code'] = ts_code
                        raw_for_csv.append(item_raw)
                local['raw_for_csv'] = raw_for_csv

            deps.log_progress('数据采集', stock_index, total_stocks, stock_code or '', stock_name, local_api_end - local_api_start)
            normalize_queue.put(local)
            fetch_queue.task_done()

    def normalize_worker() -> None:
        while True:
            item = normalize_queue.get()
            if item is None:
                normalize_queue.task_done()
                break

            stock_item = item['stock']
            raw = item.get('raw') or []
            ts_code = stock_item.get('ts_code')

            local = {
                'records': 0,
                'db_rows': 0,
                'api_time': item.get('api_time', 0.0),
                'csv_time': 0.0,
                'db_time': 0.0,
                'api_span': item.get('api_span'),
                'csv_span': None,
                'db_span': None,
                'anomalies': [],
                'errors': [],
                'index': item.get('index'),
                'stock_code': item.get('stock_code'),
                'stock_name': item.get('stock_name')
            }

            if raw and ts_code:
                prev_preclose = None
                if not init_mode:
                    prev_preclose = deps.repository.fetch_last_close(ts_code)
                records, record_anomalies = deps.normalizer.normalize_records(
                    raw, stock_item, fundamentals_map, prev_preclose
                )
                local['records'] = len(records)
                local['records_data'] = records
                local['anomalies'] = record_anomalies
            else:
                local['records_data'] = []

            raw_for_csv = item.get('raw_for_csv') or []
            if local['records'] and save_to_csv and raw_for_csv:
                csv_start = time.time()
                deps.csv_writer.enqueue_raw(ts_code, raw_for_csv)
                csv_end = time.time()
                local['csv_time'] = csv_end - csv_start
                local['csv_span'] = (csv_start, csv_end)

            write_queue_item = {
                'local': local,
                'records': local.get('records_data') or []
            }
            normalize_queue.task_done()
            write_queue.put(write_queue_item)

    def write_worker() -> None:
        while True:
            item = write_queue.get()
            if item is None:
                write_queue.task_done()
                break

            local = item['local']
            records = item['records']

            if records and save_to_db:
                with db_write_semaphore:
                    db_start = time.time()
                    try:
                        local['db_rows'] += deps.partition_writer.write_records(records)
                    except Exception as exc:
                        record_failed_dates([record.get('trade_date') for record in records])
                        local.setdefault('errors', []).append(str(exc))
                    finally:
                        db_end = time.time()
                        local['db_time'] += db_end - db_start
                        local['db_span'] = (db_start, db_end)

            if local.get('db_rows'):
                deps.log_progress('数据入库-', local.get('index', 0), total_stocks, local.get('stock_code', ''), local.get('stock_name', ''), local.get('db_time', 0.0))

            merge_local(local)
            write_queue.task_done()

    fetch_threads = []
    for idx in range(pipeline_fetch_workers):
        t = threading.Thread(target=fetch_worker, args=(idx,), daemon=True)
        t.start()
        fetch_threads.append(t)

    normalize_threads = []
    for _ in range(pipeline_normalize_workers):
        t = threading.Thread(target=normalize_worker, daemon=True)
        t.start()
        normalize_threads.append(t)

    write_threads = []
    for _ in range(pipeline_write_workers):
        t = threading.Thread(target=write_worker, daemon=True)
        t.start()
        write_threads.append(t)

    for idx, stock in enumerate(stocks, start=1):
        fetch_queue.put({'stock': stock, 'index': idx, 'fallback_dates': trade_dates or []})

    for _ in fetch_threads:
        fetch_queue.put(None)
    fetch_queue.join()

    for _ in normalize_threads:
        normalize_queue.put(None)
    normalize_queue.join()

    for _ in write_threads:
        write_queue.put(None)
    write_queue.join()

    for t in fetch_threads:
        t.join()
    for t in normalize_threads:
        t.join()
    for t in write_threads:
        t.join()

    result['anomalies'] = anomalies
    result['anomaly_omitted'] = anomaly_omitted
    return result
