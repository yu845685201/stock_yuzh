"""
1分钟K线同步用例编排
"""
import threading
import time
from datetime import date
from queue import Queue
from typing import Any, Callable, Dict, List, Optional, Tuple

from .domain import Kline1MinNormalizer
from .ports import (
    Kline1MinCsvPort,
    Kline1MinFundamentalsPort,
    Kline1MinRepositoryPort,
    Kline1MinPartitionPort,
    Kline1MinReportPort,
    Kline1MinSourcePort,
    Kline1MinStockPort,
    Kline1MinTradeCalendarPort
)
from .partition_writer import Kline1MinPartitionWriter


class Kline1MinSyncUseCase:
    def __init__(
        self,
        source: Kline1MinSourcePort,
        repository: Kline1MinRepositoryPort,
        partition_port: Kline1MinPartitionPort,
        csv_writer: Kline1MinCsvPort,
        report: Kline1MinReportPort,
        fundamentals: Kline1MinFundamentalsPort,
        trade_calendar: Kline1MinTradeCalendarPort,
        stock_port: Kline1MinStockPort,
        config: Dict[str, Any],
        logger
    ):
        self.source = source
        self.repository = repository
        self.partition_port = partition_port
        self.csv_writer = csv_writer
        self.report = report
        self.fundamentals = fundamentals
        self.trade_calendar = trade_calendar
        self.stock_port = stock_port
        self.config = config
        self.logger = logger
        self.normalizer = Kline1MinNormalizer(repository)
        self.partition_writer = Kline1MinPartitionWriter(partition_port, config, logger)

    def execute(
        self,
        init_mode: bool = False,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        save_to_csv: bool = True,
        save_to_db: bool = True
    ) -> Dict[str, Any]:
        result = {
            'success': False,
            'records': 0,
            'db_rows': 0,
            'errors': [],
            'report_path': None,
            'failed_dates': []
        }

        api_time = 0.0
        csv_time = 0.0
        db_time = 0.0
        merge_time = 0.0
        api_span: Optional[Tuple[float, float]] = None
        csv_span: Optional[Tuple[float, float]] = None
        db_span: Optional[Tuple[float, float]] = None
        merge_span: Optional[Tuple[float, float]] = None
        pipeline_span: Optional[Tuple[float, float]] = None
        pipeline_time = 0.0
        anomalies: List[Dict[str, Any]] = []
        anomaly_limit = max(0, int(self.config.get('sync.kline_anomaly_report_limit', 5000) or 0))
        anomaly_omitted = 0
        failed_dates: set = set()

        def normalize_failed_date(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, date):
                return value.strftime('%Y%m%d')
            s = str(value).strip()
            if not s:
                return None
            if '-' in s and len(s) >= 10:
                s = s[0:10].replace('-', '')
            if len(s) >= 8 and s[0:8].isdigit():
                return s[0:8]
            return None

        def record_failed_dates(values: List[Any]) -> None:
            if not values:
                return
            normalized = []
            for item in values:
                item_norm = normalize_failed_date(item)
                if item_norm:
                    normalized.append(item_norm)
            if not normalized:
                return
            failed_dates.update(normalized)

        start_ts = time.time()
        try:
            if not self.source.connect():
                raise RuntimeError('Tdx API连接失败')

            stocks = self.stock_port.fetch_stocks(ts_codes)
            if not stocks:
                result['success'] = True
                return result

            stock_ts_codes = [s['ts_code'] for s in stocks if s.get('ts_code')]

            trade_dates: Optional[List[str]] = None
            if not init_mode:
                if start_date and end_date:
                    trade_dates = self.trade_calendar.get_trade_dates(start_date, end_date)
                else:
                    today_str = date.today().strftime('%Y-%m-%d')
                    trade_dates = self.trade_calendar.get_trade_dates(today_str, today_str)

                if not trade_dates:
                    result['success'] = True
                    return result
            else:
                trade_dates = []

            fundamentals_map = self.fundamentals.build_fundamentals_map(
                init_mode=init_mode,
                ts_codes=stock_ts_codes,
                start_date=start_date,
                end_date=end_date
            )

            total_stocks = len(stocks)
            db_max_writers = max(1, int(self.config.get('sync.kline_1min_db_max_writers', 2) or 2))
            db_write_semaphore = threading.BoundedSemaphore(db_max_writers)

            pipeline_enabled = bool(self.config.get('sync.kline_1min_pipeline_enabled', True))
            pipeline_queue_size = max(100, int(self.config.get('sync.kline_1min_pipeline_queue_size', 200) or 200))
            pipeline_fetch_workers = max(1, int(self.config.get('sync.kline_1min_pipeline_fetch_workers', 4) or 4))
            pipeline_normalize_workers = max(1, int(self.config.get('sync.kline_1min_pipeline_normalize_workers', 4) or 4))
            pipeline_write_workers = max(1, int(self.config.get('sync.kline_1min_pipeline_write_workers', 2) or 2))

            def run_with_failure_capture(raw_loader: Callable[[], Optional[List[Dict[str, Any]]]], fallback_dates: List[str]) -> List[Dict[str, Any]]:
                try:
                    raw_data = raw_loader() or []
                except Exception:
                    record_failed_dates(fallback_dates)
                    raise
                if not raw_data:
                    record_failed_dates(fallback_dates)
                    return []
                return raw_data

            def process_stock(stock_item: Dict[str, Any], stock_index: int) -> Dict[str, Any]:
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
                if not init_mode:
                    prev_preclose = self.repository.fetch_last_close(ts_code)
                db_rows = 0

                def write_records_to_db(records: List[Dict[str, Any]]) -> None:
                    nonlocal db_rows, local_db_time, db_start_wall, db_end_wall
                    if not records or not save_to_db:
                        return

                    with db_write_semaphore:
                        db_start = time.time()
                        if db_start_wall is None:
                            db_start_wall = db_start
                        try:
                            db_rows += self.partition_writer.write_records(records)
                        except Exception:
                            record_failed_dates([record.get('trade_date') for record in records])
                            raise
                        finally:
                            db_end = time.time()

                    local_db_time += db_end - db_start
                    db_end_wall = db_end

                if init_mode:
                    api_start = time.time()
                    if api_start_wall is None:
                        api_start_wall = api_start
                    raw = run_with_failure_capture(
                        lambda: self.source.fetch_kline_all(stock_code, 'minute1'),
                        trade_dates or []
                    )
                    api_end_wall = time.time()
                    local_api_time += api_end_wall - api_start
                    self._log_progress('数据采集', stock_index, total_stocks, stock_code, stock_name, api_end_wall - api_start)

                    if raw:
                        if save_to_csv:
                            for item in raw:
                                if isinstance(item, dict):
                                    item['ts_code'] = ts_code
                                    raw_for_csv.append(item)
                        prev_preclose = None
                        records, record_anomalies = self.normalizer.normalize_records(
                            raw, stock_item, fundamentals_map, prev_preclose
                        )
                        record_count += len(records)
                        write_records_to_db(records)
                        local_anomalies.extend(record_anomalies)
                else:
                    for trade_date_chunk in self._chunk_list(trade_dates, 3):
                        start_trade_date = trade_date_chunk[0]
                        end_trade_date = trade_date_chunk[-1]
                        api_start = time.time()
                        if api_start_wall is None:
                            api_start_wall = api_start
                        raw = run_with_failure_capture(
                            lambda: self.source.fetch_kline_range(
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
                        self._log_progress('数据采集', stock_index, total_stocks, stock_code, stock_name, api_end_wall - api_start)

                        if not raw:
                            continue

                        if save_to_csv:
                            for item in raw:
                                if isinstance(item, dict):
                                    item['ts_code'] = ts_code
                                    raw_for_csv.append(item)

                        allowed_dates = set(trade_date_chunk)
                        records, record_anomalies = self.normalizer.normalize_records(
                            raw, stock_item, fundamentals_map, prev_preclose, allowed_dates=allowed_dates
                        )
                        if records:
                            prev_preclose = records[-1].get('close')
                            record_count += len(records)
                            write_records_to_db(records)
                        local_anomalies.extend(record_anomalies)

                if record_count and save_to_csv and raw_for_csv:
                    csv_start = time.time()
                    if csv_start_wall is None:
                        csv_start_wall = csv_start
                    filtered_raw = self._filter_raw_kline_by_dates(raw_for_csv, trade_dates)
                    self.csv_writer.enqueue_raw(ts_code, filtered_raw)
                    csv_end_wall = time.time()
                    local_csv_time += csv_end_wall - csv_start
                    self._log_progress('csv 生成', stock_index, total_stocks, stock_code, stock_name, csv_end_wall - csv_start)

                if db_rows:
                    self._log_progress('数据入库-', stock_index, total_stocks, stock_code, stock_name, local_db_time)

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

            def merge_stock_result(res: Dict[str, Any]) -> None:
                nonlocal api_time, csv_time, db_time, api_span, csv_span, db_span, anomaly_omitted

                result['records'] += res.get('records', 0)
                result['db_rows'] += res.get('db_rows', 0)
                api_time += res.get('api_time', 0.0)
                csv_time += res.get('csv_time', 0.0)
                db_time += res.get('db_time', 0.0)

                if res.get('errors'):
                    result['errors'].extend(res['errors'])

                if 'anomaly_omitted' in res:
                    if anomaly_limit > 0:
                        remain = anomaly_limit - len(anomalies)
                        if remain > 0:
                            anomalies.extend(res['anomalies'][:remain])
                    else:
                        anomalies.extend(res['anomalies'])
                    anomaly_omitted += res.get('anomaly_omitted', 0)
                else:
                    if anomaly_limit > 0:
                        remain = anomaly_limit - len(anomalies)
                        if remain > 0:
                            anomalies.extend(res['anomalies'][:remain])
                        omitted = len(res['anomalies']) - max(remain, 0)
                        if omitted > 0:
                            anomaly_omitted += omitted
                    else:
                        anomalies.extend(res['anomalies'])


                if res['api_span']:
                    api_span = (
                        min(api_span[0], res['api_span'][0]) if api_span else res['api_span'][0],
                        max(api_span[1], res['api_span'][1]) if api_span else res['api_span'][1]
                    )
                if res['csv_span']:
                    csv_span = (
                        min(csv_span[0], res['csv_span'][0]) if csv_span else res['csv_span'][0],
                        max(csv_span[1], res['csv_span'][1]) if csv_span else res['csv_span'][1]
                    )
                if res['db_span']:
                    db_span = (
                        min(db_span[0], res['db_span'][0]) if db_span else res['db_span'][0],
                        max(db_span[1], res['db_span'][1]) if db_span else res['db_span'][1]
                    )

            max_workers = self.config.get('sync.kline_max_workers', 1)
            raw_batch_size = self.config.get('sync.kline_stock_batch_size', 1000)
            try:
                stock_batch_size = max(1, int(raw_batch_size or 1000))
            except (TypeError, ValueError):
                self.logger.warning(f"kline_stock_batch_size配置无效({raw_batch_size})，已回退为1000")
                stock_batch_size = 1000
            total_batches = (total_stocks + stock_batch_size - 1) // stock_batch_size

            if pipeline_enabled and init_mode:
                pipeline_start = time.time()
                pipeline_span = (pipeline_start, pipeline_start)
                pipeline_result = self._run_pipeline(
                    stocks=stocks,
                    total_stocks=total_stocks,
                    fundamentals_map=fundamentals_map,
                    trade_dates=trade_dates,
                    save_to_csv=save_to_csv,
                    save_to_db=save_to_db,
                    db_write_semaphore=db_write_semaphore,
                    pipeline_queue_size=pipeline_queue_size,
                    pipeline_fetch_workers=pipeline_fetch_workers,
                    pipeline_normalize_workers=pipeline_normalize_workers,
                    pipeline_write_workers=pipeline_write_workers,
                    init_mode=init_mode,
                    record_failed_dates=record_failed_dates,
                    run_with_failure_capture=run_with_failure_capture
                )
                merge_stock_result(pipeline_result)
                pipeline_end = time.time()
                pipeline_span = (pipeline_start, pipeline_end)
                pipeline_time = pipeline_end - pipeline_start
            else:
                for batch_no, batch_start in enumerate(range(0, total_stocks, stock_batch_size), start=1):
                    stock_batch = stocks[batch_start:batch_start + stock_batch_size]
                    batch_end = batch_start + len(stock_batch)
                    self.logger.info(
                        f"1分钟K线批次开始: 第{batch_no}/{total_batches}批, 股票[{batch_start + 1}-{batch_end}]"
                    )

                    if max_workers and max_workers > 1 and len(stock_batch) > 1:
                        from concurrent.futures import ThreadPoolExecutor, as_completed
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = [
                                executor.submit(process_stock, s, batch_start + i + 1)
                                for i, s in enumerate(stock_batch)
                            ]
                            for future in as_completed(futures):
                                merge_stock_result(future.result())
                    else:
                        for i, stock in enumerate(stock_batch):
                            merge_stock_result(process_stock(stock, batch_start + i + 1))

                    self.logger.info(
                        f"1分钟K线批次完成: 第{batch_no}/{total_batches}批, 累计记录{result['records']}条, 累计写库{result['db_rows']}条"
                    )

            if save_to_csv:
                csv_flush_start = time.time()
                flush_stats = self.csv_writer.flush()
                csv_flush_end = time.time()
                csv_time += csv_flush_end - csv_flush_start
                if csv_span:
                    csv_span = (csv_span[0], max(csv_span[1], csv_flush_end))
                else:
                    csv_span = (csv_flush_start, csv_flush_end)
                if flush_stats and isinstance(flush_stats, dict):
                    result['errors'].extend(flush_stats.get('errors', []))

            result['success'] = True
        except Exception as e:
            result['errors'].append(str(e))
            print(f"同步1分钟K线失败: {e}")
        finally:
            self.source.disconnect()

        end_ts = time.time()
        api_time_val = round(api_time, 2)
        csv_time_val = round(csv_time, 2)
        db_time_val = round(db_time, 2)
        total_time_val = round(end_ts - start_ts, 2)
        db_wall_val = round(db_span[1] - db_span[0], 2) if db_span else 0
        db_time_per_record = round(db_time_val / result['records'], 6) if result.get('records', 0) > 0 else 0
        db_wall_ratio = round(db_wall_val / total_time_val, 6) if total_time_val > 0 else 0
        merge_wall_val = round(merge_span[1] - merge_span[0], 2) if merge_span else 0

        timing = {
            'api_time': api_time_val,
            'csv_time': csv_time_val,
            'db_time': db_time_val,
            'merge_time': round(merge_time, 2),
            'pipeline_time': round(pipeline_time, 2),
            'copy_time': 0,
            'copy_rows': 0,
            'copy_used': False,
            'partition_cleanup_mode': self.partition_writer.cleanup_mode,
            'total_time': total_time_val,
            'parallel': max_workers if 'max_workers' in locals() else self.config.get('sync.kline_max_workers', 1),
            'stock_batch_size': stock_batch_size if 'stock_batch_size' in locals() else self.config.get('sync.kline_stock_batch_size', 1000),
            'db_max_writers': db_max_writers if 'db_max_writers' in locals() else self.config.get('sync.kline_1min_db_max_writers', 2),
            'pipeline_enabled': pipeline_enabled if 'pipeline_enabled' in locals() else False,
            'pipeline_fetch_workers': pipeline_fetch_workers if 'pipeline_fetch_workers' in locals() else 0,
            'pipeline_normalize_workers': pipeline_normalize_workers if 'pipeline_normalize_workers' in locals() else 0,
            'pipeline_write_workers': pipeline_write_workers if 'pipeline_write_workers' in locals() else 0,
            'api_wall': round(api_span[1] - api_span[0], 2) if api_span else 0,
            'csv_wall': round(csv_span[1] - csv_span[0], 2) if csv_span else 0,
            'db_wall': db_wall_val,
            'pipeline_wall': round(pipeline_span[1] - pipeline_span[0], 2) if pipeline_span else 0,
            'merge_wall': merge_wall_val,
            'db_time_per_record': db_time_per_record,
            'db_wall_ratio': db_wall_ratio,
            'anomaly_limit': anomaly_limit,
            'anomaly_omitted': anomaly_omitted
        }

        result['duration'] = timing['total_time']
        if failed_dates:
            result['failed_dates'] = sorted(failed_dates)
        result['report_path'] = self.report.write_report(result, anomalies, timing)

        return result

    def _run_pipeline(
        self,
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
            'anomalies': []
        }

        fetch_queue: "Queue[Optional[Dict[str, Any]]]" = Queue(maxsize=pipeline_queue_size)
        normalize_queue: "Queue[Optional[Dict[str, Any]]]" = Queue(maxsize=pipeline_queue_size)

        anomalies: List[Dict[str, Any]] = []
        anomaly_limit = max(0, int(self.config.get('sync.kline_anomaly_report_limit', 5000) or 0))
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
                        lambda: self.source.fetch_kline_all(stock_code, 'minute1'),
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

                self._log_progress('数据采集', stock_index, total_stocks, stock_code or '', stock_name, local_api_end - local_api_start)
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
                        prev_preclose = self.repository.fetch_last_close(ts_code)
                    records, record_anomalies = self.normalizer.normalize_records(
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
                    self.csv_writer.enqueue_raw(ts_code, raw_for_csv)
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
                            local['db_rows'] += self.partition_writer.write_records(records)
                        except Exception as exc:
                            record_failed_dates([record.get('trade_date') for record in records])
                            local.setdefault('errors', []).append(str(exc))
                        finally:
                            db_end = time.time()
                            local['db_time'] += db_end - db_start
                            local['db_span'] = (db_start, db_end)

                if local.get('db_rows'):
                    self._log_progress('数据入库-', local.get('index', 0), total_stocks, local.get('stock_code', ''), local.get('stock_name', ''), local.get('db_time', 0.0))

                merge_local(local)
                write_queue.task_done()

        write_queue: "Queue[Optional[Dict[str, Any]]]" = Queue(maxsize=pipeline_queue_size)

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

    def _chunk_list(self, items: List[str], size: int) -> List[List[str]]:
        if not items or size <= 0:
            return []
        return [items[i:i + size] for i in range(0, len(items), size)]

    def _filter_raw_kline_by_dates(self, raw_data: List[Dict[str, Any]], trade_dates: Optional[List[str]]) -> List[Dict[str, Any]]:
        if not raw_data or not trade_dates:
            return raw_data

        allowed = set(trade_dates)
        filtered: List[Dict[str, Any]] = []
        seen_keys = set()
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            time_value = item.get('Time') or item.get('time') or item.get('trade_date') or item.get('date')
            trade_date = self.normalizer._normalize_date_str(time_value)
            if trade_date and trade_date in allowed:
                key = time_value or trade_date
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                filtered.append(item)
        return filtered

    def _log_progress(self, stage: str, current: int, total: int, stock_code: str, stock_name: str, elapsed: float) -> None:
        if total <= 0:
            return
        percent = (current / total) * 100
        message = (
            f"{stage}-[{current}/{total}]"
            f"{stock_code}-{stock_name}，"
            f"进度{percent:.2f}%，"
            f"阶段耗时 {elapsed:.2f} s"
        )
        self.logger.info(message)
