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
from .pipeline import (
    Kline1MinSharedState,
    Kline1MinDeps,
    process_one_stock,
    merge_stock_result,
    run_pipeline,
)
from ..utils.kline_normalize import normalize_date_str


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

            ctx = Kline1MinSharedState(result=result, anomaly_limit=anomaly_limit)
            deps = Kline1MinDeps(
                source=self.source,
                repository=self.repository,
                partition_writer=self.partition_writer,
                csv_writer=self.csv_writer,
                normalizer=self.normalizer,
                logger=self.logger,
                config=self.config,
                fundamentals_map=fundamentals_map,
                trade_dates=trade_dates,
                init_mode=init_mode,
                save_to_csv=save_to_csv,
                save_to_db=save_to_db,
                total_stocks=total_stocks,
                db_write_semaphore=db_write_semaphore,
                log_progress=self._log_progress,
                filter_raw=self._filter_raw_kline_by_dates,
                chunk_list=self._chunk_list,
                record_failed_dates=record_failed_dates,
                run_with_failure_capture=run_with_failure_capture
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
                pipeline_result = run_pipeline(
                    deps,
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
                merge_stock_result(pipeline_result, ctx)
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
                                executor.submit(process_one_stock, s, batch_start + i + 1, ctx=ctx, deps=deps)
                                for i, s in enumerate(stock_batch)
                            ]
                            for future in as_completed(futures):
                                merge_stock_result(future.result(), ctx)
                    else:
                        for i, stock in enumerate(stock_batch):
                            merge_stock_result(process_one_stock(stock, batch_start + i + 1, ctx=ctx, deps=deps), ctx)

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
            trade_date = normalize_date_str(time_value)
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
