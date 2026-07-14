"""
1分钟K线同步适配器
"""
import threading
from queue import Queue
from typing import Any, Dict, List, Optional

from ..database import DatabaseConnection
from ..sync.csv_writer import CsvWriter
from ..data_sources.tdx_api_source import TdxApiSource
from .ports import (
    Kline1MinCsvPort,
    Kline1MinFundamentalsPort,
    Kline1MinReportPort,
    Kline1MinRepositoryPort,
    Kline1MinPartitionPort,
    Kline1MinSourcePort,
    Kline1MinStockPort,
    Kline1MinTradeCalendarPort
)


class TdxSourceAdapter(Kline1MinSourcePort):
    def __init__(self, source: TdxApiSource):
        self.source = source

    def connect(self) -> bool:
        return self.source.connect()

    def disconnect(self) -> None:
        self.source.disconnect()

    def fetch_kline_all(self, stock_code: str, kline_type: str) -> Optional[List[Dict[str, Any]]]:
        return self.source.get_kline_all(stock_code, kline_type)

    def fetch_kline_range(
        self,
        stock_code: str,
        kline_type: str,
        start_date: str,
        end_date: str,
        limit: int
    ) -> Optional[List[Dict[str, Any]]]:
        return self.source.get_kline_history(
            stock_code,
            kline_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )


class DatabaseKline1MinRepository(Kline1MinRepositoryPort):
    def __init__(self, db_conn: DatabaseConnection):
        self.db_conn = db_conn

    def fetch_last_close(self, ts_code: str) -> Optional[float]:
        return self.db_conn.fetch_last_his_kline_1min_close(ts_code)

    def fetch_prev_close(self, ts_code: str, trade_date: str, trade_time: str) -> Optional[float]:
        return self.db_conn.fetch_prev_his_kline_1min_close(ts_code, trade_date, trade_time)


class DatabaseKline1MinPartitionAdapter(Kline1MinPartitionPort):
    def __init__(self, db_conn: DatabaseConnection):
        self.db_conn = db_conn

    def ensure_partition(self, trade_date) -> None:
        self.db_conn.ensure_his_kline_1min_partition(trade_date)

    def cleanup_partition(self, trade_date, mode: str) -> None:
        self.db_conn.cleanup_his_kline_1min_partition(trade_date, mode)

    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        return self.db_conn.insert_his_kline_1min_partition(records)


class CsvAsyncWriter(Kline1MinCsvPort):
    def __init__(self, csv_writer: CsvWriter, max_workers: int = 2, max_queue: int = 20000):
        self.csv_writer = csv_writer
        self.max_workers = max_workers
        self.queue: "Queue[Dict[str, Any]]" = Queue(maxsize=max_queue)
        self._threads: List[threading.Thread] = []
        self._errors: List[str] = []
        self._started = False
        self._lock = threading.Lock()

    def _start(self) -> None:
        if self._started:
            return
        self._started = True
        for _ in range(max(1, self.max_workers)):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self._threads.append(t)

    def _worker(self) -> None:
        while True:
            item = self.queue.get()
            if item is None:
                self.queue.task_done()
                break
            try:
                self.csv_writer.write_his_kline_1min_raw(item['ts_code'], item['raw_data'])
            except Exception as e:
                with self._lock:
                    self._errors.append(str(e))
            finally:
                self.queue.task_done()

    def enqueue_raw(self, ts_code: str, raw_data: List[Dict[str, Any]]) -> None:
        if not raw_data:
            return
        self._start()
        self.queue.put({'ts_code': ts_code, 'raw_data': raw_data})

    def flush(self) -> Dict[str, Any]:
        if not self._started:
            return {'errors': []}
        for _ in self._threads:
            self.queue.put(None)
        self.queue.join()
        for t in self._threads:
            t.join()
        return {'errors': list(self._errors)}


class FundamentalsPortAdapter(Kline1MinFundamentalsPort):
    def __init__(self, manager):
        self.manager = manager

    def build_fundamentals_map(
        self,
        init_mode: bool,
        ts_codes: List[str],
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> Dict[str, Dict[str, Any]]:
        return self.manager._build_fundamentals_map(
            init_mode=init_mode,
            ts_codes=ts_codes,
            start_date=start_date,
            end_date=end_date
        )


class TradeCalendarPortAdapter(Kline1MinTradeCalendarPort):
    def __init__(self, manager):
        self.manager = manager

    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        return self.manager._get_trade_dates(start_date, end_date)


class StockPortAdapter(Kline1MinStockPort):
    def __init__(self, manager):
        self.manager = manager

    def fetch_stocks(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return self.manager.db_conn.fetch_stock_basic(ts_codes)


class ReportPortAdapter(Kline1MinReportPort):
    def __init__(self, manager):
        self.manager = manager

    def write_report(self, result: Dict[str, Any], anomalies: List[Dict[str, Any]], timing: Dict[str, Any]) -> Optional[str]:
        return self.manager._write_kline_1min_report(result, anomalies, timing)
