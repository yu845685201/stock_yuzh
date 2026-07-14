"""
1分钟K线同步端口定义
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class Kline1MinSourcePort(ABC):
    """K线采集端口"""

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def fetch_kline_all(self, stock_code: str, kline_type: str) -> Optional[List[Dict[str, Any]]]:
        pass

    @abstractmethod
    def fetch_kline_range(
        self,
        stock_code: str,
        kline_type: str,
        start_date: str,
        end_date: str,
        limit: int
    ) -> Optional[List[Dict[str, Any]]]:
        pass


class Kline1MinRepositoryPort(ABC):
    """K线写库端口"""

    @abstractmethod
    def fetch_last_close(self, ts_code: str) -> Optional[float]:
        pass

    @abstractmethod
    def fetch_prev_close(self, ts_code: str, trade_date: str, trade_time: str) -> Optional[float]:
        pass


class Kline1MinPartitionPort(ABC):
    """1分钟K线分区写库端口"""

    @abstractmethod
    def ensure_partition(self, trade_date) -> None:
        pass

    @abstractmethod
    def cleanup_partition(self, trade_date, mode: str) -> None:
        pass

    @abstractmethod
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        pass


class Kline1MinCsvPort(ABC):
    """CSV写入端口"""

    @abstractmethod
    def enqueue_raw(self, ts_code: str, raw_data: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def flush(self) -> Dict[str, Any]:
        pass


class Kline1MinReportPort(ABC):
    """报告输出端口"""

    @abstractmethod
    def write_report(
        self,
        result: Dict[str, Any],
        anomalies: List[Dict[str, Any]],
        timing: Dict[str, Any]
    ) -> Optional[str]:
        pass


class Kline1MinFundamentalsPort(ABC):
    """基本面映射端口"""

    @abstractmethod
    def build_fundamentals_map(
        self,
        init_mode: bool,
        ts_codes: List[str],
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> Dict[str, Dict[str, Any]]:
        pass


class Kline1MinTradeCalendarPort(ABC):
    """交易日历端口"""

    @abstractmethod
    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        pass


class Kline1MinStockPort(ABC):
    """股票基础信息端口"""

    @abstractmethod
    def fetch_stocks(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        pass
