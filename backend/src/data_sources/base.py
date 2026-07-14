"""
数据源基类接口
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime, date


class DataSourceBase(ABC):
    """数据源基类，定义统一的数据源接口"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化数据源

        Args:
            config: 数据源配置
        """
        self.config = config
        self._connected = False

    @abstractmethod
    def connect(self) -> bool:
        """
        连接数据源

        Returns:
            bool: 连接是否成功
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """断开数据源连接"""
        pass

    @abstractmethod
    def get_stock_list(self) -> List[Dict[str, Any]]:
        """
        获取股票列表

        Returns:
            List[Dict]: 股票列表，每个字典包含股票基本信息
        """
        pass

    @abstractmethod
    def get_financial_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """
        获取财务数据

        Args:
            code: 股票代码
            year: 年份
            quarter: 季度 (1-4)

        Returns:
            Optional[Dict]: 财务数据，如果获取失败返回None
        """
        pass

    @abstractmethod
    def fetch_kline_all(self, code: str, kline_type: str = 'minute1') -> List[Dict[str, Any]]:
        """
        获取全量K线数据

        Args:
            code: 股票代码
            kline_type: K线类型
        """
        pass

    @abstractmethod
    def fetch_kline_range(
        self,
        code: str,
        kline_type: str = 'minute1',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 800
    ) -> List[Dict[str, Any]]:
        """
        获取区间K线数据

        Args:
            code: 股票代码
            kline_type: K线类型
            start_date: 开始日期
            end_date: 结束日期
            limit: 返回条数上限
        """
        pass

    @property
    def is_connected(self) -> bool:
        """返回是否已连接"""
        return self._connected
