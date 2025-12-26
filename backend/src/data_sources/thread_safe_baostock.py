"""
线程安全的Baostock数据源实现
解决baostock全局会话的线程安全问题
"""

import threading
import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime

import baostock as bs
from ..utils.data_transformer import DataTransformer
from ..utils.api_rate_limiter import ApiRateLimiter

logger = logging.getLogger(__name__)


class ThreadSafeBaostockSource:
    """线程安全的Baostock数据源，使用线程本地存储实现连接隔离"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化线程安全的Baostock数据源

        Args:
            config: 配置字典
        """
        self.config = config
        self.data_path = config.get('data_path', 'uat/data')

        # 线程本地存储
        self._local = threading.local()

        # 初始化API限流器（线程安全）
        financial_rate_limit_config = config.get('financial_data_rate_limit', {})
        if financial_rate_limit_config.get('enabled', True):
            self.rate_limiter = ApiRateLimiter(
                calls_per_period=financial_rate_limit_config.get('calls_per_period', 1000),
                sleep_duration=financial_rate_limit_config.get('sleep_duration', 1.0),
                enabled=True
            )
        else:
            self.rate_limiter = ApiRateLimiter(enabled=False)

    def _ensure_connection(self) -> bool:
        """
        确保当前线程有有效的baostock连接

        Returns:
            bool: 连接是否成功
        """
        if not hasattr(self._local, 'connected') or not self._local.connected:
            try:
                lg = bs.login()
                self._local.connected = lg.error_code == '0'
                if not self._local.connected:
                    logger.error(f"Baostock登录失败: {lg.error_msg}")
                    return False
                logger.debug(f"线程 {threading.current_thread().name} Baostock连接成功")
                return True
            except Exception as e:
                logger.error(f"线程 {threading.current_thread().name} Baostock连接异常: {e}")
                return False
        return True

    def _get_connection_status(self) -> bool:
        """
        获取当前线程的连接状态

        Returns:
            bool: 是否已连接
        """
        return getattr(self._local, 'connected', False)

    def disconnect(self) -> None:
        """
        断开当前线程的baostock连接
        """
        if hasattr(self._local, 'connected') and self._local.connected:
            try:
                bs.logout()
                self._local.connected = False
                logger.debug(f"线程 {threading.current_thread().name} Baostock连接已断开")
            except Exception as e:
                logger.error(f"线程 {threading.current_thread().name} Baostock断开异常: {e}")

    def get_stock_list(self) -> List[Dict[str, Any]]:
        """获取股票列表 - 线程安全版本"""
        if not self._ensure_connection():
            return []

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            # 获取证券信息
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                logger.error(f"查询股票列表失败: {rs.error_msg}")
                return []

            stock_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()

                # 只保留type=1的股票信息
                if row[4] != '1':
                    continue

                # 解析baostock返回的数据
                baostock_code = row[0]
                stock_name = row[1]
                ipo_date = DataTransformer.format_date_string(row[2])
                out_date = DataTransformer.format_date_string(row[3])
                stock_type = row[4]
                status = row[5]

                # 解析股票代码和生成ts_code
                stock_code = DataTransformer.extract_stock_code(baostock_code)
                ts_code = DataTransformer.generate_ts_code(stock_code, baostock_code[:2])
                market_info = DataTransformer.get_market_info(stock_code, baostock_code)

                # 生成拼音缩写
                cnspell = DataTransformer.generate_pinyin(stock_name)

                # 映射上市状态
                list_status = DataTransformer.map_list_status(status)

                stock_info = {
                    'ts_code': ts_code,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'cnspell': cnspell,
                    'market_code': market_info['market_code'],
                    'market_name': market_info['market_name'],
                    'exchange_code': market_info['exchange_code'],
                    'sector_code': None,
                    'sector_name': None,
                    'industry_code': None,
                    'industry_name': None,
                    'list_status': list_status,
                    'list_date': ipo_date,
                    'delist_date': out_date
                }

                stock_list.append(stock_info)

            return stock_list
        except Exception as e:
            logger.error(f"获取股票列表异常: {e}")
            return []

    def get_financial_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """获取财务数据 - 线程安全版本"""
        if not self._ensure_connection():
            return None

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            # 获取财务数据
            rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
            if rs.error_code != '0':
                logger.debug(f"查询财务数据失败: {rs.error_msg}")
                return None

            # 获取第一条记录
            if rs.next():
                row = rs.get_row_data()
                field_names = rs.fields

                # 使用字段名动态定位totalShare和floatShare
                total_share_idx = field_names.index('totalShare') if 'totalShare' in field_names else 9
                float_share_idx = field_names.index('floatShare') if 'floatShare' in field_names else 10

                # 构建数据字典
                financial_data = {
                    'stock_code': code,
                    'disclosure_date': self._get_disclosure_date(year, quarter),
                    'total_share': float(row[total_share_idx]) if row[total_share_idx] else None,
                    'float_share': float(row[float_share_idx]) if row[float_share_idx] else None
                }

                return financial_data

            return None
        except Exception as e:
            logger.debug(f"获取财务数据异常: {e}")
            return None

    def get_stock_fundamentals(self, ts_code: str, year: int = None, quarter: int = None) -> Optional[Dict[str, Any]]:
        """
        获取股票基本面数据 - 线程安全版本

        Args:
            ts_code: ts代码（如：sz.000001）
            year: 年份（可选）
            quarter: 季度（可选，1-4）

        Returns:
            基本面数据字典，包含总股本和流通股本
        """
        if not self._ensure_connection():
            return None

        try:
            # 根据参数决定查询逻辑
            if year and quarter:
                query_sequence = [(year, quarter)]
                logger.debug(f"查询指定季度：{year}年Q{quarter}")
            else:
                # 优化后的季度回退逻辑：只查询前两个季度
                current_year = datetime.now().year
                current_quarter = (datetime.now().month - 1) // 3 + 1

                query_sequence = []

                # 第一次尝试：前一个季度
                if current_quarter > 1:
                    query_sequence.append((current_year, current_quarter - 1))
                else:
                    query_sequence.append((current_year - 1, 4))

                # 第二次尝试：前两个季度
                if current_quarter > 2:
                    query_sequence.append((current_year, current_quarter - 2))
                elif current_quarter > 1:
                    query_sequence.append((current_year - 1, 4))
                else:
                    query_sequence.append((current_year - 1, 3))

            # 按查询序列执行，最多查询2次
            financial_data = None
            for i, (query_year, query_quarter) in enumerate(query_sequence[:2]):
                try:
                    logger.debug(f"第{i+1}次查询：{query_year}年Q{query_quarter}")
                    financial_data = self.get_financial_data(ts_code, query_year, query_quarter)
                    if financial_data:
                        logger.debug(f"找到 {query_year}年Q{query_quarter}的数据")
                        break
                except Exception as e:
                    logger.debug(f"查询{query_year}年Q{query_quarter}失败: {e}")
                    continue

            if financial_data:
                # 从ts_code提取stock_code
                stock_code = ts_code.split('.')[1] if '.' in ts_code else ts_code

                # 映射字段
                fundamentals = {
                    'stock_code': stock_code,
                    'ts_code': ts_code,
                    'disclosure_date': financial_data.get('disclosure_date'),
                    'total_share': financial_data.get('total_share'),
                    'float_share': financial_data.get('float_share'),
                    'data_source': 'baostock',
                    'create_time': datetime.now()
                }
                return fundamentals

            return None
        except Exception as e:
            logger.error(f"获取基本面数据异常: {e}")
            return None

    def _get_disclosure_date(self, year: int, quarter: int) -> datetime:
        """
        获取信息披露日期

        Args:
            year: 年份
            quarter: 季度

        Returns:
            信息披露日期（季度末日期，datetime类型）
        """
        quarter_end_dates = {
            1: datetime(year, 3, 31, 16, 0, 0),
            2: datetime(year, 6, 30, 16, 0, 0),
            3: datetime(year, 9, 30, 16, 0, 0),
            4: datetime(year, 12, 31, 16, 0, 0)
        }

        return quarter_end_dates.get(quarter, datetime(year, 12, 31, 16, 0, 0))

    def __enter__(self):
        """上下文管理器入口"""
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if exc_type or exc_val:
            # 发生异常时断开连接
            self.disconnect()