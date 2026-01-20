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
    """线程安全的Baostock数据源，使用全局连接状态确保只login一次"""

    # 类级别的全局连接状态和锁
    _connection_lock = threading.Lock()
    _is_connected = False

    def __init__(self, config: Dict[str, Any]):
        """
        初始化线程安全的Baostock数据源

        Args:
            config: 配置字典
        """
        self.config = config
        self.data_path = config.get('data_path', 'uat/data')

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

    def connect(self) -> bool:
        """
        连接baostock - 使用全局锁确保只执行一次login
        
        Returns:
            bool: 连接是否成功
        """
        with ThreadSafeBaostockSource._connection_lock:
            if not ThreadSafeBaostockSource._is_connected:
                try:
                    lg = bs.login()
                    ThreadSafeBaostockSource._is_connected = lg.error_code == '0'
                    if not ThreadSafeBaostockSource._is_connected:
                        logger.error(f"Baostock登录失败: {lg.error_msg}")
                        return False
                    logger.debug("Baostock全局连接成功")
                    return True
                except Exception as e:
                    logger.error(f"Baostock连接异常: {e}")
                    return False
            return True

    def disconnect(self) -> None:
        """
        断开baostock连接 - 使用全局锁确保只执行一次logout
        """
        with ThreadSafeBaostockSource._connection_lock:
            if ThreadSafeBaostockSource._is_connected:
                try:
                    bs.logout()
                    ThreadSafeBaostockSource._is_connected = False
                    logger.debug("Baostock全局连接已断开")
                except Exception as e:
                    logger.error(f"Baostock断开异常: {e}")

    def _ensure_connection(self) -> bool:
        """
        确保有有效的baostock连接（直接调用connect方法）

        Returns:
            bool: 连接是否成功
        """
        return self.connect()

    def _get_connection_status(self) -> bool:
        """
        获取全局连接状态

        Returns:
            bool: 是否已连接
        """
        return ThreadSafeBaostockSource._is_connected

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
        # 检查全局连接状态（连接由execute_sync统一管理）
        if not ThreadSafeBaostockSource._is_connected:
            logger.debug("Baostock未连接，跳过获取财务数据")
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

                # 使用字段名动态定位totalShare和liqaShare（产品设计文档要求）
                total_share_idx = field_names.index('totalShare') if 'totalShare' in field_names else 9
                # 按照产品设计文档：流通股本字段为liqaShare
                liqa_share_idx = field_names.index('liqaShare') if 'liqaShare' in field_names else 10

                # 构建数据字典
                financial_data = {
                    'stock_code': code,
                    'disclosure_date': self._get_disclosure_date(year, quarter),
                    'total_share': float(row[total_share_idx]) if row[total_share_idx] else None,
                    'float_share': float(row[liqa_share_idx]) if row[liqa_share_idx] else None  # liqaShare字段
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
        # 检查全局连接状态（连接由execute_sync统一管理）
        if not ThreadSafeBaostockSource._is_connected:
            logger.debug("Baostock未连接，跳过获取基本面数据")
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

    def _get_disclosure_date(self, year: int, quarter: int) -> str:
        """
        获取信息披露日期 - 严格按照数据模型文档要求返回yyyyMMdd格式字符串

        Args:
            year: 年份
            quarter: 季度

        Returns:
            信息披露日期（季度末日期，yyyyMMdd格式字符串）
        """
        # 季度末日期映射 - 返回yyyyMMdd格式字符串，符合varchar(8)类型
        quarter_end_dates = {
            1: f'{year}0331',
            2: f'{year}0630',
            3: f'{year}0930',
            4: f'{year}1231'
        }

        return quarter_end_dates.get(quarter, f'{year}1231')

    def fetch_daily_k_data_raw(self, code: str, start_date: str, end_date: str, adjustflag: str = '1') -> List[Dict[str, str]]:
        """
        获取原始日线K线数据 - 线程安全版本

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            adjustflag: 复权类型

        Returns:
            原始数据字典列表
        """
        # 检查全局连接状态
        if not ThreadSafeBaostockSource._is_connected:
            return []

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            fields = "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
            rs = bs.query_history_k_data_plus(code, fields, start_date=start_date, end_date=end_date, frequency="d", adjustflag=adjustflag)

            if rs is None or rs.error_code != '0':
                error_msg = rs.error_msg if rs else "Result is None"
                logger.error(f"查询日线数据失败: {error_msg} code={code}, start={start_date}, end={end_date}")
                return []

            data_list = []
            while (rs.error_code == '0') & rs.next():
                # 获取原始行数据（字符串列表）
                row_data = rs.get_row_data()
                # 映射到字段名
                item = dict(zip(fields.split(','), row_data))
                data_list.append(item)

            return data_list

        except Exception as e:
            logger.error(f"获取原始日线数据异常: {e}")
            return []

    def get_daily_k_data(self, code: str, start_date: str, end_date: str, adjustflag: str = '1') -> List[Dict[str, Any]]:
        """
        获取日线K线数据 - 线程安全版本

        Args:
            code: 股票代码 (e.g., 'sz.000001')
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            adjustflag: 复权类型，默认'1'：后复权，'2'：前复权，'3'：不复权

        Returns:
            日线数据列表
        """
        # 检查全局连接状态
        if not ThreadSafeBaostockSource._is_connected:
            return []

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            fields = "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
            rs = bs.query_history_k_data_plus(code, fields, start_date=start_date, end_date=end_date, frequency="d", adjustflag=adjustflag)

            if rs is None or rs.error_code != '0':
                error_msg = rs.error_msg if rs else "Result is None (likely date format error)"
                logger.error(f"查询日线数据失败: {error_msg} code={code}, start={start_date}, end={end_date}")
                return []

            data_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()

                try:
                    # 转换数据格式
                    # 处理数值类型，空字符串转None
                    def to_float(val):
                        return float(val) if val and val != '' else None

                    close_val = to_float(row[4])
                    pre_close_val = to_float(row[5])

                    item = {
                        'ts_code': code,
                        'trade_date': DataTransformer.format_date_string(row[0]),
                        'open': to_float(row[1]),
                        'high': to_float(row[2]),
                        'low': to_float(row[3]),
                        'close': close_val,
                        'pre_close': pre_close_val,
                        'change': round(close_val - pre_close_val, 2) if close_val is not None and pre_close_val is not None else None,
                        'pct_chg': to_float(row[11]),
                        'vol': to_float(row[6]),
                        'amount': to_float(row[7]),
                        'turnover_rate': to_float(row[9]),
                        'volume_ratio': None,
                        'pe': to_float(row[12]),
                        'pb': to_float(row[13]),
                        'ps': to_float(row[14]),
                        'pcf': to_float(row[15]),
                        'total_share': None,
                        'float_share': None,
                        'free_share': None,
                        'total_mv': None,
                        'circ_mv': None,
                        'adj_factor': None,
                        'is_st': row[16],
                        'trade_status': row[10],
                        'create_time': datetime.now(),
                        'update_time': datetime.now()
                    }
                    data_list.append(item)
                except Exception as parse_err:
                    logger.error(f"解析K线数据行异常: {parse_err}, row: {row}")
                    continue

            return data_list

        except Exception as e:
            logger.error(f"获取日线数据异常: {e}")
            return []

    def __enter__(self):
        """上下文管理器入口"""
        self._ensure_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if exc_type or exc_val:
            # 发生异常时断开连接
            self.disconnect()