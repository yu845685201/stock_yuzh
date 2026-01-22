"""
Baostock数据源实现 - 严格按照产品设计文档要求
用于获取基本面数据
"""

import baostock as bs
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from .base import DataSourceBase
from ..utils.data_transformer import DataTransformer
from ..utils.api_rate_limiter import ApiRateLimiter

logger = logging.getLogger(__name__)


class BaostockSource(DataSourceBase):
    """Baostock数据源，获取基本面数据"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.data_path = config.get('data_path', 'uat/data')

        # 初始化API限流器 - 使用基本面数据专用配置
        financial_rate_limit_config = config.get('financial_data_rate_limit', {})
        if financial_rate_limit_config.get('enabled', True):
            self.rate_limiter = ApiRateLimiter(
                calls_per_period=financial_rate_limit_config.get('calls_per_period', 1000),
                sleep_duration=financial_rate_limit_config.get('sleep_duration', 1.0),
                enabled=True
            )
        else:
            # 如果禁用限流，创建一个禁用的限流器实例
            self.rate_limiter = ApiRateLimiter(enabled=False)

    def _execute_query_with_retry(self, query_func, error_context: str = ""):
        """
        执行Baostock查询并带重试机制

        Args:
            query_func: 查询函数（无参lambda）
            error_context: 错误上下文描述

        Returns:
            查询结果对象

        Raises:
            Exception: 重试耗尽后抛出最后一次异常
        """
        max_retries = 3
        last_error = None

        for attempt in range(max_retries):
            try:
                # 每次尝试前确保连接
                if not self._connected:
                    self.connect()

                return query_func()
            except Exception as e:
                last_error = e
                # 检查是否是网络相关错误
                error_str = str(e)

                logger.warning(f"Baostock查询异常 (尝试 {attempt+1}/{max_retries}) {error_context}: {e}")

                if attempt < max_retries - 1:
                    # 尝试断开重连
                    try:
                        self.disconnect()
                        time.sleep(1) # 简单等待
                        self.connect()
                    except Exception as re_e:
                        logger.error(f"重连失败: {re_e}")

        # 重试耗尽，抛出最后一次异常
        raise last_error

    def connect(self) -> bool:
        """连接baostock"""
        try:
            lg = bs.login()
            self._connected = lg.error_code == '0'
            if not self._connected:
                print(f"Baostock登录失败: {lg.error_msg}")
            return self._connected
        except Exception as e:
            print(f"Baostock连接异常: {e}")
            return False

    def disconnect(self) -> None:
        """断开baostock连接"""
        if self._connected:
            bs.logout()
            self._connected = False

    def get_stock_list(self) -> List[Dict[str, Any]]:
        """获取股票列表 - 严格按照文档要求"""
        if not self._connected:
            return []

        try:
            # 获取证券信息
            rs = self._execute_query_with_retry(lambda: bs.query_stock_basic(), "query_stock_basic")
            if rs.error_code != '0':
                print(f"查询股票列表失败: {rs.error_msg}")
                return []

            stock_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()

                # 严格按照文档要求：不再进行type=1和2的过滤，全都入库
                # if row[4] not in ('1', '2'):  # type不为1或2则跳过
                #    continue

                # 解析baostock返回的数据
                baostock_code = row[0]  # 如: sz.000001
                stock_name = row[1]
                ipo_date = DataTransformer.format_date_string(row[2])
                out_date = DataTransformer.format_date_string(row[3])
                stock_type = row[4]  # type=1 (股票), 2(指数), 3(其它), 4(可转债), 5(ETF)
                status = row[5]

                # 严格按照文档要求处理字段
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
                    'sector_code': None,  # 严格按照文档要求：留空
                    'sector_name': None,  # 严格按照文档要求：留空
                    'industry_code': None,  # 严格按照文档要求：留空
                    'industry_name': None,  # 严格按照文档要求：留空
                    'list_status': list_status,
                    'list_date': ipo_date,
                    'delist_date': out_date,
                    'type': stock_type  # 新增type字段
                }

                stock_list.append(stock_info)

            return stock_list
        except Exception as e:
            print(f"获取股票列表异常: {e}")
            return []


    def _get_stock_name(self, code: str) -> Optional[str]:
        """
        获取股票名称

        Args:
            code: 股票编码

        Returns:
            股票名称
        """
        try:
            rs = self._execute_query_with_retry(lambda: bs.query_stock_basic(), "query_stock_basic")
            if rs.error_code != '0':
                return None

            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                if DataTransformer.extract_stock_code(row[0]) == code:
                    return row[1]

            return None
        except Exception:
            return None


    def get_financial_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """获取财务数据 - 严格按照文档要求"""
        if not self._connected:
            return None

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            # 获取财务数据 - 简化调用
            rs = self._execute_query_with_retry(
                lambda: bs.query_profit_data(code=code, year=year, quarter=quarter),
                f"query_profit_data {code} {year}Q{quarter}"
            )
            if rs.error_code != '0':
                print(f"查询财务数据失败: {rs.error_msg}")
                return None

            # 获取第一条记录
            if rs.next():
                row = rs.get_row_data()
                field_names = rs.fields  # 获取字段名列表

                # 使用字段名动态定位totalShare和liqaShare（产品设计文档要求）
                total_share_idx = field_names.index('totalShare') if 'totalShare' in field_names else 9
                # 按照产品设计文档：流通股本字段为liqaShare
                liqa_share_idx = field_names.index('liqaShare') if 'liqaShare' in field_names else 10

                # 严格按照文档要求构建数据字典
                financial_data = {
                    'stock_code': code,
                    'disclosure_date': self._get_disclosure_date(year, quarter),
                    'total_share': float(row[total_share_idx]) if row[total_share_idx] else None,  # 总股本，单位：股
                    'float_share': float(row[liqa_share_idx]) if row[liqa_share_idx] else None  # 流通股本（liqaShare字段），单位：股
                }

                return financial_data

            return None
        except Exception as e:
            print(f"获取财务数据异常: {e}")
            return None

    def get_stock_fundamentals(self, ts_code: str, year: int = None, quarter: int = None) -> Optional[Dict[str, Any]]:
        """
        获取股票基本面数据 - 严格按照产品设计文档要求

        Args:
            ts_code: ts代码（如：sz.000001）
            year: 年份（可选）
            quarter: 季度（可选，1-4）

        Returns:
            基本面数据字典，包含总股本和流通股本
        """
        if not self._connected:
            return None

        try:
            # 根据参数决定查询逻辑
            if year and quarter:
                # 如果指定了年份和季度，只查询指定的季度
                query_sequence = [(year, quarter)]
                print(f"查询指定季度：{year}年Q{quarter}")
            else:
                # 优化后的季度回退逻辑：只查询前两个季度
                current_year = datetime.now().year
                current_quarter = (datetime.now().month - 1) // 3 + 1

                # 构建优化后的查询序列：前一个季度 → 前两个季度
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

                # 按照实际表结构映射字段
                fundamentals = {
                    'stock_code': stock_code,
                    'ts_code': ts_code,  # 直接使用传入的ts_code
                    'disclosure_date': financial_data.get('disclosure_date'),
                    'total_share': financial_data.get('total_share'),
                    'float_share': financial_data.get('float_share'),
                    'data_source': 'baostock',
                    'create_time': datetime.now()
                }
                return fundamentals

            return None
        except Exception as e:
            print(f"获取基本面数据异常: {e}")
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
        获取原始日线K线数据（不进行类型转换和计算）

        Args:
            code: 股票代码 (e.g., 'sz.000001')
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            adjustflag: 复权类型，默认'1'：后复权

        Returns:
            原始数据字典列表（值为字符串）
        """
        if not self._connected:
            return []

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            fields = "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
            rs = self._execute_query_with_retry(
                lambda: bs.query_history_k_data_plus(code, fields, start_date=start_date, end_date=end_date, frequency="d", adjustflag=adjustflag),
                f"fetch_daily_k_data_raw {code} {start_date}-{end_date}"
            )

            if rs is None or rs.error_code != '0':
                error_msg = rs.error_msg if rs else "Result is None"
                print(f"查询日线数据失败: {error_msg} code={code}, start={start_date}, end={end_date}")
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
            print(f"获取原始日线数据异常: {e}")
            return []

    def fetch_5min_k_data_raw(self, code: str, start_date: str, end_date: str, adjustflag: str = '1') -> List[Dict[str, str]]:
        """
        获取原始5分钟K线数据

        Args:
            code: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            adjustflag: 复权类型，默认'1'：后复权

        Returns:
            原始数据字典列表
        """
        if not self._connected:
            return []

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            fields = "date,time,open,high,low,close,volume,amount,adjustflag"
            rs = self._execute_query_with_retry(
                lambda: bs.query_history_k_data_plus(code, fields, start_date=start_date, end_date=end_date, frequency="5", adjustflag=adjustflag),
                f"fetch_5min_k_data_raw {code} {start_date}-{end_date}"
            )

            if rs is None or rs.error_code != '0':
                error_msg = rs.error_msg if rs else "Result is None"
                print(f"查询5分钟数据失败: {error_msg} code={code}, start={start_date}, end={end_date}")
                return []

            data_list = []
            while (rs.error_code == '0') & rs.next():
                row_data = rs.get_row_data()
                item = dict(zip(fields.split(','), row_data))
                data_list.append(item)

            return data_list

        except Exception as e:
            print(f"获取原始5分钟数据异常: {e}")
            return []

    def get_daily_k_data(self, code: str, start_date: str, end_date: str, adjustflag: str = '1') -> List[Dict[str, Any]]:
        """
        获取日线K线数据

        Args:
            code: 股票代码 (e.g., 'sz.000001')
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            adjustflag: 复权类型，默认'1'：后复权，'2'：前复权，'3'：不复权

        Returns:
            日线数据列表
        """
        if not self._connected:
            return []

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            fields = "date,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
            rs = self._execute_query_with_retry(
                lambda: bs.query_history_k_data_plus(code, fields, start_date=start_date, end_date=end_date, frequency="d", adjustflag=adjustflag),
                f"get_daily_k_data {code} {start_date}-{end_date}"
            )

            if rs is None or rs.error_code != '0':
                error_msg = rs.error_msg if rs else "Result is None (likely date format error)"
                print(f"查询日线数据失败: {error_msg} code={code}, start={start_date}, end={end_date}")
                return []

            data_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()

                try:
                    # 转换数据格式
                    # row indices:
                    # 0:date, 1:open, 2:high, 3:low, 4:close, 5:preclose, 6:volume, 7:amount,
                    # 8:adjustflag, 9:turn, 10:tradestatus, 11:pctChg, 12:peTTM, 13:pbMRQ,
                    # 14:psTTM, 15:pcfNcfTTM, 16:isST

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
                        'volume_ratio': None, # Baostock不提供
                        'pe': to_float(row[12]),
                        'pb': to_float(row[13]),
                        'ps': to_float(row[14]),
                        'pcf': to_float(row[15]),
                        'total_share': None, # K线接口不提供
                        'float_share': None, # K线接口不提供
                        'free_share': None, # K线接口不提供
                        'total_mv': None, # K线接口不提供
                        'circ_mv': None, # K线接口不提供
                        'adj_factor': None, # 后复权模式下不直接提供因子
                        'is_st': row[16],
                        'trade_status': row[10],
                        'create_time': datetime.now(),
                        'update_time': datetime.now()
                    }
                    data_list.append(item)
                except Exception as parse_err:
                    print(f"解析K线数据行异常: {parse_err}, row: {row}")
                    continue

            return data_list

        except Exception as e:
            print(f"获取日线数据异常: {e}")
            return []
