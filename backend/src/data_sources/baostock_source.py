"""
Baostock数据源实现 - 严格按照产品设计文档要求
用于获取基本面数据
"""

import baostock as bs
import logging
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
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                print(f"查询股票列表失败: {rs.error_msg}")
                return []

            stock_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()

                # 严格按照文档要求：只保留type=1的股票信息
                if row[4] != '1':  # type字段不为1则跳过
                    continue

                # 解析baostock返回的数据
                baostock_code = row[0]  # 如: sz.000001
                stock_name = row[1]
                ipo_date = DataTransformer.format_date_string(row[2])
                out_date = DataTransformer.format_date_string(row[3])
                stock_type = row[4]  # type=1 (股票)
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
                    'delist_date': out_date
                }

                stock_list.append(stock_info)

            return stock_list
        except Exception as e:
            print(f"获取股票列表异常: {e}")
            return []

    
    def get_daily_data(
        self,
        code: str,
        start_date: date = None,
        end_date: date = None
    ) -> List[Dict[str, Any]]:
        """获取日K线数据 - 严格按照文档要求"""
        if not self._connected:
            return []

        try:
            # 设置默认日期范围
            if not start_date:
                start_date = date(2020, 1, 1)
            if not end_date:
                end_date = date.today()

            # 生成TS代码 - baostock API需要完整格式
            market = 'sz' if code.startswith(('00', '30')) else 'sh'
            ts_code = DataTransformer.generate_ts_code(code, market)

            # 获取K线数据
            rs = bs.query_history_k_data_plus(
                ts_code,
                "date,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg",
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                frequency="d"
            )

            if rs.error_code != '0':
                print(f"查询K线数据失败: {rs.error_msg}")
                return []

            data_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                try:
                    # 解析数据
                    trade_date = datetime.strptime(row[0], '%Y-%m-%d').date()
                    open_price = float(row[1]) if row[1] else None
                    high_price = float(row[2]) if row[2] else None
                    low_price = float(row[3]) if row[3] else None
                    close_price = float(row[4]) if row[4] else None
                    volume = int(row[5]) if row[5] else None
                    amount = float(row[6]) if row[6] else None
                    adjust_flag = int(row[7]) if row[7] else 3  # 默认不复权
                    turn = float(row[8]) if row[8] else None
                    trade_status = int(row[9]) if row[9] else 1  # 默认正常交易
                    pct_chg = float(row[10]) if row[10] else None

                    # ts_code已在上面生成

                    # 检查是否为ST股
                    stock_name = self._get_stock_name(code)
                    is_st = DataTransformer.check_is_st(stock_name) if stock_name else False

                    daily_record = {
                        'ts_code': ts_code,
                        'stock_code': code,
                        'stock_name': stock_name,
                        'trade_date': trade_date,
                        'open': round(open_price, 4) if open_price else None,
                        'high': round(high_price, 4) if high_price else None,
                        'low': round(low_price, 4) if low_price else None,
                        'close': round(close_price, 4) if close_price else None,
                        'preclose': None,  # 需要计算
                        'volume': volume,
                        'amount': round(amount, 4) if amount else None,
                        'trade_status': trade_status,
                        'is_st': is_st,
                        'adjust_flag': adjust_flag,
                        'change_rate': pct_chg,  # baostock已提供涨跌幅
                        'turnover_rate': round(turn, 6) if turn else None,
                        'pe_ttm': None,  # 无法直接获取
                        'pb_rate': None,  # 无法直接获取
                        'ps_ttm': None,  # 无法直接获取
                        'pcf_ttm': None  # 无法直接获取
                    }

                    data_list.append(daily_record)
                except (ValueError, IndexError) as e:
                    print(f"解析数据行失败: {row}, 错误: {e}")
                    continue

            # 后处理：计算昨日收盘价
            data_list = self._post_process_daily_data(data_list)

            return data_list
        except Exception as e:
            print(f"获取日K线数据异常: {e}")
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
            rs = bs.query_stock_basic()
            if rs.error_code != '0':
                return None

            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                if DataTransformer.extract_stock_code(row[0]) == code:
                    return row[1]

            return None
        except Exception:
            return None

    def _post_process_daily_data(self, daily_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        后处理日K线数据 - 计算昨日收盘价
        """
        if len(daily_data) <= 1:
            return daily_data

        # 按日期排序
        daily_data.sort(key=lambda x: x['trade_date'])

        # 计算昨日收盘价
        for i in range(len(daily_data)):
            if i > 0:
                daily_data[i]['preclose'] = daily_data[i-1]['close']

        return daily_data

    def get_financial_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """获取财务数据 - 严格按照文档要求"""
        if not self._connected:
            return None

        try:
            # API限流检查
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            # 获取财务数据 - 简化调用
            rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
            if rs.error_code != '0':
                print(f"查询财务数据失败: {rs.error_msg}")
                return None

            # 获取第一条记录
            if rs.next():
                row = rs.get_row_data()
                field_names = rs.fields  # 获取字段名列表

                # 使用字段名动态定位totalShare和floatShare
                total_share_idx = field_names.index('totalShare') if 'totalShare' in field_names else 9
                float_share_idx = field_names.index('floatShare') if 'floatShare' in field_names else 10

                # 严格按照文档要求构建数据字典
                financial_data = {
                    'stock_code': code,
                    'disclosure_date': self._get_disclosure_date(year, quarter),
                    'total_share': float(row[total_share_idx]) if row[total_share_idx] else None,  # 总股本，单位：股
                    'float_share': float(row[float_share_idx]) if row[float_share_idx] else None  # 流通股本，单位：股
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

    def _get_disclosure_date(self, year: int, quarter: int) -> datetime:
        """
        获取信息披露日期 - 严格按照实际表结构要求

        Args:
            year: 年份
            quarter: 季度

        Returns:
            信息披露日期（季度末日期，timestamp类型）
        """
        # 季度末日期映射 - 返回datetime以匹配timestamp字段
        quarter_end_dates = {
            1: datetime(year, 3, 31, 16, 0, 0),  # 下午4点作为披露时间
            2: datetime(year, 6, 30, 16, 0, 0),
            3: datetime(year, 9, 30, 16, 0, 0),
            4: datetime(year, 12, 31, 16, 0, 0)
        }

        return quarter_end_dates.get(quarter, datetime(year, 12, 31, 16, 0, 0))

    def get_minute_data(
        self,
        code: str,
        data_type: str = '1min',
        start_date: date = None,
        end_date: date = None
    ) -> List[Dict[str, Any]]:
        """
        获取分钟K线数据 - Baostock暂不支持分钟数据

        Args:
            code: 股票代码
            data_type: 数据类型 ('1min', '5min', '15min', '30min', '60min')
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            List[Dict]: 空列表，Baostock不支持分钟数据
        """
        logger.warning(f"Baostock不支持分钟数据采集: code={code}, data_type={data_type}")
        return []