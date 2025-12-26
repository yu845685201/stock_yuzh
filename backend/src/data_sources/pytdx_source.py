"""
Pytdx数据源实现 - 严格按照产品设计文档要求
用于读取通达信数据文件
"""

import os
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from pytdx.reader import TdxDailyBarReader, TdxLCMinBarReader
from .base import DataSourceBase
from ..utils.data_transformer import DataTransformer
from ..services.stock_name_service import StockNameService


class PytdxSource(DataSourceBase):
    """Pytdx数据源，读取通达信数据文件"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.vipdoc_path = config.get('vipdoc_path', 'uat/vipdoc')
        self.data_path = config.get('data_path', 'uat/data')
        self.stock_name_service = None  # 股票名称服务，后续注入

    def set_stock_name_service(self, stock_name_service: StockNameService):
        """
        设置股票名称服务

        Args:
            stock_name_service: 股票名称服务实例
        """
        self.stock_name_service = stock_name_service

    def _get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        获取股票名称 - 优先使用StockNameService

        Args:
            stock_code: 股票代码

        Returns:
            str: 股票名称，获取失败时返回None
        """
        if self.stock_name_service:
            return self.stock_name_service.get_stock_name(stock_code)
        return None  # 如果没有设置服务，返回None

    def connect(self) -> bool:
        """Pytdx不需要连接，直接检查目录是否存在"""
        if os.path.exists(self.vipdoc_path):
            self._connected = True
            return True
        return False

    def disconnect(self) -> None:
        """Pytdx不需要断开连接"""
        self._connected = False

    def get_stock_list(self) -> List[Dict[str, Any]]:
        """从通达信目录获取股票列表"""
        stock_list = []

        # 遍历所有市场目录：北交所(bj)、沪市(sh)、深证(sz)
        for market in ['bj', 'sh', 'sz']:
            market_path = os.path.join(self.vipdoc_path, market, 'lday')
            if os.path.exists(market_path):
                for filename in os.listdir(market_path):
                    if filename.endswith('.day'):
                        # 从.day文件名提取6位代码
                        code = filename.replace('.day', '')

                        # 严格按照文档要求生成字段
                        ts_code = DataTransformer.generate_ts_code(code, market)
                        market_info = DataTransformer.get_market_info(code, f"{market}.{code}")

                        stock_info = {
                            'ts_code': ts_code,
                            'stock_code': code,
                            'stock_name': self._get_stock_name(code) or '',  # 使用StockNameService获取名称
                            'cnspell': None,  # 需要后续通过baostock获取名称后生成
                            'market_code': market_info['market_code'],
                            'market_name': market_info['market_name'],
                            'exchange_code': market_info['exchange_code'],
                            'sector_code': None,  # 无法从通达信文件直接获取
                            'sector_name': None,  # 无法从通达信文件直接获取
                            'industry_code': None,  # 无法从通达信文件直接获取
                            'industry_name': None,  # 无法从通达信文件直接获取
                            'list_status': 'L',  # 默认上市状态，需要后续从baostock更新
                            'list_date': None,  # 需要从baostock获取
                            'delist_date': None  # 需要从baostock获取
                        }
                        stock_list.append(stock_info)

        return stock_list

    def get_daily_data(
        self,
        code: str,
        start_date: date = None,
        end_date: date = None
    ) -> List[Dict[str, Any]]:
        """读取通达信日K线文件 - 严格按照文档要求使用TdxDailyBarReader"""

        # 使用pytdx的TdxDailyBarReader - 严格按照产品设计文档要求
        reader = TdxDailyBarReader()
        daily_data = []

        # 支持三个市场：北交所(bj)、沪市(sh)、深证(sz)
        markets = ['bj', 'sh', 'sz']

        for market in markets:
            filepath = os.path.join(self.vipdoc_path, market, 'lday', f'{market}{code}.day')

            if not os.path.exists(filepath):
                continue

            try:
                # 使用pytdx Reader获取DataFrame - 严格按照文档要求
                df = reader.get_df(filepath)

                # 转换为系统标准格式
                converted_data = self._convert_daily_dataframe(df, code, market, start_date, end_date)
                daily_data.extend(converted_data)

            except Exception as e:
                print(f"使用pytdx Reader读取文件 {filepath} 失败: {e}")

        # 后处理：计算涨跌幅和preclose字段
        daily_data = self._post_process_daily_data(daily_data)

        return daily_data

    def _convert_daily_dataframe(self, df: pd.DataFrame, code: str, market: str,
                                start_date: date = None, end_date: date = None) -> List[Dict[str, Any]]:
        """
        将pytdx Reader的日K线DataFrame转换为系统标准格式

        Args:
            df: pytdx Reader返回的DataFrame
            code: 股票代码
            market: 市场代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            转换后的数据列表
        """
        daily_data = []

        for date_index, row in df.iterrows():
            # 解析日期和格式转换 - date是索引，yyyy-MM-dd → yyyyMMdd
            trade_date = pd.to_datetime(date_index).date()
            trade_date_str = trade_date.strftime('%Y%m%d')

            # 过滤日期范围
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue

            # 生成标准字段
            ts_code = DataTransformer.generate_ts_code(code, market)

            daily_record = {
                'ts_code': ts_code,
                'stock_code': code,
                'stock_name': None,  # 通过股票基本信息表映射获取
                'trade_date': trade_date_str,
                'open': round(float(row['open']), 4),
                'high': round(float(row['high']), 4),
                'low': round(float(row['low']), 4),
                'close': round(float(row['close']), 4),
                'preclose': None,  # 在后处理中计算
                'volume': int(row['volume']),
                'amount': round(float(row['amount']), 4),
                'trade_status': None,  # 无法直接从文件获取，需要结合交易日历判断
                'is_st': None,  # 通过股票名称判断是否包含ST标记
                'adjust_flag': 3,  # 默认为不复权状态
                'change_rate': None,  # 在后处理中计算
                'turnover_rate': None,  # 在后处理中计算
                'pe_ttm': None,  # 无法直接从文件获取，需要财务数据配合计算
                'pb_rate': None,  # 无法直接从文件获取，需要财务数据配合计算
                'ps_ttm': None,  # 无法直接从文件获取，需要财务数据配合计算
                'pcf_ttm': None  # 无法直接从文件获取，需要财务数据配合计算
            }

            daily_data.append(daily_record)

        return daily_data

    def _convert_minute_dataframe(self, df: pd.DataFrame, code: str, market: str,
                                 data_type: str = '1min', start_date: date = None, end_date: date = None) -> List[Dict[str, Any]]:
        """
        将pytdx Reader的分钟K线DataFrame转换为系统标准格式

        Args:
            df: pytdx Reader返回的DataFrame
            code: 股票代码
            market: 市场代码
            data_type: 数据类型 ('1min' 或 '5min')
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            转换后的数据列表
        """
        minute_data = []

        for datetime_index, row in df.iterrows():
            # 解析日期时间 - datetime是索引，yyyy-MM-dd hh:mm:ss
            datetime_obj = pd.to_datetime(datetime_index)
            trade_date = datetime_obj.date()
            trade_time = datetime_obj.time()

            # 按照文档要求进行格式转换
            # 根据数据类型确定trade_date格式
            if data_type == '5min':
                # 5分钟K线：trade_date转换为yyyyMM格式（文档要求）
                trade_date_str = trade_date.strftime('%Y%m')    # yyyyMM格式
            else:
                # 1分钟K线：trade_date转换为yyyyMMdd格式（文档要求）
                trade_date_str = trade_date.strftime('%Y%m%d')  # yyyyMMdd格式

            trade_time_str = trade_time.strftime('%H%M')     # hhmm格式
            trade_datetime_str = datetime_obj.strftime('%Y%m%d%H%M')  # yyyyMMddhhmm格式

            # 过滤日期范围
            if start_date and trade_date < start_date:
                continue
            if end_date and trade_date > end_date:
                continue

            minute_record = {
                'ts_code': f'{market}.{code}',
                'stock_code': code,
                'stock_name': None,  # 通过股票基本信息表映射获取
                'trade_date': trade_date_str,
                'trade_time': trade_time_str,
                'trade_datetime': trade_datetime_str,  # 新增字段，符合文档要求
                'open': round(float(row['open']), 4),
                'high': round(float(row['high']), 4),
                'low': round(float(row['low']), 4),
                'close': round(float(row['close']), 4),
                'preclose': None,  # 在后处理中计算
                'volume': int(row['volume']),
                'amount': round(float(row['amount']), 4),
                'adjust_flag': 3,  # 默认为不复权状态
                'change_rate': None,  # 在后处理中计算
                'turnover_rate': None  # 在后处理中计算
            }

            minute_data.append(minute_record)

        return minute_data

    def _post_process_daily_data(self, daily_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        后处理日K线数据 - 计算需要昨日收盘价的字段
        """
        if len(daily_data) <= 1:
            return daily_data

        # 按日期排序
        daily_data.sort(key=lambda x: x['trade_date'])

        # 计算涨跌幅和preclose字段
        for i in range(len(daily_data)):
            if i == 0:
                # 第一条记录：根据文档要求，如果没有获取到前一天数据，就使用本条数据open字段值
                daily_data[i]['preclose'] = daily_data[i]['open']
            else:
                # 使用前一交易日的收盘价作为preclose
                daily_data[i]['preclose'] = daily_data[i-1]['close']
                # 计算涨跌幅
                if daily_data[i]['preclose'] and daily_data[i]['close']:
                    daily_data[i]['change_rate'] = DataTransformer.calculate_change_rate(
                        daily_data[i]['close'], daily_data[i]['preclose']
                    )

        return daily_data

    def get_minute_data(
        self,
        code: str,
        data_type: str = '1min',  # 1min, 5min
        start_date: date = None,
        end_date: date = None
    ) -> List[Dict[str, Any]]:
        """
        读取通达信分钟K线文件 - 严格按照文档要求使用TdxLCMinBarReader

        Args:
            code: 股票代码
            data_type: 数据类型 ('1min' 或 '5min')
            start_date: 开始日期
            end_date: 结束日期
        """
        # 使用pytdx的TdxLCMinBarReader - 严格按照产品设计文档要求
        reader = TdxLCMinBarReader()
        minute_data = []

        # 根据数据类型确定目录和文件扩展名
        if data_type == '1min':
            subdir = 'minline'
            ext = '.lc1'
        elif data_type == '5min':
            subdir = 'fzline'
            ext = '.lc5'
        else:
            return minute_data

        # 支持三个市场：北交所(bj)、沪市(sh)、深证(sz)
        for market in ['bj', 'sh', 'sz']:
            # 严格按照要求：文件名是股票的不包含"."的ts编码（如sh000001.lc1）
            filename_without_dot = f'{market}{code}'  # 例如：sh000001
            filepath = os.path.join(self.vipdoc_path, market, subdir, f'{filename_without_dot}{ext}')

            if not os.path.exists(filepath):
                continue

            try:
                # 使用pytdx Reader获取DataFrame - 严格按照文档要求
                df = reader.get_df(filepath)

                # 转换为系统标准格式
                converted_data = self._convert_minute_dataframe(df, code, market, data_type, start_date, end_date)
                minute_data.extend(converted_data)

            except Exception as e:
                print(f"使用pytdx Reader读取文件 {filepath} 失败: {e}")

        # 后处理：计算涨跌幅和preclose字段
        minute_data = self._post_process_minute_data(minute_data, data_type)

        return minute_data

    def _post_process_minute_data(self, minute_data: List[Dict[str, Any]], data_type: str = '1min') -> List[Dict[str, Any]]:
        """
        后处理分钟K线数据 - 计算涨跌幅

        Args:
            minute_data: 分钟K线数据
            data_type: 数据类型 ('1min' 或 '5min')
        """
        if len(minute_data) <= 1:
            return minute_data

        # 按日期和时间排序
        minute_data.sort(key=lambda x: (x['trade_date'], x['trade_time']))

        # 计算涨跌幅和preclose字段
        for i in range(len(minute_data)):
            if i == 0:
                # 第一条记录：根据文档要求，如果没有获取到前一(分钟/5分钟)数据，就使用本条数据open字段值
                minute_data[i]['preclose'] = minute_data[i]['open']
            else:
                # 使用上一根K线的收盘价作为preclose
                preclose = minute_data[i-1]['close']
                minute_data[i]['preclose'] = preclose
                # 计算涨跌幅
                if preclose and minute_data[i]['close']:
                    minute_data[i]['change_rate'] = DataTransformer.calculate_change_rate(
                        minute_data[i]['close'], preclose
                    )

        return minute_data

    def get_financial_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Pytdx主要用于行情数据，财务数据建议使用baostock"""
        return None