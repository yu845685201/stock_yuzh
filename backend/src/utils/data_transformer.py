"""
数据转换工具 - 严格按照产品设计文档要求实现
"""

import struct
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, date, time
from pypinyin import pinyin, Style


class DataTransformer:
    """数据转换器，严格按照文档规范处理数据"""

    # 证券类型系数表 - 严格按照文档要求
    SECURITY_TYPE_COEFFICIENTS = {
        # 深圳市场
        'SZ_A_STOCK': {'price_coeff': 0.01, 'volume_coeff': 0.01, 'code_pattern': r'^00[0-9]{4}|^30[0-9]{3}'},
        'SZ_B_STOCK': {'price_coeff': 0.01, 'volume_coeff': 0.01, 'code_pattern': r'^20[0-9]{3}'},
        'SZ_INDEX': {'price_coeff': 0.01, 'volume_coeff': 1.0, 'code_pattern': r'^39[0-9]{3}'},
        'SZ_FUND': {'price_coeff': 0.001, 'volume_coeff': 0.01, 'code_pattern': r'^1[5-6][0-9]{3}'},
        'SZ_BOND': {'price_coeff': 0.001, 'volume_coeff': 0.01, 'code_pattern': r'^1[0-4][0-9]{3}|^10[0-9]{3}|^1[1-9][0-9]{3}|^20[0-9]{3}'},

        # 上海市场
        'SH_A_STOCK': {'price_coeff': 0.01, 'volume_coeff': 0.01, 'code_pattern': r'^6[0-9]{5}|^68[0-9]{4}'},
        'SH_B_STOCK': {'price_coeff': 0.001, 'volume_coeff': 0.01, 'code_pattern': r'^9[0-9]{5}'},
        'SH_INDEX': {'price_coeff': 0.01, 'volume_coeff': 1.0, 'code_pattern': r'^00[0-9]{3}|^88[0-9]{3}|^99[0-9]{3}'},
        'SH_FUND': {'price_coeff': 0.001, 'volume_coeff': 1.0, 'code_pattern': r'^5[0-1][0-9]{3}'},
        'SH_BOND': {'price_coeff': 0.001, 'volume_coeff': 1.0, 'code_pattern': r'^01[0-9]{3}|^1[0-9][0-9]{3}|^2[0-9][0-9]{3}'},

        # 北交所市场
        'BJ_A_STOCK': {'price_coeff': 0.01, 'volume_coeff': 0.01, 'code_pattern': r'^8[3-4][0-9]{4}|^87[0-9]{4}'},
        'BJ_INDEX': {'price_coeff': 0.01, 'volume_coeff': 1.0, 'code_pattern': r'^80[0-9]{3}|^89[0-9]{3}'}
    }

    @staticmethod
    def get_security_type(code: str, market: str) -> str:
        """
        根据股票代码和市场获取证券类型

        Args:
            code: 股票代码
            market: 市场代码 (sh/sz/bj)

        Returns:
            证券类型
        """
        import re

        for sec_type, config in DataTransformer.SECURITY_TYPE_COEFFICIENTS.items():
            if re.match(config['code_pattern'], code):
                # 确保市场匹配
                if sec_type.startswith('SZ') and market == 'sz':
                    return sec_type
                elif sec_type.startswith('SH') and market == 'sh':
                    return sec_type
                elif sec_type.startswith('BJ') and market == 'bj':
                    return sec_type

        # 默认返回A股类型
        if market == 'sz':
            return 'SZ_A_STOCK'
        elif market == 'bj':
            return 'BJ_A_STOCK'
        else:
            return 'SH_A_STOCK'

    @staticmethod
    def parse_day_file_data(raw_data: bytes, code: str, market: str) -> Dict[str, Any]:
        """
        解析.day文件数据 - 严格按照文档要求

        Args:
            raw_data: 原始字节数据 (32字节)
            code: 股票代码
            market: 市场代码

        Returns:
            解析后的数据字典
        """
        # 获取证券类型系数
        sec_type = DataTransformer.get_security_type(code, market)
        coeff = DataTransformer.SECURITY_TYPE_COEFFICIENTS[sec_type]

        # 解析原始数据 - 32字节格式：通达信标准格式
        # 格式：日期(4) + 开(4) + 高(4) + 低(4) + 收(4) + 成交额(4) + 成交量(4) + 保留(4)
        # 通达信价格以分为单位，需要应用证券类型系数
        values = struct.unpack('<iiiiiiii', raw_data)

        # 应用转换系数 - 统一使用证券类型系数
        trade_date = datetime.strptime(str(values[0]), '%Y%m%d').date()
        open_price = round(values[1] * coeff['price_coeff'], 4)
        high_price = round(values[2] * coeff['price_coeff'], 4)
        low_price = round(values[3] * coeff['price_coeff'], 4)
        close_price = round(values[4] * coeff['price_coeff'], 4)
        # 修复：昨收价不从文件直接解析，通过后处理计算
        preclose_price = None  # 将在后处理中通过前一日收盘价计算
        amount = round(values[5], 4)  # 成交额（修正字段位置）
        volume = int(values[6] * coeff['volume_coeff'])  # 成交量应用系数（修正字段位置）

        return {
            'trade_date': trade_date,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'preclose': preclose_price,  # 严格按照文档要求从文件解析获取
            'amount': amount,
            'volume': volume
        }

    @staticmethod
    def parse_minute_file_data(raw_data: bytes, code: str, market: str) -> Optional[Dict[str, Any]]:
        """
        解析.lc1/.lc5文件数据
        支持两种格式：
        1. 32字节格式 (新的.lc5/.lc1):
           u16(date) + u16(time) + f32(open) + f32(high) + f32(low) + f32(close) + f32(amount) + u32(volume) + u32(reserved)
           无需价格系数转换
        2. 28字节格式 (旧的.lc1):
           i32(datetime/ts) + f32(open) + f32(high) + f32(low) + f32(close) + f32(amount) + f32(volume)
           需要价格系数转换

        Args:
            raw_data: 原始字节数据
            code: 股票代码
            market: 市场代码

        Returns:
            解析后的数据字典，解析失败时返回None
        """
        try:
            # 32字节格式解析 (通常用于.lc5)
            if len(raw_data) == 32:
                # 解析结构: date(H), time(H), open(f), high(f), low(f), close(f), amount(f), volume(I), reserved(I)
                values = struct.unpack('<HHfffffII', raw_data)

                date_u16 = values[0]
                time_u16 = values[1]
                open_price = round(values[2], 4)
                high_price = round(values[3], 4)
                low_price = round(values[4], 4)
                close_price = round(values[5], 4)
                amount = round(values[6], 4)
                volume = int(values[7])
                # reserved = values[8]

                # 解析日期: High 5 bits=Year offset(2004), Mid 5 bits=Month, Low 6 bits=Day
                # 注意：位操作取决于具体格式，这里使用验证过的逻辑
                # 验证逻辑：year>>11, month>>6 & 0x1F, day & 0x3F
                year = (date_u16 >> 11) + 2004
                month = (date_u16 >> 6) & 0x1F
                day = date_u16 & 0x3F

                # 解析时间: minutes from 00:00
                hour = time_u16 // 60
                minute = time_u16 % 60

                try:
                    trade_date = date(year, month, day)
                    trade_time = time(hour, minute)

                    # 格式化为字符串
                    trade_date_str = trade_date.strftime('%Y%m%d')
                    trade_time_str = trade_time.strftime('%H%M')
                    trade_datetime_str = f"{trade_date_str}{trade_time_str}"

                    return {
                        'trade_date': trade_date,  # 保持date对象用于过滤
                        'trade_time': trade_time,  # 保持time对象
                        'trade_date_str': trade_date_str, # 格式化字符串
                        'trade_time_str': trade_time_str, # 格式化字符串
                        'trade_datetime': trade_datetime_str, # 完整时间字符串
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'amount': amount,
                        'volume': volume
                    }
                except ValueError:
                    return None

            # 兼容旧代码逻辑 (如果不幸传入了非32字节数据，或者旧格式)
            # ... 原有逻辑 ...
            # 获取证券类型系数
            sec_type = DataTransformer.get_security_type(code, market)
            coeff = DataTransformer.SECURITY_TYPE_COEFFICIENTS[sec_type]

            # 解析原始数据 (分钟线数据格式 32字节或28字节?)
            # 原代码假设 raw_data 是某种格式，用 'i6f' 解包 (28字节)
            if len(raw_data) == 28:
                values = struct.unpack('<i6f', raw_data)

                # 解析价格和成交量数据 (应用系数)
                open_price = round(values[1] * coeff['price_coeff'], 4)
                high_price = round(values[2] * coeff['price_coeff'], 4)
                low_price = round(values[3] * coeff['price_coeff'], 4)
                close_price = round(values[4] * coeff['price_coeff'], 4)
                amount = round(values[5], 4)
                volume = int(values[6] * coeff['volume_coeff'])

                # 时间戳解析 (同原逻辑)
                tongdaxin_timestamp = values[0]
                if tongdaxin_timestamp > 0:
                    timestamp_str = str(int(tongdaxin_timestamp))
                    trade_date = None
                    trade_time = None

                    if len(timestamp_str) == 12:  # YYYYMMDDHHMM
                        year = int(timestamp_str[:4])
                        month = int(timestamp_str[4:6])
                        day = int(timestamp_str[6:8])
                        hour = int(timestamp_str[8:10])
                        minute = int(timestamp_str[10:12])
                        if (1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31 and
                            0 <= hour <= 23 and 0 <= minute <= 59):
                            trade_date = date(year, month, day)
                            trade_time = time(hour, minute)
                    elif len(timestamp_str) == 8:  # YYYYMMDD
                        year = int(timestamp_str[:4])
                        month = int(timestamp_str[4:6])
                        day = int(timestamp_str[6:8])
                        if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                            trade_date = date(year, month, day)
                            trade_time = time(9, 30)
                    elif len(timestamp_str) == 10:  # Unix
                         try:
                            dt = datetime.fromtimestamp(tongdaxin_timestamp)
                            trade_date = dt.date()
                            trade_time = dt.time()
                         except:
                            pass

                    if trade_date and trade_time:
                        return {
                            'trade_date': trade_date,
                            'trade_time': trade_time,
                            'open': open_price,
                            'high': high_price,
                            'low': low_price,
                            'close': close_price,
                            'amount': amount,
                            'volume': volume
                        }

            return None

        except Exception as e:
            # 解析失败时返回None
            # print(f"Parse error: {e}")
            return None

    @staticmethod
    def generate_ts_code(code: str, market: str) -> str:
        """
        生成TS代码 - 严格按照文档要求

        Args:
            code: 股票编码 (6位)
            market: 市场代码 (sh/sz)

        Returns:
            TS代码格式：{market}.{code}，如sz.000001或sh.000001
        """
        return f"{market}.{code}"

    @staticmethod
    def parse_min5_file_data(raw_data: bytes, code: str, market: str) -> Optional[Dict[str, Any]]:
        """
        解析5分钟K线数据 - parse_minute_file_data的别名

        Args:
            raw_data: 原始字节数据
            code: 股票代码
            market: 市场代码

        Returns:
            解析后的数据字典，解析失败时返回None
        """
        return DataTransformer.parse_minute_file_data(raw_data, code, market)

    @staticmethod
    def extract_stock_code(full_code: str) -> str:
        """
        提取股票编码 - 严格按照文档要求

        Args:
            full_code: 完整代码 (如sz.000001或sh.600000)

        Returns:
            6位股票编码
        """
        # 去除市场前缀，提取6位代码
        if '.' in full_code:
            return full_code.split('.')[1]
        elif full_code.startswith(('sz.', 'sh.')):
            return full_code[3:]
        else:
            return full_code[-6:] if len(full_code) >= 6 else full_code

    @staticmethod
    def get_market_info(code: str, full_code: str = None) -> Dict[str, str]:
        """
        获取市场信息 - 严格按照文档要求

        Args:
            code: 股票编码
            full_code: 完整代码 (可选)

        Returns:
            市场信息字典
        """
        if full_code:
            if full_code.startswith('sz.'):
                market_code = 'sz'
                market_name = '深圳证券交易所'
                exchange_code = 'SZSE'
            elif full_code.startswith('sh.'):
                market_code = 'sh'
                market_name = '上海证券交易所'
                exchange_code = 'SSE'
            else:
                # 从股票编码推断
                market_code = 'sz' if code.startswith(('00', '30')) else 'sh'
                market_name = '深圳证券交易所' if market_code == 'sz' else '上海证券交易所'
                exchange_code = 'SZSE' if market_code == 'sz' else 'SSE'
        else:
            market_code = 'sz' if code.startswith(('00', '30')) else 'sh'
            market_name = '深圳证券交易所' if market_code == 'sz' else '上海证券交易所'
            exchange_code = 'SZSE' if market_code == 'sz' else 'SSE'

        return {
            'market_code': market_code,
            'market_name': market_name,
            'exchange_code': exchange_code
        }

    @staticmethod
    def generate_pinyin(name: str) -> Optional[str]:
        """
        生成拼音缩写 - 严格按照文档要求

        Args:
            name: 股票名称

        Returns:
            拼音缩写
        """
        if not name:
            return None

        try:
            # 获取拼音首字母
            pinyin_list = pinyin(name, style=Style.FIRST_LETTER)
            return ''.join([item[0] for item in pinyin_list]).upper()
        except Exception:
            return None

    @staticmethod
    def map_list_status(status: str) -> str:
        """
        映射上市状态 - 严格按照文档要求

        Args:
            status: baostock状态 (1/0)

        Returns:
            文档要求的状态 (L/D/P)
        """
        if status == '1':
            return 'L'  # 上市
        elif status == '0':
            return 'D'  # 退市
        else:
            return 'P'  # 暂停上市

    @staticmethod
    def calculate_change_rate(close: float, preclose: float) -> Optional[float]:
        """
        计算涨跌幅 - 严格按照文档公式

        Args:
            close: 收盘价
            preclose: 昨收盘价

        Returns:
            涨跌幅百分比
        """
        result = DataTransformer.calculate_change_rate_with_details(close, preclose)
        return result['change_rate'] if result else None

    @staticmethod
    def calculate_change_rate_with_details(close: float, preclose: float) -> Optional[Dict[str, Any]]:
        """
        计算涨跌幅并返回详细信息

        Args:
            close: 收盘价
            preclose: 昨收盘价

        Returns:
            包含涨跌幅和计算详情的字典，或None
        """
        if not close or not preclose or preclose == 0:
            return None

        # 计算原始涨跌幅
        raw_change_rate = (close - preclose) / preclose * 100
        change_rate = round(raw_change_rate, 4)

        # 检查是否超出精度限制
        is_truncated = False
        truncated_value = None

        if abs(change_rate) >= 10000:
            # 截断到最大可存储值 9999.9999%
            change_rate = 9999.9999 if change_rate > 0 else -9999.9999
            is_truncated = True
            truncated_value = change_rate
            print(f"⚠️  涨跌幅超出精度限制，截断为: {change_rate}%")

        return {
            'change_rate': change_rate,
            'raw_change_rate': round(raw_change_rate, 4),
            'is_truncated': is_truncated,
            'truncated_value': truncated_value,
            'close_price': close,
            'preclose_price': preclose,
            'calculation': f"({close} - {preclose}) / {preclose} * 100 = {raw_change_rate:.4f}%"
        }

    @staticmethod
    def calculate_turnover_rate(volume: int, float_share: float) -> Optional[float]:
        """
        计算换手率 - 严格按照文档公式

        Args:
            volume: 成交量
            float_share: 流通股本

        Returns:
            换手率百分比
        """
        if not volume or not float_share or float_share == 0:
            return None

        turnover_rate = round(volume / float_share * 100, 4)

        # 修复：超出数据库精度限制时截断而非设置为None
        if abs(turnover_rate) >= 10000:
            # 截断到最大可存储值 9999.9999%
            turnover_rate = 9999.9999 if turnover_rate > 0 else -9999.9999
            print(f"⚠️  换手率超出精度限制，截断为: {turnover_rate}%")

        return turnover_rate

    @staticmethod
    def check_is_st(stock_name: str) -> bool:
        """
        检查是否为ST股 - 严格按照文档要求

        Args:
            stock_name: 股票名称

        Returns:
            是否为ST股
        """
        if not stock_name:
            return False

        return stock_name.startswith(('ST', '*ST', 'S*ST', 'SST'))

    @staticmethod
    def format_date_string(date_str: str) -> Optional[date]:
        """
        格式化日期字符串

        Args:
            date_str: 日期字符串

        Returns:
            格式化后的日期
        """
        if not date_str:
            return None

        try:
            return datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            return None