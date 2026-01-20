"""
数据转换工具 - 严格按照产品设计文档要求实现
"""

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
