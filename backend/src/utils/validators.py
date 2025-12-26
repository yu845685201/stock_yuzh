"""
数据验证工具
"""

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional


class ValidationError(Exception):
    """数据验证错误"""
    pass


class DataValidator:
    """数据验证器"""

    @staticmethod
    def validate_stock_code(code: str) -> bool:
        """
        验证股票代码格式

        Args:
            code: 股票代码

        Returns:
            bool: 是否有效
        """
        if not isinstance(code, str):
            return False
        return bool(re.match(r'^[0-9]{6}$', code))

    @staticmethod
    def validate_date_range(start_date: date, end_date: date) -> bool:
        """
        验证日期范围

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            bool: 是否有效
        """
        if not isinstance(start_date, date) or not isinstance(end_date, date):
            return False
        return start_date <= end_date

    @staticmethod
    def validate_year(year: int) -> bool:
        """
        验证年份

        Args:
            year: 年份

        Returns:
            bool: 是否有效
        """
        current_year = datetime.now().year
        return isinstance(year, int) and 1990 <= year <= current_year

    @staticmethod
    def validate_quarter(quarter: int) -> bool:
        """
        验证季度

        Args:
            quarter: 季度

        Returns:
            bool: 是否有效
        """
        return isinstance(quarter, int) and 1 <= quarter <= 4

    @staticmethod
    def validate_price(price: Optional[float]) -> bool:
        """
        验证价格

        Args:
            price: 价格

        Returns:
            bool: 是否有效
        """
        if price is None:
            return True
        return isinstance(price, (int, float)) and price > 0

    @staticmethod
    def validate_volume(volume: Optional[int]) -> bool:
        """
        验证成交量

        Args:
            volume: 成交量

        Returns:
            bool: 是否有效
        """
        if volume is None:
            return True
        return isinstance(volume, (int, float)) and volume >= 0

    @staticmethod
    def validate_stock_data(data: Dict[str, Any]) -> List[str]:
        """
        验证股票数据完整性

        Args:
            data: 股票数据字典

        Returns:
            List[str]: 错误信息列表
        """
        errors = []

        # 检查必需字段
        required_fields = ['code']
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"缺少必需字段: {field}")

        # 验证股票代码
        if 'code' in data:
            if not DataValidator.validate_stock_code(data['code']):
                errors.append(f"无效的股票代码: {data['code']}")

        # 验证日期
        if 'date' in data and data['date'] is not None:
            if not isinstance(data['date'], date):
                try:
                    if isinstance(data['date'], str):
                        datetime.strptime(data['date'], '%Y-%m-%d').date()
                    else:
                        errors.append(f"无效的日期格式: {data['date']}")
                except ValueError:
                    errors.append(f"无效的日期格式: {data['date']}")

        # 验证价格字段
        price_fields = ['open', 'high', 'low', 'close']
        for field in price_fields:
            if field in data and data[field] is not None:
                if not DataValidator.validate_price(data[field]):
                    errors.append(f"无效的{field}价格: {data[field]}")

        # 验证成交量
        if 'volume' in data and data['volume'] is not None:
            if not DataValidator.validate_volume(data['volume']):
                errors.append(f"无效的成交量: {data['volume']}")

        return errors

    @staticmethod
    def validate_financial_data(data: Dict[str, Any]) -> List[str]:
        """
        验证财务数据完整性

        Args:
            data: 财务数据字典

        Returns:
            List[str]: 错误信息列表
        """
        errors = []

        # 检查必需字段
        required_fields = ['code', 'year', 'quarter']
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"缺少必需字段: {field}")

        # 验证股票代码
        if 'code' in data:
            if not DataValidator.validate_stock_code(data['code']):
                errors.append(f"无效的股票代码: {data['code']}")

        # 验证年份
        if 'year' in data:
            if not DataValidator.validate_year(data['year']):
                errors.append(f"无效的年份: {data['year']}")

        # 验证季度
        if 'quarter' in data:
            if not DataValidator.validate_quarter(data['quarter']):
                errors.append(f"无效的季度: {data['quarter']}")

        # 验证数值字段
        numeric_fields = [
            'roe_avg', 'np_margin', 'gp_margin', 'net_profit',
            'eps_ttm', 'mb_revenue', 'total_share', 'liqa_share'
        ]
        for field in numeric_fields:
            if field in data and data[field] is not None:
                if not isinstance(data[field], (int, float)):
                    errors.append(f"无效的{field}数值类型: {data[field]}")

        return errors

    @staticmethod
    def validate_and_raise(data: Dict[str, Any], data_type: str = 'stock') -> None:
        """
        验证数据并在有错误时抛出异常

        Args:
            data: 数据字典
            data_type: 数据类型 ('stock' 或 'financial')

        Raises:
            ValidationError: 验证失败时抛出
        """
        if data_type == 'stock':
            errors = DataValidator.validate_stock_data(data)
        elif data_type == 'financial':
            errors = DataValidator.validate_financial_data(data)
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")

        if errors:
            raise ValidationError(f"数据验证失败: {'; '.join(errors)}")