"""
季度计算工具函数
根据股票上市日期计算基本面数据采集的起始季度
严格按照产品设计文档要求实现
"""
from datetime import datetime, date
from typing import Tuple, Optional


# 产品设计文档规定:baostock只能查到最早2007年的基本面信息
DEFAULT_START_YEAR = 2007
DEFAULT_START_QUARTER = 1
DEFAULT_START_DATE = date(2007, 1, 1)


def calculate_start_quarter(list_date: Optional[str]) -> Tuple[int, int]:
    """
    根据股票上市日期计算采集起始季度 - 严格按照产品设计文档要求

    产品设计文档规定:
    - 如果股票的list_date早于2007年1月1日,从[year=2007, quarter=1]开始查询
    - 如果股票的list_date晚于2007年1月1日(包含这天),从股票list_date所在季度开始查询
    - 如果list_date为NULL,使用默认策略(从2007Q1开始)

    Args:
        list_date: 股票上市日期,yyyyMMdd格式字符串,可能为None

    Returns:
        (起始年份, 起始季度)

    Examples:
        >>> calculate_start_quarter("20040329")  # 早于2007年
        (2007, 1)
        >>> calculate_start_quarter("20180627")  # 晚于2007年
        (2018, 2)
        >>> calculate_start_quarter(None)  # 空值
        (2007, 1)
        >>> calculate_start_quarter("20070101")  # 等于边界
        (2007, 1)
    """
    # 情况1: list_date为NULL - 使用默认策略
    if not list_date or not isinstance(list_date, str) or len(list_date) != 8:
        return (DEFAULT_START_YEAR, DEFAULT_START_QUARTER)

    try:
        # 解析yyyyMMdd格式的日期字符串
        list_date_obj = datetime.strptime(list_date, '%Y%m%d').date()
    except ValueError:
        # 日期格式错误 - 使用默认策略
        return (DEFAULT_START_YEAR, DEFAULT_START_QUARTER)

    # 情况2: list_date < 2007-01-01 - 从2007Q1开始
    if list_date_obj < DEFAULT_START_DATE:
        return (DEFAULT_START_YEAR, DEFAULT_START_QUARTER)

    # 情况3: list_date >= 2007-01-01 - 从list_date所在季度开始
    year = list_date_obj.year
    quarter = (list_date_obj.month - 1) // 3 + 1

    return (year, quarter)


def get_current_previous_quarter() -> Tuple[int, int]:
    """
    获取当前日期的前一个季度

    Returns:
        (前一季度年份, 前一季度季度号)

    Examples:
        # 假设当前日期为2025-04-19
        >>> get_current_previous_quarter()
        (2025, 1)
        # 假设当前日期为2025-01-15
        >>> get_current_previous_quarter()
        (2024, 4)
    """
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    # 计算前一个季度
    if current_quarter > 1:
        return (current_year, current_quarter - 1)
    else:
        return (current_year - 1, 4)
