"""
数据库连接管理 - 支持连接池优化

R-06 拆分后本文件为瘦外观（facade）：DatabaseConnection 由各 mixin 组合而成，
对外保持 ``from src.database import DatabaseConnection`` 与全部既有方法不变。

职责分布：
- pool.py          连接池（DatabaseConnectionPool 单例）
- base.py          连接上下文与通用执行（ConnectionMixin）
- type_convert.py  日期/时间解析与转换表达式（TypeConvertMixin）
- ddl.py           表/分区/约束/索引 DDL 与迁移（DdlMixin）
- dao_fundamentals.py  基本面/交易日历/股票基础 DAO（FundamentalsDaoMixin）
- dao_kline_1min.py    1分钟K线分区 DAO（Kline1MinDaoMixin）
- dao_kline_day.py     日K线 DAO（KlineDayDaoMixin）
- dao_anal.py          立体K线 DAO（AnalDaoMixin）
"""

from .pool import DatabaseConnectionPool
from .base import ConnectionMixin
from .type_convert import TypeConvertMixin
from .ddl import DdlMixin
from .dao_fundamentals import FundamentalsDaoMixin
from .dao_kline_1min import Kline1MinDaoMixin
from .dao_kline_day import KlineDayDaoMixin
from .dao_anal import AnalDaoMixin


class DatabaseConnection(
    ConnectionMixin,
    TypeConvertMixin,
    DdlMixin,
    FundamentalsDaoMixin,
    Kline1MinDaoMixin,
    KlineDayDaoMixin,
    AnalDaoMixin,
):
    """数据库连接管理类 - 向后兼容，支持连接池优化"""
