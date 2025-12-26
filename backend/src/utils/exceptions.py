"""
自定义异常类
"""


class StockAnalysisError(Exception):
    """股票分析系统基础异常"""
    pass


class DataSourceError(StockAnalysisError):
    """数据源相关异常"""
    pass


class DataSourceConnectionError(DataSourceError):
    """数据源连接异常"""
    pass


class DataSourceNotFoundError(DataSourceError):
    """数据源未找到异常"""
    pass


class DataValidationError(StockAnalysisError):
    """数据验证异常"""
    pass


class DatabaseError(StockAnalysisError):
    """数据库操作异常"""
    pass


class DatabaseConnectionError(DatabaseError):
    """数据库连接异常"""
    pass


class ConfigurationError(StockAnalysisError):
    """配置错误异常"""
    pass


class SyncError(StockAnalysisError):
    """数据同步异常"""
    pass


class TaskExecutionError(SyncError):
    """任务执行异常"""
    pass


class RetryExhaustedError(TaskExecutionError):
    """重试次数耗尽异常"""
    pass