"""
性能装饰器模块
为现有方法添加性能监控功能
"""

import time
import functools
from typing import Callable, Any, Optional
from .performance_monitor import performance_monitor


def monitor_performance(operation_name: str = None):
    """
    性能监控装饰器

    Args:
        operation_name: 操作名称，如果为None则使用函数名
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 确定操作名称
            op_name = operation_name or f"{func.__module__}.{func.__name__}"

            # 使用性能监控器
            with performance_monitor.monitor_operation(op_name) as metrics:
                try:
                    # 调用原函数
                    result = func(*args, **kwargs)

                    # 记录成功的结果统计
                    if isinstance(result, dict):
                        if 'stocks_count' in result:
                            metrics.record_stock_processed()
                            # 如果是股票数量，需要乘以数量
                            stocks_count = result['stocks_count']
                            for _ in range(stocks_count):
                                metrics.record_stock_processed()

                        if 'daily_data_count' in result:
                            metrics.record_data_records(result['daily_data_count'])

                        if 'financial_data_count' in result:
                            metrics.record_data_records(result['financial_data_count'])

                        if 'min5_data_count' in result:
                            metrics.record_data_records(result['min5_data_count'])

                    elif isinstance(result, int):
                        # 如果返回的是数量，记录为数据记录
                        metrics.record_data_records(result)

                    elif isinstance(result, list):
                        # 如果返回的是列表，记录为股票数量
                        metrics.record_stock_processed()
                        metrics.record_data_records(len(result))

                    return result

                except Exception as e:
                    # 记录错误
                    metrics.record_error()
                    raise

        return wrapper
    return decorator


def monitor_api_calls(api_name: str = None):
    """
    API调用监控装饰器

    Args:
        api_name: API名称
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            api_name_str = api_name or f"{func.__module__}.{func.__name__}"

            start_time = time.time()
            success = False

            try:
                result = func(*args, **kwargs)
                success = True
                return result

            except Exception as e:
                success = False
                raise

            finally:
                response_time = time.time() - start_time
                performance_monitor.record_api_call(success, response_time)

        return wrapper
    return decorator


def monitor_db_operations(operation_name: str = None):
    """
    数据库操作监控装饰器

    Args:
        operation_name: 操作名称
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            op_name = operation_name or f"{func.__module__}.{func.__name__}"

            start_time = time.time()
            success = False

            try:
                result = func(*args, **kwargs)
                success = True

                # 如果是批量操作，记录影响的记录数
                if isinstance(result, int) and result > 0:
                    # 假设返回的是影响的行数
                    pass

                return result

            except Exception as e:
                success = False
                raise

            finally:
                response_time = time.time() - start_time
                performance_monitor.record_db_operation(success, response_time)

        return wrapper
    return decorator


class PerformanceContext:
    """性能上下文管理器，用于手动监控代码块"""

    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.metrics = None

    def __enter__(self):
        self.metrics = performance_monitor.monitor_operation(self.operation_name).__enter__()
        return self.metrics

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.metrics.record_error()
        return performance_monitor.monitor_operation(self.operation_name).__exit__(exc_type, exc_val, exc_tb)