"""
重试装饰器工具
"""

import time
from functools import wraps
from typing import Callable, Type, Tuple, Any


def retry(max_attempts: int = 3, delay: float = 1.0,
          exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """
    重试装饰器

    Args:
        max_attempts: 最大重试次数
        delay: 初始延迟时间（秒）
        exceptions: 需要重试的异常类型

    Usage:
        @retry(max_attempts=3, delay=1.0, exceptions=(ConnectionError,))
        def connect_database():
            # 连接数据库的逻辑
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        # 指数退避策略
                        wait_time = delay * (2 ** attempt)
                        print(f"操作失败，{wait_time}秒后重试 (尝试 {attempt + 1}/{max_attempts}): {e}")
                        time.sleep(wait_time)
                    else:
                        print(f"操作失败，已达到最大重试次数 {max_attempts}")

            raise last_exception

        return wrapper
    return decorator


def retry_async(max_attempts: int = 3, delay: float = 1.0,
                exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """
    异步重试装饰器

    Args:
        max_attempts: 最大重试次数
        delay: 初始延迟时间（秒）
        exceptions: 需要重试的异常类型
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        wait_time = delay * (2 ** attempt)
                        print(f"异步操作失败，{wait_time}秒后重试 (尝试 {attempt + 1}/{max_attempts}): {e}")
                        await asyncio.sleep(wait_time)
                    else:
                        print(f"异步操作失败，已达到最大重试次数 {max_attempts}")

            raise last_exception

        return wrapper
    return decorator