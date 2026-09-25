"""
轻量函数式重试（R-14：统一 tdx_api_source / baostock_source / thread_safe_baostock
三处手写重试循环；重试次数、退避序列、日志与重连语义由调用方经参数保留）
"""

import time
from typing import Any, Callable, Optional, Tuple, Type


def run_with_retry(
    fn: Callable[[], Any],
    *,
    attempts: int,
    backoff: Optional[Callable[[int], float]] = None,
    retry_on: Tuple[Type[BaseException], ...] = (Exception,),
    on_error: Optional[Callable[[int, BaseException], None]] = None,
) -> Any:
    """按次数重试执行 fn。

    Args:
        fn: 单次尝试的无参函数；抛异常即失败，返回值即成功
        attempts: 总尝试次数（≥1）
        backoff: 第 attempt 次（1-based）失败后的休眠秒数；返回 0/None 不休眠；
                 最后一次失败后不调用（与原实现 `if attempt < retries` 语义一致）
        retry_on: 触发重试的异常类型元组
        on_error: 每次失败回调 (attempt(1-based), exc)，调用方借此保留原日志/重连语义

    Returns:
        fn 的返回值

    Raises:
        重试耗尽后抛出最后一次捕获的异常
    """
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as e:
            last_error = e
            if on_error is not None:
                on_error(attempt, e)
            if attempt < attempts and backoff is not None:
                delay = backoff(attempt)
                if delay:
                    time.sleep(delay)
    raise last_error
