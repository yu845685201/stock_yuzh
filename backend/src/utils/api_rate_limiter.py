"""
API限流器
用于控制API调用频率，避免超过服务端限制

功能：
- 每N次API调用后休眠指定秒数
- 线程安全
- 可配置限流参数
- 支持完全禁用限流
- 静默执行，无额外输出
"""

import time
import threading
from typing import Optional


class ApiRateLimiter:
    """
    API调用限流器

    功能：
    - 每N次API调用后休眠指定秒数
    - 线程安全
    - 可配置限流参数
    - 静默执行，无额外输出
    """

    def __init__(self, calls_per_period: int = 50, sleep_duration: float = 1.0, enabled: bool = True):
        """
        初始化限流器

        Args:
            calls_per_period: 一个周期内的API调用次数，默认50次。
                             当设置为0或负数时，表示不限流
            sleep_duration: 达到限制后的休眠时间（秒），默认1秒
            enabled: 是否启用限流，默认True
        """
        # 参数验证
        if sleep_duration < 0:
            raise ValueError("sleep_duration不能为负数")
        # 允许0或负数表示不限流
        if calls_per_period == 0 or calls_per_period < 0:
            self.unlimited = True
            self.calls_per_period = float('inf')  # 设为无穷大
        else:
            self.unlimited = False
            self.calls_per_period = calls_per_period

        self.sleep_duration = sleep_duration
        self.enabled = enabled
        self.call_count = 0
        self.lock = threading.Lock()

    def wait_if_needed(self) -> None:
        """
        检查是否需要限流，如果需要则休眠

        此方法是线程安全的，可在多线程环境中使用
        """
        # 如果限流被禁用，直接返回
        if not self.enabled:
            return

        # 如果设置为不限流，直接返回
        if self.unlimited:
            return

        with self.lock:
            self.call_count += 1

            # 当达到调用次数限制时，准备休眠并重置计数器
            if self.call_count >= self.calls_per_period:
                should_sleep = True
                self.call_count = 0  # 立即重置计数器，避免竞态条件
            else:
                should_sleep = False

        # 在锁外进行休眠，避免阻塞其他线程
        if should_sleep:
            self._sleep_with_retry()

    def _sleep_with_retry(self) -> None:
        """带重试的休眠方法"""
        try:
            time.sleep(self.sleep_duration)
        except (OSError, ValueError) as e:
            # 休眠异常时重试（符合用户需求：限流异常重试）
            time.sleep(0.1)
            try:
                time.sleep(self.sleep_duration)
            except (OSError, ValueError):
                # 如果仍然失败，放弃休眠，继续执行
                pass

    def reset(self) -> None:
        """重置调用计数器"""
        with self.lock:
            self.call_count = 0

    def get_stats(self) -> dict:
        """
        获取当前统计信息

        Returns:
            包含调用次数和配置信息的字典
        """
        with self.lock:
            if self.unlimited:
                return {
                    'current_calls': self.call_count,
                    'max_calls_per_period': 'unlimited',
                    'sleep_duration': self.sleep_duration,
                    'remaining_calls': 'unlimited',
                    'unlimited': True
                }
            else:
                return {
                    'current_calls': self.call_count,
                    'max_calls_per_period': self.calls_per_period,
                    'sleep_duration': self.sleep_duration,
                    'remaining_calls': self.calls_per_period - self.call_count,
                    'unlimited': False
                }