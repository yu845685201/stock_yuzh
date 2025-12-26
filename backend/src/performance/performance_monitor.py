"""
性能监控模块
用于监控系统性能指标，包括CPU、内存、API调用等
"""

import time
import psutil
import threading
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from contextlib import contextmanager


@dataclass
class PerformanceMetrics:
    """性能指标数据类"""
    operation: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: float = 0.0

    # API调用统计
    api_calls: int = 0
    api_success: int = 0
    api_failures: int = 0
    api_response_time: float = 0.0

    # 数据库操作统计
    db_operations: int = 0
    db_success: int = 0
    db_failures: int = 0
    db_response_time: float = 0.0

    # 数据处理统计
    stocks_processed: int = 0
    data_records: int = 0
    error_count: int = 0

    # 内存使用统计
    memory_start: float = 0.0
    memory_peak: float = 0.0
    memory_end: float = 0.0

    def record_error(self):
        """记录错误（实例方法版本）"""
        self.error_count += 1

    def record_data_records(self, count: int):
        """记录数据记录数量（实例方法版本）"""
        self.data_records += count

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'operation': self.operation,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'duration': self.duration,
            'api_calls': self.api_calls,
            'api_success': self.api_success,
            'api_failures': self.api_failures,
            'api_success_rate': self.api_success / max(self.api_calls, 1),
            'avg_api_response_time': self.api_response_time / max(self.api_calls, 1),
            'db_operations': self.db_operations,
            'db_success': self.db_success,
            'db_failures': self.db_failures,
            'db_success_rate': self.db_success / max(self.db_operations, 1),
            'avg_db_response_time': self.db_response_time / max(self.db_operations, 1),
            'stocks_processed': self.stocks_processed,
            'data_records': self.data_records,
            'error_count': self.error_count,
            'memory_start': self.memory_start,
            'memory_peak': self.memory_peak,
            'memory_end': self.memory_end,
            'memory_delta': self.memory_end - self.memory_start,
            'throughput': self.stocks_processed / max(self.duration, 0.001)  # 股票/秒
        }


class PerformanceMonitor:
    """性能监控器"""

    def __init__(self):
        self.current_metrics: Optional[PerformanceMetrics] = None
        self.metrics_history: List[PerformanceMetrics] = []
        self._memory_monitor_thread: Optional[threading.Thread] = None
        self._monitoring = False
        self._lock = threading.Lock()

    @contextmanager
    def monitor_operation(self, operation: str):
        """性能监控上下文管理器"""
        # 启动监控
        metrics = PerformanceMetrics(
            operation=operation,
            start_time=datetime.now(),
            memory_start=self._get_memory_usage()
        )

        with self._lock:
            self.current_metrics = metrics

        # 启动内存监控线程
        self._start_memory_monitor()

        try:
            yield metrics
        finally:
            # 结束监控
            metrics.end_time = datetime.now()
            metrics.duration = (metrics.end_time - metrics.start_time).total_seconds()
            metrics.memory_end = self._get_memory_usage()

            # 停止内存监控
            self._stop_memory_monitor()

            # 保存指标
            with self._lock:
                self.metrics_history.append(metrics)
                self.current_metrics = None

    def record_api_call(self, success: bool, response_time: float):
        """记录API调用"""
        with self._lock:
            if self.current_metrics:
                self.current_metrics.api_calls += 1
                if success:
                    self.current_metrics.api_success += 1
                else:
                    self.current_metrics.api_failures += 1
                self.current_metrics.api_response_time += response_time

    def record_db_operation(self, success: bool, response_time: float):
        """记录数据库操作"""
        with self._lock:
            if self.current_metrics:
                self.current_metrics.db_operations += 1
                if success:
                    self.current_metrics.db_success += 1
                else:
                    self.current_metrics.db_failures += 1
                self.current_metrics.db_response_time += response_time

    def record_stock_processed(self):
        """记录处理的股票数量"""
        with self._lock:
            if self.current_metrics:
                self.current_metrics.stocks_processed += 1

    def record_data_records(self, count: int):
        """记录数据记录数量"""
        with self._lock:
            if self.current_metrics:
                self.current_metrics.data_records += count

    def record_error(self):
        """记录错误"""
        with self._lock:
            if self.current_metrics:
                self.current_metrics.error_count += 1

    def _get_memory_usage(self) -> float:
        """获取当前内存使用量（MB）"""
        try:
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024  # 转换为MB
        except Exception:
            return 0.0

    def _start_memory_monitor(self):
        """启动内存监控线程"""
        self._monitoring = True
        self._memory_monitor_thread = threading.Thread(target=self._monitor_memory)
        self._memory_monitor_thread.daemon = True
        self._memory_monitor_thread.start()

    def _stop_memory_monitor(self):
        """停止内存监控线程"""
        self._monitoring = False
        if self._memory_monitor_thread:
            self._memory_monitor_thread.join(timeout=1.0)

    def _monitor_memory(self):
        """内存监控线程"""
        while self._monitoring:
            with self._lock:
                if self.current_metrics:
                    current_memory = self._get_memory_usage()
                    self.current_metrics.memory_peak = max(
                        self.current_metrics.memory_peak, current_memory
                    )
            time.sleep(0.1)  # 每100ms检查一次

    def get_current_metrics(self) -> Optional[PerformanceMetrics]:
        """获取当前性能指标"""
        with self._lock:
            return self.current_metrics

    def get_metrics_history(self, operation: str = None,
                          hours: int = 24) -> List[PerformanceMetrics]:
        """获取历史性能指标"""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        with self._lock:
            history = self.metrics_history.copy()

        # 过滤条件
        if operation:
            history = [m for m in history if m.operation == operation]

        history = [m for m in history if m.start_time >= cutoff_time]

        return history

    def get_real_time_metrics(self) -> Dict[str, Dict[str, Any]]:
        """获取实时性能指标"""
        with self._lock:
            recent_metrics = [m for m in self.metrics_history
                            if m.start_time >= datetime.now() - timedelta(hours=1)]

        # 按操作类型分组
        real_time = {}
        for metrics in recent_metrics:
            if metrics.operation not in real_time:
                real_time[metrics.operation] = {
                    'execution_count': 0,
                    'total_duration': 0.0,
                    'total_stocks': 0,
                    'total_records': 0,
                    'api_success_rate': 0.0,
                    'db_success_rate': 0.0,
                    'avg_memory_usage': 0.0
                }

            op_data = real_time[metrics.operation]
            op_data['execution_count'] += 1
            op_data['total_duration'] += metrics.duration
            op_data['total_stocks'] += metrics.stocks_processed
            op_data['total_records'] += metrics.data_records

            if metrics.api_calls > 0:
                op_data['api_success_rate'] += (metrics.api_success / metrics.api_calls)

            if metrics.db_operations > 0:
                op_data['db_success_rate'] += (metrics.db_success / metrics.db_operations)

            op_data['avg_memory_usage'] += metrics.memory_peak

        # 计算平均值
        for op_data in real_time.values():
            if op_data['execution_count'] > 0:
                op_data['api_success_rate'] /= op_data['execution_count']
                op_data['db_success_rate'] /= op_data['execution_count']
                op_data['avg_memory_usage'] /= op_data['execution_count']

        return real_time

    def clear_history(self, days: int = 30):
        """清理历史数据"""
        cutoff_time = datetime.now() - timedelta(days=days)

        with self._lock:
            self.metrics_history = [
                m for m in self.metrics_history
                if m.start_time >= cutoff_time
            ]


# 全局性能监控器实例
performance_monitor = PerformanceMonitor()