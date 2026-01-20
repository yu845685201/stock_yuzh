"""
性能跟踪器 - 用于记录数据采集各关键节点的耗时
"""

import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field


@dataclass
class PerformanceMetric:
    """性能指标"""
    name: str
    start_time: float
    end_time: Optional[float] = None
    elapsed_time: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def start(self):
        """开始计时"""
        self.start_time = time.time()

    def end(self):
        """结束计时"""
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time

    def get_elapsed_time(self) -> Optional[float]:
        """获取耗时"""
        if self.elapsed_time is not None:
            return self.elapsed_time
        elif self.start_time:
            return time.time() - self.start_time
        return None

    def __str__(self) -> str:
        elapsed = self.get_elapsed_time()
        if elapsed is not None:
            return f"{self.name}: {elapsed:.3f}s"
        return f"{self.name}: 进行中"


class PerformanceTracker:
    """性能跟踪器，记录关键节点耗时"""

    def __init__(self, operation_name: str = "数据采集"):
        """
        初始化性能跟踪器

        Args:
            operation_name: 操作名称
        """
        self.operation_name = operation_name
        self.metrics: Dict[str, PerformanceMetric] = {}
        self.start_time = time.time()
        self.end_time: Optional[float] = None

    def start_metric(self, metric_name: str, details: Dict[str, Any] = None):
        """
        开始记录一个性能指标

        Args:
            metric_name: 指标名称
            details: 详细信息
        """
        if metric_name not in self.metrics:
            self.metrics[metric_name] = PerformanceMetric(
                name=metric_name,
                start_time=time.time(),
                details=details or {}
            )
        else:
            # 如果指标已存在，重新开始计时
            self.metrics[metric_name].start()

    def end_metric(self, metric_name: str, details: Dict[str, Any] = None):
        """
        结束记录一个性能指标

        Args:
            metric_name: 指标名称
            details: 详细信息
        """
        if metric_name in self.metrics:
            self.metrics[metric_name].end()
            if details:
                self.metrics[metric_name].details.update(details)

    def add_metric_detail(self, metric_name: str, key: str, value: Any):
        """
        为性能指标添加详细信息

        Args:
            metric_name: 指标名称
            key: 信息键
            value: 信息值
        """
        if metric_name in self.metrics:
            self.metrics[metric_name].details[key] = value

    def get_metric_time(self, metric_name: str) -> Optional[float]:
        """
        获取指定指标的耗时

        Args:
            metric_name: 指标名称

        Returns:
            耗时（秒），如果指标不存在或未完成则返回None
        """
        if metric_name in self.metrics:
            return self.metrics[metric_name].get_elapsed_time()
        return None

    def finish(self):
        """完成整个操作的跟踪"""
        self.end_time = time.time()

    def get_total_time(self) -> float:
        """获取总耗时"""
        end_time = self.end_time or time.time()
        return end_time - self.start_time

    def print_summary(self):
        """打印性能统计摘要"""
        total_time = self.get_total_time()
        print(f"\n⏱️  {self.operation_name}性能统计:")
        print(f"  总耗时: {total_time:.2f}s")
        print("\n📊 各节点耗时详情:")

        for name, metric in self.metrics.items():
            elapsed = metric.get_elapsed_time()
            if elapsed is not None:
                percentage = (elapsed / total_time) * 100 if total_time > 0 else 0
                print(f"  - {metric.name}: {elapsed:.3f}s ({percentage:.1f}%)")

                # 显示详细信息
                if metric.details:
                    for key, value in metric.details.items():
                        print(f"    {key}: {value}")
            else:
                print(f"  - {metric.name}: 进行中...")

    def get_summary_dict(self) -> Dict[str, Any]:
        """
        获取性能统计摘要字典

        Returns:
            包含所有性能统计信息的字典
        """
        total_time = self.get_total_time()
        summary = {
            'operation_name': self.operation_name,
            'total_time': total_time,
            'metrics': {}
        }

        for name, metric in self.metrics.items():
            elapsed = metric.get_elapsed_time()
            if elapsed is not None:
                percentage = (elapsed / total_time) * 100 if total_time > 0 else 0
                summary['metrics'][name] = {
                    'elapsed_time': elapsed,
                    'percentage': percentage,
                    'details': metric.details
                }

        return summary
