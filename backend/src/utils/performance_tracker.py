"""
性能跟踪器 - 用于记录日K线数据采集各关键节点的耗时
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

    def __init__(self, operation_name: str = "日K线数据采集"):
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


class DailyKLinePerformanceTracker(PerformanceTracker):
    """日K线数据采集专用性能跟踪器"""

    def __init__(self):
        super().__init__("日K线数据采集")

    def start_file_scanning(self):
        """开始文件扫描"""
        self.start_metric("读取day文件", {"阶段": "文件扫描"})

    def end_file_scanning(self, files_count: int, data_count: int):
        """结束文件扫描"""
        self.end_metric("读取day文件", {
            "扫描文件数": files_count,
            "数据记录数": data_count
        })

    def start_data_assembly(self):
        """开始数据组装"""
        self.start_metric("数据组装", {"阶段": "数据处理"})

    def end_data_assembly(self, records_count: int):
        """结束数据组装"""
        self.end_metric("数据组装", {
            "组装记录数": records_count
        })

    def start_csv_generation(self):
        """开始CSV生成"""
        self.start_metric("生成CSV文件", {"阶段": "文件输出"})

    def end_csv_generation(self, csv_files_count: int, records_count: int):
        """结束CSV生成"""
        self.end_metric("生成CSV文件", {
            "生成文件数": csv_files_count,
            "写入记录数": records_count
        })

    def start_database_write(self):
        """开始数据库写入"""
        self.start_metric("写库操作", {"阶段": "数据库操作"})

    def end_database_write(self, records_count: int, batches_count: int):
        """结束数据库写入"""
        self.end_metric("写库操作", {
            "写入记录数": records_count,
            "批次数": batches_count
        })

    def print_daily_summary(self):
        """打印日K线数据采集专用的性能摘要"""
        self.print_summary()

        # 添加额外分析
        file_time = self.get_metric_time("读取day文件")
        csv_time = self.get_metric_time("生成CSV文件")
        db_time = self.get_metric_time("写库操作")

        if file_time and csv_time and db_time:
            iob_time = csv_time + db_time
            print(f"\n🔍 性能分析:")
            print(f"  - 纯I/O操作耗时(CSV+写库): {iob_time:.3f}s")
            print(f"  - I/O操作占比: {(iob_time / self.get_total_time() * 100):.1f}%")

            # 性能建议
            if file_time > iob_time * 2:
                print(f"  💡 建议: 文件读取是瓶颈，可考虑优化文件解析或增加缓存")
            elif iob_time > file_time * 2:
                print(f"  💡 建议: I/O操作是瓶颈，可考虑批量优化或异步处理")