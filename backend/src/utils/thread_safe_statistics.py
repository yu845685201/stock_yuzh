"""
线程安全的统计信息收集器
用于多线程环境下的性能统计
"""

import threading
import time
from typing import Dict, Any, Optional
from datetime import datetime
from ..models.collection_result import CollectionResult, CollectionStatistics


class ThreadSafeStatistics:
    """线程安全的统计信息收集器"""

    def __init__(self):
        """初始化线程安全统计器"""
        self.lock = threading.Lock()

        # 基础统计
        self.total_stocks = 0
        self.successful = 0
        self.failed = 0
        self.no_data = 0
        self.error_count = 0

        # 时间统计
        self.start_time = datetime.now()
        self.end_time: Optional[datetime] = None
        self.duration: Optional[float] = None

        # 批次统计
        self.batch_count = 0

        # 详细耗时统计
        self.timing = {
            'baostock_total': 0.0,
            'csv_total': 0.0,
            'db_total': 0.0,
            'baostock_calls': 0,
            'csv_batches': 0,
            'db_batches': 0
        }

        # 集合统计对象
        self.collection_stats = CollectionStatistics()

        # CSV文件列表
        self.csv_files = []

    def add_result(self, result: CollectionResult) -> None:
        """
        添加采集结果到统计中

        Args:
            result: CollectionResult对象
        """
        with self.lock:
            self.total_stocks += 1

            if result.execution_time:
                if result.is_success:
                    self.successful += 1
                    self.timing['baostock_total'] += result.execution_time
                    self.timing['baostock_calls'] += 1
                elif result.is_no_data:
                    self.no_data += 1
                    self.timing['baostock_total'] += result.execution_time
                    self.timing['baostock_calls'] += 1
                else:  # ERROR
                    self.error_count += 1
                    self.failed += 1
                    self.timing['baostock_total'] += result.execution_time
                    self.timing['baostock_calls'] += 1

            # 更新聚合统计
            self.collection_stats.add_result(result)

    def increment_batch_count(self) -> None:
        """增加批次计数"""
        with self.lock:
            self.batch_count += 1

    def add_csv_timing(self, duration: float) -> None:
        """
        添加CSV写入时间

        Args:
            duration: CSV写入耗时（秒）
        """
        with self.lock:
            self.timing['csv_total'] += duration
            self.timing['csv_batches'] += 1

    def add_database_timing(self, duration: float) -> None:
        """
        添加数据库写入时间

        Args:
            duration: 数据库写入耗时（秒）
        """
        with self.lock:
            self.timing['db_total'] += duration
            self.timing['db_batches'] += 1

    def add_csv_file(self, file_path: str) -> None:
        """
        添加CSV文件路径

        Args:
            file_path: CSV文件路径
        """
        with self.lock:
            self.csv_files.append(file_path)

    def finish(self) -> None:
        """完成统计，计算总耗时"""
        with self.lock:
            self.end_time = datetime.now()
            if self.start_time:
                self.duration = (self.end_time - self.start_time).total_seconds()

    def get_stats(self) -> Dict[str, Any]:
        """
        获取当前统计信息

        Returns:
            统计信息字典
        """
        with self.lock:
            stats = {
                'total_stocks': self.total_stocks,
                'successful': self.successful,
                'failed': self.failed,
                'no_data': self.no_data,
                'error_count': self.error_count,
                'csv_files': self.csv_files.copy(),
                'start_time': self.start_time,
                'batch_count': self.batch_count,
                'timing': self.timing.copy(),
                'collection_stats': self.collection_stats.to_dict()
            }

            if self.end_time and self.duration:
                stats['end_time'] = self.end_time
                stats['duration'] = self.duration

            # 计算成功率
            if self.total_stocks > 0:
                stats['success_rate'] = self.collection_stats.real_success_rate
                stats['completion_rate'] = self.collection_stats.completion_rate
                stats['error_rate'] = self.collection_stats.error_rate

            # 向后兼容：failed现在只包含真正的异常失败
            stats['failed'] = self.error_count

            return stats

    def get_progress_info(self) -> tuple[int, int, float]:
        """
        获取进度信息

        Returns:
            tuple: (当前数量, 总数, 进度百分比)
        """
        with self.lock:
            current = self.successful + self.no_data + self.error_count
            total = self.total_stocks
            percentage = (current / total * 100) if total > 0 else 0.0
            return current, total, percentage

    def __str__(self) -> str:
        """字符串表示"""
        current, total, percentage = self.get_progress_info()
        return f"ThreadSafeStatistics(progress={current}/{total}={percentage:.1f}%)"

    def __repr__(self) -> str:
        return self.__str__()