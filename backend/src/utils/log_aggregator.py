"""
日志汇总器 - 用于收集和显示CSV/数据库写入的汇总信息
"""

import time
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field


@dataclass
class OperationSummary:
    """操作汇总信息"""
    operation_type: str  # 'csv' 或 'database'
    files_count: int = 0
    records_count: int = 0
    elapsed_time: float = 0.0
    success_count: int = 0
    error_count: int = 0
    details: List[str] = field(default_factory=list)


@dataclass
class AnomalySummary:
    """异常汇总信息"""
    total_anomalies: int = 0
    error_count: int = 0
    warning_count: int = 0
    anomaly_types: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    affected_stocks: Set[str] = field(default_factory=set)
    affected_dates: Set[str] = field(default_factory=set)
    anomaly_records: List[Dict[str, Any]] = field(default_factory=list)


class LogAggregator:
    """
    日志汇总器 - 收集CSV和数据库写入的统计信息
    提供统一的汇总报告功能

    用于替代原有的单个文件日志输出，提供批量处理的汇总信息展示

    依赖模块:
    - time: 用于计算操作耗时
    - typing: 提供类型注解支持
    - dataclasses: 用于定义数据类
    """

    def __init__(self) -> None:
        """初始化日志汇总器"""
        self.summaries: Dict[str, OperationSummary] = {}
        self.anomaly_summary: AnomalySummary = AnomalySummary()
        self.start_time: Optional[float] = None

    def start_operation(self, operation_type: str):
        """
        开始操作统计

        Args:
            operation_type: 操作类型 ('csv' 或 'database')
        """
        if operation_type not in self.summaries:
            self.summaries[operation_type] = OperationSummary(operation_type=operation_type)

        if self.start_time is None:
            self.start_time = time.time()

    def add_file_summary(self, filename: str, records_count: int, operation_type: str = 'csv'):
        """
        添加文件汇总信息

        Args:
            filename: 文件名
            records_count: 记录数量
            operation_type: 操作类型
        """
        if operation_type not in self.summaries:
            self.start_operation(operation_type)

        summary = self.summaries[operation_type]
        summary.files_count += 1
        summary.records_count += records_count
        summary.success_count += 1

        # 移除文件详情收集，避免显示无意义的文件列表

    def add_batch_summary(self, batch_count: int, records_count: int, operation_type: str = 'database'):
        """
        批量添加汇总信息（主要用于数据库批次）

        Args:
            batch_count: 批次数量
            records_count: 记录数量
            operation_type: 操作类型
        """
        if operation_type not in self.summaries:
            self.start_operation(operation_type)

        summary = self.summaries[operation_type]
        summary.files_count += batch_count  # 对于数据库，files_count表示批次数
        summary.records_count += records_count
        summary.success_count += batch_count

    def add_error(self, operation_type: str, error_msg: str = ""):
        """
        添加错误统计

        Args:
            operation_type: 操作类型
            error_msg: 错误信息
        """
        if operation_type not in self.summaries:
            self.start_operation(operation_type)

        summary = self.summaries[operation_type]
        summary.error_count += 1

    def finish_operation(self, operation_type: str):
        """
        完成操作并计算耗时

        Args:
            operation_type: 操作类型
        """
        if operation_type in self.summaries and self.start_time:
            self.summaries[operation_type].elapsed_time = time.time() - self.start_time

    def get_summary(self, operation_type: str) -> Optional[OperationSummary]:
        """
        获取指定操作的汇总信息

        Args:
            operation_type: 操作类型

        Returns:
            操作汇总信息
        """
        return self.summaries.get(operation_type)

    def print_summary(self, operation_type: str = None):
        """
        打印汇总报告

        Args:
            operation_type: 操作类型，如果为None则打印所有操作
        """
        if not self.summaries:
            return

        # 计算总耗时
        total_time = time.time() - self.start_time if self.start_time else 0

        if operation_type:
            summaries_to_print = {operation_type: self.summaries[operation_type]} if operation_type in self.summaries else {}
        else:
            summaries_to_print = self.summaries

        for op_type, summary in summaries_to_print.items():
            self._print_operation_summary(summary, total_time)

    def _print_operation_summary(self, summary: OperationSummary, total_time: float):
        """
        打印单个操作的汇总信息

        Args:
            summary: 操作汇总信息
            total_time: 总耗时
        """
        if summary.operation_type == 'csv':
            print(f"\n📊 CSV写入汇总:")
            print(f"   📁 生成文件数: {summary.files_count}")
            print(f"   📝 总记录数: {summary.records_count:,}")
            if summary.elapsed_time > 0:
                print(f"   ⏱️  耗时: {summary.elapsed_time:.2f}s")
                if summary.records_count > 0:
                    speed = summary.records_count / summary.elapsed_time
                    print(f"   🚀 写入速度: {speed:.0f} 记录/秒")

            # 移除文件详情显示，避免无意义的输出

        elif summary.operation_type == 'database':
            print(f"\n📊 数据库写入汇总:")
            print(f"   📦 处理批次数: {summary.files_count}")
            print(f"   📝 总记录数: {summary.records_count:,}")
            if summary.elapsed_time > 0:
                print(f"   ⏱️  耗时: {summary.elapsed_time:.2f}s")
                if summary.records_count > 0:
                    speed = summary.records_count / summary.elapsed_time
                    print(f"   🚀 写入速度: {speed:.0f} 记录/秒")

        # 显示错误信息
        if summary.error_count > 0:
            print(f"   ❌ 错误数量: {summary.error_count}")

    def set_anomaly_summary(self, anomaly_summary: Dict[str, Any]) -> None:
        """
        设置异常汇总信息

        Args:
            anomaly_summary: 异常汇总信息字典
        """
        self.anomaly_summary.total_anomalies = anomaly_summary.get('total_anomalies', 0)
        self.anomaly_summary.error_count = anomaly_summary.get('error_count', 0)
        self.anomaly_summary.warning_count = anomaly_summary.get('warning_count', 0)
        self.anomaly_summary.anomaly_types = anomaly_summary.get('anomaly_types', {})
        self.anomaly_summary.affected_stocks = anomaly_summary.get('affected_stocks', set())
        self.anomaly_summary.affected_dates = anomaly_summary.get('affected_dates', set())

    def add_anomaly_record(self, anomaly_record: Dict[str, Any]) -> None:
        """
        添加异常记录

        Args:
            anomaly_record: 异常记录字典
        """
        self.anomaly_summary.anomaly_records.append(anomaly_record)

    def print_anomaly_summary(self):
        """打印异常汇总报告"""
        if self.anomaly_summary.total_anomalies == 0:
            return

        print(f"\n🚨 数据异常检测汇总:")
        print(f"   📊 异常总数: {self.anomaly_summary.total_anomalies}")
        print(f"   ❌ 错误数量: {self.anomaly_summary.error_count}")
        print(f"   ⚠️  警告数量: {self.anomaly_summary.warning_count}")

        if self.anomaly_summary.affected_stocks:
            print(f"   📈 涉及股票数: {len(self.anomaly_summary.affected_stocks)}")
            # 显示前10只异常股票
            stocks_list = list(self.anomaly_summary.affected_stocks)[:10]
            print(f"   🏷️  异常股票示例: {', '.join(stocks_list)}")
            if len(self.anomaly_summary.affected_stocks) > 10:
                print(f"      ... 还有 {len(self.anomaly_summary.affected_stocks) - 10} 只股票")

        if self.anomaly_summary.affected_dates:
            print(f"   📅 涉及交易日数: {len(self.anomaly_summary.affected_dates)}")
            # 显示前5个异常日期
            dates_list = sorted(list(self.anomaly_summary.affected_dates))[:5]
            print(f"   📆 异常日期示例: {', '.join(dates_list)}")
            if len(self.anomaly_summary.affected_dates) > 5:
                print(f"      ... 还有 {len(self.anomaly_summary.affected_dates) - 5} 个交易日")

        # 按类型显示异常统计
        if self.anomaly_summary.anomaly_types:
            print(f"\n   📋 异常类型分布:")
            for anomaly_type, info in sorted(self.anomaly_summary.anomaly_types.items(),
                                            key=lambda x: x[1]['count'], reverse=True):
                severity_icon = "❌" if info['severity'] == 'error' else "⚠️"
                print(f"      {severity_icon} {info['description']}: {info['count']} 次")

    def reset(self):
        """重置所有统计信息"""
        self.summaries.clear()
        self.anomaly_summary = AnomalySummary()
        self.start_time = None