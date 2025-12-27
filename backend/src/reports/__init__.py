"""
报告生成模块

提供各种格式的数据报告生成功能，包括：
- 异常检测报告
- 数据质量报告
- 性能统计报告
"""

from .anomaly_report_generator import AnomalyReportGenerator, ReportConfig

__all__ = ['AnomalyReportGenerator', 'ReportConfig']