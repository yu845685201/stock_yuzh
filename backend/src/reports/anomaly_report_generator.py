"""
异常报告生成器 - 生成Markdown格式的异常数据报告
"""

import os
from pathlib import Path
from datetime import date, datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from ..config import ConfigManager
from ..utils.daily_kline_anomaly_detector import AnomalyRecord


@dataclass
class ReportConfig:
    """报告配置"""
    output_dir: str
    environment: str
    report_date: date
    include_raw_data: bool = True
    group_by_type: bool = True


class AnomalyReportGenerator:
    """
    异常报告生成器

    功能：
    1. 生成Markdown格式的异常报告
    2. 按异常类型分组组织内容
    3. 包含完整的异常信息和原始数据
    4. 支持环境隔离的目录管理
    """

    def __init__(self, config_manager: ConfigManager):
        """
        初始化报告生成器

        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager
        self.logger = self._get_logger()

    def _get_logger(self):
        """获取日志记录器"""
        import logging
        return logging.getLogger(__name__)

    def generate_report(
        self,
        anomaly_records: List[AnomalyRecord],
        raw_data_map: Dict[str, Dict[str, Any]] = None,
        report_date: Optional[date] = None
    ) -> str:
        """
        生成异常报告

        Args:
            anomaly_records: 异常记录列表
            raw_data_map: 原始数据映射 {ts_code_trade_date: raw_data}
            report_date: 报告日期，默认为今天

        Returns:
            生成的报告文件路径
        """
        if not anomaly_records:
            self.logger.info("没有异常记录，跳过报告生成")
            return ""

        # 配置报告参数
        config = self._get_report_config(report_date or date.today())

        # 确保输出目录存在
        output_path = Path(config.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 生成报告文件名
        filename = f"anomaly_report_{config.report_date.strftime('%Y%m%d')}.md"
        file_path = output_path / filename

        # 生成报告内容
        report_content = self._generate_report_content(
            anomaly_records, raw_data_map or {}, config
        )

        # 写入文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(report_content)

        self.logger.info(f"异常报告已生成: {file_path}")
        return str(file_path)

    def _get_report_config(self, report_date: date) -> ReportConfig:
        """获取报告配置"""
        env = self.config_manager.env

        # 获取数据路径配置
        data_paths = self.config_manager.get_data_paths()
        base_path = Path(data_paths.get('csv', f'{env}/data')).parent

        # 创建异常报告目录
        output_dir = base_path / 'anomaly_reports'

        return ReportConfig(
            output_dir=str(output_dir),
            environment=env,
            report_date=report_date,
            include_raw_data=True,
            group_by_type=True
        )

    def _generate_report_content(
        self,
        anomaly_records: List[AnomalyRecord],
        raw_data_map: Dict[str, Dict[str, Any]],
        config: ReportConfig
    ) -> str:
        """生成报告内容"""

        # 按类型分组异常记录
        grouped_anomalies = self._group_anomalies_by_type(anomaly_records)

        # 生成报告内容
        content_parts = []

        # 报告头部
        content_parts.append(self._generate_header(config, len(anomaly_records)))

        # 汇总统计
        content_parts.append(self._generate_summary(anomaly_records, grouped_anomalies))

        # 按类型详细报告
        content_parts.append(self._generate_detailed_report(grouped_anomalies, raw_data_map))

        # 报告尾部
        content_parts.append(self._generate_footer())

        return '\n\n'.join(content_parts)

    def _group_anomalies_by_type(self, anomaly_records: List[AnomalyRecord]) -> Dict[str, List[AnomalyRecord]]:
        """按异常类型分组"""
        grouped = {}
        for record in anomaly_records:
            anomaly_type = record.anomaly_type
            if anomaly_type not in grouped:
                grouped[anomaly_type] = []
            grouped[anomaly_type].append(record)
        return grouped

    def _generate_header(self, config: ReportConfig, total_anomalies: int) -> str:
        """生成报告头部"""
        return f"""# 日K线数据异常检测报告

## 📊 报告信息

- **报告日期**: {config.report_date.strftime('%Y年%m月%d日')}
- **环境**: {config.environment.upper()}
- **异常总数**: {total_anomalies}
- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---"""

    def _generate_summary(
        self,
        anomaly_records: List[AnomalyRecord],
        grouped_anomalies: Dict[str, List[AnomalyRecord]]
    ) -> str:
        """生成汇总统计"""

        # 统计信息
        error_count = sum(1 for r in anomaly_records if r.severity == 'error')
        warning_count = sum(1 for r in anomaly_records if r.severity == 'warning')
        affected_stocks = set(r.ts_code for r in anomaly_records)
        affected_dates = set(str(r.trade_date) for r in anomaly_records)

        # 异常类型统计
        type_stats = []
        for anomaly_type, records in grouped_anomalies.items():
            type_stats.append(f"- **{records[0].description}**: {len(records)} 次")

        return f"""## 📈 汇总统计

### 严重程度分布
- ❌ **错误数量**: {error_count}
- ⚠️ **警告数量**: {warning_count}

### 影响范围
- 📊 **涉及股票数**: {len(affected_stocks)}
- 📅 **涉及交易日数**: {len(affected_dates)}
- 🏷️ **异常股票示例**: {', '.join(list(affected_stocks)[:5])}{'...' if len(affected_stocks) > 5 else ''}

### 异常类型分布
{chr(10).join(type_stats)}"""

    def _generate_detailed_report(
        self,
        grouped_anomalies: Dict[str, List[AnomalyRecord]],
        raw_data_map: Dict[str, Dict[str, Any]]
    ) -> str:
        """生成详细报告"""
        content_parts = []

        content_parts.append("## 🔍 详细异常信息\n")

        # 按异常类型分组显示
        for anomaly_type, records in grouped_anomalies.items():
            content_parts.append(self._generate_type_section(anomaly_type, records, raw_data_map))

        return '\n'.join(content_parts)

    def _generate_type_section(
        self,
        anomaly_type: str,
        records: List[AnomalyRecord],
        raw_data_map: Dict[str, Dict[str, Any]]
    ) -> str:
        """生成异常类型章节"""

        severity_icon = "❌" if records[0].severity == 'error' else "⚠️"

        content_parts = []
        content_parts.append(f"### {severity_icon} {records[0].description}")
        content_parts.append(f"**异常数量**: {len(records)} 次\n")

        # 按股票分组
        stock_groups = {}
        for record in records:
            if record.ts_code not in stock_groups:
                stock_groups[record.ts_code] = []
            stock_groups[record.ts_code].append(record)

        # 生成每个股票的详细信息
        for ts_code, stock_records in stock_groups.items():
            content_parts.append(self._generate_stock_details(ts_code, stock_records, raw_data_map))

        return '\n'.join(content_parts)

    def _generate_stock_details(
        self,
        ts_code: str,
        records: List[AnomalyRecord],
        raw_data_map: Dict[str, Dict[str, Any]]
    ) -> str:
        """生成股票详细信息"""

        content_parts = []
        content_parts.append(f"#### 📈 {ts_code}")

        # 异常记录表格
        content_parts.append("| 交易日 | 异常字段 | 实际值 | 期望范围 | 严重程度 |")
        content_parts.append("|--------|----------|--------|----------|----------|")

        for record in records:
            severity_icon = "❌" if record.severity == 'error' else "⚠️"

            # 处理复杂的actual_value（字典类型）
            if isinstance(record.actual_value, dict):
                actual_value_str = self._format_complex_actual_value(record.actual_value)
            else:
                actual_value_str = str(record.actual_value)

            content_parts.append(
                f"| {record.trade_date} | {record.field_name} | {actual_value_str} | "
                f"{record.expected_range} | {severity_icon} {record.severity} |"
            )

        # 原始数据
        data_key = f"{ts_code}_{records[0].trade_date}"
        if data_key in raw_data_map:
            content_parts.append(self._generate_raw_data_section(raw_data_map[data_key]))

        content_parts.append("")  # 空行分隔

        return '\n'.join(content_parts)

    def _format_complex_actual_value(self, actual_value: Dict[str, Any]) -> str:
        """
        格式化复杂的actual_value（字典类型）

        Args:
            actual_value: 异常记录的actual_value字典

        Returns:
            格式化后的字符串
        """
        if not isinstance(actual_value, dict):
            return str(actual_value)

        lines = []

        # 截断值
        if 'truncated_value' in actual_value:
            lines.append(f"截断值: {actual_value['truncated_value']}%")

        # 原始值
        if 'raw_value' in actual_value:
            lines.append(f"原始值: {actual_value['raw_value']}%")

        # 计算过程
        if 'calculation' in actual_value:
            lines.append(f"计算: {actual_value['calculation']}")

        # 价格信息
        if 'close_price' in actual_value and 'preclose_price' in actual_value:
            lines.append(f"收盘价: {actual_value['close_price']}")
            lines.append(f"昨收盘: {actual_value['preclose_price']}")

        return " | ".join(lines)

    def _generate_raw_data_section(self, raw_data: Dict[str, Any]) -> str:
        """生成原始数据章节"""

        content_parts = []
        content_parts.append("**原始数据记录**:")
        content_parts.append("```json")

        # 格式化原始数据
        import json
        try:
            # 处理日期对象
            formatted_data = {}
            for key, value in raw_data.items():
                if isinstance(value, date):
                    formatted_data[key] = value.strftime('%Y-%m-%d')
                elif isinstance(value, (int, float, str, bool, type(None))):
                    formatted_data[key] = value
                else:
                    formatted_data[key] = str(value)

            content_parts.append(json.dumps(formatted_data, ensure_ascii=False, indent=2))
        except Exception as e:
            content_parts.append(f"# 数据格式化失败: {e}")
            content_parts.append(str(raw_data))

        content_parts.append("```")

        return '\n'.join(content_parts)

    def _generate_footer(self) -> str:
        """生成报告尾部"""
        return f"""---

## 📝 说明

本报告由A股盘后静态分析系统自动生成，包含日K线数据采集过程中检测到的所有异常信息。

### 异常类型说明
- **price_invalid**: 价格小于等于零
- **price_out_of_range**: 价格超出合理范围
- **volume_invalid**: 成交量为负数
- **volume_excessive**: 成交量异常巨大
- **amount_invalid**: 成交额为负数
- **price_logic_error**: 价格逻辑错误（如最高价<最低价）
- **open_price_logic_error**: 开盘价不在价格区间内
- **close_price_logic_error**: 收盘价不在价格区间内
- **change_rate_excessive**: 涨跌幅超过限制
- **change_rate_calculation_error**: 涨跌幅计算错误
- **change_rate_precision_overflow**: 涨跌幅超出精度限制被截断

### 严重程度说明
- **❌ Error**: 严重错误，需要立即处理
- **⚠️ Warning**: 警告，建议关注和检查

---
*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""