"""
实时合规检查器 - 轻量级、高性能
在数据采集过程中进行即时验证，确保数据源真实性和格式合规性
"""

import logging
from typing import Dict, Any, List
from .core_shield import CoreComplianceShield

logger = logging.getLogger(__name__)

class RealTimeComplianceChecker:
    """
    实时合规检查器 - 轻量级、高性能

    重点验证：
    1. 数据源合规性 - 100%严格控制
    2. 数据格式合规性
    3. 实时违规预警
    """

    def __init__(self):
        self.core_shield = CoreComplianceShield()
        self.warnings = []
        self.last_check_time = None

    def validate_data_source(self, data_type: str, source_name: str) -> bool:
        """
        验证数据源合规性 - 100%严格控制

        Args:
            data_type: 数据类型
            source_name: 数据源名称

        Returns:
            bool: 是否合规
        """
        logger.info(f"验证数据源: {data_type} -> {source_name}")

        # 直接检查数据源是否在白名单中，避免空数据验证问题
        authorized_sources = self.core_shield.data_sources_whitelist.get(data_type, [])
        is_compliant = source_name in authorized_sources

        if not is_compliant:
            violation = f"数据源 {source_name} 不被授权用于 {data_type} 数据采集"
            self.warnings.append({
                'type': 'unauthorized_source',
                'message': violation,
                'timestamp': self._get_timestamp()
            })
            logger.error(violation)

        return is_compliant

    def validate_data(self, data_type: str, source_name: str, data: Dict[str, Any]) -> bool:
        """
        验证数据合规性 - 实时检查

        Args:
            data_type: 数据类型
            source_name: 数据源名称
            data: 待验证数据

        Returns:
            bool: 是否合规
        """
        self.last_check_time = self._get_timestamp()

        # 使用核心护盾进行完整验证
        is_compliant = self.core_shield.check_compliance(data_type, source_name, data)

        if not is_compliant:
            logger.warning(f"数据合规性检查失败: {data_type} from {source_name}")

        return is_compliant

    def validate_batch(self, data_type: str, source_name: str, data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量验证数据 - 高效处理

        Args:
            data_type: 数据类型
            source_name: 数据源名称
            data_list: 数据列表

        Returns:
            Dict: 验证结果统计
        """
        results = {
            'total': len(data_list),
            'compliant': 0,
            'non_compliant': 0,
            'compliance_rate': 0.0,
            'violations': []
        }

        logger.info(f"开始批量验证: {data_type} - {len(data_list)} 条记录")

        for i, data in enumerate(data_list):
            if self.validate_data(data_type, source_name, data):
                results['compliant'] += 1
            else:
                results['non_compliant'] += 1
                results['violations'].append({
                    'record_index': i,
                    'stock_code': data.get('stock_code', 'unknown'),
                    'timestamp': self._get_timestamp()
                })

        # 计算合规率
        if results['total'] > 0:
            results['compliance_rate'] = (results['compliant'] / results['total']) * 100

        logger.info(f"批量验证完成: 合规率 {results['compliance_rate']:.1f}%")

        return results

    def get_warnings(self) -> List[Dict[str, Any]]:
        """获取所有警告信息"""
        return self.warnings.copy()

    def clear_warnings(self):
        """清除警告信息"""
        self.warnings.clear()

    def get_compliance_summary(self) -> Dict[str, Any]:
        """获取合规性摘要"""
        return self.core_shield.get_compliance_report()

    def _get_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def check_field_completeness(self, data_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        检查字段完整性 - 详细分析

        Args:
            data_type: 数据类型
            data: 数据字典

        Returns:
            Dict: 字段完整性分析结果
        """
        critical_fields = self.core_shield.critical_fields.get(data_type, [])
        unavailable_fields = self.core_shield.get_unavailable_fields_report(data_type)

        analysis = {
            'total_fields': len(data),
            'critical_fields': {
                'required': critical_fields,
                'present': [],
                'missing': []
            },
            'unavailable_fields': unavailable_fields,
            'completeness_rate': 0.0
        }

        # 检查关键字段
        for field in critical_fields:
            if field in data and data[field] is not None and str(data[field]).strip() != '':
                analysis['critical_fields']['present'].append(field)
            else:
                analysis['critical_fields']['missing'].append(field)

        # 计算完整性率
        if len(critical_fields) > 0:
            present_count = len(analysis['critical_fields']['present'])
            analysis['completeness_rate'] = (present_count / len(critical_fields)) * 100

        return analysis

    def validate_date_format(self, data_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        验证日期格式 - 专门检查K线数据

        Args:
            data_type: 数据类型
            data: 数据字典

        Returns:
            Dict: 日期格式验证结果
        """
        result = {
            'valid': True,
            'issues': []
        }

        if data_type in ['daily_kline', 'minute_kline']:
            trade_date = str(data.get('trade_date', ''))
            trade_time = data.get('trade_time')

            # 检查交易日期格式
            if trade_date:
                if len(trade_date) == 8 and trade_date.isdigit():
                    # 通达信格式：20241201
                    formatted_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                    data['trade_date'] = formatted_date
                elif len(trade_date) == 10 and trade_date.count('-') == 2:
                    # 标准格式：2024-12-01
                    pass
                else:
                    result['valid'] = False
                    result['issues'].append(f"无效的交易日期格式: {trade_date}")

            # 检查交易时间格式（仅分钟K线）
            if data_type == 'minute_kline' and trade_time:
                time_str = str(trade_time)
                if ':' in time_str:
                    # 标准格式：09:30:00
                    pass
                elif len(time_str) == 6 and time_str.isdigit():
                    # 通达信格式：093000
                    formatted_time = f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
                    data['trade_time'] = formatted_time
                else:
                    result['valid'] = False
                    result['issues'].append(f"无效的交易时间格式: {time_str}")

        return result