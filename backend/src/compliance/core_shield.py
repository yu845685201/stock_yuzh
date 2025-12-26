"""
核心合规护盾 - 100%合规性保护
严格按照产品设计文档要求，确保数据采集的合规性
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class CoreComplianceShield:
    """
    核心合规护盾 - 立即部署，保障关键合规性

    核心保护原则：
    1. 禁止Mock数据使用
    2. 确保授权数据源
    3. 验证关键字段
    """

    def __init__(self):
        # 数据源白名单 - 严格按照产品设计文档
        self.data_sources_whitelist = {
            'basic_info': ['BaostockSource'],  # 文档指定baostock
            'fundamentals': ['BaostockSource'],  # 文档指定baostock
            'daily_kline': ['PytdxSource'],  # 文档指定pytdx文件方案
            'minute_kline': ['PytdxSource']  # 文档指定pytdx文件方案
        }

        # 关键字段清单 - 基于产品设计文档要求
        self.critical_fields = {
            'basic_info': [
                'ts_code', 'stock_code', 'stock_name', 'cnspell',
                'market_code', 'market_name', 'exchange_code',
                'list_status', 'list_date'
            ],
            'fundamentals': [
                'stock_code', 'disclosure_date', 'total_share', 'float_share'
            ],
            'daily_kline': [
                'ts_code', 'stock_code', 'trade_date', 'open', 'high',
                'low', 'close', 'preclose', 'volume', 'amount'
            ],
            'minute_kline': [
                'ts_code', 'stock_code', 'trade_date', 'trade_time',
                'open', 'high', 'low', 'close', 'volume', 'amount'
            ]
        }

        # 无法获取的字段清单 - 按文档要求明确列出
        self.unavailable_fields = {
            'basic_info': [
                'sector_code', 'sector_name'  # 文档明确标注"无法通过baostock接口直接获取"
            ],
            'fundamentals': [
                # 文档中未明确所有字段，根据baostock实际能力标注
            ],
            'daily_kline': [
                'stock_name', 'industry_code', 'industry_name', 'is_st',
                'trade_status', 'adjust_flag', 'change_rate', 'turnover_rate',
                'pe_ttm', 'pb_rate', 'ps_ttm', 'pcf_ttm'
            ],
            'minute_kline': [
                'stock_name', 'preclose', 'change_rate', 'turnover_rate',
                'adjust_flag', 'trade_status', 'pe_ttm', 'pb_rate',
                'ps_ttm', 'pcf_ttm'
            ]
        }

        # 统计信息
        self.compliance_stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'violations': []
        }

    def check_compliance(self, data_type: str, source_name: str, data: Dict[str, Any], source_only_mode: bool = False) -> bool:
        """
        核心合规检查 - 100%严格执行

        Args:
            data_type: 数据类型 (basic_info, fundamentals, daily_kline, minute_kline)
            source_name: 数据源类名
            data: 实际数据
            source_only_mode: 是否仅检查数据源授权，跳过数据验证

        Returns:
            bool: 是否合规
        """
        self.compliance_stats['total_checks'] += 1

        # 1. 检查数据源授权
        if not self._check_source_authorization(data_type, source_name):
            return self._record_violation('unauthorized_source', data_type, source_name)

        # 如果是仅数据源验证模式，跳过后续检查
        if source_only_mode:
            self.compliance_stats['passed_checks'] += 1
            return True

        # 2. 检查数据真实性（无Mock数据）
        if not self._check_data_authenticity(data_type, data):
            return self._record_violation('mock_data_detected', data_type, source_name)

        # 3. 检查关键字段完整性
        if not self._check_critical_fields(data_type, data):
            return self._record_violation('missing_critical_fields', data_type, source_name)

        # 4. 检查数据格式合规性
        if not self._check_data_format(data_type, data):
            return self._record_violation('invalid_format', data_type, source_name)

        self.compliance_stats['passed_checks'] += 1
        return True

    def _check_source_authorization(self, data_type: str, source_name: str) -> bool:
        """检查数据源是否在白名单中"""
        authorized_sources = self.data_sources_whitelist.get(data_type, [])
        return source_name in authorized_sources

    def _check_data_authenticity(self, data_type: str, data: Dict[str, Any]) -> bool:
        """检查数据真实性 - 禁止Mock数据"""

        # 检查明显的Mock数据特征（通用检测）
        data_str = str(data).lower()
        stock_name = str(data.get('stock_name', '')).lower()

        # 通用Mock数据检测模式
        mock_indicators = [
            'test' in stock_name,  # 股票名称包含test
            'mock' in data_str,   # 任何字段包含mock
            'fake' in data_str,   # 任何字段包含fake
            'sample' in stock_name,  # 股票名称包含sample
        ]

        # 检查明显的测试数据模式（基于数据类型）
        if data_type in ['daily_kline', 'minute_kline']:
            # 检查价格数据是否过于规整（如都是整数）
            price_fields = ['open', 'high', 'low', 'close']
            all_integers = all(
                isinstance(data.get(field, 0), (int, float)) and
                data.get(field, 0) == int(data.get(field, 0))
                for field in price_fields if field in data
            )

            if all_integers and len(price_fields) > 0:
                mock_indicators.append(True)

        return not any(mock_indicators)

    def _check_critical_fields(self, data_type: str, data: Dict[str, Any]) -> bool:
        """检查关键字段是否存在且非空"""
        critical_fields = self.critical_fields.get(data_type, [])

        for field in critical_fields:
            if field not in data or data[field] is None or data[field] == '':
                return False

        return True

    def _check_data_format(self, data_type: str, data: Dict[str, Any]) -> bool:
        """检查数据格式是否符合文档要求"""
        if data_type in ['daily_kline', 'minute_kline']:
            # 检查日期格式
            trade_date = str(data.get('trade_date', ''))
            if not (trade_date.replace('-', '').isdigit() and len(trade_date) in [8, 10]):
                return False

            # 检查价格数据格式（应该为正数）
            price_fields = ['open', 'high', 'low', 'close']
            for field in price_fields:
                price = data.get(field)
                if price is not None and (not isinstance(price, (int, float)) or price <= 0):
                    return False

        elif data_type == 'basic_info':
            # 检查TS代码格式
            ts_code = str(data.get('ts_code', ''))
            if not ('.' in ts_code and len(ts_code.split('.')) == 2):
                return False

            # 检查股票代码格式（6位数字）
            stock_code = str(data.get('stock_code', ''))
            if not (len(stock_code) == 6 and stock_code.isdigit()):
                return False

        return True

    def _record_violation(self, violation_type: str, data_type: str, source_name: str) -> bool:
        """记录违规"""
        violation = {
            'timestamp': datetime.now(),
            'type': violation_type,
            'data_type': data_type,
            'source': source_name
        }

        self.compliance_stats['violations'].append(violation)
        self.compliance_stats['failed_checks'] += 1

        logger.error(f"合规性违规: {violation_type} - 数据类型: {data_type}, 数据源: {source_name}")
        return False

    def get_compliance_report(self) -> Dict[str, Any]:
        """获取合规报告"""
        total = self.compliance_stats['total_checks']
        passed = self.compliance_stats['passed_checks']

        compliance_rate = (passed / total * 100) if total > 0 else 0

        return {
            'summary': {
                'total_checks': total,
                'passed_checks': passed,
                'failed_checks': self.compliance_stats['failed_checks'],
                'compliance_rate': f"{compliance_rate:.1f}%",
                'status': 'COMPLIANT' if compliance_rate == 100 else 'NON_COMPLIANT'
            },
            'violations': self.compliance_stats['violations'],
            'data_sources_whitelist': self.data_sources_whitelist,
            'unavailable_fields': self.unavailable_fields,
            'timestamp': datetime.now()
        }

    def reset_stats(self):
        """重置统计信息"""
        self.compliance_stats = {
            'total_checks': 0,
            'passed_checks': 0,
            'failed_checks': 0,
            'violations': []
        }

    def get_unavailable_fields_report(self, data_type: str) -> List[str]:
        """获取指定数据类型的无法获取字段清单"""
        return self.unavailable_fields.get(data_type, [])