"""
智能验证增强器 - 智能化、可修复
提供高级验证功能和自动修复能力
"""

import logging
import re
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, date
from .realtime_checker import RealTimeComplianceChecker

logger = logging.getLogger(__name__)

class SmartValidator:
    """
    智能验证增强器 - 智能化、可修复

    核心功能：
    1. 字段映射验证和自动修复
    2. 数据格式智能校正
    3. 业务逻辑合规性检查
    4. 自动修复常见问题
    """

    def __init__(self):
        self.compliance_checker = RealTimeComplianceChecker()
        self.auto_fix_enabled = True
        self.fix_history = []
        self.validation_rules = self._load_validation_rules()

    def _load_validation_rules(self) -> Dict[str, Any]:
        """加载验证规则"""
        return {
            'basic_info': {
                'ts_code_pattern': r'^\d{6}\.(SZ|SH|BJ)$',
                'stock_code_pattern': r'^\d{6}$',
                'market_codes': ['sz', 'sh', 'bj'],
                'exchange_codes': ['SZSE', 'SSE', 'BJSE'],
                'list_statuses': ['L', 'D', 'P']
            },
            'daily_kline': {
                'price_range': {'min': 0.01, 'max': 10000},  # 价格范围（元）
                'volume_range': {'min': 0, 'max': 1e12},  # 成交量范围
                'amount_range': {'min': 0, 'max': 1e15}   # 成交额范围
            },
            'minute_kline': {
                'time_pattern': r'^\d{2}:\d{2}:\d{2}$',
                'price_range': {'min': 0.01, 'max': 10000},
                'volume_range': {'min': 0, 'max': 1e10}
            }
        }

    def validate_and_fix_batch(self, data_type: str, data_batch: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        批量验证并尝试自动修复

        Args:
            data_type: 数据类型
            data_batch: 数据批次

        Returns:
            Dict: 验证和修复结果
        """
        results = {
            'total': len(data_batch),
            'valid': [],
            'fixed': [],
            'invalid': [],
            'fix_logs': [],
            'validation_summary': {
                'field_mapping_issues': 0,
                'format_issues': 0,
                'business_logic_issues': 0
            }
        }

        logger.info(f"开始智能验证和修复: {data_type} - {len(data_batch)} 条记录")

        for i, data in enumerate(data_batch):
            try:
                # 1. 执行全面验证
                validation_result = self._comprehensive_validate(data_type, data)

                if validation_result['is_valid']:
                    results['valid'].append(data)
                else:
                    # 2. 尝试自动修复
                    if self.auto_fix_enabled:
                        fixed_data, fix_success = self._auto_fix_data(data_type, data, validation_result['issues'])

                        if fix_success:
                            # 3. 重新验证修复后的数据
                            revalidation = self._comprehensive_validate(data_type, fixed_data)
                            if revalidation['is_valid']:
                                results['fixed'].append(fixed_data)
                                results['fix_logs'].append(f"自动修复成功: 记录 {i} ({data.get('stock_code', 'unknown')})")
                            else:
                                results['invalid'].append(fixed_data)
                                results['fix_logs'].append(f"修复后仍不合规: 记录 {i}")
                        else:
                            results['invalid'].append(data)
                            results['fix_logs'].append(f"自动修复失败: 记录 {i}")
                    else:
                        results['invalid'].append(data)

                # 4. 统计问题类型
                for issue in validation_result['issues']:
                    issue_type = issue['type']
                    if issue_type in results['validation_summary']:
                        results['validation_summary'][issue_type] += 1

            except Exception as e:
                logger.error(f"处理记录 {i} 时发生异常: {str(e)}")
                results['invalid'].append(data)
                results['fix_logs'].append(f"处理异常: 记录 {i} - {str(e)}")

        # 计算修复成功率
        total_issues = len(results['fixed']) + len(results['invalid'])
        fix_success_rate = (len(results['fixed']) / total_issues * 100) if total_issues > 0 else 0

        logger.info(f"智能验证和修复完成: 修复成功率 {fix_success_rate:.1f}%")

        # 计算整体合规率，增加除零保护
        overall_compliance_rate = 0
        if len(data_batch) > 0:
            overall_compliance_rate = (len(results['valid']) + len(results['fixed'])) / len(data_batch) * 100

        return {
            **results,
            'fix_success_rate': fix_success_rate,
            'overall_compliance_rate': overall_compliance_rate
        }

    def _comprehensive_validate(self, data_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        全面验证数据

        Args:
            data_type: 数据类型
            data: 待验证数据

        Returns:
            Dict: 验证结果
        """
        result = {
            'is_valid': True,
            'issues': []
        }

        # 1. 字段映射验证
        mapping_issues = self._validate_field_mapping(data_type, data)
        result['issues'].extend(mapping_issues)

        # 2. 数据格式验证
        format_issues = self._validate_data_format(data_type, data)
        result['issues'].extend(format_issues)

        # 3. 业务逻辑验证
        logic_issues = self._validate_business_logic(data_type, data)
        result['issues'].extend(logic_issues)

        # 4. 数据一致性验证
        consistency_issues = self._validate_data_consistency(data_type, data)
        result['issues'].extend(consistency_issues)

        result['is_valid'] = len(result['issues']) == 0
        return result

    def _validate_field_mapping(self, data_type: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """验证字段映射"""
        issues = []
        critical_fields = self.compliance_checker.core_shield.critical_fields.get(data_type, [])

        for field in critical_fields:
            if field not in data:
                issues.append({
                    'type': 'field_mapping_issues',
                    'field': field,
                    'severity': 'error',
                    'message': f'缺少关键字段: {field}'
                })
            elif data[field] is None or str(data[field]).strip() == '':
                issues.append({
                    'type': 'field_mapping_issues',
                    'field': field,
                    'severity': 'error',
                    'message': f'关键字段为空: {field}'
                })

        return issues

    def _validate_data_format(self, data_type: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """验证数据格式"""
        issues = []
        rules = self.validation_rules.get(data_type, {})

        if data_type == 'basic_info':
            # 验证TS代码格式
            ts_code = str(data.get('ts_code', ''))
            if ts_code and not re.match(rules['ts_code_pattern'], ts_code):
                issues.append({
                    'type': 'format_issues',
                    'field': 'ts_code',
                    'severity': 'error',
                    'message': f'TS代码格式错误: {ts_code}'
                })

            # 验证股票代码格式
            stock_code = str(data.get('stock_code', ''))
            if stock_code and not re.match(rules['stock_code_pattern'], stock_code):
                issues.append({
                    'type': 'format_issues',
                    'field': 'stock_code',
                    'severity': 'error',
                    'message': f'股票代码格式错误: {stock_code}'
                })

        elif data_type in ['daily_kline', 'minute_kline']:
            # 验证价格范围
            price_fields = ['open', 'high', 'low', 'close']
            price_range = rules['price_range']

            for field in price_fields:
                price = data.get(field)
                if price is not None:
                    if not isinstance(price, (int, float)) or price <= 0:
                        issues.append({
                            'type': 'format_issues',
                            'field': field,
                            'severity': 'error',
                            'message': f'价格数据无效: {field}={price}'
                        })
                    elif price < price_range['min'] or price > price_range['max']:
                        issues.append({
                            'type': 'format_issues',
                            'field': field,
                            'severity': 'warning',
                            'message': f'价格数据异常: {field}={price} (范围: {price_range["min"]}-{price_range["max"]})'
                        })

            # 验证成交量和成交额
            volume = data.get('volume', 0)
            amount = data.get('amount', 0)

            if not isinstance(volume, (int, float)) or volume < 0:
                issues.append({
                    'type': 'format_issues',
                    'field': 'volume',
                    'severity': 'error',
                    'message': f'成交量数据无效: {volume}'
                })

            if not isinstance(amount, (int, float)) or amount < 0:
                issues.append({
                    'type': 'format_issues',
                    'field': 'amount',
                    'severity': 'error',
                    'message': f'成交额数据无效: {amount}'
                })

        elif data_type == 'minute_kline':
            # 验证时间格式
            trade_time = str(data.get('trade_time', ''))
            if trade_time and not re.match(rules['time_pattern'], trade_time):
                issues.append({
                    'type': 'format_issues',
                    'field': 'trade_time',
                    'severity': 'error',
                    'message': f'交易时间格式错误: {trade_time}'
                })

        return issues

    def _validate_business_logic(self, data_type: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """验证业务逻辑"""
        issues = []

        if data_type in ['daily_kline', 'minute_kline']:
            # 验证价格逻辑：最高价 >= 最低价
            high = data.get('high', 0)
            low = data.get('low', 0)

            if high > 0 and low > 0 and high < low:
                issues.append({
                    'type': 'business_logic_issues',
                    'field': 'price_logic',
                    'severity': 'error',
                    'message': f'价格逻辑错误: high({high}) < low({low})'
                })

            # 验证开盘收盘价逻辑
            open_price = data.get('open', 0)
            close_price = data.get('close', 0)
            high_price = data.get('high', 0)
            low_price = data.get('low', 0)

            if all([open_price, close_price, high_price, low_price]):
                if not (low_price <= open_price <= high_price):
                    issues.append({
                        'type': 'business_logic_issues',
                        'field': 'open_price_logic',
                        'severity': 'error',
                        'message': f'开盘价逻辑错误: {low_price} <= open({open_price}) <= {high_price} 不成立'
                    })

                if not (low_price <= close_price <= high_price):
                    issues.append({
                        'type': 'business_logic_issues',
                        'field': 'close_price_logic',
                        'severity': 'error',
                        'message': f'收盘价逻辑错误: {low_price} <= close({close_price}) <= {high_price} 不成立'
                    })

            
        return issues

    def _validate_data_consistency(self, data_type: str, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """验证数据一致性"""
        issues = []

        if data_type == 'basic_info':
            # 验证TS代码与股票代码的一致性
            ts_code = str(data.get('ts_code', ''))
            stock_code = str(data.get('stock_code', ''))

            if ts_code and stock_code:
                expected_ts_code = f"{stock_code}.{ts_code.split('.')[-1]}"
                if ts_code != expected_ts_code:
                    issues.append({
                        'type': 'consistency_issues',
                        'field': 'code_consistency',
                        'severity': 'error',
                        'message': f'代码不一致: ts_code({ts_code}) != stock_code({stock_code})'
                    })

        elif data_type in ['daily_kline', 'minute_kline']:
            # 验证日期格式一致性
            trade_date = data.get('trade_date', '')
            if trade_date:
                if len(trade_date) == 10:  # YYYY-MM-DD
                    try:
                        datetime.strptime(trade_date, '%Y-%m-%d')
                    except ValueError:
                        issues.append({
                            'type': 'consistency_issues',
                            'field': 'trade_date_format',
                            'severity': 'error',
                            'message': f'交易日期格式错误: {trade_date}'
                        })

        return issues

    def _auto_fix_data(self, data_type: str, data: Dict[str, Any], issues: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], bool]:
        """
        自动修复数据

        Args:
            data_type: 数据类型
            data: 原始数据
            issues: 问题列表

        Returns:
            Tuple[Dict, bool]: 修复后的数据和是否修复成功
        """
        fixed_data = data.copy()
        fix_count = 0

        for issue in issues:
            field = issue.get('field', '')
            message = issue.get('message', '')

            try:
                if field == 'ts_code':
                    # 修复TS代码
                    stock_code = fixed_data.get('stock_code')
                    market_code = fixed_data.get('market_code', 'sz')
                    if stock_code and market_code:
                        fixed_data['ts_code'] = f"{stock_code}.{market_code.upper()}"
                        fix_count += 1

                elif field == 'trade_time' and data_type == 'minute_kline':
                    # 修复时间格式
                    trade_time = str(fixed_data.get('trade_time', ''))
                    if len(trade_time) == 6 and trade_time.isdigit():
                        fixed_data['trade_time'] = f"{trade_time[:2]}:{trade_time[2:4]}:{trade_time[4:6]}"
                        fix_count += 1

                elif field == 'trade_date':
                    # 修复日期格式
                    trade_date = str(fixed_data.get('trade_date', ''))
                    if len(trade_date) == 8 and trade_date.isdigit():
                        fixed_data['trade_date'] = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
                        fix_count += 1

                # 记录修复历史
                if fix_count > 0:
                    self.fix_history.append({
                        'timestamp': datetime.now(),
                        'data_type': data_type,
                        'field': field,
                        'original_value': data.get(field),
                        'fixed_value': fixed_data.get(field),
                        'issue_message': message
                    })

            except Exception as e:
                logger.warning(f"自动修复字段 {field} 时发生异常: {str(e)}")

        success = fix_count > 0 and fix_count == len([i for i in issues if i['severity'] == 'error'])
        return fixed_data, success

    def enable_auto_fix(self, enabled: bool = True):
        """启用/禁用自动修复"""
        self.auto_fix_enabled = enabled
        logger.info(f"自动修复已{'启用' if enabled else '禁用'}")

    def get_fix_history(self) -> List[Dict[str, Any]]:
        """获取修复历史"""
        return self.fix_history.copy()

    def clear_fix_history(self):
        """清除修复历史"""
        self.fix_history.clear()