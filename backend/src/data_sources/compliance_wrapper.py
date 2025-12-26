"""
数据源合规包装器 - 零侵入式集成
为现有数据源添加合规性验证，不影响原有业务逻辑
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from ..compliance.realtime_checker import RealTimeComplianceChecker

logger = logging.getLogger(__name__)

class ComplianceWrapper:
    """
    数据源合规包装器 - 为现有数据源添加合规性验证

    设计原则：
    1. 零侵入式 - 不修改原有数据源代码
    2. 透明代理 - 保持原有接口不变
    3. 实时验证 - 在数据获取过程中进行合规检查
    """

    def __init__(self, data_source):
        """
        初始化合规包装器

        Args:
            data_source: 原始数据源实例
        """
        self.data_source = data_source
        self.source_name = data_source.__class__.__name__
        self.compliance_checker = RealTimeComplianceChecker()
        self.wrapper_stats = {
            'total_requests': 0,
            'compliant_requests': 0,
            'blocked_requests': 0,
            'data_type_stats': {}
        }

    def get_stock_list(self) -> List[Dict[str, Any]]:
        """
        获取股票列表 - 带合规验证
        """
        self.wrapper_stats['total_requests'] += 1
        data_type = 'basic_info'

        # 1. 验证数据源授权
        if not self.compliance_checker.validate_data_source(data_type, self.source_name):
            self.wrapper_stats['blocked_requests'] += 1
            logger.error(f"数据源 {self.source_name} 未被授权用于 {data_type} 数据采集")
            return []

        try:
            # 2. 调用原始数据源方法
            raw_data = self.data_source.get_stock_list()

            # 3. 实时合规验证
            compliant_data = []
            violations = []

            for stock_info in raw_data:
                if self.compliance_checker.validate_data(data_type, self.source_name, stock_info):
                    compliant_data.append(stock_info)
                else:
                    violations.append({
                        'stock_code': stock_info.get('stock_code', 'unknown'),
                        'reason': 'compliance_check_failed'
                    })

            # 4. 记录统计信息
            self.wrapper_stats['compliant_requests'] += 1
            self._update_data_type_stats(data_type, len(raw_data), len(compliant_data))

            # 5. 记录违规情况
            if violations:
                logger.warning(f"发现 {len(violations)} 条不合规的股票基本信息，已过滤")
                for violation in violations:
                    logger.debug(f"过滤股票: {violation['stock_code']} - {violation['reason']}")

            logger.info(f"股票基本信息获取完成: {len(compliant_data)}/{len(raw_data)} 条合规数据")
            return compliant_data

        except Exception as e:
            logger.error(f"获取股票列表时发生异常: {str(e)}")
            self.wrapper_stats['blocked_requests'] += 1
            return []

    def get_daily_data(
        self,
        code: str,
        start_date: date = None,
        end_date: date = None
    ) -> List[Dict[str, Any]]:
        """
        获取日K线数据 - 带合规验证
        """
        self.wrapper_stats['total_requests'] += 1
        data_type = 'daily_kline'

        # 验证数据源授权
        if not self.compliance_checker.validate_data_source(data_type, self.source_name):
            self.wrapper_stats['blocked_requests'] += 1
            return []

        try:
            # 调用原始数据源方法
            raw_data = self.data_source.get_daily_data(code, start_date, end_date)

            # 批量合规验证
            validation_result = self.compliance_checker.validate_batch(data_type, self.source_name, raw_data)

            # 过滤不合规数据
            compliant_data = []
            for i, data in enumerate(raw_data):
                if i not in [v['record_index'] for v in validation_result['violations']]:
                    # 验证日期格式
                    date_validation = self.compliance_checker.validate_date_format(data_type, data)
                    if date_validation['valid']:
                        compliant_data.append(data)
                    else:
                        logger.debug(f"过滤K线数据: 日期格式错误 - {data.get('trade_date', '')}")

            # 记录统计信息
            self.wrapper_stats['compliant_requests'] += 1
            self._update_data_type_stats(data_type, len(raw_data), len(compliant_data))

            logger.info(f"日K线数据获取完成: {len(compliant_data)}/{len(raw_data)} 条合规数据")
            return compliant_data

        except Exception as e:
            logger.error(f"获取日K线数据时发生异常: {str(e)}")
            self.wrapper_stats['blocked_requests'] += 1
            return []

    def get_financial_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """
        获取财务数据 - 带合规验证
        """
        self.wrapper_stats['total_requests'] += 1
        data_type = 'fundamentals'

        # 验证数据源授权
        if not self.compliance_checker.validate_data_source(data_type, self.source_name):
            self.wrapper_stats['blocked_requests'] += 1
            return None

        try:
            # 调用原始数据源方法
            raw_data = self.data_source.get_financial_data(code, year, quarter)

            if raw_data:
                # 验证数据合规性
                if self.compliance_checker.validate_data(data_type, self.source_name, raw_data):
                    self.wrapper_stats['compliant_requests'] += 1
                    self._update_data_type_stats(data_type, 1, 1)
                    logger.info(f"财务数据获取成功: {code} {year}Q{quarter}")
                    return raw_data
                else:
                    logger.warning(f"财务数据合规性检查失败: {code} {year}Q{quarter}")
                    self._update_data_type_stats(data_type, 1, 0)
                    return None
            else:
                logger.info(f"财务数据为空: {code} {year}Q{quarter}")
                return None

        except Exception as e:
            logger.error(f"获取财务数据时发生异常: {str(e)}")
            self.wrapper_stats['blocked_requests'] += 1
            return None

    def get_minute_data(
        self,
        code: str,
        trade_date: date = None
    ) -> List[Dict[str, Any]]:
        """
        获取分钟K线数据 - 带合规验证
        注意：这个方法需要在原始数据源中实现
        """
        self.wrapper_stats['total_requests'] += 1
        data_type = 'minute_kline'

        # 验证数据源授权
        if not self.compliance_checker.validate_data_source(data_type, self.source_name):
            self.wrapper_stats['blocked_requests'] += 1
            return []

        try:
            # 尝试调用原始数据源的分钟数据方法（如果存在）
            if hasattr(self.data_source, 'get_minute_data'):
                raw_data = self.data_source.get_minute_data(code, trade_date)
            else:
                logger.warning(f"数据源 {self.source_name} 不支持分钟数据获取")
                return []

            # 批量合规验证
            validation_result = self.compliance_checker.validate_batch(data_type, self.source_name, raw_data)

            # 过滤不合规数据，特别检查时间格式
            compliant_data = []
            for i, data in enumerate(raw_data):
                if i not in [v['record_index'] for v in validation_result['violations']]:
                    # 验证日期时间格式
                    datetime_validation = self.compliance_checker.validate_date_format(data_type, data)
                    if datetime_validation['valid']:
                        compliant_data.append(data)
                    else:
                        logger.debug(f"过滤分钟K线数据: 日期时间格式错误 - {data.get('trade_date', '')} {data.get('trade_time', '')}")

            # 记录统计信息
            self.wrapper_stats['compliant_requests'] += 1
            self._update_data_type_stats(data_type, len(raw_data), len(compliant_data))

            logger.info(f"分钟K线数据获取完成: {len(compliant_data)}/{len(raw_data)} 条合规数据")
            return compliant_data

        except Exception as e:
            logger.error(f"获取分钟K线数据时发生异常: {str(e)}")
            self.wrapper_stats['blocked_requests'] += 1
            return []

    def _update_data_type_stats(self, data_type: str, total: int, compliant: int):
        """更新数据类型统计信息"""
        if data_type not in self.wrapper_stats['data_type_stats']:
            self.wrapper_stats['data_type_stats'][data_type] = {
                'total_processed': 0,
                'compliant_count': 0
            }

        self.wrapper_stats['data_type_stats'][data_type]['total_processed'] += total
        self.wrapper_stats['data_type_stats'][data_type]['compliant_count'] += compliant

    def get_wrapper_stats(self) -> Dict[str, Any]:
        """获取包装器统计信息"""
        return {
            'wrapper_stats': self.wrapper_stats,
            'compliance_summary': self.compliance_checker.get_compliance_summary(),
            'warnings': self.compliance_checker.get_warnings()
        }

    def reset_stats(self):
        """重置统计信息"""
        self.wrapper_stats = {
            'total_requests': 0,
            'compliant_requests': 0,
            'blocked_requests': 0,
            'data_type_stats': {}
        }
        self.compliance_checker.clear_warnings()

    def __getattr__(self, name):
        """
        代理未覆盖的方法到原始数据源
        确保包装器透明性
        """
        return getattr(self.data_source, name)