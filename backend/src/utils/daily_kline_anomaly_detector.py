"""
日K线数据异常检测器
用于检测股票日K线数据中的异常情况并记录
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Any, Set, Tuple
from ..config import ConfigManager


@dataclass
class AnomalyRecord:
    """异常记录"""
    ts_code: str           # 股票代码
    trade_date: date      # 交易日期
    anomaly_type: str     # 异常类型
    description: str      # 异常描述
    field_name: str       # 异常字段
    actual_value: Any     # 实际值
    expected_range: str   # 期望范围
    severity: str         # 严重程度 (error/warning)


class DailyKlineAnomalyDetector:
    """
    日K线数据异常检测器

    功能：
    1. 检测价格异常
    2. 检测成交量异常
    3. 检测价格逻辑异常
    4. 检测成交额逻辑异常
    5. 检测涨跌幅异常
    6. 提供异常汇总统计
    """

    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化异常检测器

        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager or ConfigManager()
        self.logger = logging.getLogger(__name__)
        self.anomalies: List[AnomalyRecord] = []

        # 加载配置
        self._load_detection_config()

    def _load_detection_config(self) -> None:
        """加载异常检测配置"""
        self.config = self.config_manager.get('anomaly_detection', {
            'enabled': True,
            'price_range': {'min': 0.01, 'max': 10000},
            'volume_limit': 1e12,
            'amount_limit': 1e15,
                        'max_change_rate': 20
        })

    def detect_anomalies_batch(self, daily_data: List[Dict[str, Any]]) -> List[AnomalyRecord]:
        """
        批量检测日K线数据异常

        Args:
            daily_data: 日K线数据列表

        Returns:
            异常记录列表
        """
        if not self.config.get('enabled', True):
            return []

        self.anomalies = []
        self.logger.info(f"开始异常检测: {len(daily_data)} 条记录")

        for i, data in enumerate(daily_data):
            try:
                anomalies = self._detect_single_anomaly(data)
                self.anomalies.extend(anomalies)

            except Exception as e:
                self.logger.error(f"检测第 {i} 条记录时发生异常: {e}")
                continue

        self.logger.info(f"异常检测完成: 发现 {len(self.anomalies)} 个异常")
        return self.anomalies

    def _detect_single_anomaly(self, data: Dict[str, Any]) -> List[AnomalyRecord]:
        """
        检测单条记录的异常

        Args:
            data: 单条日K线数据

        Returns:
            异常记录列表
        """
        anomalies = []

        ts_code = data.get('ts_code', '')
        trade_date = data.get('trade_date')

        if not ts_code or not trade_date:
            return anomalies

        # 1. 检测价格异常
        price_anomalies = self._detect_price_anomalies(data, ts_code, trade_date)
        anomalies.extend(price_anomalies)

        # 2. 检测成交量异常
        volume_anomalies = self._detect_volume_anomalies(data, ts_code, trade_date)
        anomalies.extend(volume_anomalies)

        # 3. 检测价格逻辑异常
        price_logic_anomalies = self._detect_price_logic_anomalies(data, ts_code, trade_date)
        anomalies.extend(price_logic_anomalies)

        
        # 4. 检测涨跌幅异常
        change_rate_anomalies = self._detect_change_rate_anomalies(data, ts_code, trade_date)
        anomalies.extend(change_rate_anomalies)

        # 5. 检测涨跌幅精度限制异常
        precision_anomalies = self._detect_change_rate_precision_anomalies(data, ts_code, trade_date)
        anomalies.extend(precision_anomalies)

        return anomalies

    def _detect_price_anomalies(self, data: Dict[str, Any], ts_code: str, trade_date: date) -> List[AnomalyRecord]:
        """检测价格异常"""
        anomalies = []
        price_range = self.config.get('price_range', {'min': 0.01, 'max': 10000})

        price_fields = ['open', 'high', 'low', 'close']
        for field in price_fields:
            price = data.get(field)

            if price is None:
                continue

            # 检查价格为零或负数
            if price <= 0:
                anomalies.append(AnomalyRecord(
                    ts_code=ts_code,
                    trade_date=trade_date,
                    anomaly_type='price_invalid',
                    description=f'{field}价格小于等于零',
                    field_name=field,
                    actual_value=price,
                    expected_range='> 0',
                    severity='error'
                ))

            # 检查价格超出合理范围
            elif price < price_range['min'] or price > price_range['max']:
                anomalies.append(AnomalyRecord(
                    ts_code=ts_code,
                    trade_date=trade_date,
                    anomaly_type='price_out_of_range',
                    description=f'{field}价格超出合理范围',
                    field_name=field,
                    actual_value=price,
                    expected_range=f'{price_range["min"]} - {price_range["max"]}',
                    severity='warning'
                ))

        return anomalies

    def _detect_volume_anomalies(self, data: Dict[str, Any], ts_code: str, trade_date: date) -> List[AnomalyRecord]:
        """检测成交量异常"""
        anomalies = []
        volume = data.get('volume')
        amount = data.get('amount')

        # 检查成交量为负数
        if volume is not None and volume < 0:
            anomalies.append(AnomalyRecord(
                ts_code=ts_code,
                trade_date=trade_date,
                anomaly_type='volume_invalid',
                description='成交量为负数',
                field_name='volume',
                actual_value=volume,
                expected_range='>= 0',
                severity='error'
            ))

        # 检查成交量异常巨大
        volume_limit = self.config.get('volume_limit', 1e12)
        if volume is not None and volume > volume_limit:
            anomalies.append(AnomalyRecord(
                ts_code=ts_code,
                trade_date=trade_date,
                anomaly_type='volume_excessive',
                description='成交量异常巨大',
                field_name='volume',
                actual_value=volume,
                expected_range=f'<= {volume_limit:,}',
                severity='warning'
            ))

        # 检查成交额为负数
        if amount is not None and amount < 0:
            anomalies.append(AnomalyRecord(
                ts_code=ts_code,
                trade_date=trade_date,
                anomaly_type='amount_invalid',
                description='成交额为负数',
                field_name='amount',
                actual_value=amount,
                expected_range='>= 0',
                severity='error'
            ))

        return anomalies

    def _detect_price_logic_anomalies(self, data: Dict[str, Any], ts_code: str, trade_date: date) -> List[AnomalyRecord]:
        """检测价格逻辑异常"""
        anomalies = []

        high = data.get('high', 0)
        low = data.get('low', 0)
        open_price = data.get('open', 0)
        close_price = data.get('close', 0)

        # 检查最高价 < 最低价
        if high > 0 and low > 0 and high < low:
            anomalies.append(AnomalyRecord(
                ts_code=ts_code,
                trade_date=trade_date,
                anomaly_type='price_logic_error',
                description='最高价小于最低价',
                field_name='price_logic',
                actual_value=f'high={high}, low={low}',
                expected_range='high >= low',
                severity='error'
            ))

        # 检查开盘价不在[最低价,最高价]区间
        if all([open_price, high, low]) and not (low <= open_price <= high):
            anomalies.append(AnomalyRecord(
                ts_code=ts_code,
                trade_date=trade_date,
                anomaly_type='open_price_logic_error',
                description='开盘价不在当日价格区间内',
                field_name='open',
                actual_value=open_price,
                expected_range=f'[{low}, {high}]',
                severity='error'
            ))

        # 检查收盘价不在[最低价,最高价]区间
        if all([close_price, high, low]) and not (low <= close_price <= high):
            anomalies.append(AnomalyRecord(
                ts_code=ts_code,
                trade_date=trade_date,
                anomaly_type='close_price_logic_error',
                description='收盘价不在当日价格区间内',
                field_name='close',
                actual_value=close_price,
                expected_range=f'[{low}, {high}]',
                severity='error'
            ))

        return anomalies

    
    def _detect_change_rate_anomalies(self, data: Dict[str, Any], ts_code: str, trade_date: date) -> List[AnomalyRecord]:
        """检测涨跌幅异常"""
        anomalies = []

        change_rate = data.get('change_rate')
        preclose = data.get('preclose')
        close = data.get('close')

        if change_rate is None:
            return anomalies

        # 根据股票类型确定涨跌幅限制
        max_change_rate = self.config.get('max_change_rate', 20)
        is_st = data.get('is_st', False)

        # ST股使用更严格的限制
        if is_st:
            st_config = self.config.get('st_stock_config', {})
            max_change_rate = st_config.get('max_change_rate', 6)

        # 检查涨跌幅超过限制
        if abs(change_rate) > max_change_rate:
            anomalies.append(AnomalyRecord(
                ts_code=ts_code,
                trade_date=trade_date,
                anomaly_type='change_rate_excessive',
                description='涨跌幅超过限制',
                field_name='change_rate',
                actual_value=f'{change_rate:.2f}%',
                expected_range=f'±{max_change_rate}%',
                severity='warning'
            ))

        # 增强的涨跌幅计算验证，避免除零错误
        if preclose and close and abs(preclose) > 1e-10:  # 避免除零
            try:
                expected_change_rate = (close - preclose) / preclose * 100
                # 允许更大的计算误差，考虑浮点精度
                tolerance = 0.05 if abs(change_rate) > 10 else 0.01
                if abs(expected_change_rate - change_rate) > tolerance:
                    anomalies.append(AnomalyRecord(
                        ts_code=ts_code,
                        trade_date=trade_date,
                        anomaly_type='change_rate_calculation_error',
                        description='涨跌幅计算错误',
                        field_name='change_rate',
                        actual_value=f'{change_rate:.2f}%',
                        expected_range=f'{expected_change_rate:.2f}%',
                        severity='error'
                    ))
            except Exception as e:
                self.logger.warning(f"计算涨跌幅验证失败 {ts_code}: {e}")

        return anomalies

    def _detect_change_rate_precision_anomalies(self, data: Dict[str, Any], ts_code: str, trade_date: date) -> List[AnomalyRecord]:
        """检测涨跌幅精度限制异常"""
        anomalies = []

        change_rate = data.get('change_rate')
        preclose = data.get('preclose')
        close = data.get('close')

        # 检查涨跌幅是否被截断为精度限制值
        if change_rate is not None and abs(change_rate) >= 9999.9999:
            # 重新计算原始涨跌幅以获取详细信息
            try:
                from ..utils.data_transformer import DataTransformer
                calc_details = DataTransformer.calculate_change_rate_with_details(close, preclose)

                if calc_details and calc_details.get('is_truncated'):
                    anomalies.append(AnomalyRecord(
                        ts_code=ts_code,
                        trade_date=trade_date,
                        anomaly_type='change_rate_precision_overflow',
                        description='涨跌幅超出精度限制被截断',
                        field_name='change_rate',
                        actual_value={
                            'truncated_value': calc_details['truncated_value'],
                            'raw_value': calc_details['raw_change_rate'],
                            'calculation': calc_details['calculation'],
                            'close_price': calc_details['close_price'],
                            'preclose_price': calc_details['preclose_price']
                        },
                        expected_range='-9999.9999% ~ 9999.9999%',
                        severity='warning'
                    ))
            except Exception as e:
                self.logger.error(f"计算涨跌幅详情失败 {ts_code}: {e}")

        return anomalies

    def get_anomaly_summary(self) -> Dict[str, Any]:
        """
        获取异常汇总统计

        Returns:
            异常汇总字典
        """
        if not self.anomalies:
            return {
                'total_anomalies': 0,
                'error_count': 0,
                'warning_count': 0,
                'anomaly_types': {},
                'affected_stocks': set(),
                'affected_dates': set()
            }

        # 统计各类异常
        anomaly_types = {}
        affected_stocks = set()
        affected_dates = set()
        error_count = 0
        warning_count = 0

        for anomaly in self.anomalies:
            # 按类型统计
            anomaly_type = anomaly.anomaly_type
            if anomaly_type not in anomaly_types:
                anomaly_types[anomaly_type] = {
                    'count': 0,
                    'description': anomaly.description,
                    'severity': anomaly.severity
                }
            anomaly_types[anomaly_type]['count'] += 1

            # 统计影响的股票和日期
            affected_stocks.add(anomaly.ts_code)
            affected_dates.add(str(anomaly.trade_date))

            # 按严重程度统计
            if anomaly.severity == 'error':
                error_count += 1
            else:
                warning_count += 1

        return {
            'total_anomalies': len(self.anomalies),
            'error_count': error_count,
            'warning_count': warning_count,
            'anomaly_types': anomaly_types,
            'affected_stocks': affected_stocks,
            'affected_dates': affected_dates
        }

    def get_anomaly_records_by_severity(self, severity: str = None) -> List[AnomalyRecord]:
        """
        按严重程度获取异常记录

        Args:
            severity: 严重程度 ('error', 'warning', None表示全部)

        Returns:
            异常记录列表
        """
        if severity is None:
            return self.anomalies.copy()

        return [anomaly for anomaly in self.anomalies if anomaly.severity == severity]

    def clear_anomalies(self) -> None:
        """清除异常记录"""
        self.anomalies.clear()