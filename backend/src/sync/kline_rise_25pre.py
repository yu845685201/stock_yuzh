"""
立体K线（anal_kline_rise_25pre）生成算法（R-07a 自 sync_manager.py 逐字移动为纯函数模块）
"""

from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple


def generate_kline_rise_25pre(kline_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not kline_rows:
        return []

    rows = sorted(kline_rows, key=lambda x: x.get('trade_datetime'))
    results: List[Dict[str, Any]] = []

    threshold_rate = Decimal('0.025')
    current: Optional[Dict[str, Any]] = None
    reference_price: Optional[Decimal] = None
    segment_sequence: Dict[Tuple[Optional[str], Any, Any], int] = {}

    def _to_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _sign(value: Decimal) -> int:
        if value > 0:
            return 1
        if value < 0:
            return -1
        return 0

    def start_segment(
        row: Dict[str, Any],
        open_price: Decimal
    ) -> Dict[str, Any]:
        return {
            'ts_code': row.get('ts_code'),
            'stock_code': row.get('stock_code'),
            'stock_name': row.get('stock_name'),
            'trade_begin_date': row.get('trade_date'),
            'trade_begin_time': row.get('trade_time'),
            'trade_begin_datetime': row.get('trade_datetime'),
            'trade_date': row.get('trade_date'),
            'trade_time': row.get('trade_time'),
            'trade_datetime': row.get('trade_datetime'),
            'segment_index': None,
            'open': open_price,
            'high': open_price,
            'low': open_price,
            'close': open_price,
            'volume': Decimal('0'),
            'amount': Decimal('0'),
            'change_rate': None,
            'turnover_rate': Decimal('0')
        }

    def apply_slice(
        segment: Dict[str, Any],
        slice_open: Decimal,
        slice_close: Decimal,
        slice_fraction: Decimal,
        row: Dict[str, Any],
        volume_value: Decimal,
        amount_value: Decimal,
        turnover_value: Decimal
    ) -> None:
        if slice_fraction > 0:
            segment['volume'] += volume_value * slice_fraction
            segment['amount'] += amount_value * slice_fraction
            segment['turnover_rate'] += turnover_value * slice_fraction

        slice_high = max(slice_open, slice_close)
        slice_low = min(slice_open, slice_close)
        segment['high'] = slice_high if segment['high'] is None else max(segment['high'], slice_high)
        segment['low'] = slice_low if segment['low'] is None else min(segment['low'], slice_low)
        segment['close'] = slice_close
        segment['trade_date'] = row.get('trade_date')
        segment['trade_time'] = row.get('trade_time')
        segment['trade_datetime'] = row.get('trade_datetime')

    def get_threshold_ratio(
        open_price_value: Decimal,
        close_price_value: Decimal,
        threshold_price_value: Decimal
    ) -> Optional[Decimal]:
        if close_price_value == open_price_value:
            return Decimal('1') if threshold_price_value == open_price_value else None
        ratio = (threshold_price_value - open_price_value) / (close_price_value - open_price_value)
        if ratio <= 0 or ratio > 1:
            return None
        return ratio

    for row in rows:
        open_price = _to_decimal(row.get('open'))
        close_price = _to_decimal(row.get('close'))
        if open_price is None or close_price is None:
            continue

        volume = _to_decimal(row.get('volume')) or Decimal('0')
        amount = _to_decimal(row.get('amount')) or Decimal('0')
        turnover_rate = _to_decimal(row.get('turnover_rate')) or Decimal('0')

        if current is None:
            current = start_segment(row, open_price)
            reference_price = open_price

        if reference_price is None:
            continue

        remaining_fraction = Decimal('1')
        consumed_ratio = Decimal('0')

        while True:
            direction = _sign(close_price - reference_price)
            if direction == 0:
                break

            threshold_price = reference_price * (1 + direction * threshold_rate)
            ratio = get_threshold_ratio(open_price, close_price, threshold_price)
            if ratio is None:
                break

            if ratio < consumed_ratio:
                break

            slice_fraction = ratio - consumed_ratio
            if slice_fraction < 0:
                break

            slice_start_price = open_price + (close_price - open_price) * consumed_ratio
            slice_open = slice_start_price if slice_fraction > 0 else threshold_price
            apply_slice(current, slice_open, threshold_price, slice_fraction, row, volume, amount, turnover_rate)
            current['change_rate'] = Decimal(direction) * Decimal('2.5')
            segment_key = (
                current.get('ts_code'),
                current.get('trade_date'),
                current.get('trade_time')
            )
            current['segment_index'] = segment_sequence.get(segment_key, 0)
            segment_sequence[segment_key] = current['segment_index'] + 1
            results.append(current)

            reference_price = threshold_price
            current = start_segment(row, open_price)

            consumed_ratio = ratio
            remaining_fraction = Decimal('1') - consumed_ratio
            if remaining_fraction <= 0:
                break

        if remaining_fraction > 0 and current is not None:
            remaining_open = open_price + (close_price - open_price) * consumed_ratio
            apply_slice(current, remaining_open, close_price, remaining_fraction, row, volume, amount, turnover_rate)

    return results
