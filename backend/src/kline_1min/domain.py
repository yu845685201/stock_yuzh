"""
1分钟K线同步领域逻辑
"""
from bisect import bisect_right
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from ..utils.kline_normalize import (
    normalize_date_str,
    normalize_time_str,
    split_datetime,
    to_float,
    scale_price,
    scale_amount,
    scale_volume,
    get_kline_limit_rate,
    build_fundamentals_records_map,
    match_fundamentals,
)
from .ports import Kline1MinRepositoryPort


class Kline1MinNormalizer:
    def __init__(self, repository: Kline1MinRepositoryPort):
        self.repository = repository

    def build_fundamentals_map(
        self,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        return build_fundamentals_records_map(records)

    def match_fundamentals(
        self,
        ts_code: str,
        trade_date: str,
        fundamentals_map: Dict[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        return match_fundamentals(ts_code, trade_date, fundamentals_map)

    def normalize_records(
        self,
        raw_records: List[Any],
        stock: Dict[str, Any],
        fundamentals_map: Dict[str, Dict[str, Any]],
        previous_preclose: Optional[float],
        allowed_dates: Optional[set] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        normalized: List[Dict[str, Any]] = []
        anomalies: List[Dict[str, Any]] = []

        for raw in raw_records:
            record = self.normalize_record(raw, stock)
            if record:
                normalized.append(record)

        if not normalized:
            return [], []

        if allowed_dates:
            normalized = [item for item in normalized if item.get('trade_date') in allowed_dates]

        normalized.sort(key=lambda x: (x['trade_date'], x['trade_time']))

        prev_close = to_float(previous_preclose)
        if (prev_close is None or prev_close == 0) and normalized:
            first_record = normalized[0]
            first_last = to_float(first_record.get('_raw_last'))
            if first_last is not None and first_last != 0:
                prev_close = first_last
            else:
                prev_db_close = to_float(self.repository.fetch_prev_close(
                    first_record.get('ts_code'),
                    first_record.get('trade_date'),
                    first_record.get('trade_time')
                ))
                if prev_db_close is not None and prev_db_close != 0:
                    prev_close = prev_db_close

        for record in normalized:
            last_value = to_float(record.pop('_raw_last', None))
            if last_value is not None and last_value != 0:
                preclose = last_value
            elif prev_close is not None and prev_close != 0:
                preclose = prev_close
            else:
                preclose = to_float(record.get('open'))
            record['preclose'] = preclose

            close = to_float(record.get('close'))
            record['close'] = close
            if preclose and close is not None:
                record['change_rate'] = (close - preclose) / preclose * 100
            else:
                record['change_rate'] = None

            fundamentals = self.match_fundamentals(record['ts_code'], record['trade_date'], fundamentals_map)
            if fundamentals:
                record['fundamentals_disclosure_date'] = fundamentals.get('disclosure_date')
                record['total_share'] = to_float(fundamentals.get('total_share'))
                record['float_share'] = to_float(fundamentals.get('float_share'))
            else:
                record['fundamentals_disclosure_date'] = None
                record['total_share'] = None
                record['float_share'] = None

            float_share = record.get('float_share')
            volume = record.get('volume')
            if float_share and volume is not None and float_share != 0:
                record['turnover_rate'] = volume / float_share * 100
            else:
                record['turnover_rate'] = None

            record['source'] = 'TDXAPI'

            limit_rate = get_kline_limit_rate(record.get('stock_code'), record.get('stock_name'))
            change_rate = record.get('change_rate')
            if change_rate is not None and abs(change_rate) > limit_rate:
                anomalies.append({
                    'ts_code': record.get('ts_code'),
                    'trade_date': record.get('trade_date'),
                    'trade_time': record.get('trade_time'),
                    'change_rate': round(change_rate, 6),
                    'limit_rate': limit_rate
                })

            prev_close = close

        return normalized, anomalies

    def normalize_record(self, raw: Any, stock: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        trade_date = normalize_date_str(
            raw.get('trade_date') or raw.get('date') or raw.get('tradeDate')
        )
        trade_time = normalize_time_str(
            raw.get('trade_time') or raw.get('time') or raw.get('tradeTime')
        )

        trade_datetime = (
            raw.get('trade_datetime') or raw.get('datetime') or raw.get('Time') or raw.get('time')
        )
        if (not trade_date or not trade_time) and trade_datetime:
            trade_date, trade_time = split_datetime(trade_datetime)

        if not trade_date or not trade_time:
            return None

        trade_datetime = f"{trade_date}{trade_time}"

        open_v = scale_price(to_float(raw.get('open') or raw.get('Open')))
        high_v = scale_price(to_float(raw.get('high') or raw.get('High')))
        low_v = scale_price(to_float(raw.get('low') or raw.get('Low')))
        close_v = scale_price(to_float(raw.get('close') or raw.get('Close')))
        last_v = scale_price(to_float(raw.get('last') or raw.get('Last')))
        volume_v = scale_volume(to_float(raw.get('volume') or raw.get('Volume')))
        amount_v = scale_amount(to_float(raw.get('amount') or raw.get('Amount')))

        return {
            'ts_code': stock.get('ts_code'),
            'stock_code': stock.get('stock_code'),
            'stock_name': stock.get('stock_name'),
            'trade_date': trade_date,
            'trade_time': trade_time,
            'trade_datetime': trade_datetime,
            'open': open_v,
            'high': high_v,
            'low': low_v,
            'close': close_v,
            'preclose': None,
            '_raw_last': last_v,
            'volume': volume_v,
            'amount': amount_v
        }
