"""
1分钟K线同步领域逻辑
"""
from bisect import bisect_right
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from ..utils.data_transformer import DataTransformer
from .ports import Kline1MinRepositoryPort


class Kline1MinNormalizer:
    def __init__(self, repository: Kline1MinRepositoryPort):
        self.repository = repository

    def build_fundamentals_map(
        self,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        fundamentals_map: Dict[str, Dict[str, Any]] = {}
        for item in records:
            ts_code = item.get('ts_code')
            if not ts_code:
                continue
            disclosure_date = self._normalize_date_str(item.get('disclosure_date'))
            if not disclosure_date:
                continue
            record = {
                'disclosure_date': disclosure_date,
                'total_share': item.get('total_share'),
                'float_share': item.get('float_share')
            }
            entry = fundamentals_map.setdefault(ts_code, {'dates': [], 'records': []})
            entry['dates'].append(disclosure_date)
            entry['records'].append(record)
        for ts_code, entry in fundamentals_map.items():
            combined = sorted(zip(entry['dates'], entry['records']), key=lambda x: x[0])
            entry['dates'] = [x[0] for x in combined]
            entry['records'] = [x[1] for x in combined]
        return fundamentals_map

    def match_fundamentals(
        self,
        ts_code: str,
        trade_date: str,
        fundamentals_map: Dict[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if not trade_date:
            return None
        entry = fundamentals_map.get(ts_code)
        if not entry:
            return None
        dates = entry.get('dates', [])
        if not dates:
            return None
        idx = bisect_right(dates, trade_date)
        if idx <= 0:
            return None
        return entry['records'][idx - 1]

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

        prev_close = self._to_float(previous_preclose)
        if (prev_close is None or prev_close == 0) and normalized:
            first_record = normalized[0]
            first_last = self._to_float(first_record.get('_raw_last'))
            if first_last is not None and first_last != 0:
                prev_close = first_last
            else:
                prev_db_close = self._to_float(self.repository.fetch_prev_close(
                    first_record.get('ts_code'),
                    first_record.get('trade_date'),
                    first_record.get('trade_time')
                ))
                if prev_db_close is not None and prev_db_close != 0:
                    prev_close = prev_db_close

        for record in normalized:
            last_value = self._to_float(record.pop('_raw_last', None))
            if last_value is not None and last_value != 0:
                preclose = last_value
            elif prev_close is not None and prev_close != 0:
                preclose = prev_close
            else:
                preclose = self._to_float(record.get('open'))
            record['preclose'] = preclose

            close = self._to_float(record.get('close'))
            record['close'] = close
            if preclose and close is not None:
                record['change_rate'] = (close - preclose) / preclose * 100
            else:
                record['change_rate'] = None

            fundamentals = self.match_fundamentals(record['ts_code'], record['trade_date'], fundamentals_map)
            if fundamentals:
                record['fundamentals_disclosure_date'] = fundamentals.get('disclosure_date')
                record['total_share'] = self._to_float(fundamentals.get('total_share'))
                record['float_share'] = self._to_float(fundamentals.get('float_share'))
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

            limit_rate = self._get_kline_limit_rate(record.get('stock_code'), record.get('stock_name'))
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

        trade_date = self._normalize_date_str(
            raw.get('trade_date') or raw.get('date') or raw.get('tradeDate')
        )
        trade_time = self._normalize_time_str(
            raw.get('trade_time') or raw.get('time') or raw.get('tradeTime')
        )

        trade_datetime = (
            raw.get('trade_datetime') or raw.get('datetime') or raw.get('Time') or raw.get('time')
        )
        if (not trade_date or not trade_time) and trade_datetime:
            trade_date, trade_time = self._split_datetime(trade_datetime)

        if not trade_date or not trade_time:
            return None

        trade_datetime = f"{trade_date}{trade_time}"

        open_v = self._scale_price(self._to_float(raw.get('open') or raw.get('Open')))
        high_v = self._scale_price(self._to_float(raw.get('high') or raw.get('High')))
        low_v = self._scale_price(self._to_float(raw.get('low') or raw.get('Low')))
        close_v = self._scale_price(self._to_float(raw.get('close') or raw.get('Close')))
        last_v = self._scale_price(self._to_float(raw.get('last') or raw.get('Last')))
        volume_v = self._scale_volume(self._to_float(raw.get('volume') or raw.get('Volume')))
        amount_v = self._scale_amount(self._to_float(raw.get('amount') or raw.get('Amount')))

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

    def _normalize_date_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime('%Y%m%d')
        if isinstance(value, date):
            return value.strftime('%Y%m%d')

        s = str(value).strip()
        if len(s) >= 8 and s[0:8].isdigit():
            if '-' in s and len(s) >= 10:
                return s[0:10].replace('-', '')
            return s[0:8]
        if '-' in s and len(s) >= 10:
            return s[0:10].replace('-', '')
        return None

    def _normalize_time_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        s = str(value).strip()
        if ':' in s:
            parts = s.split(':')
            if len(parts) >= 2:
                hh = parts[0].zfill(2)
                mm = parts[1].zfill(2)
                return f"{hh}{mm}"
        if s.isdigit():
            if len(s) == 4:
                return s
            if len(s) == 3:
                return s.zfill(4)
            if len(s) >= 5:
                return s[0:4]
        return None

    def _split_datetime(self, value: Any) -> Tuple[Optional[str], Optional[str]]:
        s = str(value).strip()
        try:
            dt = datetime.fromisoformat(s)
            return dt.strftime('%Y%m%d'), dt.strftime('%H%M')
        except Exception:
            pass
        if len(s) >= 12 and s[0:12].isdigit():
            return s[0:8], s[8:12]
        if ' ' in s:
            date_part, time_part = s.split(' ', 1)
            date_str = self._normalize_date_str(date_part)
            time_str = self._normalize_time_str(time_part)
            return date_str, time_str
        return None, None

    def _to_float(self, value: Any) -> Optional[float]:
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _scale_price(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return value / 1000.0

    def _scale_amount(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return value / 1000.0

    def _scale_volume(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return value * 100.0

    def _get_kline_limit_rate(self, stock_code: Optional[str], stock_name: Optional[str]) -> float:
        if stock_name and DataTransformer.check_is_st(stock_name):
            return 5.1
        if stock_code:
            if stock_code.startswith(('300', '301')):
                return 20.1
            if stock_code.startswith('68'):
                return 20.1
        return 10.1
