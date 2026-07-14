"""
1分钟K线分区写入器
"""
import threading
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional

from .ports import Kline1MinPartitionPort


class Kline1MinPartitionWriter:
    def __init__(self, partition_port: Kline1MinPartitionPort, config: Dict[str, Any], logger):
        self.partition_port = partition_port
        self.config = config
        self.logger = logger
        self._lock = threading.Lock()
        self._cleaned_dates: set = set()
        self._cleanup_events: Dict[date, threading.Event] = {}
        self._cleanup_errors: Dict[date, Exception] = {}

        raw_mode = str(self.config.get('sync.kline_1min_partition_cleanup', 'truncate') or 'truncate').strip()
        self.cleanup_mode = raw_mode if raw_mode in ('truncate', 'drop_create') else 'truncate'

    def write_records(self, records: List[Dict[str, Any]]) -> int:
        if not records:
            return 0

        grouped = self._group_by_trade_date(records)
        total_rows = 0
        for trade_date, date_records in grouped.items():
            self._ensure_partition_and_cleanup(trade_date)
            total_rows += self.partition_port.insert_records(date_records)
        return total_rows

    def _group_by_trade_date(self, records: List[Dict[str, Any]]) -> Dict[date, List[Dict[str, Any]]]:
        grouped: Dict[date, List[Dict[str, Any]]] = {}
        for item in records:
            trade_date = self._parse_date(item.get('trade_date'))
            trade_time = self._parse_time(item.get('trade_time'))
            trade_datetime = self._parse_datetime(item.get('trade_datetime'))

            if trade_datetime is None and trade_date and trade_time:
                trade_datetime = datetime.combine(trade_date, trade_time)

            if trade_date is None or trade_time is None or trade_datetime is None:
                continue

            item['trade_date'] = trade_date
            item['trade_time'] = trade_time
            item['trade_datetime'] = trade_datetime

            grouped.setdefault(trade_date, []).append(item)
        return grouped

    def _ensure_partition_and_cleanup(self, trade_date: date) -> None:
        if trade_date is None:
            return

        event: Optional[threading.Event] = None
        do_cleanup = False
        with self._lock:
            if trade_date in self._cleaned_dates:
                return
            if trade_date in self._cleanup_errors:
                raise self._cleanup_errors[trade_date]
            event = self._cleanup_events.get(trade_date)
            if event is None:
                event = threading.Event()
                self._cleanup_events[trade_date] = event
                do_cleanup = True

        if not do_cleanup:
            event.wait()
            with self._lock:
                if trade_date in self._cleanup_errors:
                    raise self._cleanup_errors[trade_date]
                if trade_date in self._cleaned_dates:
                    return
            raise RuntimeError("分区清理失败")

        try:
            self.partition_port.ensure_partition(trade_date)
            self.partition_port.cleanup_partition(trade_date, self.cleanup_mode)
            with self._lock:
                self._cleaned_dates.add(trade_date)
        except Exception as exc:
            with self._lock:
                self._cleanup_errors[trade_date] = exc
            raise
        finally:
            with self._lock:
                if trade_date in self._cleanup_events:
                    self._cleanup_events[trade_date].set()
                    del self._cleanup_events[trade_date]

    def _parse_date(self, value: Any) -> Optional[date]:
        if value is None or value == '':
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        s = str(value).strip()
        if not s:
            return None
        if '-' in s and len(s) >= 10:
            s = s[0:10]
            try:
                return datetime.strptime(s, '%Y-%m-%d').date()
            except ValueError:
                return None
        if len(s) >= 8 and s[0:8].isdigit():
            try:
                return datetime.strptime(s[0:8], '%Y%m%d').date()
            except ValueError:
                return None
        return None

    def _parse_time(self, value: Any) -> Optional[time]:
        if value is None or value == '':
            return None
        if isinstance(value, time) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.time()
        s = str(value).strip()
        if not s:
            return None
        if ':' in s:
            parts = s.split(':')
            if len(parts) >= 2:
                hh = parts[0].zfill(2)
                mm = parts[1].zfill(2)
                try:
                    return datetime.strptime(f"{hh}{mm}", '%H%M').time()
                except ValueError:
                    return None
        if s.isdigit():
            if len(s) >= 4:
                try:
                    return datetime.strptime(s[0:4], '%H%M').time()
                except ValueError:
                    return None
        return None

    def _parse_datetime(self, value: Any) -> Optional[datetime]:
        if value is None or value == '':
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time.min)
        s = str(value).strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            pass
        if len(s) >= 12 and s[0:12].isdigit():
            try:
                return datetime.strptime(s[0:12], '%Y%m%d%H%M')
            except ValueError:
                return None
        if ' ' in s:
            date_part, time_part = s.split(' ', 1)
            date_value = self._parse_date(date_part)
            time_value = self._parse_time(time_part)
            if date_value and time_value:
                return datetime.combine(date_value, time_value)
        return None
