"""
anal_kline_rise_25pre DAO：upsert 与最后一根结束时间查询（自 connection.py 拆分，R-06 纯物理移动）
"""

from typing import List, Dict, Any, Optional
from datetime import datetime


class AnalDaoMixin:
    """立体K线 DAO"""

    def upsert_anal_kline_rise_25pre(self, kline_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert 立体K线数据
        """
        if not kline_data:
            return 0

        upsert_sql = """
        INSERT INTO anal_kline_rise_25pre
        (ts_code, stock_code, stock_name,
         trade_begin_date, trade_begin_time, trade_begin_datetime,
         trade_date, trade_time, segment_index, trade_datetime,
         open, high, low, close, volume, amount, change_rate, turnover_rate,
         create_time, update_time)
        VALUES %s
        ON CONFLICT (ts_code, trade_date, trade_time, segment_index)
        DO UPDATE SET
            stock_code = EXCLUDED.stock_code,
            stock_name = EXCLUDED.stock_name,
            trade_begin_date = EXCLUDED.trade_begin_date,
            trade_begin_time = EXCLUDED.trade_begin_time,
            trade_begin_datetime = EXCLUDED.trade_begin_datetime,
            trade_date = EXCLUDED.trade_date,
            trade_time = EXCLUDED.trade_time,
            segment_index = EXCLUDED.segment_index,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            change_rate = EXCLUDED.change_rate,
            turnover_rate = EXCLUDED.turnover_rate,
            update_time = NOW()
        """

        params_list = []
        now = datetime.now()
        for item in kline_data:
            params_list.append((
                item['ts_code'],
                item['stock_code'],
                item['stock_name'],
                self._parse_date_value(item.get('trade_begin_date')),
                self._parse_time_value(item.get('trade_begin_time')),
                self._parse_datetime_value(item.get('trade_begin_datetime')),
                self._parse_date_value(item.get('trade_date')),
                self._parse_time_value(item.get('trade_time')),
                item.get('segment_index', 0),
                self._parse_datetime_value(item.get('trade_datetime')),
                item.get('open'),
                item.get('high'),
                item.get('low'),
                item.get('close'),
                item.get('volume'),
                item.get('amount'),
                item.get('change_rate'),
                item.get('turnover_rate'),
                item.get('create_time', now),
                now
            ))

        return self.execute_values(upsert_sql, params_list, page_size=2000)

    def fetch_last_anal_kline_rise_25pre_end_time(self, ts_code: str) -> Optional[str]:
        """
        获取指定股票最后一根立体K线的结束时间trade_datetime
        """
        query = """
        SELECT trade_datetime
        FROM anal_kline_rise_25pre
        WHERE ts_code = %s
        ORDER BY trade_datetime DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code,))
        if result:
            value = result.get('trade_datetime')
            if isinstance(value, datetime):
                return value.strftime('%Y%m%d%H%M')
            if value is None:
                return None
            return str(value)
        return None
