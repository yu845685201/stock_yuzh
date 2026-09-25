"""
his_kline_1min 分区写入与查询 DAO（自 connection.py 拆分，R-06 纯物理移动）
"""

import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta


class Kline1MinDaoMixin:
    """1分钟K线分区 DAO"""

    def ensure_his_kline_1min_partition(self, trade_date_value: Any) -> None:
        """按交易日创建1分钟K线分区（按需）"""
        trade_date = self._parse_date_value(trade_date_value)
        if not trade_date:
            return
        partition_name = f"his_kline_1min_p{trade_date.strftime('%Y%m%d')}"
        start_date = trade_date
        end_date = trade_date + timedelta(days=1)
        sql = (
            "CREATE TABLE IF NOT EXISTS {partition} "
            "PARTITION OF his_kline_1min FOR VALUES FROM (%s) TO (%s)"
        ).format(partition=partition_name)
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, (start_date, end_date))
                conn.commit()

    def cleanup_his_kline_1min_partition(self, trade_date_value: Any, mode: str) -> None:
        """按交易日清理1分钟K线分区"""
        trade_date = self._parse_date_value(trade_date_value)
        if not trade_date:
            return
        partition_name = f"his_kline_1min_p{trade_date.strftime('%Y%m%d')}"
        cleanup_mode = (mode or 'truncate').strip()
        if cleanup_mode not in ('truncate', 'drop_create'):
            cleanup_mode = 'truncate'

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                if cleanup_mode == 'drop_create':
                    cursor.execute(f"DROP TABLE IF EXISTS {partition_name}")
                    cursor.execute(
                        "CREATE TABLE {partition} PARTITION OF his_kline_1min FOR VALUES FROM (%s) TO (%s)".format(
                            partition=partition_name
                        ),
                        (trade_date, trade_date + timedelta(days=1))
                    )
                else:
                    cursor.execute("SELECT to_regclass(%s)", (f"public.{partition_name}",))
                    exists_row = cursor.fetchone()
                    if exists_row and exists_row[0]:
                        cursor.execute(f"TRUNCATE TABLE {partition_name}")
                conn.commit()

    def insert_his_kline_1min_partition(self, kline_data: List[Dict[str, Any]]) -> int:
        """批量写入1分钟K线分区表（路由到父表）"""
        if not kline_data:
            return 0

        insert_sql = """
        INSERT INTO his_kline_1min
        (ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
         open, high, low, close, preclose, volume, amount, change_rate, turnover_rate,
         fundamentals_disclosure_date, total_share, float_share, source, create_time, update_time)
        VALUES %s
        ON CONFLICT (ts_code, trade_date, trade_time) DO NOTHING
        """

        raw_batch_size = self.config_manager.get('sync.kline_1min_partition_batch_size', 5000)
        try:
            batch_size = max(500, int(raw_batch_size or 5000))
        except (TypeError, ValueError):
            batch_size = 5000

        total_rows = 0
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                now = datetime.now()
                for i in range(0, len(kline_data), batch_size):
                    batch = kline_data[i:i + batch_size]
                    params_list = []
                    for item in batch:
                        trade_date_value = self._parse_date_value(item.get('trade_date'))
                        trade_time_value = self._parse_time_value(item.get('trade_time'))
                        trade_datetime_value = self._parse_datetime_value(item.get('trade_datetime'))
                        if not trade_datetime_value and trade_date_value and trade_time_value:
                            trade_datetime_value = datetime.combine(trade_date_value, trade_time_value)
                        if not trade_date_value or not trade_time_value or not trade_datetime_value:
                            continue
                        params_list.append((
                            item['ts_code'],
                            item['stock_code'],
                            item['stock_name'],
                            trade_date_value,
                            trade_time_value,
                            trade_datetime_value,
                            item.get('open'),
                            item.get('high'),
                            item.get('low'),
                            item.get('close'),
                            item.get('preclose'),
                            item.get('volume'),
                            item.get('amount'),
                            item.get('change_rate'),
                            item.get('turnover_rate'),
                            item.get('fundamentals_disclosure_date'),
                            item.get('total_share'),
                            item.get('float_share'),
                            item.get('source'),
                            item.get('create_time', now),
                            now
                        ))

                    if not params_list:
                        continue
                    psycopg2.extras.execute_values(cursor, insert_sql, params_list, page_size=batch_size)
                    if cursor.rowcount and cursor.rowcount > 0:
                        total_rows += cursor.rowcount
                conn.commit()

        return total_rows

    def fetch_last_his_kline_1min_close(self, ts_code: str) -> Optional[float]:
        """
        获取指定股票最后一条1分钟K线的close
        """
        query = """
        SELECT close
        FROM his_kline_1min
        WHERE ts_code = %s
        ORDER BY trade_date DESC, trade_time DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code,))
        if result:
            value = result.get('close')
            return float(value) if value is not None else None
        return None

    def fetch_prev_his_kline_1min_close(self, ts_code: str, trade_date: str, trade_time: str) -> Optional[float]:
        """
        获取指定股票在当前交易时间之前最近一条1分钟K线的close
        """
        parsed_date = self._parse_date_value(trade_date)
        parsed_time = self._parse_time_value(trade_time)
        if not parsed_date or not parsed_time:
            return None
        query = """
        SELECT close
        FROM his_kline_1min
        WHERE ts_code = %s
          AND (trade_date < %s OR (trade_date = %s AND trade_time < %s))
        ORDER BY trade_date DESC, trade_time DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code, parsed_date, parsed_date, parsed_time))
        if result:
            value = result.get('close')
            return float(value) if value is not None else None
        return None

    def fetch_his_kline_1min_by_ts_code(self, ts_code: str) -> List[Dict[str, Any]]:
        """
        获取指定股票全量1分钟K线数据
        """
        query = """
        SELECT ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
               open, high, low, close, preclose, volume, amount, change_rate, turnover_rate
        FROM his_kline_1min
        WHERE ts_code = %s
        ORDER BY trade_datetime ASC
        """
        return self.execute_query(query, (ts_code,))

    def fetch_his_kline_1min_after(self, ts_code: str, trade_datetime: str) -> List[Dict[str, Any]]:
        """
        获取指定股票在某时间点之后的1分钟K线数据
        """
        parsed_dt = self._parse_datetime_value(trade_datetime)
        if not parsed_dt:
            return []
        query = """
        SELECT ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
               open, high, low, close, preclose, volume, amount, change_rate, turnover_rate
        FROM his_kline_1min
        WHERE ts_code = %s AND trade_datetime > %s
        ORDER BY trade_datetime ASC
        """
        return self.execute_query(query, (ts_code, parsed_dt))
