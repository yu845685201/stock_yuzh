"""
his_kline_day DAO：upsert / 整股替换 / 前收盘查询（自 connection.py 拆分，R-06 纯物理移动）
"""

from psycopg2.extras import execute_values as pg_execute_values
from typing import List, Dict, Any, Optional
from datetime import datetime

# his_kline_day 21 列清单（upsert 与 replace 两处 SQL 共用同一常量，逐字取自原 SQL 列清单）
KLINE_DAY_COLUMNS = (
    'ts_code', 'stock_code', 'stock_name', 'trade_date',
    'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'raw_close', 'adjust_flag',
    'change_rate', 'turnover_rate',
    'fundamentals_disclosure_date', 'total_share', 'float_share', 'source', 'create_time', 'update_time',
)

_KLINE_DAY_COLUMN_SQL = "(" + ", ".join(KLINE_DAY_COLUMNS) + ")"


class KlineDayDaoMixin:
    """日K线 DAO"""

    def upsert_his_kline_day(self, kline_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert 日K线数据

        Args:
            kline_data: 日K线数据列表

        Returns:
            影响的行数
        """
        if not kline_data:
            return 0

        upsert_sql = f"""
        INSERT INTO his_kline_day
        {_KLINE_DAY_COLUMN_SQL}
        VALUES %s
        ON CONFLICT (ts_code, trade_date)
        DO UPDATE SET
            stock_code = EXCLUDED.stock_code,
            stock_name = EXCLUDED.stock_name,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            preclose = EXCLUDED.preclose,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            raw_close = EXCLUDED.raw_close,
            adjust_flag = EXCLUDED.adjust_flag,
            change_rate = EXCLUDED.change_rate,
            turnover_rate = EXCLUDED.turnover_rate,
            fundamentals_disclosure_date = EXCLUDED.fundamentals_disclosure_date,
            total_share = EXCLUDED.total_share,
            float_share = EXCLUDED.float_share,
            source = EXCLUDED.source,
            update_time = NOW()
        """

        params_list = []
        now = datetime.now()
        for item in kline_data:
            params_list.append((
                item['ts_code'],
                item['stock_code'],
                item['stock_name'],
                item['trade_date'],
                item.get('open'),
                item.get('high'),
                item.get('low'),
                item.get('close'),
                item.get('preclose'),
                item.get('volume'),
                item.get('amount'),
                item.get('raw_close'),
                item.get('adjust_flag'),
                item.get('change_rate'),
                item.get('turnover_rate'),
                item.get('fundamentals_disclosure_date'),
                item.get('total_share'),
                item.get('float_share'),
                item.get('source'),
                item.get('create_time', now),
                now
            ))

        return self.execute_values(upsert_sql, params_list, page_size=2000)

    def replace_his_kline_day(self, ts_code: str, kline_data: List[Dict[str, Any]]) -> int:
        """
        整股原子替换日K线：单事务内先删该股全部行再批量插入（全量初始化/除权重刷用）

        Args:
            ts_code: 股票ts_code
            kline_data: 该股全量日K数据列表

        Returns:
            插入的行数
        """
        if not kline_data:
            return 0

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM his_kline_day WHERE ts_code = %s", (ts_code,))
                insert_sql = f"""
                INSERT INTO his_kline_day
                {_KLINE_DAY_COLUMN_SQL}
                VALUES %s
                """
                params_list = []
                now = datetime.now()
                for item in kline_data:
                    params_list.append((
                        item['ts_code'],
                        item['stock_code'],
                        item['stock_name'],
                        item['trade_date'],
                        item.get('open'),
                        item.get('high'),
                        item.get('low'),
                        item.get('close'),
                        item.get('preclose'),
                        item.get('volume'),
                        item.get('amount'),
                        item.get('raw_close'),
                        item.get('adjust_flag'),
                        item.get('change_rate'),
                        item.get('turnover_rate'),
                        item.get('fundamentals_disclosure_date'),
                        item.get('total_share'),
                        item.get('float_share'),
                        item.get('source'),
                        item.get('create_time', now),
                        now
                    ))
                pg_execute_values(cursor, insert_sql, params_list, page_size=2000)
            conn.commit()
        return len(kline_data)

    def fetch_prev_his_kline_day_close(self, ts_code: str, trade_date: str) -> Optional[float]:
        """
        获取指定股票在当前交易日之前最近一条日K线的close
        """
        query = """
        SELECT close
        FROM his_kline_day
        WHERE ts_code = %s
          AND trade_date < %s
        ORDER BY trade_date DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code, trade_date))
        if result:
            value = result.get('close')
            return float(value) if value is not None else None
        return None
