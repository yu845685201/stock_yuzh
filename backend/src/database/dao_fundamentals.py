"""
base_fundamentals_info / base_trade_calendar / base_stock_basic DAO（自 connection.py 拆分，R-06 纯物理移动）
"""

import os
from typing import List, Dict, Any, Optional
from datetime import datetime

# base_fundamentals_info SELECT 列清单（4 处查询 SQL 共用同一常量，逐字取自原 SQL 列清单）
FUNDAMENTALS_COLUMNS = ('ts_code', 'stat_date', 'disclosure_date', 'total_share', 'float_share')

_FUNDAMENTALS_COLUMN_SQL = ", ".join(FUNDAMENTALS_COLUMNS)
_FUNDAMENTALS_COLUMN_SQL_T = ", ".join(f"t.{c}" for c in FUNDAMENTALS_COLUMNS)


class FundamentalsDaoMixin:
    """基本面 / 交易日历 / 股票基础 DAO"""

    def initialize_tables(self) -> None:
        """初始化数据库表 - 读取doc/init.sql"""
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        sql_path = os.path.join(repo_root, 'doc', 'init.sql')
        with open(sql_path, 'r', encoding='utf-8') as f:
            create_tables_sql = f.read()

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for statement in create_tables_sql.split(';'):
                    stmt = statement.strip()
                    if not stmt:
                        continue
                    cursor.execute(stmt + ';')
                conn.commit()

    def upsert_fundamentals_data(self, fundamentals_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert基本面数据

        Args:
            fundamentals_data: 基本面数据列表（含 stat_date 报告期、disclosure_date 真实披露日）

        Returns:
            影响的行数
        """
        if not fundamentals_data:
            return 0

        upsert_sql = """
        INSERT INTO base_fundamentals_info
        (ts_code, stock_code, stock_name, stat_date, disclosure_date, total_share, float_share, create_time, update_time)
        VALUES (%(ts_code)s, %(stock_code)s, %(stock_name)s, %(stat_date)s, %(disclosure_date)s, %(total_share)s, %(float_share)s,
                %(create_time)s, NOW())
        ON CONFLICT (ts_code, stat_date)
        DO UPDATE SET
            stock_code = EXCLUDED.stock_code,
            stock_name = EXCLUDED.stock_name,
            disclosure_date = EXCLUDED.disclosure_date,
            total_share = EXCLUDED.total_share,
            float_share = EXCLUDED.float_share,
            update_time = NOW()
        """

        params_list = []
        for item in fundamentals_data:
            params = {
                'ts_code': item['ts_code'],
                'stock_code': item['stock_code'],
                'stock_name': item['stock_name'],
                'stat_date': item['stat_date'],
                'disclosure_date': item['disclosure_date'],
                'total_share': item['total_share'],
                'float_share': item['float_share'],
                'create_time': item.get('create_time', datetime.now())  # 确保始终有值
            }
            params_list.append(params)

        return self.execute_batch(upsert_sql, params_list)

    def upsert_trade_calendar(self, calendar_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert交易日历数据

        Args:
            calendar_data: 交易日历数据列表

        Returns:
            影响的行数
        """
        if not calendar_data:
            return 0

        upsert_sql = """
        INSERT INTO base_trade_calendar
        (calendar_date, is_trading_day)
        VALUES (%(calendar_date)s, %(is_trading_day)s)
        ON CONFLICT (calendar_date)
        DO UPDATE SET
            is_trading_day = EXCLUDED.is_trading_day
        """

        params_list = []
        for item in calendar_data:
            params_list.append({
                'calendar_date': item['calendar_date'],
                'is_trading_day': item['is_trading_day']
            })

        return self.execute_batch(upsert_sql, params_list)

    def fetch_trade_calendar(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取交易日历区间数据

        Args:
            start_date: yyyy-mm-dd
            end_date: yyyy-mm-dd
        """
        query = """
        SELECT calendar_date
        FROM base_trade_calendar
        WHERE calendar_date >= %s AND calendar_date <= %s AND is_trading_day = 1
        ORDER BY calendar_date ASC
        """
        return self.execute_query(query, (start_date, end_date))

    def fetch_stock_basic(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        获取股票基本信息（优先按type=1过滤）
        """
        params: List[Any] = []
        base_query = "SELECT ts_code, stock_code, stock_name FROM base_stock_info"
        where_clause = []

        if ts_codes:
            where_clause.append("ts_code = ANY(%s)")
            params.append(ts_codes)

        # 尝试按type=1过滤，如果字段不存在则回退
        try:
            query = base_query
            if where_clause:
                query += " WHERE " + " AND ".join(where_clause) + " AND type = '1'"
            else:
                query += " WHERE type = '1'"
            return self.execute_query(query, tuple(params))
        except Exception:
            query = base_query
            if where_clause:
                query += " WHERE " + " AND ".join(where_clause)
            return self.execute_query(query, tuple(params))

    def fetch_fundamentals_all(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        获取所有基本面数据
        """
        params: List[Any] = []
        query = f"""
        SELECT {_FUNDAMENTALS_COLUMN_SQL}
        FROM base_fundamentals_info
        """
        if ts_codes:
            query += " WHERE ts_code = ANY(%s)"
            params.append(ts_codes)
        return self.execute_query(query, tuple(params))

    def fetch_fundamentals_latest(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        获取每只股票最新基本面数据
        """
        params: List[Any] = []
        query = f"""
        SELECT {_FUNDAMENTALS_COLUMN_SQL_T}
        FROM base_fundamentals_info t
        INNER JOIN (
            SELECT ts_code, MAX(disclosure_date) AS disclosure_date
            FROM base_fundamentals_info
            GROUP BY ts_code
        ) m ON t.ts_code = m.ts_code AND t.disclosure_date = m.disclosure_date
        """
        if ts_codes:
            query += " WHERE t.ts_code = ANY(%s)"
            params.append(ts_codes)
        return self.execute_query(query, tuple(params))

    def fetch_fundamentals_range_with_prev(
        self,
        ts_codes: Optional[List[str]],
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        获取指定日期范围内的基本面数据，并补充每只股票在开始日期之前最近一条数据
        """
        params: List[Any] = [start_date, end_date]
        ts_filter = ""
        if ts_codes:
            ts_filter = " AND ts_code = ANY(%s)"
            params.append(ts_codes)

        query = f"""
        WITH in_range AS (
            SELECT {_FUNDAMENTALS_COLUMN_SQL}
            FROM base_fundamentals_info
            WHERE disclosure_date >= %s AND disclosure_date <= %s
            {ts_filter}
        ),
        prev_one AS (
            SELECT DISTINCT ON (ts_code)
                {_FUNDAMENTALS_COLUMN_SQL}
            FROM base_fundamentals_info
            WHERE disclosure_date < %s
            {ts_filter}
            ORDER BY ts_code, disclosure_date DESC
        )
        SELECT * FROM in_range
        UNION ALL
        SELECT * FROM prev_one
        """

        # params for prev_one: start_date + optional ts_codes
        params_prev: List[Any] = [start_date]
        if ts_codes:
            params_prev.append(ts_codes)

        return self.execute_query(query, tuple(params + params_prev))
