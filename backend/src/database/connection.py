"""
数据库连接管理 - 支持连接池优化
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from datetime import datetime, date, time, timedelta
from ..config import ConfigManager
import logging
import threading
import os

class DatabaseConnectionPool:
    """数据库连接池管理类 - 提供高性能的连接复用"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, config_manager: ConfigManager = None):
        """单例模式确保全局只有一个连接池"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化连接池

        Args:
            config_manager: 配置管理器
        """
        if hasattr(self, '_initialized'):
            return

        self.config_manager = config_manager or ConfigManager()
        self.db_config = self.config_manager.get_database_config()
        self.logger = logging.getLogger(__name__)

        # 连接池配置
        pool_config = self.config_manager.get('database.pool', {})
        self.min_conn = pool_config.get('min_connections', 2)
        self.max_conn = pool_config.get('max_connections', 10)

        # 创建连接池
        self._create_pool()
        self._initialized = True

        self.logger.info(f"数据库连接池已初始化: min={self.min_conn}, max={self.max_conn}")

    def _create_pool(self):
        """创建连接池"""
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.min_conn,
                maxconn=self.max_conn,
                **self.db_config
            )
            self.logger.info("连接池创建成功")
        except Exception as e:
            self.logger.error(f"连接池创建失败: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """从连接池获取连接的上下文管理器"""
        conn = None
        try:
            conn = self.pool.getconn()
            conn.autocommit = False
            yield conn
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"数据库操作失败: {e}")
            raise e
        finally:
            if conn:
                self.pool.putconn(conn)

    def get_pool_status(self) -> Dict[str, Any]:
        """获取连接池状态信息"""
        try:
            return {
                'min_connections': self.min_conn,
                'max_connections': self.max_conn,
                'current_connections': getattr(self.pool, '_used', 0) if hasattr(self.pool, '_used') else 'unknown'
            }
        except Exception as e:
            self.logger.error(f"获取连接池状态失败: {e}")
            return {'error': str(e)}

    def close_pool(self):
        """关闭连接池"""
        try:
            if hasattr(self, 'pool'):
                self.pool.closeall()
                self.logger.info("连接池已关闭")
        except Exception as e:
            self.logger.error(f"关闭连接池失败: {e}")

class DatabaseConnection:
    """数据库连接管理类 - 向后兼容，支持连接池优化"""

    def __init__(self, config_manager: ConfigManager = None, use_pool: bool = True):
        """
        初始化数据库连接

        Args:
            config_manager: 配置管理器
            use_pool: 是否使用连接池，默认True
        """
        self.config_manager = config_manager or ConfigManager()
        self.db_config = self.config_manager.get_database_config()
        self.use_pool = use_pool and self.config_manager.get('database.pool.enabled', True)

        if self.use_pool:
            self.pool_manager = DatabaseConnectionPool(config_manager)
            self.logger = logging.getLogger(__name__)
            self.logger.info("数据库连接已配置为使用连接池模式")
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.info("数据库连接已配置为使用传统模式")

    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器 - 支持连接池和传统模式"""
        if self.use_pool:
            # 使用连接池模式
            with self.pool_manager.get_connection() as conn:
                yield conn
        else:
            # 使用传统模式
            conn = None
            try:
                conn = psycopg2.connect(**self.db_config)
                conn.autocommit = False
                yield conn
            except psycopg2.Error as e:
                if conn:
                    conn.rollback()
                raise e
            finally:
                if conn:
                    conn.close()

    def _parse_date_value(self, value: Any) -> Optional[date]:
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
            try:
                return datetime.strptime(s[0:10], '%Y-%m-%d').date()
            except ValueError:
                return None
        if len(s) >= 8 and s[0:8].isdigit():
            try:
                return datetime.strptime(s[0:8], '%Y%m%d').date()
            except ValueError:
                return None
        return None

    def _parse_time_value(self, value: Any) -> Optional[time]:
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
        if s.isdigit() and len(s) >= 4:
            try:
                return datetime.strptime(s[0:4], '%H%M').time()
            except ValueError:
                return None
        return None

    def _parse_datetime_value(self, value: Any) -> Optional[datetime]:
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
            date_value = self._parse_date_value(date_part)
            time_value = self._parse_time_value(time_part)
            if date_value and time_value:
                return datetime.combine(date_value, time_value)
        return None

    def _get_column_data_type(self, cursor, table: str, column: str) -> Optional[str]:
        query = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        """
        cursor.execute(query, (table, column))
        row = cursor.fetchone()
        return row[0] if row else None

    def _build_date_convert_expr(self, column: str) -> str:
        text_col = f"{column}::text"
        return (
            "CASE "
            f"WHEN {column} IS NULL THEN NULL "
            f"WHEN {text_col} ~ '^\\d{{8}}$' THEN to_date({text_col}, 'YYYYMMDD') "
            f"WHEN {text_col} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN {text_col}::date "
            "ELSE NULL END"
        )

    def _build_time_convert_expr(self, column: str) -> str:
        text_col = f"{column}::text"
        return (
            "CASE "
            f"WHEN {column} IS NULL THEN NULL "
            f"WHEN {text_col} ~ '^\\d{{3,4}}$' THEN to_timestamp(lpad({text_col}, 4, '0'), 'HH24MI')::time "
            f"WHEN {text_col} ~ '^\\d{{2}}:\\d{{2}}(:\\d{{2}})?$' THEN {text_col}::time "
            "ELSE NULL END"
        )

    def _build_datetime_convert_expr(self, column: str, date_column: Optional[str] = None, time_column: Optional[str] = None) -> str:
        text_col = f"{column}::text"
        parts = [
            f"WHEN {column} IS NULL THEN NULL",
            f"WHEN {text_col} ~ '^\\d{{12}}$' THEN to_timestamp({text_col}, 'YYYYMMDDHH24MI')",
            f"WHEN {text_col} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN {text_col}::timestamp",
        ]
        if date_column and time_column:
            date_text = f"{date_column}::text"
            time_text = f"{time_column}::text"
            parts.append(
                f"WHEN {date_text} ~ '^\\d{{8}}$' AND {time_text} ~ '^\\d{{3,4}}$' "
                f"THEN to_timestamp(lpad({date_text}, 8, '0') || lpad({time_text}, 4, '0'), 'YYYYMMDDHH24MI')"
            )
            parts.append(
                f"WHEN {date_text} ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' AND {time_text} ~ '^\\d{{2}}:\\d{{2}}(:\\d{{2}})?$' "
                f"THEN ({date_text} || ' ' || {time_text})::timestamp"
            )
        parts.append("ELSE NULL")
        return "CASE " + " ".join(parts) + " END"

    def _upgrade_anal_kline_rise_25pre_types(self, cursor) -> None:
        table_name = 'anal_kline_rise_25pre'
        alter_clauses = []

        for column in ('trade_begin_date', 'trade_date'):
            data_type = self._get_column_data_type(cursor, table_name, column)
            if data_type in ('character varying', 'text'):
                alter_clauses.append(
                    f"ALTER COLUMN {column} TYPE DATE USING {self._build_date_convert_expr(column)}"
                )

        for column in ('trade_begin_time', 'trade_time'):
            data_type = self._get_column_data_type(cursor, table_name, column)
            if data_type in ('character varying', 'text'):
                alter_clauses.append(
                    f"ALTER COLUMN {column} TYPE TIME USING {self._build_time_convert_expr(column)}"
                )

        datetime_columns = (
            ('trade_begin_datetime', 'trade_begin_date', 'trade_begin_time'),
            ('trade_datetime', 'trade_date', 'trade_time'),
        )
        for column, date_column, time_column in datetime_columns:
            data_type = self._get_column_data_type(cursor, table_name, column)
            if data_type in ('character varying', 'text'):
                alter_clauses.append(
                    f"ALTER COLUMN {column} TYPE TIMESTAMP USING "
                    f"{self._build_datetime_convert_expr(column, date_column, time_column)}"
                )

        if alter_clauses:
            cursor.execute(f"ALTER TABLE {table_name} " + ", ".join(alter_clauses))

    def _rollback_anal_kline_rise_25pre_types(self, cursor) -> None:
        table_name = 'anal_kline_rise_25pre'
        alter_clauses = []

        for column in ('trade_begin_date', 'trade_date'):
            data_type = self._get_column_data_type(cursor, table_name, column)
            if data_type == 'date':
                alter_clauses.append(
                    f"ALTER COLUMN {column} TYPE VARCHAR(8) USING to_char({column}, 'YYYYMMDD')"
                )

        for column in ('trade_begin_time', 'trade_time'):
            data_type = self._get_column_data_type(cursor, table_name, column)
            if data_type == 'time without time zone':
                alter_clauses.append(
                    f"ALTER COLUMN {column} TYPE VARCHAR(4) USING to_char({column}, 'HH24MI')"
                )

        datetime_columns = (
            'trade_begin_datetime',
            'trade_datetime',
        )
        for column in datetime_columns:
            data_type = self._get_column_data_type(cursor, table_name, column)
            if data_type == 'timestamp without time zone':
                alter_clauses.append(
                    f"ALTER COLUMN {column} TYPE VARCHAR(12) USING to_char({column}, 'YYYYMMDDHH24MI')"
                )

        if alter_clauses:
            cursor.execute(f"ALTER TABLE {table_name} " + ", ".join(alter_clauses))

    def _table_exists(self, cursor, table_name: str) -> bool:
        cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        return cursor.fetchone()[0] is not None

    def _get_table_relkind(self, cursor, table_name: str) -> Optional[str]:
        cursor.execute(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname = %s
            """,
            (table_name,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def _rename_table_constraints(self, cursor, table_name: str, suffix: str, remove_suffix: bool = False) -> None:
        cursor.execute(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = %s::regclass
            """,
            (table_name,)
        )
        rows = cursor.fetchall()
        for row in rows:
            conname = row[0]
            if remove_suffix:
                if not conname.endswith(suffix):
                    continue
                new_name = conname[: -len(suffix)]
            else:
                if conname.endswith(suffix):
                    continue
                new_name = f"{conname}{suffix}"
            if len(new_name) > 63:
                new_name = new_name[:63]
            cursor.execute(f"ALTER TABLE {table_name} RENAME CONSTRAINT {conname} TO {new_name}")

    def _get_table_indexes(self, cursor, table_name: str) -> List[str]:
        cursor.execute(
            """
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public' AND tablename = %s
            """,
            (table_name,)
        )
        return [row[0] for row in cursor.fetchall()]

    def _rename_table_indexes(self, cursor, table_name: str, suffix: str, remove_suffix: bool = False) -> None:
        rows = self._get_table_indexes(cursor, table_name)
        for index_name in rows:
            if remove_suffix:
                if not index_name.endswith(suffix):
                    continue
                new_name = index_name[: -len(suffix)]
            else:
                if index_name.endswith(suffix):
                    continue
                new_name = f"{index_name}{suffix}"
            if len(new_name) > 63:
                new_name = new_name[:63]
            cursor.execute(f"ALTER INDEX {index_name} RENAME TO {new_name}")

    def _rename_sequence_for_table(self, cursor, table_name: str, column: str, new_sequence_name: str) -> None:
        cursor.execute("SELECT pg_get_serial_sequence(%s, %s)", (table_name, column))
        row = cursor.fetchone()
        if not row or not row[0]:
            return
        sequence_full = row[0]
        sequence_name = sequence_full.split('.')[-1]
        if sequence_name != new_sequence_name:
            cursor.execute(f"ALTER SEQUENCE {sequence_name} RENAME TO {new_sequence_name}")
        cursor.execute(f"ALTER SEQUENCE {new_sequence_name} OWNED BY {table_name}.{column}")
        cursor.execute(
            f"ALTER TABLE {table_name} ALTER COLUMN {column} SET DEFAULT nextval('{new_sequence_name}'::regclass)"
        )

    def _ensure_update_modified_function(self, cursor) -> None:
        cursor.execute(
            """
            CREATE OR REPLACE FUNCTION update_modified_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.update_time = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        )

    def migrate_his_kline_1min_partitions(self) -> None:
        """迁移1分钟K线为按天分区表"""
        self.logger.info("开始迁移1分钟K线分区表")
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                relkind = self._get_table_relkind(cursor, 'his_kline_1min')
                if relkind is None:
                    raise RuntimeError("未找到his_kline_1min表")
                if relkind == 'p':
                    raise RuntimeError("his_kline_1min已是分区表，无需迁移")
                if self._table_exists(cursor, 'his_kline_1min_bak'):
                    raise RuntimeError("检测到his_kline_1min_bak已存在，请先清理或回滚")

                cursor.execute("ALTER TABLE his_kline_1min RENAME TO his_kline_1min_bak")
                self._rename_table_constraints(cursor, 'his_kline_1min_bak', '_bak')
                self._rename_table_indexes(cursor, 'his_kline_1min_bak', '_bak')
                self._rename_sequence_for_table(cursor, 'his_kline_1min_bak', 'id', 'his_kline_1min_bak_id_seq')
                conn.commit()

                create_table_sql = """
                CREATE TABLE his_kline_1min (
                    id BIGSERIAL,
                    ts_code VARCHAR(20),
                    stock_code VARCHAR(20),
                    stock_name VARCHAR(20),
                    trade_date DATE,
                    trade_time TIME,
                    trade_datetime TIMESTAMP,
                    open NUMERIC(20, 4),
                    high NUMERIC(20, 4),
                    low NUMERIC(20, 4),
                    close NUMERIC(20, 4),
                    preclose NUMERIC(20, 4),
                    volume NUMERIC(20, 0),
                    amount NUMERIC(20, 4),
                    adjust_flag SMALLINT,
                    change_rate NUMERIC(10, 6),
                    turnover_rate NUMERIC(10, 6),
                    fundamentals_disclosure_date VARCHAR(8),
                    total_share NUMERIC(20, 4),
                    float_share NUMERIC(20, 4),
                    source VARCHAR(20),
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) PARTITION BY RANGE (trade_date)
                """
                cursor.execute(create_table_sql)
                cursor.execute(
                    "ALTER TABLE his_kline_1min ADD CONSTRAINT uk_his_kline_1min_code_date_time "
                    "UNIQUE (ts_code, trade_date, trade_time)"
                )
                cursor.execute(
                    "CREATE INDEX idx_his_kline_1min_ts_code_trade_date ON his_kline_1min (ts_code, trade_date)"
                )
                self._ensure_update_modified_function(cursor)
                cursor.execute(
                    "CREATE TRIGGER update_his_kline_1min_modtime "
                    "BEFORE UPDATE ON his_kline_1min FOR EACH ROW EXECUTE FUNCTION update_modified_column()"
                )
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS his_kline_1min_staging")
                conn.commit()

                self._upgrade_anal_kline_rise_25pre_types(cursor)
                conn.commit()

                cursor.execute("SELECT DISTINCT trade_date FROM his_kline_1min_bak ORDER BY trade_date")
                rows = cursor.fetchall()

                date_expr = self._build_date_convert_expr('trade_date')
                time_expr = self._build_time_convert_expr('trade_time')
                datetime_expr = self._build_datetime_convert_expr('trade_datetime', 'trade_date', 'trade_time')

                for row in rows:
                    trade_date_value = self._parse_date_value(row[0])
                    if not trade_date_value:
                        continue
                    partition_name = f"his_kline_1min_p{trade_date_value.strftime('%Y%m%d')}"
                    cursor.execute(
                        "CREATE TABLE IF NOT EXISTS {partition} "
                        "PARTITION OF his_kline_1min FOR VALUES FROM (%s) TO (%s)".format(
                            partition=partition_name
                        ),
                        (trade_date_value, trade_date_value + timedelta(days=1))
                    )

                    insert_sql = f"""
                    WITH src AS (
                        SELECT
                            ts_code,
                            stock_code,
                            stock_name,
                            {date_expr} AS trade_date,
                            {time_expr} AS trade_time,
                            {datetime_expr} AS trade_datetime,
                            open,
                            high,
                            low,
                            close,
                            preclose,
                            volume,
                            amount,
                            adjust_flag,
                            change_rate,
                            turnover_rate,
                            fundamentals_disclosure_date,
                            total_share,
                            float_share,
                            source,
                            create_time,
                            update_time
                        FROM his_kline_1min_bak
                    )
                    INSERT INTO his_kline_1min (
                        ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
                        open, high, low, close, preclose, volume, amount, adjust_flag,
                        change_rate, turnover_rate, fundamentals_disclosure_date, total_share,
                        float_share, source, create_time, update_time
                    )
                    SELECT
                        ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
                        open, high, low, close, preclose, volume, amount, adjust_flag,
                        change_rate, turnover_rate, fundamentals_disclosure_date, total_share,
                        float_share, source, create_time, update_time
                    FROM (
                        SELECT DISTINCT ON (ts_code, trade_date, trade_time)
                            ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
                            open, high, low, close, preclose, volume, amount, adjust_flag,
                            change_rate, turnover_rate, fundamentals_disclosure_date, total_share,
                            float_share, source, create_time, update_time
                        FROM src
                        WHERE trade_date = %s
                        ORDER BY ts_code, trade_date, trade_time, update_time DESC NULLS LAST
                    ) deduped
                    """
                    cursor.execute(insert_sql, (trade_date_value,))
                    conn.commit()

        self.logger.info("1分钟K线分区表迁移完成")

    def rollback_his_kline_1min_partitions(self) -> None:
        """回滚1分钟K线分区表"""
        self.logger.info("开始回滚1分钟K线分区表")
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                if not self._table_exists(cursor, 'his_kline_1min_bak'):
                    raise RuntimeError("未找到his_kline_1min_bak，无法回滚")

                if self._table_exists(cursor, 'his_kline_1min'):
                    cursor.execute("DROP TABLE IF EXISTS his_kline_1min CASCADE")
                cursor.execute("ALTER TABLE his_kline_1min_bak RENAME TO his_kline_1min")
                self._rename_table_constraints(cursor, 'his_kline_1min', '_bak', remove_suffix=True)
                self._rename_table_indexes(cursor, 'his_kline_1min', '_bak', remove_suffix=True)
                self._rename_sequence_for_table(cursor, 'his_kline_1min', 'id', 'his_kline_1min_id_seq')

                self._rollback_anal_kline_rise_25pre_types(cursor)
                conn.commit()

        self.logger.info("1分钟K线分区表回滚完成")

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        执行查询语句

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果列表
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        执行查询并返回所有结果 - 兼容方法

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果列表
        """
        return self.execute_query(query, params)

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """
        执行查询并返回第一条结果

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果字典或None
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return dict(result) if result else None

    def execute_update(self, query: str, params: tuple = None) -> int:
        """
        执行更新语句

        Args:
            query: SQL更新语句
            params: 更新参数

        Returns:
            影响的行数
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount

    def execute_batch(self, query: str, params_list: List[tuple]) -> int:
        """
        批量执行语句

        Args:
            query: SQL语句
            params_list: 参数列表

        Returns:
            影响的行数
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, params_list)
                conn.commit()
                return cursor.rowcount

    def execute_values(self, query: str, params_list: List[tuple], page_size: int = 1000) -> int:
        """
        批量执行语句（使用execute_values，适合大批量插入）

        Args:
            query: SQL语句（包含VALUES %s）
            params_list: 参数列表
            page_size: 每批次大小

        Returns:
            影响的行数
        """
        if not params_list:
            return 0
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, query, params_list, page_size=page_size)
                conn.commit()
                return cursor.rowcount

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
            fundamentals_data: 基本面数据列表

        Returns:
            影响的行数
        """
        if not fundamentals_data:
            return 0

        upsert_sql = """
        INSERT INTO base_fundamentals_info
        (ts_code, stock_code, stock_name, disclosure_date, total_share, float_share, create_time, update_time)
        VALUES (%(ts_code)s, %(stock_code)s, %(stock_name)s, %(disclosure_date)s, %(total_share)s, %(float_share)s,
                %(create_time)s, NOW())
        ON CONFLICT (ts_code, disclosure_date)
        DO UPDATE SET
            stock_code = EXCLUDED.stock_code,
            stock_name = EXCLUDED.stock_name,
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

    def upsert_his_kline_1min(self, kline_data: List[Dict[str, Any]]) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

    def truncate_his_kline_1min_staging(self) -> None:
        """保留旧接口占位（已弃用）"""
        return None

    def set_his_kline_1min_staging_unlogged(self, enable: bool) -> None:
        """保留旧接口占位（已弃用）"""
        return None

    def copy_his_kline_1min_staging(self, kline_data: List[Dict[str, Any]]):
        """保留旧接口占位（已弃用）"""
        return 0, False

    def insert_his_kline_1min_staging(self, kline_data: List[Dict[str, Any]]) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

    def merge_his_kline_1min_from_staging(self) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

    def _merge_his_kline_1min_from_staging_legacy(self) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

    def _merge_his_kline_1min_from_staging_optimized(self) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

    def _get_kline_1min_merge_batch_size(self) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

    def _ensure_his_kline_1min_staging_index(self, cursor) -> None:
        """保留旧接口占位（已弃用）"""
        return None

    def _materialize_his_kline_1min_dedup(self, cursor) -> None:
        """保留旧接口占位（已弃用）"""
        return None

    def _fetch_his_kline_1min_dedup_ts_codes(self, cursor) -> List[str]:
        """保留旧接口占位（已弃用）"""
        return []

    def _merge_his_kline_1min_batch_update(self, cursor, ts_codes: List[str]) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

    def _merge_his_kline_1min_batch_insert(self, cursor, ts_codes: List[str]) -> int:
        """保留旧接口占位（已弃用）"""
        return 0

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

        upsert_sql = """
        INSERT INTO his_kline_day
        (ts_code, stock_code, stock_name, trade_date,
         open, high, low, close, preclose, volume, amount, change_rate, turnover_rate,
         fundamentals_disclosure_date, total_share, float_share, source, create_time, update_time)
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

    def ensure_anal_kline_rise_25pre_constraints(self) -> None:
        """
        确保立体K线表存在唯一约束
        """
        ddl = """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uk_anal_kline_rise_25pre_code_time'
                  AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ) THEN
                ALTER TABLE anal_kline_rise_25pre
                DROP CONSTRAINT uk_anal_kline_rise_25pre_code_time;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uk_anal_kline_rise_25pre_code_time_segment'
                  AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ) THEN
                ALTER TABLE anal_kline_rise_25pre
                ADD CONSTRAINT uk_anal_kline_rise_25pre_code_time_segment UNIQUE (ts_code, trade_date, trade_time, segment_index);
            END IF;
        END $$;
        """
        self.execute_update(ddl)

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
        query = """
        SELECT ts_code, disclosure_date, total_share, float_share
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
        query = """
        SELECT t.ts_code, t.disclosure_date, t.total_share, t.float_share
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

    def fetch_fundamentals_in_range(self, ts_codes: Optional[List[str]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取指定日期范围内基本面数据
        """
        params: List[Any] = [start_date, end_date]
        query = """
        SELECT ts_code, disclosure_date, total_share, float_share
        FROM base_fundamentals_info
        WHERE disclosure_date >= %s AND disclosure_date <= %s
        """
        if ts_codes:
            query += " AND ts_code = ANY(%s)"
            params.append(ts_codes)
        return self.execute_query(query, tuple(params))

    def fetch_fundamentals_up_to(self, ts_codes: Optional[List[str]], end_date: str) -> List[Dict[str, Any]]:
        """
        获取截止到指定日期的基本面数据（用于匹配交易日前最近披露）
        """
        params: List[Any] = [end_date]
        query = """
        SELECT ts_code, disclosure_date, total_share, float_share
        FROM base_fundamentals_info
        WHERE disclosure_date <= %s
        """
        if ts_codes:
            query += " AND ts_code = ANY(%s)"
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
            SELECT ts_code, disclosure_date, total_share, float_share
            FROM base_fundamentals_info
            WHERE disclosure_date >= %s AND disclosure_date <= %s
            {ts_filter}
        ),
        prev_one AS (
            SELECT DISTINCT ON (ts_code)
                ts_code, disclosure_date, total_share, float_share
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

    def fetch_last_his_kline_1min_preclose(self, ts_code: str) -> Optional[float]:
        """
        获取指定股票最后一条1分钟K线的preclose
        """
        query = """
        SELECT preclose
        FROM his_kline_1min
        WHERE ts_code = %s
        ORDER BY trade_date DESC, trade_time DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code,))
        if result:
            value = result.get('preclose')
            return float(value) if value is not None else None
        return None

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

    def test_connection(self) -> bool:
        """
        测试数据库连接

        Returns:
            连接是否成功
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            print(f"数据库连接测试失败: {e}")
            return False
