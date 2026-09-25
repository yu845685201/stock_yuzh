"""
表/分区/约束/索引 DDL 与迁移（自 connection.py 拆分，R-06 纯物理移动）
"""

from typing import List, Optional
from datetime import timedelta


class DdlMixin:
    """DDL 迁移与辅助方法"""

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
