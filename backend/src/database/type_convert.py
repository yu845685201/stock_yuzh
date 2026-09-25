"""
日期/时间解析与 SQL 类型转换表达式（自 connection.py 拆分，R-06 纯物理移动）
"""

from typing import Any, Optional
from datetime import datetime, date, time


class TypeConvertMixin:
    """日期/时间解析与转换表达式构建"""

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
