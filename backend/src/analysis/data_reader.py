"""数据读取层（方案 §3.5）。

职责：
1. 股票池：``base_stock_info.list_status='L'`` ∩ ``his_kline_day`` 有数据（Q1 决策，退市股不分析）；
2. 锚定日解析：默认取 ``his_kline_day`` 最大 ``trade_date``，支持 ``--as-of`` 回看；
3. 流式加载：psycopg2 ``COPY (SELECT ...) TO STDOUT WITH CSV`` → pandas（比逐行 fetch 快一个数量级）；
4. 数据预检（R3）：前复权断裂 / 缺日 → ``{ts_code: [原因...]}``。

只读：本层对数据库只 SELECT，不与任何 ``sync_*`` 写路径耦合。

性能参考（本机 uat 库实测）：250 交易日窗口 ≈ 133 万行 / 95 MB 文本，
COPY 0.8s + read_csv 0.7s，内存约 260 MB。全历史（1,650 万行）为 opt-in。
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd

from .module_base import StockInfo
from .limit_up_rules import board_of
from .date_utils import _dotted, _plain_date, _gap_runs
from .precheck import (
    PrecheckResult,
    SUSPECT_REASON_LIMIT,
    MISSING_MODE_GAP,
    MISSING_MODE_ALL,
    MISSING_MODE_OFF,
    run_precheck,
)

logger = logging.getLogger(__name__)

#: 必带列
BASE_COLUMNS = ("ts_code", "stock_code", "trade_date")
#: COPY 可用列白名单（防止列名注入）
_ALLOWED_COLUMNS = {
    "ts_code", "stock_code", "stock_name", "trade_date", "open", "high", "low",
    "close", "preclose", "volume", "amount", "is_st", "change_rate", "turnover_rate",
    "raw_close", "adjust_flag", "trade_status",
}


class DataReader:
    """his_kline_day / base_stock_info / base_trade_calendar 只读访问。"""

    def __init__(self, config_manager, conn=None):
        self.config_manager = config_manager
        self._conn = conn

    # ------------------------------------------------------------------ 连接
    @property
    def conn(self):
        if self._conn is None:
            import psycopg2

            cfg = self.config_manager.get_database_config()
            self._conn = psycopg2.connect(**cfg)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def __enter__(self) -> "DataReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _query(self, sql: str, params: tuple = None) -> List[tuple]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    def _copy(self, select_sql: str, params: Sequence = ()) -> pd.DataFrame:
        """把 SELECT 结果通过 COPY 流式取回并解析为 DataFrame。"""
        cur = self.conn.cursor()
        try:
            rendered = cur.mogrify(select_sql, tuple(params)).decode("utf-8")
            buf = io.StringIO()
            cur.copy_expert(f"COPY ({rendered}) TO STDOUT WITH CSV HEADER", buf)
        finally:
            cur.close()
        buf.seek(0)
        return pd.read_csv(
            buf,
            dtype={"ts_code": "str", "stock_code": "str", "trade_date": "str", "is_st": "int8"},
        )

    # ------------------------------------------------------------ 锚定日
    def resolve_as_of(self, as_of: Optional[str] = None) -> str:
        """解析锚定日。默认取 his_kline_day 全表最大 trade_date。"""
        if as_of:
            value = str(as_of).replace("-", "")
            if len(value) != 8 or not value.isdigit():
                raise ValueError(f"非法 --as-of: {as_of}，应为 yyyyMMdd")
            if not self._query("SELECT 1 FROM his_kline_day WHERE trade_date = %s LIMIT 1", (value,)):
                raise ValueError(f"锚定日 {value} 在 his_kline_day 中无任何数据")
            return value
        rows = self._query("SELECT MAX(trade_date) FROM his_kline_day")
        if not rows or not rows[0][0]:
            raise ValueError("his_kline_day 为空，无法确定锚定日")
        return str(rows[0][0])

    def latest_trade_date(self) -> Optional[str]:
        rows = self._query("SELECT MAX(trade_date) FROM his_kline_day")
        return str(rows[0][0]) if rows and rows[0][0] else None

    def last_trading_day(self, on_or_before: Optional[str] = None) -> Optional[str]:
        """``base_trade_calendar`` 中不晚于给定日期的最近交易日（用于新鲜度校验）。"""
        sql = "SELECT MAX(calendar_date) FROM base_trade_calendar WHERE is_trading_day = 1"
        params: tuple = ()
        if on_or_before:
            sql += " AND calendar_date <= %s"
            params = (_dotted(on_or_before),)
        rows = self._query(sql, params)
        if not rows or not rows[0][0]:
            return None
        return _plain_date(rows[0][0])

    def nth_trading_day_back(self, as_of: str, n: int) -> Optional[str]:
        """锚定日往前第 ``n`` 个交易日的日期（``n=0`` 即锚定日本身）。

        交易日历不足 ``n+1`` 天时返回 None（调用方应退化为全历史）。
        """
        if n <= 0:
            return as_of
        rows = self._query(
            """
            SELECT calendar_date FROM base_trade_calendar
            WHERE is_trading_day = 1 AND calendar_date <= %s
            ORDER BY calendar_date DESC LIMIT %s
            """,
            (_dotted(as_of), n + 1),
        )
        if len(rows) < n + 1:
            return None
        return _plain_date(rows[-1][0])

    def resolve_window_start(self, as_of: str, lookback) -> Optional[str]:
        """按分析窗口规格求窗口起始日；``None`` 表示全历史。

        - ``days``：按交易日历精确回溯（不受长假影响）；
        - ``years`` / ``months``：按自然日历回溯；
        - ``all``：全历史。
        """
        if lookback is None or getattr(lookback, "is_all", False):
            return None
        kind = getattr(lookback, "kind", None)
        if kind == "days":
            return self.nth_trading_day_back(as_of, int(lookback.value))
        return lookback.calendar_start(as_of)

    # ------------------------------------------------------------ 股票池
    def load_stock_universe(self) -> List[StockInfo]:
        """在市股票池（Q1：退市股不分析）。"""
        rows = self._query(
            """
            SELECT b.ts_code, b.stock_code, b.stock_name, b.list_date
            FROM base_stock_info b
            WHERE b.list_status = 'L'
              AND EXISTS (SELECT 1 FROM his_kline_day k WHERE k.ts_code = b.ts_code)
            ORDER BY b.stock_code
            """
        )
        universe: List[StockInfo] = []
        for ts_code, stock_code, stock_name, list_date in rows:
            stock_code = str(stock_code)
            universe.append(
                StockInfo(
                    ts_code=ts_code,
                    stock_code=stock_code,
                    stock_name=stock_name or "",
                    list_date=_plain_date(list_date) if list_date else None,
                    board=board_of(stock_code),
                )
            )
        return universe

    # ------------------------------------------------------------ K 线
    def load_kline(
        self,
        columns: Iterable[str],
        as_of: str,
        start_date: Optional[str] = None,
        ts_codes: Optional[Sequence[str]] = None,
    ) -> pd.DataFrame:
        """按需取列、按分析窗口流式加载日 K。

        Args:
            columns: 模块声明的业务列；``ts_code/stock_code/trade_date`` 自动带上，
                     并强制包含 ``close``/``high``（涨停判定护栏需要）。
            as_of: 锚定日 yyyyMMdd（含）。
            start_date: 窗口起始日 yyyyMMdd（含）；``None`` = 全历史。
            ts_codes: 限定股票；None = 全部。
        """
        wanted: List[str] = [c for c in dict.fromkeys([*columns, "close", "high"])]
        illegal = [c for c in wanted if c not in _ALLOWED_COLUMNS]
        if illegal:
            raise ValueError(f"不支持读取的列: {', '.join(illegal)}")

        select_parts = ["k.ts_code", "k.stock_code", "k.trade_date"]
        for col in wanted:
            if col in BASE_COLUMNS:
                continue
            if col == "is_st":
                select_parts.append("(CASE WHEN k.is_st THEN 1 ELSE 0 END) AS is_st")
            else:
                select_parts.append(f"k.{col}")

        where = ["k.trade_date <= %s"]
        params: List = [as_of]
        if start_date:
            where.append("k.trade_date >= %s")
            params.append(start_date)
        if ts_codes:
            where.append("k.ts_code = ANY(%s)")
            params.append(list(ts_codes))

        select_sql = (
            f"SELECT {', '.join(select_parts)} FROM his_kline_day k "
            f"WHERE {' AND '.join(where)} ORDER BY k.ts_code, k.trade_date"
        )
        logger.debug("加载日K：窗口 %s ~ %s，列=%s", start_date or "全历史", as_of, wanted)
        return self._copy(select_sql, params)

    # ------------------------------------------------------------ 数据预检
    def precheck(
        self,
        kdf: pd.DataFrame,
        as_of: str,
        start_date: Optional[str] = None,
        break_tolerance_pct: float = 0.6,
        check_missing_day: bool = True,
        universe: Optional[Sequence[StockInfo]] = None,
        reason_limit: int = SUSPECT_REASON_LIMIT,
        missing_day_mode: str = MISSING_MODE_GAP,
    ) -> PrecheckResult:
        """数据预检（R3）。规则实现见 precheck.run_precheck（R-09 拆分，委托保留导入面）。"""
        return run_precheck(
            self,
            kdf,
            as_of,
            start_date=start_date,
            break_tolerance_pct=break_tolerance_pct,
            check_missing_day=check_missing_day,
            universe=universe,
            reason_limit=reason_limit,
            missing_day_mode=missing_day_mode,
        )

    def _load_trading_days(self, as_of: str, start_date: Optional[str] = None) -> List[str]:
        """窗口内的交易日（升序）。``start_date=None`` 表示自市场起点至今。"""
        sql = (
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE is_trading_day = 1 AND calendar_date <= %s"
        )
        params: List = [_dotted(as_of)]
        if start_date:
            sql += " AND calendar_date >= %s"
            params.append(_dotted(start_date))
        sql += " ORDER BY calendar_date"
        rows = self._query(sql, tuple(params))
        return [_plain_date(v) for (v,) in rows]
