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
from .limit_up_rules import annotate_limit_up, board_of

logger = logging.getLogger(__name__)

#: 必带列
BASE_COLUMNS = ("ts_code", "stock_code", "trade_date")
#: COPY 可用列白名单（防止列名注入）
_ALLOWED_COLUMNS = {
    "ts_code", "stock_code", "stock_name", "trade_date", "open", "high", "low",
    "close", "preclose", "volume", "amount", "is_st", "change_rate", "turnover_rate",
    "raw_close", "adjust_flag", "trade_status",
}
#: 存疑原因串最大条数（超出截断，避免 CSV 列爆长）
SUSPECT_REASON_LIMIT = 5

#: 缺日处理模式
MISSING_MODE_GAP = "gap"   # 只把「孤立单日缺行」判为存疑；连续多日缺行按疑似停牌处理
MISSING_MODE_ALL = "all"   # 方案原文：所有交易日缺行都判为存疑
MISSING_MODE_OFF = "off"   # 不做缺日检测


@dataclass
class PrecheckResult:
    """数据预检结果。

    Attributes:
        suspects: ``{ts_code: [存疑原因...]}``，写入 CSV 的「是否存疑/存疑原因」。
        suspended: ``{ts_code: [疑似停牌明细...]}``，不计入存疑，仅聚合进运行元信息。
        break_count: 前复权断裂命中行数。
        missing_count: 计入存疑的缺日条数。
    """

    suspects: Dict[str, List[str]] = field(default_factory=dict)
    suspended: Dict[str, List[str]] = field(default_factory=dict)
    break_count: int = 0
    missing_count: int = 0

    @property
    def suspended_stock_count(self) -> int:
        return len(self.suspended)


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
        """数据预检（R3）。

        **规则 1「前复权断裂」**：``|change_rate| > 制度上限 + 容差``。
            方案原定规则为「相邻交易日 ``close[t-1]`` 与 ``preclose[t]`` 相对差 > 0.1%」，
            但实测全表 ``preclose == lag(close)`` 占 **99.987%**，该规则恒不触发。
            改用「涨跌幅突破制度上限」这一**可证伪**的一致性规则——涨跌幅在物理上
            不可能超过涨跌停制度上限，突破即为前复权序列基准断裂。
            该规则只能捕捉「超过上限」的断裂，故属于**存疑下界**。

        **规则 2「缺行」**：交易日历内缺失 K 线行。按 ``missing_day_mode`` 分流：
            - ``gap``（默认）：**孤立单日缺行**判为存疑（疑似数据缺口）；
              **连续 ≥2 交易日缺行**归入「疑似停牌」，不计入存疑。
              依据：停牌是市场事实而非数据缺陷，模块按可得行计算，涨停/连板判定不受影响；
              实测连续 ≥2 日缺行呈明显的 5 日 / 10 日簇（典型停牌形态），若一律判存疑，
              会有 8.7% 的股票被标记，淹没真正的数据问题。
            - ``all``：方案原文行为，所有缺行都判存疑。
            - ``off``：不做缺行检测。

        Args:
            kdf: ``load_kline`` 结果（含 ts_code/stock_code/trade_date/change_rate/close/high/is_st）。
            as_of: 锚定日 yyyyMMdd。
            start_date: 预检窗口起始日，应与 ``load_kline`` 一致；``None`` = 全历史。
            break_tolerance_pct: 断裂容差（百分点）。
            check_missing_day: 是否执行缺行检测。
            universe: 股票池（提供 list_date，用于把缺行检测限定在上市之后）。
            reason_limit: 单只股票保留的存疑原因条数上限。
            missing_day_mode: 缺行分流模式（gap/all/off）。
        """
        result = PrecheckResult()
        if kdf.empty:
            return result
        if missing_day_mode not in {MISSING_MODE_GAP, MISSING_MODE_ALL, MISSING_MODE_OFF}:
            raise ValueError(f"非法 missing_day_mode: {missing_day_mode}")

        info_by_ts = {s.ts_code: s for s in (universe or [])}
        do_missing = bool(check_missing_day) and missing_day_mode != MISSING_MODE_OFF
        trading_days = self._load_trading_days(as_of, start_date) if do_missing else []
        expected_all = set(trading_days)

        for ts_code, g in kdf.groupby("ts_code", sort=False):
            stock = info_by_ts.get(ts_code)
            stock_code = str(g["stock_code"].iloc[0])
            board = stock.board if stock else board_of(stock_code)

            # ---- 规则 1：前复权断裂 ----
            ann = annotate_limit_up(g, stock_code, board)
            pct = ann["_limit_pct"]
            cr = pd.to_numeric(ann["change_rate"], errors="coerce")
            broke = pct.notna() & cr.notna() & cr.abs().gt(pct + break_tolerance_pct)
            reasons = [
                f"{d}前复权断裂(涨跌幅{c:.2f}%超出制度上限±{p:.2f}%)"
                for d, c, p in zip(ann.loc[broke, "trade_date"], cr[broke], pct[broke])
            ]
            result.break_count += len(reasons)

            # ---- 规则 2：缺行（按缺口连续段分流）----
            if expected_all:
                actual = set(g["trade_date"].astype(str))
                ordered = sorted(actual)
                start = max(ordered[0], stock.list_date or "") if stock else ordered[0]
                exp = [d for d in trading_days if d >= start]
                for j0, gap_len in _gap_runs(exp, actual):
                    if missing_day_mode == MISSING_MODE_ALL:
                        # 方案原文行为：逐日列出每条缺失
                        for k in range(gap_len):
                            j = j0 + k
                            prev = exp[j - 1] if j > 0 else "无"
                            reasons.append(f"{exp[j]}缺日(上一交易日{prev})")
                        result.missing_count += gap_len
                    elif gap_len == 1:
                        prev = exp[j0 - 1] if j0 > 0 else "无"
                        reasons.append(f"{exp[j0]}缺日(上一交易日{prev})")
                        result.missing_count += 1
                    else:
                        result.suspended.setdefault(ts_code, []).append(
                            f"{exp[j0]}起连续{gap_len}个交易日无K线"
                        )

            if reasons:
                if len(reasons) > reason_limit:
                    total = len(reasons)
                    reasons = reasons[:reason_limit] + [f"...共{total}处"]
                result.suspects[ts_code] = reasons

        return result

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


def _dotted(value: str) -> str:
    v = str(value).replace("-", "")
    return f"{v[:4]}-{v[4:6]}-{v[6:]}"


def _plain_date(value) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    return str(value).replace("-", "")


def _gap_runs(expected: Sequence[str], actual: set) -> List[tuple]:
    """把「期望交易日中存在、实际缺失」的日子切成连续段。

    ``expected`` 本身是**连续交易日列表**（不含周末/节假日），因此相邻缺失日
    在 ``expected`` 中必然相邻，无需再做索引连续性判断。

    Returns:
        ``[(段首在 expected 中的下标, 段内交易日数), ...]``，按时间升序。
    """
    runs: List[tuple] = []
    run_start: Optional[int] = None
    run_len = 0
    for j, day in enumerate(expected):
        if day in actual:
            if run_start is not None:
                runs.append((run_start, run_len))
                run_start, run_len = None, 0
            continue
        if run_start is None:
            run_start, run_len = j, 1
        else:
            run_len += 1
    if run_start is not None:
        runs.append((run_start, run_len))
    return runs
