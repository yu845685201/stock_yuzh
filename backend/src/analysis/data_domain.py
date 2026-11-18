"""数据域策略（港美股指标与港股个股分析链整合方案 V1.0 §2/§3）。

引擎对 A 股的数据耦合集中在三处调用（股票池 / K 线加载 / 数据预检）+ 锚定日解析。
本模块把三处调用抽为按 ``ChainConfig.domain`` 分发的域策略：

- ``a_share``：现状三个函数的搬运封装，**行为逐字节不变**（回归红线，方案 §8.1）；
- ``overseas_indices``：外围指数域（config overseas_kline_indices，6 指数）；
- ``overseas_stocks``：港股个股域（config overseas_kline_stocks，33 只，
  A-H 两地公司附 a_code → a_close 对照列）。

外围域关键语义（§3.2/§3.3）：
- 锚定日按域内股票池的 ``overseas_kline_day`` 自身日历解析（默认 = 池内最新
  市场日；A50 期货夜盘行 trade_date 计入下一交易日，不参与港美域锚定）；
- 窗口忽略链 lookback 的 A 股日历换算，按 ``max(模块 effective_lookback) +
  WINDOW_BUFFER`` 取每股最近 N 行（市场自身日历）；
- ``change_rate`` 自算（相邻市场交易日 pct_change）、``is_st`` 恒 0（列存在仅为
  引擎兜底列不缺）、``a_close`` 经 as-of 回挂 A 侧 ``his_kline_day`` 前复权收盘
  （≤行日最近，两侧复权基准独立滚动——比值仅参考，同报告层 V2.0 块④口径）；
- 预检**无前复权断裂规则**（无涨跌停制度，跨源质量由采集层双源对拍哨兵负责）；
  保留缺日（gap 分流）+ 新增尾部滞后（>3 个该市场交易日 → 存疑「数据滞后」，
  行日历以 hkHSI/usDJI 为各市场基准）。

制度性红线（§1.1）：涨跌停系模块（limit_up/consecutive/limit_down/burst/premium）
依赖 A 股制度，外围链严禁配置（引擎不做硬拦截，靠链 YAML 纪律 + README + skill）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .date_utils import _gap_runs
from .module_base import StockInfo
from .precheck import (
    MISSING_MODE_ALL,
    MISSING_MODE_OFF,
    PrecheckResult,
    SUSPECT_REASON_LIMIT,
)

logger = logging.getLogger(__name__)

#: 加载窗口相对模块回看天数的余量（引擎原常量，收敛到本模块；chain_engine 转导入）
WINDOW_BUFFER = 2

#: 存疑原因前缀：尾部滞后（外围域新增，方案 §3.3；ah_5d_diff 据此返回「未对齐」）
LAG_REASON_PREFIX = "数据滞后"

#: 外围域模块可声明的列（change_rate/is_st 由域内加工，a_close 由域内回挂）
_OVERSEAS_COMPUTABLE_COLUMNS = {"open", "high", "low", "close", "volume",
                                "change_rate", "is_st", "a_close"}

#: A-H 汇率注入键（引擎装配进 ctx.upstream，模块只读——方案 §4.2）
FX_HKDCNY_KEY = "fx_hkdcny"


@dataclass(frozen=True)
class OverseasStockInfo(StockInfo):
    """外围股票池成员（frozen dataclass 继承安全，引擎按基类访问不受影响）。"""

    a_code: Optional[str] = None  # A-H 两地公司的 A 股 ts_code（仅港股个股域）


@dataclass
class DomainLoad:
    """域内加载产物（引擎通用的窗口/K线/环境值载体）。"""

    kdf: pd.DataFrame
    window_start: Optional[str] = None
    window_expanded: bool = False
    #: 窗口说明（进日志与 meta 的 lookback 字段；外围域注明"市场自身日历"）
    lookback_desc: str = ""
    #: 引擎装配 ChainContext.upstream 的预置环境值（如 fx_hkdcny）
    env_extras: Dict[str, Any] = field(default_factory=dict)


class DataDomain:
    """数据域策略基类（三处引擎调用点 + 锚定日解析）。"""

    name: str = ""
    #: 引擎必带列（A 股域 change_rate/is_st；外围域全部域内加工）
    base_columns: List[str] = []
    #: 股票池为空时的报错文案
    universe_empty_error: str = "股票池为空"

    def resolve_as_of(self, reader, as_of: Optional[str],
                      config_manager=None) -> str:
        raise NotImplementedError

    def load_universe(self, reader, config_manager) -> List[StockInfo]:
        raise NotImplementedError

    def load(self, reader, columns: Sequence[str], as_of: str, chain,
             opts, module_days: int, universe: Sequence[StockInfo]) -> DomainLoad:
        raise NotImplementedError

    def precheck(self, reader, kdf: pd.DataFrame, as_of: str, load: DomainLoad,
                 universe: Sequence[StockInfo], opts) -> PrecheckResult:
        raise NotImplementedError


class AShareDomain(DataDomain):
    """A 股域：引擎现状逻辑的搬运封装（行为逐字节不变——回归红线）。"""

    name = "a_share"
    # P0-1：raw_close 为动态涨停阈值（真实昨收估计）的数据源，两入口同实现
    base_columns = ["change_rate", "is_st", "raw_close"]
    universe_empty_error = "股票池为空（base_stock_info.list_status='L' ∩ his_kline_day）"

    def resolve_as_of(self, reader, as_of: Optional[str],
                      config_manager=None) -> str:
        # config_manager 仅为域接口对齐（A 股锚定取 his_kline_day 最新交易日，不依赖池）
        return reader.resolve_as_of(as_of)

    def load_universe(self, reader, config_manager, as_of: Optional[str] = None) -> List[StockInfo]:
        """A 股域股票池（P4-3：透传 as_of 做 as-of 感知建池，见 data_reader）。"""
        return reader.load_stock_universe(as_of=as_of)

    def load(self, reader, columns, as_of, chain, opts, module_days,
             universe) -> DomainLoad:
        from .lookback import Lookback

        lookback: Lookback = opts.lookback or chain.lookback
        window_expanded = False
        if lookback.is_all:
            window_start = None
        else:
            chain_start = reader.resolve_window_start(as_of, lookback)
            candidates = [c for c in [chain_start] if c]
            if module_days > 0:
                needed = reader.nth_trading_day_back(as_of, module_days + 2)
                if needed:  # 交易日历不足则忽略，退化为链窗口
                    candidates.append(needed)
                    if chain_start and needed < chain_start:
                        window_expanded = True
            window_start = min(candidates) if candidates else None
        kdf = reader.load_kline(columns=columns, as_of=as_of, start_date=window_start)
        return DomainLoad(kdf=kdf, window_start=window_start,
                          window_expanded=window_expanded, lookback_desc=str(lookback))

    def precheck(self, reader, kdf, as_of, load, universe, opts) -> PrecheckResult:
        return reader.precheck(
            kdf,
            as_of=as_of,
            start_date=load.window_start,
            break_tolerance_pct=opts.break_tolerance_pct,
            check_missing_day=opts.check_missing_day,
            universe=universe,
            missing_day_mode=opts.missing_day_mode,
        )


class OverseasDomain(DataDomain):
    """外围域公共实现（指数域/个股域共用；差异在股票池与 a_close/fx 装配）。"""

    name = ""
    base_columns: List[str] = []
    universe_empty_error = "外围股票池为空（config review_sync.overseas_kline_indices / overseas_kline_stocks）"
    #: 本域是否装配 a_close 对照列（仅个股域）
    attach_a_close = False
    #: 本域是否装配 fx 环境值（仅个股域）
    attach_fx = False

    # ------------------------------------------------------------ 锚定日
    def resolve_as_of(self, reader, as_of: Optional[str],
                      config_manager=None) -> str:
        """外围锚定日按域内股票池的 ``overseas_kline_day`` 自身日历解析
        （引擎默认锚定日来自 his_kline_day，会静默错位到 A 股最新交易日——
        外围域必须覆盖）。缺省路径按池内 market_code 取 MAX：A50 期货夜盘行
        （sina_futures）trade_date 计入新交所下一交易日，会晚于港美现货收盘，
        全表 MAX 会把锚定日顶到尚未收盘的日期使整链「数据不足」。"""
        if as_of:
            value = str(as_of).replace("-", "")
            if len(value) != 8 or not value.isdigit():
                raise ValueError(f"非法 --as-of: {as_of}，应为 yyyyMMdd")
            if not reader._query(
                    "SELECT 1 FROM overseas_kline_day WHERE trade_date = %s LIMIT 1",
                    (value,)):
                raise ValueError(f"锚定日 {value} 在 overseas_kline_day 中无任何数据")
            return value
        codes = self._pool_codes(config_manager)
        if codes:
            rows = reader._query(
                "SELECT MAX(trade_date) FROM overseas_kline_day "
                "WHERE market_code = ANY(%s)", (codes,))
        else:
            rows = reader._query("SELECT MAX(trade_date) FROM overseas_kline_day")
        if not rows or not rows[0][0]:
            raise ValueError("overseas_kline_day 为空，无法确定锚定日（先执行 sync-overseas-kline）")
        return str(rows[0][0])

    def _pool_codes(self, config_manager) -> List[str]:
        """域内股票池 market_code 列表（config 不可用或池为空时返回空 → 全表回退）。"""
        if config_manager is None or not hasattr(config_manager, "load_config"):
            return []
        review_cfg = (config_manager.load_config().get("review_sync") or {})
        return [code for code, _name, _a in self._pool_entries_from(review_cfg)]

    # ------------------------------------------------------------ 股票池
    def _pool_entries(self, config_manager) -> List[Tuple[str, str, Optional[str]]]:
        """[(ts_code, name, a_code)]，由子类按 config 段提供（顺序即报告行序）。"""
        raise NotImplementedError

    def load_universe(self, reader, config_manager, as_of: Optional[str] = None) -> List[StockInfo]:
        review_cfg = (config_manager.load_config().get("review_sync") or {}) \
            if hasattr(config_manager, "load_config") else {}
        entries = self._pool_entries_from(review_cfg)
        return [OverseasStockInfo(ts_code=code, stock_code=code,
                                  stock_name=name or code, board=self.board_label,
                                  a_code=a_code)
                for code, name, a_code in entries]

    board_label = ""

    def _pool_entries_from(self, review_cfg) -> List[Tuple[str, str, Optional[str]]]:
        raise NotImplementedError

    # ------------------------------------------------------------ K 线加载
    def load(self, reader, columns, as_of, chain, opts, module_days,
             universe) -> DomainLoad:
        wanted = set(columns)
        illegal = wanted - _OVERSEAS_COMPUTABLE_COLUMNS
        if illegal:
            raise ValueError(f"外围域不支持读取的列: {', '.join(sorted(illegal))}")
        if "a_close" in wanted and not self.attach_a_close:
            raise ValueError(f"域 {self.name} 不装配 a_close（A-H 模块仅适用于港股个股链）")

        codes = [s.ts_code for s in universe]
        env = self._env_extras(reader) if self.attach_fx else {}
        if not codes:
            return DomainLoad(kdf=pd.DataFrame(), window_start=None,
                              lookback_desc=self._window_desc(chain, 0),
                              env_extras=env)
        # 一次取全池（80k 行量级，无需窗口下推；pandas 侧截尾）
        kdf = reader._copy(
            """
            SELECT market_code AS ts_code, market_code AS stock_code, trade_date,
                   open, high, low, close, volume
            FROM overseas_kline_day
            WHERE market_code = ANY(%s) AND trade_date <= %s
            ORDER BY market_code, trade_date
            """, (codes, as_of))
        if kdf.empty:
            return DomainLoad(kdf=kdf, window_start=None,
                              lookback_desc=self._window_desc(chain, 0),
                              env_extras=env)

        # 域内加工（模块无感知，方案 §3.2）
        kdf["change_rate"] = (kdf.groupby("ts_code", sort=False)["close"]
                              .pct_change() * 100)
        kdf["is_st"] = 0  # 港股无 ST 制度；列存在仅为引擎兜底列不缺
        if self.attach_a_close:
            kdf = self._attach_a_close(reader, kdf, as_of, universe)

        # 窗口：忽略 A 股日历换算，每股最近 N 行（N = max(模块回看) + 引擎缓冲）
        rows_per_code = module_days + WINDOW_BUFFER if module_days > 0 else 0
        if rows_per_code > 0:
            kdf = kdf.groupby("ts_code", sort=False).tail(rows_per_code)
        window_start = str(kdf["trade_date"].min()) if not kdf.empty else None
        return DomainLoad(kdf=kdf, window_start=window_start,
                          lookback_desc=self._window_desc(chain, rows_per_code),
                          env_extras=env)

    def _window_desc(self, chain, rows_per_code: int) -> str:
        if rows_per_code > 0:
            return (f"{chain.lookback}（外围域：市场自身日历，每股最近 "
                    f"{rows_per_code} 行）")
        return f"{chain.lookback}（外围域：市场自身日历，全历史）"

    def _attach_a_close(self, reader, kdf: pd.DataFrame, as_of: str,
                        universe: Sequence[StockInfo]) -> pd.DataFrame:
        """A-close 对照列：对带 a_code 的港股，按行日回挂 A 侧 ≤该日最近收盘
        （as-of backward，merge_asof 语义）；无映射的整列 NaN。A 侧一次取全（§8.4），
        15 个 a_code 全历史约 10 万行量级。"""
        a_map = {s.ts_code: s.a_code for s in universe
                 if getattr(s, "a_code", None)}
        a_codes = sorted(set(a_map.values()))
        kdf = kdf.copy()
        if not a_codes:
            kdf["a_close"] = np.nan
            return kdf
        a_df = reader._copy(
            "SELECT ts_code, trade_date, close FROM his_kline_day "
            "WHERE ts_code = ANY(%s) AND trade_date <= %s ORDER BY ts_code, trade_date",
            (a_codes, as_of))
        parts = []
        for ts_code, g in kdf.groupby("ts_code", sort=False):
            a_code = a_map.get(ts_code)
            sub = a_df[a_df["ts_code"] == a_code][["trade_date", "close"]] \
                .rename(columns={"close": "a_close"}) if (a_code and not a_df.empty) \
                else pd.DataFrame(columns=["trade_date", "a_close"])
            if sub.empty:
                g = g.assign(a_close=np.nan)
            else:
                # merge_asof 要求可排序数值键：yyyyMMdd 字符串转 int64（序不变）
                left = g[["trade_date"]].assign(_k=g["trade_date"].astype("int64"))
                right = sub.assign(_k=sub["trade_date"].astype("int64"))
                merged = pd.merge_asof(left, right[["_k", "a_close"]],
                                       on="_k", direction="backward")
                g = g.assign(a_close=merged["a_close"].to_numpy())
            parts.append(g)
        return pd.concat(parts, ignore_index=False)

    def _env_extras(self, reader) -> Dict[str, Any]:
        """fx_shkdcny 最新快照（缺→None，模块返回「汇率待补」，方案 §4.2）。"""
        try:
            rows = reader._query(
                "SELECT close FROM overseas_market_daily WHERE market_code = 'fx_shkdcny' "
                "AND close IS NOT NULL ORDER BY trade_date DESC LIMIT 1")
        except Exception as e:  # noqa: BLE001 —— 汇率缺失降级，不阻塞链
            logger.warning("fx_shkdcny 读取失败（模块将返回「汇率待补」）: %s", e)
            return {FX_HKDCNY_KEY: None}
        return {FX_HKDCNY_KEY: float(rows[0][0]) if rows else None}

    # ------------------------------------------------------------ 数据预检
    def precheck(self, reader, kdf, as_of, load, universe, opts) -> PrecheckResult:
        """外围预检（§3.3）：无前复权断裂规则（无涨跌停制度）；缺日（gap 分流，
        各市场自身日历）+ 尾部滞后（>3 个该市场交易日 → 「数据滞后」）。"""
        result = PrecheckResult()
        market_days = self._market_calendars(reader, as_of)
        do_missing = bool(opts.check_missing_day) and opts.missing_day_mode != MISSING_MODE_OFF

        info_by_ts = {s.ts_code: s for s in universe}
        actual_by_ts: Dict[str, set] = {}
        if not kdf.empty:
            for ts_code, g in kdf.groupby("ts_code", sort=False):
                actual_by_ts[ts_code] = set(g["trade_date"].astype(str))

        for ts_code, stock in info_by_ts.items():
            actual = actual_by_ts.get(ts_code, set())
            ordered = sorted(actual)
            calendar = market_days["usDJI" if ts_code.startswith("us") else "hkHSI"]
            reasons: List[str] = []
            if calendar:
                if do_missing and ordered:
                    exp = [d for d in calendar if d >= ordered[0]]
                    for j0, gap_len in _gap_runs(exp, actual):
                        if opts.missing_day_mode == MISSING_MODE_ALL:
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
                                f"{exp[j0]}起连续{gap_len}个交易日无K线")
                # 尾部滞后：窗口内无行或最近行距锚定日超 3 个市场交易日
                lag = len([d for d in calendar if not ordered or d > ordered[-1]])
                if lag > 3:
                    detail = (f"窗口内无K线" if not ordered
                              else f"最近行{ordered[-1]}距锚定日{lag}个市场交易日")
                    reasons.append(f"{LAG_REASON_PREFIX}({detail}，疑似假期未补采或停牌)")
            if reasons:
                if len(reasons) > SUSPECT_REASON_LIMIT:
                    total = len(reasons)
                    reasons = reasons[:SUSPECT_REASON_LIMIT] + [f"...共{total}处"]
                result.suspects[ts_code] = reasons
        return result

    @staticmethod
    def _market_calendars(reader, as_of: str) -> Dict[str, List[str]]:
        """各市场行日历（hkHSI=港股、usDJI=美股基准，升序 yyyyMMdd）。

        行日历而非节假日表：overseas_kline_day 的行集即采集口径下的市场交易日
        （与 V2.0 §3.4 对齐判定同源），单指数缺行不影响日历（三只港指行集一致）。
        """
        out: Dict[str, List[str]] = {}
        for mc in ("hkHSI", "usDJI"):
            try:
                rows = reader._query(
                    "SELECT trade_date FROM overseas_kline_day "
                    "WHERE market_code = %s AND trade_date <= %s ORDER BY trade_date",
                    (mc, as_of))
            except Exception as e:  # noqa: BLE001 —— 日历读取失败降级为不设防
                logger.warning("外围市场日历读取失败（%s）: %s", mc, e)
                out[mc] = []
                continue
            out[mc] = [str(r[0]) for r in rows]
        return out


class OverseasIndicesDomain(OverseasDomain):
    """外围指数域：config overseas_kline_indices（6 指数）。"""

    name = "overseas_indices"
    board_label = "外围指数"
    attach_a_close = False
    attach_fx = False

    def _pool_entries_from(self, review_cfg) -> List[Tuple[str, str, Optional[str]]]:
        return [(e.get("code"), e.get("name") or "", None)
                for e in (review_cfg.get("overseas_kline_indices") or [])
                if e.get("code")]


class OverseasStocksDomain(OverseasDomain):
    """港股个股域：config overseas_kline_stocks（33 只，15 只带 a_code）。

    attach_a_close/attach_fx=True：a_close 对照列与 fx_hkdcny 环境值仅本域装配。
    """

    name = "overseas_stocks"
    board_label = "港股"
    attach_a_close = True
    attach_fx = True

    def _pool_entries_from(self, review_cfg) -> List[Tuple[str, str, Optional[str]]]:
        return [(e.get("code"), e.get("name") or "",
                 (e.get("a_code") or "").strip() or None)
                for e in (review_cfg.get("overseas_kline_stocks") or [])
                if e.get("code")]


#: 域注册表（方案 §2：未知值加载时报错）
_DOMAINS = {
    AShareDomain.name: AShareDomain(),
    OverseasIndicesDomain.name: OverseasIndicesDomain(),
    OverseasStocksDomain.name: OverseasStocksDomain(),
}

_VALID_DOMAINS = ('a_share', 'overseas_indices', 'overseas_stocks')


def get_domain(name: str) -> DataDomain:
    try:
        return _DOMAINS[name]
    except KeyError:
        raise ValueError(
            f"未知数据域: {name}（可用: {', '.join(_VALID_DOMAINS)}）") from None


def _gap_runs(expected: List[str], actual: set) -> List[Tuple[int, int]]:
    """缺口连续段 [(起始下标, 长度)]（复用 date_utils 实现）。"""
    from .date_utils import _gap_runs as _impl

    return _impl(expected, actual)
