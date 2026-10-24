"""涨停制度分段阈值与向量化判定（方案 §3.9，M1/M2 共享；P0-1/P0-2 重构 2026-10-24）。

判定口径
--------
``涨停 ⟺ change_rate ≥ 动态阈值(制度上限, 估计昨收) ∧ close ≥ high``

- **价格量级动态阈值法（P0-1）**：固定下浮 0.10pct 对低价股系统性漏判——
  真实涨停价 ``round(昨收×(1+pct), 2)`` 的实际涨幅 ∈
  ``[pct − 0.005/昨收, pct + 0.005/昨收]``（半个最小报价单位），2.04 元主板涨停
  仅 +9.80%，固定阈值 9.90 永远漏判。动态下浮 = ``0.005 / max(估计昨收, 0.5)``：
  2.04 元 → 下浮 0.245pct（阈值 9.755，命中 9.80 且下一个可达 tick 9.31 不误判）；
  100 元 → 下浮 0.005pct（退化为近似固定阈值）。
  估计昨收 = ``raw_close / (1 + change_rate/100)``（真实昨收，免精确昨收列、
  对除权日鲁棒）；``raw_close`` 缺失的行回退固定下浮 0.10 并标 ``_limit_exact=False``。
- **``close ≥ high`` 为逻辑必要条件**：收于涨停 ⟹ 收盘价即当日最高价。
  该项只剔除「涨幅达标但收盘价不是当日最高价」的伪命中。实测（2008/2010/2015/
  2022 四日期）护栏后精确率 100%/88.9%/100%/100%，召回率无变化。

制度分段
--------
| 板块/状态 | 判定上限 | 生效区间 |
|---|---|---|
| 主板（60/00）非 ST | 10% | 1996-12-16 起 |
| 主板 ST | 5% | 1996-12-16 起 |
| 创业板（30） | 20% | 2020-08-24 起（此前 10%，ST 5%） |
| 科创板（68） | 20% | 2019-07-22 起 |
| 北交所（4/8/92） | 30% | 2021-11-15 起 |
| 全市场 | 无涨跌停制度 | 1996-12-16 前 → ``数据不足`` |

无涨跌幅限制窗口（P0-2）
------------------------
注册制新股上市**前 5 个交易日**（含上市日）无涨跌幅限制：科创板（2019-07-22 起）、
创业板（2020-08-24 起）、主板（全面注册制 2023-04-10 起）；北交所仅**上市首日**
无限制。窗口内 ``limit_pct`` 返回 NaN（``_limit_known=False``），涨停/炸板判定
跳过、情绪分母不计入——次新第 2~5 日涨 25% 不再误判涨停/炸板。
``list_date`` 来源 base_stock_info，由调用方注入；未注入时行为与旧版一致。
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

BOARD_MAIN = "主板"
BOARD_GEM = "创业板"
BOARD_STAR = "科创板"
BOARD_BSE = "北交所"
BOARD_UNKNOWN = "未知"

#: 涨跌停制度生效日（此前无涨跌停，涨停语义不成立）
LIMIT_SYSTEM_START = "19961216"
#: 创业板注册制改革（涨跌幅由 10% 放宽至 20%，新股前 5 日无限制）
GEM_20PCT_FROM = "20200824"
#: 创业板开板日
GEM_START = "20091030"
#: 科创板开板日（开板即注册制，前 5 日无限制）
STAR_START = "20190722"
#: 北交所开市日（首日无限制，次日起 30%）
BSE_START = "20211115"
#: 主板全面注册制（新股前 5 日无限制）
MAIN_REG_FROM = "20230410"

#: 固定下浮（P0-1 前的旧口径；raw_close 缺失行回退用）
THRESHOLD_OFFSET = 0.10
#: 半个最小报价单位（元）——A 股 tick 0.01，半个 tick = 0.005
_TICK_HALF = 0.005
#: 动态下浮的估计昨收下限（防退市股几分钱股价把下浮放大到离谱）
_EST_PRECLOSE_FLOOR = 0.5

# 制度档位上限（百分点）——limit_pct 与 limit_pct_series 共用的单一来源（R-11）
_LIMIT_PCT_MAIN = 10.0        # 主板/创业板注册制前：10%
_LIMIT_PCT_MAIN_ST = 5.0      # 主板/创业板注册制前 ST：5%
_LIMIT_PCT_GEM_STAR = 20.0    # 创业板注册制后 / 科创板：20%
_LIMIT_PCT_BSE = 30.0         # 北交所：30%

#: 注册制新股无涨跌幅窗口（P0-2）：板块 → (制度起点, 无限制交易日数含上市日)
_NEW_NO_LIMIT = {
    BOARD_MAIN: (MAIN_REG_FROM, 5),
    BOARD_GEM: (GEM_20PCT_FROM, 5),
    BOARD_STAR: (STAR_START, 5),
    BOARD_BSE: (BSE_START, 1),
}
#: 上市窗口生效的序列首行日期容差（自然日）：首行晚于上市日超过该值视为
# "上市期不在本序列覆盖内"（滚窗/老股），不标无限制（缺行由 precheck 存疑兜底）
_LISTING_GAP_TOLERANCE_DAYS = 7

#: 股票代码前缀 → 板块
_PREFIX_BOARD = {
    "60": BOARD_MAIN,
    "00": BOARD_MAIN,
    "30": BOARD_GEM,
    "68": BOARD_STAR,
}
#: 北交所/新三板（4/8 开头，含 920 新号段）
_BSE_PREFIX_CHARS = {"4", "8", "92"}


def board_of(stock_code: str) -> str:
    """按 6 位股票代码判定板块。"""
    if not stock_code or len(stock_code) < 2:
        return BOARD_UNKNOWN
    prefix = stock_code[:2]
    if prefix in _PREFIX_BOARD:
        return _PREFIX_BOARD[prefix]
    if prefix in _BSE_PREFIX_CHARS or stock_code[:1] in {"4", "8"}:
        return BOARD_BSE
    return BOARD_UNKNOWN


def limit_pct(board: str, trade_date: str, is_st: bool) -> Optional[float]:
    """单点查询制度涨跌幅上限（百分点）。返回 None 表示该日制度不适用。"""
    if board == BOARD_MAIN:
        if trade_date < LIMIT_SYSTEM_START:
            return None
        return _LIMIT_PCT_MAIN_ST if is_st else _LIMIT_PCT_MAIN
    if board == BOARD_GEM:
        if trade_date >= GEM_20PCT_FROM:
            return _LIMIT_PCT_GEM_STAR
        if trade_date < GEM_START:
            return None
        return _LIMIT_PCT_MAIN_ST if is_st else _LIMIT_PCT_MAIN
    if board == BOARD_STAR:
        return _LIMIT_PCT_GEM_STAR if trade_date >= STAR_START else None
    if board == BOARD_BSE:
        return _LIMIT_PCT_BSE if trade_date >= BSE_START else None
    return None


def dynamic_threshold_offset(raw_preclose_est) -> pd.Series:
    """P0-1 动态下浮（百分点）：半个最小报价单位 / 估计昨收。

    0.005/5 元 = 0.10%（恰为旧固定口径）；2.04 元 = 0.245pct；
    估计昨收 ≤0.5 元按 0.5 元封底（防退市股几分钱把下浮放大到不可判）。
    """
    est = pd.to_numeric(pd.Series(raw_preclose_est), errors="coerce")
    return 100.0 * _TICK_HALF / est.clip(lower=_EST_PRECLOSE_FLOOR)


def dynamic_thresholds(limit_pct: pd.Series, change_rate: pd.Series,
                       raw_close: Optional[pd.Series] = None,
                       signed: str = "up") -> tuple:
    """P0-1 判定阈值与精确标记（涨停/跌停/炸板共用单一实现）。

    动态阈值 = 制度上限 − 0.005/估计昨收（估计昨收 = raw_close/(1+change_rate/100)，
    即真实昨收）；raw_close 缺失或不可算的行回退固定下浮 THRESHOLD_OFFSET 并标
    ``exact=False``（口径注记随行）。``signed``：``up`` 返回正向阈值（≥ 判涨停），
    ``down`` 返回负向阈值（≤ 判跌停）。

    Returns:
        (threshold: pd.Series, exact: pd.Series[bool])
    """
    limit_pct = pd.to_numeric(pd.Series(limit_pct), errors="coerce")
    change_rate = pd.to_numeric(pd.Series(change_rate), errors="coerce")
    fixed = limit_pct - THRESHOLD_OFFSET
    if raw_close is not None:
        rc = pd.to_numeric(pd.Series(raw_close), errors="coerce")
        est = rc / (1.0 + change_rate / 100.0)
        est_ok = est.gt(0) & est.notna() & limit_pct.notna()
        dyn = limit_pct - dynamic_threshold_offset(est)
        threshold = dyn.where(est_ok, fixed)
        exact = est_ok.fillna(False)
    else:
        threshold, exact = fixed, pd.Series(False, index=limit_pct.index)
    if signed == "down":
        threshold = -threshold
    return threshold, exact


def _new_stock_no_limit_mask(dates: np.ndarray, board: str,
                             list_date: Optional[str]) -> np.ndarray:
    """P0-2 无涨跌幅限制窗口掩码（True = 该行不判涨停/跌停）。

    注册制新股上市前 K 个交易日（含上市日）：主板/创业板/科创板 K=5、
    北交所 K=1。仅当上市日落入本序列覆盖范围（首行日期不晚于上市日 +
    容差）时生效；老股/滚窗（上市期在序列之前）不误标。
    """
    n = len(dates)
    rule = _NEW_NO_LIMIT.get(board)
    if not rule or not list_date or n == 0:
        return np.zeros(n, dtype=bool)
    reg_from, days = rule
    ld = str(list_date)
    if ld < reg_from:
        return np.zeros(n, dtype=bool)
    d0 = str(dates[0])
    if d0 > ld:
        # 序列首行晚于上市日：容差内视为窗口恰从上市期开始；否则上市期不在覆盖内
        gap = (pd.Timestamp(d0) - pd.Timestamp(ld)).days
        if gap > _LISTING_GAP_TOLERANCE_DAYS:
            return np.zeros(n, dtype=bool)
    after = dates >= ld
    if not after.any():
        return np.zeros(n, dtype=bool)
    # 上市以来第 N 个交易日（按本序列行序计，缺行由 precheck 存疑兜底）
    rank = np.cumsum(after)
    return after & (rank <= days)


def limit_pct_series(df: pd.DataFrame, board: str,
                     list_date: Optional[str] = None) -> pd.Series:
    """向量化计算整段序列的制度上限（百分点）；NaN 表示制度不适用。

    P0-2：传入 ``list_date`` 时，注册制新股上市前 5 个交易日（北交所首日）
    返回 NaN——调用方以 ``_limit_known=False`` 剔出涨停/炸板判定与分母。
    """
    if df.empty:
        return pd.Series(dtype="float64")

    dates = df["trade_date"].astype(str).to_numpy()
    is_st = df["is_st"].astype(bool).to_numpy() if "is_st" in df.columns else np.zeros(len(df), dtype=bool)

    if board == BOARD_MAIN:
        pct = np.where(is_st, _LIMIT_PCT_MAIN_ST, _LIMIT_PCT_MAIN)
        pct = np.where(dates < LIMIT_SYSTEM_START, np.nan, pct)
    elif board == BOARD_GEM:
        pct = np.where(dates >= GEM_20PCT_FROM, _LIMIT_PCT_GEM_STAR, np.where(is_st, _LIMIT_PCT_MAIN_ST, _LIMIT_PCT_MAIN))
        pct = np.where(dates < GEM_START, np.nan, pct)
    elif board == BOARD_STAR:
        pct = np.where(dates >= STAR_START, _LIMIT_PCT_GEM_STAR, np.nan)
    elif board == BOARD_BSE:
        pct = np.where(dates >= BSE_START, _LIMIT_PCT_BSE, np.nan)
    else:
        pct = np.full(len(df), np.nan)

    if list_date:
        no_limit = _new_stock_no_limit_mask(dates, board, list_date)
        pct = np.where(no_limit, np.nan, pct)

    return pd.Series(pct, index=df.index, dtype="float64")


def annotate_limit_up(df: pd.DataFrame, stock_code: str, board: str = None,
                      list_date: Optional[str] = None) -> pd.DataFrame:
    """为日 K 序列追加涨停判定列（向量化）。

    追加列：
        ``_limit_pct``   制度上限（百分点，NaN=制度不适用/板块未知/次新无限制窗口）
        ``_limit_up``    是否涨停（bool；制度不适用时为 False）
        ``_limit_known`` 该行是否具备判定条件（bool；P0-2 次新窗口为 False）
        ``_limit_exact`` 该行是否用了动态精确阈值（bool；raw_close 缺失回退
                         固定下浮 0.10 时为 False——口径注记随行）

    Returns:
        新的 DataFrame（不修改入参）。
    """
    out = df.copy()
    board = board or board_of(stock_code)
    out["_limit_pct"] = limit_pct_series(out, board, list_date=list_date)

    if "change_rate" in out.columns:
        change_rate = pd.to_numeric(out["change_rate"], errors="coerce")
    else:
        change_rate = pd.Series(np.nan, index=out.index, dtype="float64")

    # P0-1 动态阈值（涨停/跌停/炸板共用 dynamic_thresholds 单一实现）
    threshold, exact = dynamic_thresholds(
        out["_limit_pct"], change_rate,
        out["raw_close"] if "raw_close" in out.columns else None, signed="up")
    out["_limit_exact"] = exact
    hit = change_rate.ge(threshold)

    # 逻辑必要条件：收于涨停 ⟹ 收盘价即当日最高价。high 缺失时不启用该护栏。
    if "close" in out.columns and "high" in out.columns:
        close = pd.to_numeric(out["close"], errors="coerce")
        high = pd.to_numeric(out["high"], errors="coerce")
        guard = close.ge(high).where(high.notna(), True)
        hit &= guard

    out["_limit_known"] = out["_limit_pct"].notna() & change_rate.notna()
    out["_limit_up"] = (hit & out["_limit_known"]).fillna(False).astype(bool)
    return out


def annotate_limit_down(df: pd.DataFrame, stock_code: str, board: str = None,
                        list_date: Optional[str] = None) -> pd.DataFrame:
    """为日 K 序列追加跌停判定列（向量化，与 :func:`annotate_limit_up` 镜像）。

    判定口径：``change_rate ≤ -(制度上限 − 动态下浮)`` 且 ``close ≤ low``
    （收于跌停 ⟹ 收盘价即当日最低价，跌停价是当日可成交的最低价）；
    动态下浮与 raw_close 回退规则同 :func:`annotate_limit_up`（P0-1），
    次新无限制窗口同规则跳过（P0-2）。

    追加列：
        ``_limit_pct``        制度上限（百分点，NaN=制度不适用/次新窗口）
        ``_limit_down``       是否跌停（bool；制度不适用时为 False）
        ``_limit_down_known`` 该行是否具备判定条件（bool）
        ``_limit_exact``      动态精确阈值标记（同上）

    Returns:
        新的 DataFrame（不修改入参）。
    """
    out = df.copy()
    board = board or board_of(stock_code)
    out["_limit_pct"] = limit_pct_series(out, board, list_date=list_date)

    if "change_rate" in out.columns:
        change_rate = pd.to_numeric(out["change_rate"], errors="coerce")
    else:
        change_rate = pd.Series(np.nan, index=out.index, dtype="float64")

    threshold, exact = dynamic_thresholds(
        out["_limit_pct"], change_rate,
        out["raw_close"] if "raw_close" in out.columns else None, signed="down")
    out["_limit_exact"] = exact
    hit = change_rate.le(threshold)

    # 逻辑必要条件：收于跌停 ⟹ 收盘价即当日最低价。low 缺失时不启用该护栏。
    if "close" in out.columns and "low" in out.columns:
        close = pd.to_numeric(out["close"], errors="coerce")
        low = pd.to_numeric(out["low"], errors="coerce")
        guard = close.le(low).where(low.notna(), True)
        hit &= guard

    out["_limit_down_known"] = out["_limit_pct"].notna() & change_rate.notna()
    out["_limit_down"] = (hit & out["_limit_down_known"]).fillna(False).astype(bool)
    return out
