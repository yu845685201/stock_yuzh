"""涨停制度分段阈值与向量化判定（方案 §3.9，M1/M2 共享）。

判定口径
--------
``涨停 ⟺ change_rate ≥ 阈值(板块, 制度, ST) ∧ close ≥ high``

- **主口径为 change_rate 阈值法**（§3.9）：前复权价格在历史日期不等于真实价，
  不能用 ``close == round(preclose * 1.1, 2)`` 精确判定。
- **``close ≥ high`` 为逻辑必要条件**：收于涨停 ⟹ 收盘价即当日最高价
  （涨停价就是当日可成交的最高价）。该项**只剔除「涨幅达标但收盘价不是当日最高价」
  的伪命中，理论上不会引入漏判**。

  该护栏是实测得出：对 2008-06-10 / 2010-01-04 / 2015-09-01 / 2022-01-04 四个
  历史日期，纯阈值法精确率仅 5.7% / 21.1% / 34.5% / 90.5%，加上护栏后为
  100% / 88.9% / 100% / 100%，召回率无变化。全表 ``close > high`` 仅 96 行（0.0006%），
  护栏本身安全。

制度分段
--------
| 板块/状态 | 判定阈值 | 生效区间 |
|---|---|---|
| 主板（60/00）非 ST | change_rate ≥ 9.90 | 1996-12-16 起 |
| 主板 ST | change_rate ≥ 4.90 | 1996-12-16 起 |
| 创业板（30） | ≥ 19.90 | 2020-08-24 起（此前 10%，ST 5%） |
| 科创板（68） | ≥ 19.90 | 2019-07-22 起 |
| 北交所（4/8/92） | ≥ 29.90 | 2021-11-15 起 |
| 全市场 | 无涨跌停制度 | 1996-12-16 前 → ``数据不足`` |
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
#: 创业板注册制改革（涨跌幅由 10% 放宽至 20%）
GEM_20PCT_FROM = "20200824"
#: 创业板开板日
GEM_START = "20091030"
#: 科创板开板日
STAR_START = "20190722"
#: 北交所开市日
BSE_START = "20211115"

#: 阈值相对制度上限的下浮（§3.9：主板 10% → 判定阈值 9.90）
THRESHOLD_OFFSET = 0.10

# 制度档位上限（百分点）——limit_pct 与 limit_pct_series 共用的单一来源（R-11）
_LIMIT_PCT_MAIN = 10.0        # 主板/创业板注册制前：10%
_LIMIT_PCT_MAIN_ST = 5.0      # 主板/创业板注册制前 ST：5%
_LIMIT_PCT_GEM_STAR = 20.0    # 创业板注册制后 / 科创板：20%
_LIMIT_PCT_BSE = 30.0         # 北交所：30%

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


def limit_pct_series(df: pd.DataFrame, board: str) -> pd.Series:
    """向量化计算整段序列的制度上限（百分点）；NaN 表示制度不适用。"""
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

    return pd.Series(pct, index=df.index, dtype="float64")


def annotate_limit_up(df: pd.DataFrame, stock_code: str, board: str = None) -> pd.DataFrame:
    """为日 K 序列追加涨停判定列（向量化）。

    追加列：
        ``_limit_pct``  制度上限（百分点，NaN=制度不适用/板块未知）
        ``_limit_up``   是否涨停（bool；制度不适用时为 False）
        ``_limit_known``该行是否具备判定条件（bool）

    Returns:
        新的 DataFrame（不修改入参）。
    """
    out = df.copy()
    board = board or board_of(stock_code)
    out["_limit_pct"] = limit_pct_series(out, board)

    if "change_rate" in out.columns:
        change_rate = pd.to_numeric(out["change_rate"], errors="coerce")
    else:
        change_rate = pd.Series(np.nan, index=out.index, dtype="float64")
    threshold = out["_limit_pct"] - THRESHOLD_OFFSET
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
