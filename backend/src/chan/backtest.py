"""逐信号回测引擎（Phase 2）。

撮合规则（防作弊硬约束）
------------------------
- 信号在 ``fire_i`` 收盘后产生，**下一根K线开盘**成交；停牌（缺K线）自然顺延，
  顺延超过 ``max_delay`` 个交易日（全局日历口径）则放弃；
- 开盘涨停（开盘涨幅 ≥ 制度阈值 − 0.1pct）不买入；开盘跌停卖出顺延；
  收盘卖出遇跌停收盘（收盘=最低且跌幅达标）顺延至次日开盘；
- 次新过滤：信号时已加载K线不足 ``min_bars``（120 根）；ST 过滤：信号日或
  入场日为 ST 则跳过；
- 成本：佣金双边万 2.5、滑点双边 0.1%、印花税卖出（2023-08-28 前 0.1%，
  之后 0.05%）；净收益 = 卖出净得 / 买入净付 − 1。

出场口径（每信号各跑一遍）
--------------------------
- ``("stop", T=20)``：结构止损（收盘跌破 stop_price → 次日开盘卖）+ T 日时间止损
  （第 T 根收盘仍未止损 → 收盘卖）；
- ``("fixed", H)``：持有 H 根K后在第 H 根收盘卖（H ∈ 5/10/20/60）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np

from .signals import Signal

#: 印花税减半日（2023-08-28），之前卖出 0.1%，之后 0.05%
STAMP_TAX_CUT = "20230828"
COMMISSION = 0.00025
SLIPPAGE = 0.001
THRESHOLD_OFFSET = 0.10  # 百分点，与 limit_up_rules 一致


@dataclass
class Bars:
    """单股票对齐数组（结构/信号/撮合共用同一索引）。"""

    ts_code: str
    date: np.ndarray          # str YYYYMMDD
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    preclose: np.ndarray
    is_st: np.ndarray         # int8
    limit_pct: np.ndarray     # 制度阈值（百分点），NaN=无制度
    # 全局日历映射（停牌顺延天数判断）
    cal_idx: np.ndarray       # 每根K线在全局交易日历中的序号


@dataclass
class Trade:
    signal_kind: str
    ts_code: str
    mode: str           # stop / fixed
    param: int          # stop: 时间止损天数；fixed: 持有K线数
    fire_date: str
    entry_date: str
    entry_price: float
    exit_date: str
    exit_price: float
    bars_held: int
    ret_net: float
    exit_reason: str
    mfe: float
    mae: float


def _stamp_tax(date: str) -> float:
    return 0.0005 if date >= STAMP_TAX_CUT else 0.001


def _open_change_pct(b: Bars, j: int) -> float:
    return (b.open[j] / b.preclose[j] - 1.0) * 100.0


def _open_limit_up(b: Bars, j: int) -> bool:
    lp = b.limit_pct[j]
    return not np.isnan(lp) and _open_change_pct(b, j) >= lp - THRESHOLD_OFFSET


def _open_limit_down(b: Bars, j: int) -> bool:
    lp = b.limit_pct[j]
    return not np.isnan(lp) and _open_change_pct(b, j) <= -(lp - THRESHOLD_OFFSET)


def _close_limit_down(b: Bars, j: int) -> bool:
    lp = b.limit_pct[j]
    if np.isnan(lp):
        return False
    chg = (b.close[j] / b.preclose[j] - 1.0) * 100.0
    return chg <= -(lp - THRESHOLD_OFFSET) and b.close[j] <= b.low[j] * 1.0001


def simulate(b: Bars, sig: Signal, mode: str, param: int = 20,
             min_bars: int = 120, max_delay: int = 20) -> Optional[Trade]:
    """撮合单信号。返回 None = 无法入场（跳过并记录原因由调用方统计）。"""
    n = len(b.date)
    i = sig.fire_i
    if i < min_bars or i >= n - 1:
        return None
    if b.is_st[i]:
        return None
    e = i + 1
    if b.is_st[e]:
        return None
    if b.cal_idx[e] - b.cal_idx[i] > max_delay:
        return None  # 停牌复牌过远（如长期停牌后复牌）
    if b.open[e] <= 0 or b.preclose[e] <= 0:
        return None  # 坏K（停牌占位/数据缺失）
    if _open_limit_up(b, e):
        return None  # 开盘涨停买不进
    entry = b.open[e] * (1.0 + SLIPPAGE)

    exit_j: Optional[int] = None
    exit_price: Optional[float] = None
    reason = ""
    stop_price = sig.zd if mode == "stop_zd" else sig.stop_price
    if mode == "fixed":
        target = min(e + param - 1, n - 1)
        j = target
        while j < n and _close_limit_down(b, j):
            j += 1
        if j >= n:
            j, reason = n - 1, "eod"
            exit_price = b.close[j]
        elif j > target:
            reason = "limitdown_postpone"
        else:
            reason = "fixed"
        exit_j = j
        if exit_price is None:
            exit_price = b.close[j]
    elif mode in ("stop", "stop_zd"):
        for j in range(e, n):
            # 收盘跌破止损 → 次日开盘卖（跌停开盘顺延）
            if b.close[j] < stop_price:
                k = j + 1
                while k < n and _open_limit_down(b, k):
                    k += 1
                if k >= n:
                    exit_j, exit_price, reason = n - 1, b.close[n - 1], "eod"
                else:
                    exit_j, exit_price, reason = k, b.open[k], "stop"
                break
            if j - e + 1 >= param:
                k = j
                while k < n and _close_limit_down(b, k):
                    k += 1
                if k >= n:
                    exit_j, exit_price, reason = n - 1, b.close[n - 1], "eod"
                else:
                    exit_j, exit_price, reason = k, b.close[k], "time"
                break
        else:
            exit_j, exit_price, reason = n - 1, b.close[n - 1], "eod"
    else:
        raise ValueError(f"未知出场模式: {mode}")

    exit_net = exit_price * (1.0 - COMMISSION - SLIPPAGE - _stamp_tax(str(b.date[exit_j])))
    entry_net = entry * (1.0 + COMMISSION)
    ret = exit_net / entry_net - 1.0
    seg_h = b.high[e: exit_j + 1]
    seg_l = b.low[e: exit_j + 1]
    return Trade(
        signal_kind=sig.kind,
        ts_code=b.ts_code,
        mode=mode,
        param=param,
        fire_date=str(b.date[i]),
        entry_date=str(b.date[e]),
        entry_price=round(float(entry), 4),
        exit_date=str(b.date[exit_j]),
        exit_price=round(float(exit_price), 4),
        bars_held=exit_j - e + 1,
        ret_net=float(ret),
        exit_reason=reason,
        mfe=float(seg_h.max() / entry - 1.0),
        mae=float(seg_l.min() / entry - 1.0),
    )
