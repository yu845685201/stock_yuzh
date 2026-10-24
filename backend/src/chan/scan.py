"""缠论买点扫描核（链模块与报告层共用，同数据同结果）。

给定单只股票的高低价序列与"发射K线索引"，跑结构引擎并返回当日确认的
一买/二买信号。链模块（逐股 df）与复盘报告层（独立重算）共用
``scan_stock_anchor``，保证两处同口径。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .signals import KIND_B1, KIND_B2, Signal, derive_signals
from .structure import ChanEngine

#: 参与扫描的信号种类（三买负超额未引入，见 data/chan_validation/report.md）
SCAN_KINDS = (KIND_B1, KIND_B2)
#: 次新/历史不足下限（与验证回测口径一致：120 根K）
MIN_BARS = 120
#: 扫描窗口（交易日）：笔中枢结构约需 2 年日K + MACD warmup 130
CHAN_WINDOW_DAYS = 520


@dataclass
class ChanHit:
    """一条当日确认的缠论买点（理由串各字段已格式化）。"""

    kind: str            # 一买 / 二买
    fire_i: int
    stop_price: float    # 建议止损（一买=触发低点，二买=一买低点）
    trigger_low: float
    zg: float
    zd: float
    divergence: Optional[float]  # 一买背驰强度 |area2|/|area1|（越小越强）；二买 None
    pivot_pid: int
    refire: bool         # 同中枢此前已有过买点信号（重发标记）


def scan_buy_hits(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                  fire_i: int) -> List[ChanHit]:
    """返回 ``fire_i``（含）当日确认的一买/二买信号列表。

    结构引擎为逐K增量计算（性能红线豁免登记于模块 docstring）；
    全市场约 520 根K/股实测 ~20 秒。
    """
    res = ChanEngine().run(high, low)
    from ..analysis.indicators import macd as tdx_macd  # 局部导入避免循环依赖

    m = tdx_macd(pd.Series(close))
    sigs = derive_signals(res, dif=m.dif.to_numpy(), hist=m.hist.to_numpy())
    hits: List[ChanHit] = []
    for s in sigs:
        if s.kind not in SCAN_KINDS or s.fire_i != fire_i:
            continue
        divergence = None
        if s.kind == KIND_B1 and s.extra.get("area1"):
            divergence = abs(s.extra["area2"] / s.extra["area1"])
        hits.append(ChanHit(
            kind="一买" if s.kind == KIND_B1 else "二买",
            fire_i=s.fire_i,
            stop_price=s.stop_price,
            trigger_low=s.trigger_low,
            zg=s.zg,
            zd=s.zd,
            divergence=divergence,
            pivot_pid=s.pivot_pid,
            refire=any(x.kind in SCAN_KINDS and x.pivot_pid == s.pivot_pid
                       and x.fire_i < s.fire_i for x in sigs),
        ))
    return hits


def hit_label(h: ChanHit) -> str:
    """理由串：``一买/背驰0.42/触发8.20/中枢[7.10,7.85]``（二买无背驰字段）。"""
    pivot = f"中枢[{h.zd:.2f},{h.zg:.2f}]"
    if h.kind == "一买":
        div = f"背驰{h.divergence:.2f}/" if h.divergence is not None else ""
        return f"一买/{div}触发{h.trigger_low:.2f}/{pivot}"
    return f"二买/触发{h.trigger_low:.2f}/止损{h.stop_price:.2f}/{pivot}"


def scan_stock_anchor(df: pd.DataFrame, as_of: str) -> Tuple[str, Optional[ChanHit], Optional[str]]:
    """单股锚定日扫描（链模块与报告层共用入口）。

    Args:
        df: 单只股票日K（需含 trade_date/high/low/close/is_st，乱序自动排序）。
        as_of: 锚定日 yyyyMMdd；锚定日无K线 → insufficient。

    Returns:
        (status, hit, reason)：status ∈ ``hit`` / ``no`` / ``insufficient``。
        前置过滤与链模块口径一致：锚定日 ST → no（ST剔除）、K线不足
        MIN_BARS → insufficient（次新）、非正价 → insufficient。
    """
    if df is None or df.empty:
        return "insufficient", None, "无K线数据"
    ordered = df.sort_values("trade_date").reset_index(drop=True)
    anchor = ordered.iloc[-1]
    if as_of and str(anchor["trade_date"]) != str(as_of):
        return "insufficient", None, f"锚定日{as_of}无K线"
    if len(ordered) < MIN_BARS:
        return "insufficient", None, f"历史不足{MIN_BARS}根(次新)"
    high = pd.to_numeric(ordered["high"], errors="coerce").to_numpy(float)
    low = pd.to_numeric(ordered["low"], errors="coerce").to_numpy(float)
    close = pd.to_numeric(ordered["close"], errors="coerce").to_numpy(float)
    if not (np.isfinite(high).all() and np.isfinite(low).all()
            and np.isfinite(close).all()):
        return "insufficient", None, "K线含缺失值"
    if (low <= 0).any() or (high <= 0).any():
        return "insufficient", None, "复权价异常(非正价)"
    if bool(anchor.get("is_st")):
        return "no", None, "锚定日ST剔除"
    hits = scan_buy_hits(high, low, close, fire_i=len(ordered) - 1)
    if not hits:
        return "no", None, None
    return "hit", hits[-1], None  # 同日多信号取最后一个（极少见）
