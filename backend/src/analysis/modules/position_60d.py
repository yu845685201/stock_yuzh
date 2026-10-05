"""外围链模块 ``position_60d``：60 日窗口位置百分位（锚定日，市场自身日历）。

口径：``(c[-1]−min(w))/(max(w)−min(w))×100``，w=尾部 61 行（与 A 股指数章、
报告层 V2.0 块①、aggregator 同构同口径）；样本 <30 行不出分位（防误导，
同 V2.0「<30 行 position=None」）；窗口无波动（hi==lo）返回 ``数据不足``。

返回值：数值（%，1 位小数）/ ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult


class Position60d(AnalysisModule):
    """60 日窗口位置百分位。"""

    name: ClassVar[str] = "60日位置%"
    name_en: ClassVar[str] = "position_60d"
    description: ClassVar[str] = (
        "锚定日收盘在尾部 61 行（市场自身日历）高低区间的位置百分位 = "
        "(收盘−窗口最低)/(窗口最高−窗口最低)×100，与 A 股指数章、报告层外围指数块"
        "同构同口径；样本不足 30 行、窗口无波动或锚定日无行返回「数据不足」。"
        "外围域模块"
    )
    required_columns: ClassVar[List[str]] = ["close"]
    max_lookback: ClassVar[int] = 61
    default_params: ClassVar[Dict[str, object]] = {}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")
        ordered = df.sort_values("trade_date")
        anchor = ordered.iloc[-1]
        if ctx.as_of and str(anchor["trade_date"]) != str(ctx.as_of):
            return ModuleResult.insufficient(
                f"锚定日{ctx.as_of}无K线(最新为{anchor['trade_date']})")
        close = pd.to_numeric(ordered["close"], errors="coerce").dropna()
        w = close.tail(61)
        if len(w) < 30:
            return ModuleResult.insufficient(f"样本不足({len(w)}行少于30行)")
        lo, hi = float(w.min()), float(w.max())
        if hi <= lo:
            return ModuleResult.insufficient("窗口无波动")
        value = (float(w.iloc[-1]) - lo) / (hi - lo) * 100
        return ModuleResult(round(value, 1))
