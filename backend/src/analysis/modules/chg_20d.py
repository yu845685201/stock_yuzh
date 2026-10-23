"""外围链模块 ``chg_20d``：20 日累计涨跌幅（锚定日，市场自身日历）。

口径：``close[-1]/close[-21]-1``，与报告层 V2.0 外围指数块同构同口径；
不足 21 行返回 ``数据不足``（不降窗口）。

返回值：数值（%，2 位小数）/ ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult


class Chg20d(AnalysisModule):
    """20 日累计涨跌幅。"""

    name: ClassVar[str] = "20日累计%"
    name_en: ClassVar[str] = "chg_20d"
    description: ClassVar[str] = (
        "锚定日 20 日累计涨跌幅 = close[-1]/close[-21]−1（尾部 21 行首尾比，市场自身日历），"
        "与报告层外围指数块同构同口径；不足 21 行或锚定日无行返回「数据不足」，不降窗口。"
        "外围域模块（无涨跌停语境，勿用 A 股日历解读窗口）"
    )
    required_columns: ClassVar[List[str]] = ["close"]
    max_lookback: ClassVar[int] = 21
    default_params: ClassVar[Dict[str, object]] = {}
    # 命中统计口径（方案 §2.5）：连续比率不计命中，显示可算样本数
    hit_rule: ClassVar[str] = "sample"

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")
        ordered = df.sort_values("trade_date")
        anchor = ordered.iloc[-1]
        if ctx.as_of and str(anchor["trade_date"]) != str(ctx.as_of):
            return ModuleResult.insufficient(
                f"锚定日{ctx.as_of}无K线(最新为{anchor['trade_date']})")
        close = pd.to_numeric(ordered["close"], errors="coerce")
        if len(close) < 21 or close.iloc[-21:].isna().any():
            return ModuleResult.insufficient("不足21行有效收盘")
        value = float(close.iloc[-1]) / float(close.iloc[-21]) - 1
        return ModuleResult(round(value * 100, 2))
