"""外围链模块 ``ah_premium``：A-H 溢价率（参考口径）。

口径（整合方案 §4.2，与报告层 V2.0 块④同式）：
``溢价率 = a_close[-1] / (close[-1] × fx) − 1``——fx 为引擎装配的
``overseas_market_daily.fx_shkdcny`` 最新快照（ChainContext.upstream 键
``fx_hkdcny``，缺 → ``汇率待补``）；``a_close`` 整列 NaN → ``无A股映射``；
预检尾部滞后 → ``未对齐``。A 15:00 / H 16:00 / 汇率采集时点三者错位且两侧
复权基准独立滚动，只作观察不作结论。

返回值：数值（%，2 位小数）/ ``未对齐`` / ``无A股映射`` / ``汇率待补`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..data_domain import FX_HKDCNY_KEY, LAG_REASON_PREFIX
from ..module_base import AnalysisModule, ChainContext, ModuleResult
from .ah_5d_diff import NOT_ALIGNED, NO_A_MAPPING


class AhPremium(AnalysisModule):
    """A-H 溢价率。"""

    name: ClassVar[str] = "AH溢价%"
    name_en: ClassVar[str] = "ah_premium"
    description: ClassVar[str] = (
        "A-H 溢价率 = a_close[-1]/(close[-1]×港元兑人民币快照)−1（%，2 位小数）。"
        "fx 由引擎装配（overseas_market_daily fx_shkdcny 最新行，缺记「汇率待补」）；"
        "a_close 整列空记「无A股映射」；预检尾部滞后记「未对齐」。参考口径：A/H 收盘"
        "与汇率三者时点错位、两侧复权基准独立滚动，只作观察不作结论"
    )
    required_columns: ClassVar[List[str]] = ["close", "a_close"]
    max_lookback: ClassVar[int] = 1
    default_params: ClassVar[Dict[str, object]] = {}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if any(str(r).startswith(LAG_REASON_PREFIX) for r in (ctx.suspect_reasons or [])):
            return ModuleResult(NOT_ALIGNED)
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")
        ordered = df.sort_values("trade_date")
        if "a_close" not in ordered.columns:
            return ModuleResult(NO_A_MAPPING)
        a_close = pd.to_numeric(ordered["a_close"], errors="coerce")
        if a_close.dropna().empty:
            return ModuleResult(NO_A_MAPPING)
        fx = ctx.upstream.get(FX_HKDCNY_KEY)
        if fx is None:
            return ModuleResult("汇率待补")
        close = pd.to_numeric(ordered["close"], errors="coerce")
        anchor_close, anchor_a = close.iloc[-1], a_close.iloc[-1]
        if pd.isna(anchor_close) or pd.isna(anchor_a) or float(anchor_close) <= 0:
            return ModuleResult.insufficient("锚定行缺有效收盘")
        value = float(anchor_a) / (float(anchor_close) * float(fx)) - 1
        return ModuleResult(round(value * 100, 2))
