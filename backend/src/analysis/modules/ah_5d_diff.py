"""外围链模块 ``ah_5d_diff``：A-H 两地 5 日累计涨跌幅差值（A 侧 − H 侧）。

口径（整合方案 §4.2，参考口径同报告层 V2.0 块④）：
- H 侧 = ``close`` 尾 6 行首尾比（市场自身日历，qfq）；
- A 侧 = ``a_close`` 尾 6 行首尾比（引擎按 as-of 回挂 his_kline_day 前复权收盘）；
- 差值 = A 侧 − H 侧（百分点，正 = A 强于 H）；
- ``a_close`` 整列 NaN → ``无A股映射``（区别于数据不足：18 只仅港股的结构性事实）；
- A 侧尾部对照不足 6 行有效值 → ``数据不足``；
- 预检尾部滞后（>3 个该市场交易日）→ ``未对齐``（不参与 AH 结论）。
两侧复权基准独立滚动（H=qfq、A=前复权），本列永远为参考口径。

返回值：数值（百分点，2 位小数）/ ``未对齐`` / ``无A股映射`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..data_domain import LAG_REASON_PREFIX
from ..module_base import AnalysisModule, ChainContext, ModuleResult

NO_A_MAPPING = "无A股映射"
NOT_ALIGNED = "未对齐"


def _has_lag_reason(reasons: List[str]) -> bool:
    return any(str(r).startswith(LAG_REASON_PREFIX) for r in (reasons or []))


class Ah5dDiff(AnalysisModule):
    """A-H 5 日累计差值。"""

    name: ClassVar[str] = "AH 5日差"
    name_en: ClassVar[str] = "ah_5d_diff"
    description: ClassVar[str] = (
        "A-H 两地 5 日累计涨跌幅差值（A 侧−H 侧，百分点，正=A 强于 H）。"
        "H 侧取 close 尾 6 行首尾比（市场自身日历，qfq），A 侧取引擎回挂的 a_close"
        "（his_kline_day 前复权，as-of 最近）尾 6 行首尾比；两侧复权基准独立滚动，"
        "参考口径。a_close 整列空记「无A股映射」，A 侧对照不足 6 行记「数据不足」，"
        "预检尾部滞后（>3 个市场交易日）记「未对齐」不参与结论"
    )
    required_columns: ClassVar[List[str]] = ["close", "a_close"]
    max_lookback: ClassVar[int] = 6
    default_params: ClassVar[Dict[str, object]] = {}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if _has_lag_reason(ctx.suspect_reasons):
            return ModuleResult(NOT_ALIGNED)
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")
        ordered = df.sort_values("trade_date")
        if "a_close" not in ordered.columns:
            return ModuleResult(NO_A_MAPPING)
        a_close = pd.to_numeric(ordered["a_close"], errors="coerce")
        if a_close.dropna().empty:
            return ModuleResult(NO_A_MAPPING)

        close = pd.to_numeric(ordered["close"], errors="coerce")
        if len(close) < 6 or close.iloc[-6:].isna().any():
            return ModuleResult.insufficient("H侧不足6行有效收盘")
        chg_h = float(close.iloc[-1]) / float(close.iloc[-6]) - 1
        if len(a_close) < 6 or a_close.iloc[-6:].isna().any():
            return ModuleResult.insufficient("A侧对照不足6行有效值")
        chg_a = float(a_close.iloc[-1]) / float(a_close.iloc[-6]) - 1
        return ModuleResult(round((chg_a - chg_h) * 100, 2))
