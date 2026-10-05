"""M6 ``big_mover`` 大涨大跌标记（锚定日）。

口径：纯涨跌幅阈值标记，与涨跌停制度无关——

- ``change_rate ≥ up_pct``（默认 5.0）→ ``大涨``；
- ``change_rate ≤ down_pct``（默认 -5.0）→ ``大跌``；
- 其余 → ``否``。

不跳过新股上市首日：首日暴涨暴跌同样是异动事实（与涨跌停判定模块的「首日不判定」
口径不同，后者受制度约束而本模块不受）。阈值可在链 YAML 中以 ``up_pct``/``down_pct``
覆盖。

返回值：``大涨`` / ``大跌`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult


class BigMover(AnalysisModule):
    """锚定日大涨大跌标记。"""

    name: ClassVar[str] = "大涨大跌"
    name_en: ClassVar[str] = "big_mover"
    description: ClassVar[str] = (
        "锚定日涨跌幅是否达到异动阈值：change_rate ≥ up_pct（默认 5.0）记「大涨」，"
        "≤ down_pct（默认 -5.0）记「大跌」，其余记「否」。纯涨跌幅阈值、与涨跌停制度无关，"
        "不跳过新股上市首日；锚定日停牌、锚定日无成交或缺 change_rate 返回「数据不足」"
    )
    required_columns: ClassVar[List[str]] = ["change_rate", "volume"]
    max_lookback: ClassVar[int] = 2  # 判定只需锚定日本身，留 1 日余量便于确认
    default_params: ClassVar[Dict[str, object]] = {"up_pct": 5.0, "down_pct": -5.0}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")

        ordered = df.sort_values("trade_date")
        anchor = ordered.iloc[-1]
        as_of = str(anchor["trade_date"])

        if ctx.as_of and as_of != str(ctx.as_of):
            return ModuleResult.insufficient(f"锚定日{ctx.as_of}无K线(最新为{as_of})")

        up_pct = float(self.param("up_pct"))
        down_pct = float(self.param("down_pct"))
        if not up_pct > down_pct:
            return ModuleResult.insufficient("参数非法(要求 up_pct > down_pct)")

        change = pd.to_numeric(ordered["change_rate"], errors="coerce").iloc[-1]
        volume = pd.to_numeric(ordered["volume"], errors="coerce").iloc[-1]
        if pd.isna(change):
            return ModuleResult.insufficient("缺 change_rate")
        # 锚定日无成交（停牌标记行 volume=0）→ 未交易，不算异动
        if pd.isna(volume) or float(volume) <= 0:
            return ModuleResult.insufficient("锚定日无成交(停牌标记行)")

        value = float(change)
        if value >= up_pct:
            label = "大涨"
        elif value <= down_pct:
            label = "大跌"
        else:
            label = "否"

        detail: Dict[str, object] = {
            "trade_date": as_of,
            "change_rate": value,
            "up_pct": up_pct,
            "down_pct": down_pct,
        }
        return ModuleResult(label, detail)
