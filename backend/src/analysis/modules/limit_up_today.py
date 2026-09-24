"""M1 ``limit_up_today`` 是否涨停（锚定日）。

判定依据：``change_rate`` 阈值法 + ``close ≥ high`` 逻辑必要条件（详见
``analysis.limit_up_rules`` 模块文档与 ``README.MD``）。

返回值：``是`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..limit_up_rules import LIMIT_SYSTEM_START, annotate_limit_up
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class LimitUpToday(AnalysisModule):
    """锚定日是否涨停。"""

    name: ClassVar[str] = "是否涨停"
    name_en: ClassVar[str] = "limit_up_today"
    description: ClassVar[str] = (
        "锚定日是否涨停。按 change_rate 阈值法判定，阈值随板块与制度分段"
        "（主板10%/创业板20%/科创板20%/北交所30%/ST 5%），并以 close ≥ high 作为"
        "逻辑必要条件剔除伪命中；1996-12-16 前无涨跌停制度、板块未知、新股上市首日"
        "均返回「数据不足」"
    )
    required_columns: ClassVar[List[str]] = ["change_rate", "close", "high", "is_st"]
    max_lookback: ClassVar[int] = 2  # 判定只需锚定日本身，留 1 日余量便于确认

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")

        ordered = df.sort_values("trade_date")
        anchor = ordered.iloc[-1]
        as_of = str(anchor["trade_date"])

        # 锚定日与 ctx.as_of 不一致（停牌等）→ 数据不足，避免误用旧数据
        if ctx.as_of and as_of != str(ctx.as_of):
            return ModuleResult.insufficient(f"锚定日{ctx.as_of}无K线(最新为{as_of})")

        if ctx.stock.list_date and as_of == ctx.stock.list_date:
            return ModuleResult.insufficient("新股上市首日不判定")

        if as_of < LIMIT_SYSTEM_START:
            return ModuleResult.insufficient("1996-12-16前无涨跌停制度")

        ann = annotate_limit_up(ordered, ctx.stock.stock_code, ctx.stock.board)
        row = ann.iloc[-1]
        if not bool(row["_limit_known"]):
            return ModuleResult.insufficient("板块未知或缺 change_rate")

        detail: Dict[str, object] = {
            "trade_date": as_of,
            "change_rate": float(row["change_rate"]),
            "limit_pct": float(row["_limit_pct"]),
            "close": float(row["close"]),
            "high": float(row["high"]),
            "is_st": bool(row["is_st"]),
        }
        return ModuleResult("是" if bool(row["_limit_up"]) else "否", detail)
