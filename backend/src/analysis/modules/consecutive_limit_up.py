"""M2 ``consecutive_limit_up`` 连续涨停数。

以锚定日为终点向过去数连续涨停天数（涨停判定规则同 M1），锚定日本身未涨停则为 0。
向量化实现，不做逐日 Python 循环（§3.3 规范 3）。

返回值：整数（≥0）/ ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import numpy as np
import pandas as pd

from ..limit_up_rules import LIMIT_SYSTEM_START, annotate_limit_up
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class ConsecutiveLimitUp(AnalysisModule):
    """连续涨停数（连板数）。"""

    name: ClassVar[str] = "连续涨停数"
    name_en: ClassVar[str] = "consecutive_limit_up"
    description: ClassVar[str] = (
        "以锚定日为终点向过去数连续涨停天数，锚定日本身未涨停则为 0；"
        "涨停判定规则同 limit_up_today。回看窗口内数据不足（如次新股）以实际可得数据计算，"
        "若锚定日之前不足 1 个交易日返回「数据不足」"
    )
    required_columns: ClassVar[List[str]] = ["change_rate", "close", "high", "is_st"]
    max_lookback: ClassVar[int] = 60
    default_params: ClassVar[Dict[str, object]] = {"max_lookback": 60}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")

        ordered = df.sort_values("trade_date").reset_index(drop=True)
        anchor = ordered.iloc[-1]
        as_of = str(anchor["trade_date"])
        if ctx.as_of and as_of != str(ctx.as_of):
            return ModuleResult.insufficient(f"锚定日{ctx.as_of}无K线(最新为{as_of})")

        if ctx.stock.list_date and as_of == ctx.stock.list_date:
            return ModuleResult.insufficient("新股上市首日不判定")
        if as_of < LIMIT_SYSTEM_START:
            return ModuleResult.insufficient("1996-12-16前无涨跌停制度")

        ann = annotate_limit_up(ordered, ctx.stock.stock_code, ctx.stock.board)
        known = ann["_limit_known"].to_numpy(dtype=bool)
        up = ann["_limit_up"].to_numpy(dtype=bool)

        if not known[-1]:
            return ModuleResult.insufficient("板块未知或缺 change_rate")
        if not up[-1]:
            return ModuleResult(0, {"trade_date": as_of, "streak": 0})

        if len(ordered) < 2:
            return ModuleResult.insufficient("锚定日之前无数据")

        lookback = max(int(self.param("max_lookback")), 1)
        # 从尾部向前数连续涨停：翻转后取「锚定日 + 最多 lookback-1 个前序交易日」，
        # 再找第一个非涨停（或不可判定）的位置即为连板数（向量化，无逐日循环）。
        effective = up & known
        window = effective[::-1][:lookback]
        if window.all():
            streak = int(len(window))
            truncated = True  # 用满整个回看窗口，真实连板数可能更长
        else:
            streak = int(np.argmin(window))
            truncated = False

        detail: Dict[str, object] = {
            "trade_date": as_of,
            "streak": streak,
            "max_lookback": lookback,
            "window_rows": int(len(ordered)),
            "truncated": bool(truncated),
        }
        return ModuleResult(streak, detail)
