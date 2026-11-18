"""M4 ``burst_limit`` 是否炸板（锚定日盘中触及涨停价但未封住）。

判定依据（P0-1 单一实现，与涨停判定/报告层截面同源）：
- **触板**：盘中最高价相对昨收的涨幅 ``(high − preclose) / preclose × 100``
  达到涨停判定阈值——阈值与涨停判定共用 ``dynamic_thresholds`` 动态口径
  （制度上限 − 0.005/估计昨收；raw_close 缺失回退固定下浮 −0.10）——
  最高价即当日可成交的最高价，触板价必然计入 high；
- **未封住**：收盘未涨停（``annotate_limit_up`` 判否）；
- **炸板 = 触板 ∧ 未封住**。封住的（涨停）不算炸板，未触板的也不算。

价格均为前复权口径，``high`` 与 ``preclose`` 同基准，比值不受复权影响。

返回值：``是`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..limit_up_rules import (LIMIT_SYSTEM_START, annotate_limit_up,
                              dynamic_thresholds)
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class BurstLimit(AnalysisModule):
    """锚定日是否炸板（盘中触板未封住）。"""

    name: ClassVar[str] = "是否炸板"
    name_en: ClassVar[str] = "burst_limit"
    description: ClassVar[str] = (
        "锚定日是否炸板（盘中触及涨停价但收盘未封住）。触板按盘中最高价相对昨收的涨幅"
        "达到涨停判定阈值判定（P0-1 动态口径：制度上限−0.005/估计昨收，raw_close 缺失"
        "回退固定 −0.10，与涨停判定/报告层截面共用 dynamic_thresholds 单一实现），"
        "未封住按涨停判定（change_rate 阈值法 + close ≥ high）判否；收于涨停不算炸板。"
        "1996-12-16 前无涨跌停制度、板块未知、新股上市首日、缺 preclose/high 或 "
        "preclose ≤ 0 导致无法判定触板时返回「数据不足」"
    )
    required_columns: ClassVar[List[str]] = ["change_rate", "close", "high", "preclose", "is_st"]
    max_lookback: ClassVar[int] = 2  # 判定只需锚定日本身，留 1 日余量便于确认

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")

        ordered = df.sort_values("trade_date")
        anchor = ordered.iloc[-1]
        as_of = str(anchor["trade_date"])

        if ctx.as_of and as_of != str(ctx.as_of):
            return ModuleResult.insufficient(f"锚定日{ctx.as_of}无K线(最新为{as_of})")

        if ctx.stock.list_date and as_of == ctx.stock.list_date:
            return ModuleResult.insufficient("新股上市首日不判定")

        if as_of < LIMIT_SYSTEM_START:
            return ModuleResult.insufficient("1996-12-16前无涨跌停制度")

        ann = annotate_limit_up(ordered, ctx.stock.stock_code, ctx.stock.board,
                                   list_date=ctx.stock.list_date)
        row = ann.iloc[-1]
        if not bool(row["_limit_known"]):
            return ModuleResult.insufficient("板块未知或缺 change_rate")

        limit_pct = float(row["_limit_pct"])
        sealed = bool(row["_limit_up"])
        change_rate = float(row["change_rate"])

        # 收于涨停即封住，不可能是炸板，触板判定可整体跳过
        if sealed:
            detail: Dict[str, object] = {
                "trade_date": as_of,
                "sealed": True,
                "limit_pct": limit_pct,
                "change_rate": change_rate,
            }
            return ModuleResult("否", detail)

        high = pd.to_numeric(ann["high"], errors="coerce").iloc[-1]
        preclose = pd.to_numeric(ann["preclose"], errors="coerce").iloc[-1]
        if pd.isna(high) or pd.isna(preclose) or preclose <= 0:
            return ModuleResult.insufficient("缺 preclose/high 或 preclose≤0，无法判定是否触板")

        high_gain = (float(high) - float(preclose)) / float(preclose) * 100.0
        # P0-1 单一实现：触板与涨停共用 dynamic_thresholds 动态口径。旧内联固定
        # 下浮 −0.10 会高估 >5 元股的触板——最高价差一个最小报价单位够不到真实
        # 板价被误判（20261027 实测链 53 vs 截面 47，6 只全为此形态）。
        raw_close = ann["raw_close"] if "raw_close" in ann.columns else None
        thr_series, exact_series = dynamic_thresholds(
            ann["_limit_pct"], ann["change_rate"], raw_close, signed="up")
        threshold = float(thr_series.iloc[-1])
        exact = bool(exact_series.iloc[-1])
        touched = high_gain >= threshold

        detail = {
            "trade_date": as_of,
            "sealed": False,
            "touched": bool(touched),
            "high_gain": round(high_gain, 4),
            "threshold": round(threshold, 4),
            "limit_pct": limit_pct,
            "change_rate": change_rate,
            "dynamic_exact": exact,
        }
        return ModuleResult("是" if touched else "否", detail)
