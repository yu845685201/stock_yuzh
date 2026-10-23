"""M5 ``limit_up_premium`` 昨日涨停今日溢价。

口径：以前一可得交易日（锚定日的前一行）是否涨停为基准——

- 前一交易日涨停 → 返回今日溢价 = 今日 ``change_rate``（昨收 → 今收，前复权口径一致）；
- 前一交易日未涨停 → ``否``；
- 前一交易日不可判定（制度不适用/缺涨跌幅）→ ``数据不足``。

若该股锚定日的前一交易日停牌（缺行），以前一可得交易日为准（与
``consecutive_limit_up`` 口径一致），detail 中携带该日日期供报告层甄别。

返回值：溢价百分数（float，保留 2 位）/ ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..limit_up_rules import annotate_limit_up
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class LimitUpPremium(AnalysisModule):
    """昨日涨停股的今日溢价。"""

    name: ClassVar[str] = "昨日涨停今日溢价"
    name_en: ClassVar[str] = "limit_up_premium"
    description: ClassVar[str] = (
        "前一交易日涨停的股票今日涨跌幅（溢价，%）。以前一可得交易日是否涨停为基准"
        "（涨停判定规则同 limit_up_today），未涨停返回「否」，涨停返回今日 change_rate"
        "（保留 2 位）。前一交易日不可判定（制度不适用/缺涨跌幅）、锚定日停牌、"
        "锚定日无成交、锚定日之前无数据时返回「数据不足」"
    )
    required_columns: ClassVar[List[str]] = ["change_rate", "close", "high", "is_st", "volume"]
    max_lookback: ClassVar[int] = 2  # 前一交易日 + 锚定日
    # 命中统计口径（方案 §2.5）：正溢价/负溢价分别计数（晋级成功/失败两面都要可见）
    hit_rule: ClassVar[str] = "signed"

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")

        ordered = df.sort_values("trade_date")
        anchor = ordered.iloc[-1]
        as_of = str(anchor["trade_date"])

        if ctx.as_of and as_of != str(ctx.as_of):
            return ModuleResult.insufficient(f"锚定日{ctx.as_of}无K线(最新为{as_of})")

        if len(ordered) < 2:
            return ModuleResult.insufficient("锚定日之前无数据")

        # 锚定日无成交（停牌标记行 volume=0）→ 溢价无意义
        volume = pd.to_numeric(ordered["volume"], errors="coerce")
        if pd.isna(volume.iloc[-1]) or float(volume.iloc[-1]) <= 0:
            return ModuleResult.insufficient("锚定日无成交(停牌标记行)")

        ann = annotate_limit_up(ordered, ctx.stock.stock_code, ctx.stock.board)
        prev = ann.iloc[-2]
        if not bool(prev["_limit_known"]):
            return ModuleResult.insufficient("前一交易日涨停不可判定(制度不适用或缺涨跌幅)")

        prev_detail: Dict[str, object] = {
            "prev_date": str(prev["trade_date"]),
            "prev_change_rate": float(prev["change_rate"]),
            "limit_pct": float(prev["_limit_pct"]),
        }
        if not bool(prev["_limit_up"]):
            return ModuleResult("否", prev_detail)

        change = pd.to_numeric(ann["change_rate"], errors="coerce").iloc[-1]
        if pd.isna(change):
            return ModuleResult.insufficient("锚定日缺 change_rate")

        premium = round(float(change), 2)
        detail = {**prev_detail, "premium": premium}
        return ModuleResult(premium, detail)
