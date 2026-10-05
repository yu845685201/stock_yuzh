"""M7 ``volume_surge`` 放量倍数（锚定日）。

口径：``放量倍数 = 锚定日成交量 ÷ 前 N 个交易日均量``——

- **前 N 日不含当日**（若含当日，放量日会抬高自身基数、低估倍数）；
- N 由参数 ``window`` 控制（默认 5，即「5 日均量」）；
- 基数窗口内剔除无量行（停牌标记行 ``volume=0``），有效天数不足 N 返回 ``数据不足``；
- 成交量为前复权库内原始量纲（手/股同源一致），作比值后量纲无关。

返回值：倍数（float，保留 4 位小数）/ ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult


class VolumeSurge(AnalysisModule):
    """锚定日放量倍数（当日量 / 前N日均量）。"""

    name: ClassVar[str] = "放量倍数"
    name_en: ClassVar[str] = "volume_surge"
    description: ClassVar[str] = (
        "锚定日成交量 ÷ 前 N 个交易日均量（N 由 window 参数控制，默认 5；基数不含当日）。"
        "基数窗口剔除无量行（停牌标记行 volume=0），有效天数不足 N、锚定日无成交、"
        "锚定日停牌或均量为 0 时返回「数据不足」。倍数保留 4 位小数（保证 ≥2 倍等"
        "截面阈值筛选不受舍入失真）"
    )
    required_columns: ClassVar[List[str]] = ["volume"]
    max_lookback: ClassVar[int] = 6  # 前 5 个交易日 + 锚定日
    default_params: ClassVar[Dict[str, object]] = {"window": 5}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient("无K线数据")

        ordered = df.sort_values("trade_date")
        anchor = ordered.iloc[-1]
        as_of = str(anchor["trade_date"])

        if ctx.as_of and as_of != str(ctx.as_of):
            return ModuleResult.insufficient(f"锚定日{ctx.as_of}无K线(最新为{as_of})")

        try:
            window = int(self.param("window"))
        except (TypeError, ValueError):
            return ModuleResult.insufficient("参数非法(window 须为整数)")
        if window < 1:
            return ModuleResult.insufficient("参数非法(window 须 ≥ 1)")

        volume = pd.to_numeric(ordered["volume"], errors="coerce")
        anchor_volume = volume.iloc[-1]
        if pd.isna(anchor_volume):
            return ModuleResult.insufficient("缺 volume")
        # 锚定日无成交（停牌标记行 volume=0）→ 无量可比
        if float(anchor_volume) <= 0:
            return ModuleResult.insufficient("锚定日无成交(停牌标记行)")

        # 前 window 个交易日（不含当日），剔除无量行后须满 window 天
        prev = volume.iloc[-(window + 1):-1]
        dates = ordered["trade_date"].iloc[-(window + 1):-1]
        valid = prev[prev > 0]
        if len(valid) < window:
            return ModuleResult.insufficient(f"前{window}日均量数据不足(含无量行或历史不足)")

        base = float(valid.mean())
        if base <= 0:
            return ModuleResult.insufficient("前N日均量为0")

        ratio = round(float(anchor_volume) / base, 4)
        detail: Dict[str, object] = {
            "trade_date": as_of,
            "volume": float(anchor_volume),
            "base_volume": round(base, 4),
            "window": window,
            "window_start": str(dates.iloc[0]),
        }
        return ModuleResult(ratio, detail)
