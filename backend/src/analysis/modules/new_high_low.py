"""M8 ``new_high_low`` 60日新高新低（锚定日）。

口径：以前 N 个交易日（不含当日）的**前复权收盘价**区间为基准——

- ``close > 前 N 日收盘最大值`` → ``新高``；
- ``close < 前 N 日收盘最小值`` → ``新低``；
- 其余（含持平前高/前低）→ ``否``；
- N 由参数 ``window`` 控制（默认 60）；有效基准不足 N 天（次新股/长期停牌）返回
  ``数据不足``，不做降窗口计算（保证截面口径可比）。

前复权价口径下，除权除息日的价格跳变已被因子平滑，「新高/新低」为复权口径的技术
新高新低，与行情软件一致。

返回值：``新高`` / ``新低`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult


class NewHighLow(AnalysisModule):
    """锚定日收盘价是否创 N 日新高/新低。"""

    name: ClassVar[str] = "60日新高新低"
    name_en: ClassVar[str] = "new_high_low"
    description: ClassVar[str] = (
        "锚定日收盘价相对前 N 个交易日（不含当日）收盘区间：创新高记「新高」、创新低记"
        "「新低」（严格大于/小于，持平不算），其余记「否」。N 由 window 参数控制（默认 60），"
        "基于前复权收盘价；有效基准不足 N 天（次新股/长期停牌）、锚定日停牌或无成交时"
        "返回「数据不足」，不降窗口计算以保证截面口径可比"
    )
    required_columns: ClassVar[List[str]] = ["close", "volume"]
    max_lookback: ClassVar[int] = 61  # 前 60 个交易日 + 锚定日
    default_params: ClassVar[Dict[str, object]] = {"window": 60}
    # 命中统计分桶（方案 §2.5）：新高/新低两方向分别计数，不合并
    hit_buckets: ClassVar[List[str]] = ["新高", "新低"]

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

        close = pd.to_numeric(ordered["close"], errors="coerce")
        anchor_close = close.iloc[-1]
        if pd.isna(anchor_close):
            return ModuleResult.insufficient("缺 close")
        # 锚定日无成交（停牌标记行 volume=0，close 复制昨收）→ 未交易不判新高新低
        volume = pd.to_numeric(ordered["volume"], errors="coerce")
        anchor_volume = volume.iloc[-1] if "volume" in ordered.columns else float("nan")
        if pd.isna(anchor_volume) or float(anchor_volume) <= 0:
            return ModuleResult.insufficient("锚定日无成交(停牌标记行)")

        # 前 window 个交易日（不含当日）有效收盘基准，不足 window 天不降口径
        prev = close.iloc[-(window + 1):-1].dropna()
        if len(prev) < window:
            return ModuleResult.insufficient(f"前{window}个交易日数据不足(次新或长期停牌)")

        prev_max = float(prev.max())
        prev_min = float(prev.min())
        value = float(anchor_close)
        if value > prev_max:
            label = "新高"
        elif value < prev_min:
            label = "新低"
        else:
            label = "否"

        detail: Dict[str, object] = {
            "trade_date": as_of,
            "close": value,
            "prev_max": prev_max,
            "prev_min": prev_min,
            "window": window,
            "window_start": str(ordered["trade_date"].iloc[-(window + 1)]),
        }
        return ModuleResult(label, detail)
