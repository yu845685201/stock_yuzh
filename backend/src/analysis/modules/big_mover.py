"""M6 ``big_mover`` 大涨大跌标记（锚定日）。

口径：纯涨跌幅阈值标记，与涨跌停制度无关——

- ``change_rate ≥ up_pct``（默认 5.0）→ ``大涨``；
- ``change_rate ≤ down_pct``（默认 -5.0）→ ``大跌``；
- 其余 → ``否``。

**P1-12 自适应阈值（2026-10-22 增量，默认关）**：``adaptive=True`` 时阈值取
``max(up_pct, atr_k × ATR20%)``——高波动股（ATR% 大）的异动门槛自动抬高，
压低「波动大但非真异动」的误报；ATR 走 indicators 库（通达信口径，原则 P6）。
``adaptive=False``（缺省）时行为与增量前**逐字节一致**（回归红线：daily_review
链黄金样本不漂移）；ATR 数据不足时自适应退化回固定阈值（不硬造）。

不跳过新股上市首日：首日暴涨暴跌同样是异动事实（与涨跌停判定模块的「首日不判定」
口径不同，后者受制度约束而本模块不受）。阈值可在链 YAML 中以 ``up_pct``/``down_pct``
覆盖。

返回值：``大涨`` / ``大跌`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List, Optional

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult


class BigMover(AnalysisModule):
    """锚定日大涨大跌标记（含 P1-12 自适应阈值，默认关）。"""

    name: ClassVar[str] = "大涨大跌"
    name_en: ClassVar[str] = "big_mover"
    description: ClassVar[str] = (
        "锚定日涨跌幅是否达到异动阈值：change_rate ≥ up_pct（默认 5.0）记「大涨」，"
        "≤ down_pct（默认 -5.0）记「大跌」，其余记「否」。纯涨跌幅阈值、与涨跌停制度无关，"
        "不跳过新股上市首日；锚定日停牌、锚定日无成交或缺 change_rate 返回「数据不足」。"
        "P1-12 自适应（adaptive 默认关）：开启时阈值 = max(up_pct, atr_k×ATR20%)，"
        "高波动股门槛自动抬高（ATR 走 indicators 库）"
    )
    required_columns: ClassVar[List[str]] = ["change_rate", "volume", "high",
                                             "low", "close"]
    max_lookback: ClassVar[int] = 25   # adaptive 需 ATR20 warmup（默认关时仍只需 2）
    default_params: ClassVar[Dict[str, object]] = {
        "up_pct": 5.0, "down_pct": -5.0,
        "adaptive": False, "atr_k": 1.5,
    }
    # 命中统计分桶（方案 §2.5）：涨/跌两方向分别计数，不合并
    hit_buckets: ClassVar[List[str]] = ["大涨", "大跌"]

    def _atr_pct(self, ordered: pd.DataFrame) -> Optional[float]:
        """ATR20/close×100（indicators 库通达信口径；数据不足 → None）。"""
        try:
            from ..indicators import atr as _atr_fn
            close = pd.to_numeric(ordered["close"], errors="coerce")
            high = pd.to_numeric(ordered["high"], errors="coerce")
            low = pd.to_numeric(ordered["low"], errors="coerce")
            atr_s = _atr_fn(close, high, low, 20)
            v = atr_s.iloc[-1]
            c = close.iloc[-1]
            if pd.isna(v) or pd.isna(c) or float(c) <= 0:
                return None
            return float(v) / float(c) * 100.0
        except Exception:  # pragma: no cover - 指标异常兜底
            return None

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

        # P1-12 自适应阈值（默认关 → 阈值即固定值，行为逐字节不变）
        adaptive = bool(self.param("adaptive"))
        atr_pct: Optional[float] = None
        if adaptive:
            atr_pct = self._atr_pct(ordered)
            if atr_pct is not None:
                k = float(self.param("atr_k") or 1.5)
                up_pct = max(up_pct, k * atr_pct)
                down_pct = min(down_pct, -k * atr_pct)

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
            "up_pct": round(up_pct, 4),
            "down_pct": round(down_pct, 4),
            "adaptive": adaptive,
        }
        if adaptive:
            detail["atr_pct"] = round(atr_pct, 4) if atr_pct is not None else None
        return ModuleResult(label, detail)
