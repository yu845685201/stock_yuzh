"""M7 ``volume_surge`` 放量倍数（锚定日）。

口径：``放量倍数 = 锚定日成交量 ÷ 前 N 个交易日均量``——

- **前 N 日不含当日**（若含当日，放量日会抬高自身基数、低估倍数）；
- N 由参数 ``window`` 控制（默认 5，即「5 日均量」）；
- 基数窗口内剔除无量行（停牌标记行 ``volume=0``），有效天数不足 N 返回 ``数据不足``；
- 成交量为前复权库内原始量纲（手/股同源一致），作比值后量纲无关。

**P1-12 自适应阈值（2026-10-22 增量，默认关）**：``adaptive=True`` 时命中阈值
取 ``max(hit_threshold, atr_k × ATR20%)``——高波动股需要更大成交量才计「真放量」，
压低波动噪音误报（ATR 走 indicators 库，通达信口径，原则 P6）。自适应只改
**命中判定**，不改输出值（CSV 列仍是原始倍数，回归红线）；``adaptive=False``
（缺省）时行为与增量前**逐字节一致**；ATR 数据不足时自适应退化回固定阈值
（不硬造）。命中统计桶键保持基阈值（``≥2x``），仅比较用有效阈值——防桶爆炸。

返回值：倍数（float，保留 4 位小数）/ ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List, Optional

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult


class VolumeSurge(AnalysisModule):
    """锚定日放量倍数（当日量 / 前N日均量；P1-12 自适应命中阈值默认关）。"""

    name: ClassVar[str] = "放量倍数"
    name_en: ClassVar[str] = "volume_surge"
    description: ClassVar[str] = (
        "锚定日成交量 ÷ 前 N 个交易日均量（N 由 window 参数控制，默认 5；基数不含当日）。"
        "基数窗口剔除无量行（停牌标记行 volume=0），有效天数不足 N、锚定日无成交、"
        "锚定日停牌或均量为 0 时返回「数据不足」。倍数保留 4 位小数（保证 ≥2 倍等"
        "截面阈值筛选不受舍入失真）。P1-12 自适应（adaptive 默认关）：开启时命中阈值 = "
        "max(hit_threshold, atr_k×ATR20%)，只改命中判定不改输出值"
    )
    required_columns: ClassVar[List[str]] = ["volume", "high", "low", "close"]
    max_lookback: ClassVar[int] = 25  # adaptive 需 ATR20 warmup（默认关时仍只需 6）
    default_params: ClassVar[Dict[str, object]] = {
        "window": 5, "hit_threshold": 2.0,
        "adaptive": False, "atr_k": 1.5,
    }
    # 命中统计阈值（方案 §2.5）：≥ hit_threshold 计命中，默认 2.0 与 R3「放量≥2 倍」同源；
    # 阈值可经链 YAML params 覆盖（如 params: {hit_threshold: 3.0}）
    hit_rule: ClassVar[str] = "threshold"

    def __init__(self, params: Optional[Dict[str, object]] = None) -> None:
        super().__init__(params)
        # 自适应有效阈值（每次 analyze 刷新；hit_keys 紧随 analyze 调用，
        # 引擎单线程逐股——状态新鲜。默认关时不用）
        self._effective_threshold: Optional[float] = None

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

    def hit_keys(self, value) -> List[str]:
        """命中桶：自适应开启时用有效阈值比较，桶键仍为基阈值（防桶爆炸）。"""
        if self.hit_rule != "threshold" or not bool(self.params.get("adaptive")):
            return super().hit_keys(value)
        base = float(self.params.get("hit_threshold", 2.0))
        eff = (self._effective_threshold
               if self._effective_threshold is not None else base)
        try:
            return [f"≥{base}x"] if float(value) >= eff else []
        except (TypeError, ValueError):
            return []

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        self._effective_threshold = None
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

        # P1-12 自适应有效阈值（默认关 → 不计算，行为逐字节不变）
        if bool(self.param("adaptive")):
            base_th = float(self.param("hit_threshold") or 2.0)
            atr_pct = self._atr_pct(ordered)
            if atr_pct is not None:
                k = float(self.param("atr_k") or 1.5)
                self._effective_threshold = max(base_th, k * atr_pct)
                detail["hit_threshold_effective"] = round(self._effective_threshold, 4)
                detail["atr_pct"] = round(atr_pct, 4)
            else:
                detail["hit_threshold_effective"] = base_th   # 退化回固定阈值
                detail["adaptive_degraded"] = True

        return ModuleResult(ratio, detail)
