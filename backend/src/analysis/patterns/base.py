"""形态库核心：向量化检测与注册表（P0-2，方案 §3.2）。

设计要点（对照源项目 `SpiralDevelopment__candlestick-patterns`）：
- **保留** ``required_count + 判定式`` 的最小抽象——每个形态 = 需要几根 K 线 +
  一个布尔表达式；前 ``required_count-1`` 行不出形态（与源项目基类扫描语义一致）；
- **改造** 逐行 ``logic(idx)`` → **整列布尔表达式**（本项目模块规范第 3 条：
  禁逐行 Python 循环）；运算在 numpy 数组上进行（小序列开销比 pandas Series
  低一个量级，链里 5820 只 × 21 形态可承受）；
- **趋势上下文显式化**（源项目唯一先例 HangingMan）：吊人线要求前两日最高价
  均低于今日开盘；红三兵/三只乌鸦为本项目自建（源项目无，标 ``self-built``，
  无对拍基线）。
"""

from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from . import double, single, triple
from .geometry import as_arrays
from .spec import BEARISH, BULLISH, NEUTRAL, PatternSpec

#: 注册表（顺序 = 同日多命中优先级：三根反转 > 双根反转 > 单根 > 持续形态）
PATTERN_REGISTRY: List[PatternSpec] = [
    # ---- 三根反转（priority 1）----
    triple.MORNING_STAR, triple.EVENING_STAR, triple.MORNING_STAR_DOJI,
    triple.EVENING_STAR_DOJI, triple.HANGING_MAN,
    # ---- 双根反转（priority 2）----
    double.BULLISH_ENGOLFING, double.BEARISH_ENGOLFING,
    double.BULLISH_HARAMI, double.BEARISH_HARAMI,
    double.STAR, double.RAIN_DROP, double.SHOOTING_STAR,
    double.DARK_CLOUD_COVER, double.PIERCING_PATTERN,
    # ---- 单根（priority 3）----
    single.HAMMER, single.INVERTED_HAMMER, single.DOJI,
    single.DRAGONFLY_DOJI, single.GRAVESTONE_DOJI,
    # ---- 持续形态（priority 4，自建）----
    triple.THREE_WHITE_SOLDIERS, triple.THREE_BLACK_CROWS,
]


def detect_all(df: pd.DataFrame,
               specs: Optional[List[PatternSpec]] = None) -> Dict[str, np.ndarray]:
    """全形态检测：OHLC DataFrame（按 trade_date 升序）→ {中文名: 布尔数组}。

    前 ``required_count-1`` 行恒 False（与源项目基类扫描语义一致）；
    一字板（high==low）全部形态不命中（几何工具 + 裸占比 NaN 语义保证）。
    """
    specs = specs or PATTERN_REGISTRY
    a = as_arrays(df)
    n = len(df)
    out: Dict[str, np.ndarray] = {}
    for spec in specs:
        if n < spec.required_count:
            out[spec.name] = np.zeros(n, dtype=bool)
            continue
        with np.errstate(divide='ignore', invalid='ignore'):
            hit = np.asarray(spec.detect(a), dtype=bool)
        if spec.required_count > 1:
            hit = hit.copy()
            hit[: spec.required_count - 1] = False
        out[spec.name] = hit
    return out


def load_all() -> List[PatternSpec]:
    """自检并返回全部形态规格（导入期调用，防注册表与实现脱节）。"""
    names = [s.name for s in PATTERN_REGISTRY]
    assert len(names) == len(set(names)), f'形态中文名重复: {names}'
    keys = [s.key for s in PATTERN_REGISTRY]
    assert len(keys) == len(set(keys)), f'形态英文名重复: {keys}'
    for s in PATTERN_REGISTRY:
        assert s.direction in (BULLISH, BEARISH, NEUTRAL), s
        assert s.source in ('candlestick-patterns', 'self-built'), s
    return PATTERN_REGISTRY
