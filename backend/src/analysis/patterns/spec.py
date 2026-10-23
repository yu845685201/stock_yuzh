"""形态规格定义（P0-2）：方向常量 + PatternSpec。

独立模块（不 import 兄弟模块）——避免 base ↔ single/double/triple 循环 import。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np

#: 方向（供模块优先级与解读过滤）
BULLISH = 'bullish'
BEARISH = 'bearish'
NEUTRAL = 'neutral'


@dataclass(frozen=True)
class PatternSpec:
    """形态规格（判定式 + 元数据）。"""

    key: str                 # 英文名（对拍基线同名）
    name: str                # 中文名（模块输出/展示）
    required_count: int      # 需要几根 K 线
    direction: str           # bullish / bearish / neutral
    family: str              # single / double / triple / continuation
    source: str              # 'candlestick-patterns'（有对拍基线）/ 'self-built'
    detect: Callable[[dict], np.ndarray]   # 数组字典 → 布尔数组
