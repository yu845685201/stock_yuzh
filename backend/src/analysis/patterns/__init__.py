"""K 线形态库（P0-2，方案 §3.2）。

结构（对照源项目三层切分的本项目版）::

    spec.py       方向常量 + PatternSpec（独立模块，防循环 import）
    geometry.py   几何工具（实体/影线/占比，统一 .001+ 防除零门）
    base.py       detect_all 向量化检测 + 注册表（优先级序）+ load_all 自检
    single.py     5 个单根形态
    double.py     9 个双根形态
    triple.py     5 个三根反转（对拍）+ 2 个持续形态（自建）

口径红线：
- 19 个形态判定式与 ``SpiralDevelopment__candlestick-patterns`` 克隆逐字对应
  （对拍见 test_patterns_parity.py；该克隆 3 个缺陷形态跳过：bullish_hanging_man
  缺文件、evening_star 系未暴露——缺陷登记在案）；
- 红三兵/三只乌鸦为**自建**（源项目无），无对拍基线，README 显式标注；
- 裸分母 vs ``.001+`` 分母的形态间差异**刻意保留**（复刻源项目，禁统一）；
- 一字板（high==low）全部形态不命中（0/0=NaN → 比较 False；hammer 系
  ``(h-l)>3*(o-c)`` 亦不成立）——A 股涨跌停场景的结构性事实。
"""

from .base import PATTERN_REGISTRY, detect_all, load_all
from .spec import BEARISH, BULLISH, NEUTRAL, PatternSpec

__all__ = [
    'BEARISH', 'BULLISH', 'NEUTRAL', 'PATTERN_REGISTRY', 'PatternSpec',
    'detect_all', 'load_all',
]

#: 导入期自检（注册表与实现脱节时立即失败，防「静默少形态」）
load_all()
