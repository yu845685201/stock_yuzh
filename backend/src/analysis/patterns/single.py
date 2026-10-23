"""单根形态（5 个，P0-2；判定式与源项目逐字对应，向量化复刻）。

口径注意（对拍红线）：doji/dragonfly/gravestone 的实体占比用**裸分母**
``(high-low)``（源项目 doji.py 等，一字板 0/0=NaN → 比较 False 天然不命中）；
hammer/inverted_hammer 用 **``.001+`` 分母**（源项目 hammer.py:17）。两者不可
「顺手统一」——统一即偏离基线。
"""

from __future__ import annotations

import numpy as np

from . import geometry as g
from .spec import BULLISH, BEARISH, NEUTRAL, PatternSpec


def _hammer(a):
    return ((g.rng(a) > 3 * (a['o'] - a['c'])) &
            (g.ratio(a['c'] - a['l'], g.rng(a)) > 0.6) &
            (g.ratio(a['o'] - a['l'], g.rng(a)) > 0.6))


def _inverted_hammer(a):
    return ((g.rng(a) > 3 * (a['o'] - a['c'])) &
            (g.ratio(a['h'] - a['c'], g.rng(a)) > 0.6) &
            (g.ratio(a['h'] - a['o'], g.rng(a)) > 0.6))


def _doji(a):
    body = g.body(a)
    return ((g.raw_ratio(body, g.rng(a)) < 0.1) &
            (g.upper_shadow(a) > 3 * body) &
            (g.lower_shadow(a) > 3 * body))


def _dragonfly_doji(a):
    body = g.body(a)
    return ((g.raw_ratio(body, g.rng(a)) < 0.1) &
            (g.lower_shadow(a) > 3 * body) &
            (g.upper_shadow(a) < body))


def _gravestone_doji(a):
    body = g.body(a)
    return ((g.raw_ratio(body, g.rng(a)) < 0.1) &
            (g.upper_shadow(a) > 3 * body) &
            (g.lower_shadow(a) <= body))


HAMMER = PatternSpec('hammer', '锤子线', 1, BULLISH, 'single',
                     'candlestick-patterns', _hammer)
INVERTED_HAMMER = PatternSpec('inverted_hammer', '倒锤子线', 1, BULLISH, 'single',
                              'candlestick-patterns', _inverted_hammer)
DOJI = PatternSpec('doji', '十字星', 1, NEUTRAL, 'single',
                   'candlestick-patterns', _doji)
DRAGONFLY_DOJI = PatternSpec('dragonfly_doji', '蜻蜓十字', 1, BULLISH, 'single',
                             'candlestick-patterns', _dragonfly_doji)
GRAVESTONE_DOJI = PatternSpec('gravestone_doji', '墓碑十字', 1, BEARISH, 'single',
                              'candlestick-patterns', _gravestone_doji)
