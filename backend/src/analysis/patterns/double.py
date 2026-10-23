"""双根形态（9 个，P0-2；判定式与源项目逐字对应，向量化复刻）。

prev = 前一根 K（``po/pc/ph/pl``，geometry.as_arrays 的 shift 参照）。
``star``/``rain_drop`` 为「星」形简化判定（源项目 star.py/rain_drop.py），
方向标记 neutral——脱离三根上下文时不承诺多空。
"""

from __future__ import annotations

import numpy as np

from . import geometry as g
from .spec import BULLISH, BEARISH, NEUTRAL, PatternSpec


def _bullish_engulfing(a):
    return ((a['c'] >= a['po']) & (a['po'] > a['pc']) &
            (a['c'] > a['o']) & (a['pc'] >= a['o']) &
            ((a['c'] - a['o']) > (a['po'] - a['pc'])))


def _bearish_engulfing(a):
    return ((a['o'] >= a['pc']) & (a['pc'] > a['po']) &
            (a['o'] > a['c']) & (a['po'] >= a['c']) &
            ((a['o'] - a['c']) > (a['pc'] - a['po'])))


def _bullish_harami(a):
    return ((a['po'] > a['pc']) &
            (a['pc'] <= a['o']) & (a['o'] < a['c']) & (a['c'] <= a['po']) &
            ((a['c'] - a['o']) < (a['po'] - a['pc'])))


def _bearish_harami(a):
    return ((a['pc'] > a['po']) &
            (a['po'] <= a['c']) & (a['c'] < a['o']) & (a['o'] <= a['pc']) &
            ((a['o'] - a['c']) < (a['pc'] - a['po'])))


def _star(a):
    """双根十字星（源项目 star.py：前阳 + 今小实体 + 收盘/开盘高于前收）。

    命名说明：与单根 ``十字星``（Doji）区分——本形态是晨/昏星的中继星形，
    两根 K 线语境，故中文名「双根十字星」。
    """
    prev_body = np.abs(a['pc'] - a['po'])
    prev_rng = a['ph'] - a['pl']
    cur_body = g.body(a)
    cur_rng = g.rng(a)
    return ((a['pc'] > a['po']) &
            (g.raw_ratio(prev_body, prev_rng) >= 0.7) &
            (g.raw_ratio(cur_body, cur_rng) < 0.3) &
            (g.raw_ratio(cur_body, cur_rng) >= 0.1) &
            (a['pc'] < a['c']) & (a['pc'] < a['o']))


def _rain_drop(a):
    prev_body = np.abs(a['pc'] - a['po'])
    prev_rng = a['ph'] - a['pl']
    cur_body = g.body(a)
    cur_rng = g.rng(a)
    return ((a['pc'] < a['po']) &
            (g.raw_ratio(prev_body, prev_rng) >= 0.7) &
            (g.raw_ratio(cur_body, cur_rng) < 0.3) &
            (g.raw_ratio(cur_body, cur_rng) >= 0.1) &
            (a['pc'] > a['c']) & (a['pc'] > a['o']))


def _shooting_star(a):
    body = g.body(a)
    return ((a['po'] < a['pc']) & (a['pc'] < a['o']) &
            ((a['h'] - np.maximum(a['o'], a['c'])) >= body * 3) &
            ((np.minimum(a['c'], a['o']) - a['l']) <= body))


def _dark_cloud_cover(a):
    return ((a['pc'] > a['po']) &
            (((a['pc'] + a['po']) / 2) > a['c']) &
            (a['o'] > a['c']) & (a['o'] > a['pc']) & (a['c'] > a['po']) &
            (g.ratio(a['o'] - a['c'], g.rng(a)) > 0.6))


def _piercing_pattern(a):
    return ((a['pc'] < a['po']) & (a['o'] < a['pl']) &
            (a['po'] > a['c']) &
            (a['c'] > a['pc'] + ((a['po'] - a['pc']) / 2)))


BULLISH_ENGOLFING = PatternSpec('bullish_engulfing', '看涨吞没', 2, BULLISH,
                                'double', 'candlestick-patterns', _bullish_engulfing)
BEARISH_ENGOLFING = PatternSpec('bearish_engulfing', '看跌吞没', 2, BEARISH,
                                'double', 'candlestick-patterns', _bearish_engulfing)
BULLISH_HARAMI = PatternSpec('bullish_harami', '看涨孕线', 2, BULLISH, 'double',
                             'candlestick-patterns', _bullish_harami)
BEARISH_HARAMI = PatternSpec('bearish_harami', '看跌孕线', 2, BEARISH, 'double',
                             'candlestick-patterns', _bearish_harami)
STAR = PatternSpec('star', '双根十字星', 2, NEUTRAL, 'double',
                   'candlestick-patterns', _star)
RAIN_DROP = PatternSpec('rain_drop', '雨滴', 2, NEUTRAL, 'double',
                        'candlestick-patterns', _rain_drop)
SHOOTING_STAR = PatternSpec('shooting_star', '射击之星', 2, BEARISH, 'double',
                            'candlestick-patterns', _shooting_star)
DARK_CLOUD_COVER = PatternSpec('dark_cloud_cover', '乌云盖顶', 2, BEARISH, 'double',
                               'candlestick-patterns', _dark_cloud_cover)
PIERCING_PATTERN = PatternSpec('piercing_pattern', '穿刺', 2, BULLISH, 'double',
                               'candlestick-patterns', _piercing_pattern)
