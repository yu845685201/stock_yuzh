"""三根形态（5 个对拍 + 2 个自建，P0-2）。

对拍 5 个（判定式与源项目逐字对应）：启明星/黄昏星/启明星十字/黄昏星十字/吊人线。
自建 2 个（源项目无，标 ``self-built``，无对拍基线）：
- **红三兵**：连续三根阳线，每根实体占比 ≥0.6，收盘递进（c[-1]>c[-2]>c[-3]）；
- **三只乌鸦**：镜像（连续三根阴线，实体占比 ≥0.6，收盘递降）。

吊人线是源项目唯一显式引入趋势上下文的形态（前两日最高价均低于今日开盘），
本库保留该语义。
"""

from __future__ import annotations

import numpy as np

from . import geometry as g
from .spec import BULLISH, BEARISH, PatternSpec


def _morning_star(a):
    return ((np.maximum(a['po'], a['pc']) < a['bpc']) &
            (a['bpc'] < a['bpo']) &
            (a['c'] > a['o']) &
            (a['o'] > np.maximum(a['po'], a['pc'])))


def _evening_star(a):
    return ((np.minimum(a['po'], a['pc']) > a['bpc']) &
            (a['bpc'] > a['bpo']) &
            (a['c'] < a['o']) &
            (a['o'] < np.minimum(a['po'], a['pc'])))


def _morning_star_doji(a):
    bp_body = np.abs(a['bpc'] - a['bpo'])
    bp_rng = a['bph'] - a['bpl']
    p_body = g.body({'o': a['po'], 'c': a['pc']})
    p_rng = a['ph'] - a['pl']
    cur_body = g.body(a)
    cur_rng = g.rng(a)
    return ((a['bpc'] < a['bpo']) &
            (g.raw_ratio(bp_body, bp_rng) >= 0.7) &
            (g.raw_ratio(p_body, p_rng) < 0.1) &
            (a['c'] > a['o']) &
            (g.raw_ratio(cur_body, cur_rng) >= 0.7) &
            (a['bpc'] > a['pc']) & (a['bpc'] > a['po']) &
            (a['pc'] < a['o']) & (a['po'] < a['o']) &
            (a['c'] > a['bpc']) &
            ((a['ph'] - np.maximum(a['pc'], a['po'])) > 3 * p_body) &
            ((np.minimum(a['pc'], a['po']) - a['pl']) > 3 * p_body))


def _evening_star_doji(a):
    bp_body = np.abs(a['bpc'] - a['bpo'])
    bp_rng = a['bph'] - a['bpl']
    p_body = g.body({'o': a['po'], 'c': a['pc']})
    p_rng = a['ph'] - a['pl']
    cur_body = g.body(a)
    cur_rng = g.rng(a)
    return ((a['bpc'] > a['bpo']) &
            (g.raw_ratio(bp_body, bp_rng) >= 0.7) &
            (g.raw_ratio(p_body, p_rng) < 0.1) &
            (a['c'] < a['o']) &
            (g.raw_ratio(cur_body, cur_rng) >= 0.7) &
            (a['bpc'] < a['pc']) & (a['bpc'] < a['po']) &
            (a['pc'] > a['o']) & (a['po'] > a['o']) &
            (a['c'] < a['bpc']) &
            ((a['ph'] - np.maximum(a['pc'], a['po'])) > 3 * p_body) &
            ((np.minimum(a['pc'], a['po']) - a['pl']) > 3 * p_body))


def _hanging_man(a):
    return (((g.rng(a) > 4 * (a['o'] - a['c'])) &
             (g.ratio(a['c'] - a['l'], g.rng(a)) >= 0.75) &
             (g.ratio(a['o'] - a['l'], g.rng(a)) >= 0.75)) &
            (a['ph'] < a['o']) & (a['bph'] < a['o']))


def _three_white_soldiers(a):
    """红三兵（自建）：三连阳（每根 c>o 且实体占比 ≥0.6）+ 收盘递进。

    条件对三根全部成立：``up/solid`` 为逐根数组，取 i、i-1、i-2 三根；
    收盘递进 = c[i] > c[i-1] > c[i-2]。
    """
    body = g.body(a)
    up = a['c'] > a['o']
    solid = g.ratio(body, g.rng(a)) >= 0.6
    c = a['c']
    return (up & solid &
            np.roll(up, 1) & np.roll(solid, 1) &
            np.roll(up, 2) & np.roll(solid, 2) &
            (c > np.roll(c, 1)) & (np.roll(c, 1) > np.roll(c, 2)))


def _three_black_crows(a):
    """三只乌鸦（自建）：三连阴（每根 c<o 且实体占比 ≥0.6）+ 收盘递降。"""
    body = g.body(a)
    down = a['c'] < a['o']
    solid = g.ratio(body, g.rng(a)) >= 0.6
    c = a['c']
    return (down & solid &
            np.roll(down, 1) & np.roll(solid, 1) &
            np.roll(down, 2) & np.roll(solid, 2) &
            (c < np.roll(c, 1)) & (np.roll(c, 1) < np.roll(c, 2)))


MORNING_STAR = PatternSpec('morning_star', '启明星', 3, BULLISH, 'triple',
                           'candlestick-patterns', _morning_star)
EVENING_STAR = PatternSpec('evening_star', '黄昏星', 3, BEARISH, 'triple',
                           'candlestick-patterns', _evening_star)
MORNING_STAR_DOJI = PatternSpec('morning_star_doji', '启明星十字', 3, BULLISH,
                                'triple', 'candlestick-patterns', _morning_star_doji)
EVENING_STAR_DOJI = PatternSpec('evening_star_doji', '黄昏星十字', 3, BEARISH,
                                'triple', 'candlestick-patterns', _evening_star_doji)
HANGING_MAN = PatternSpec('hanging_man', '吊人线', 3, BEARISH, 'triple',
                          'candlestick-patterns', _hanging_man)
THREE_WHITE_SOLDIERS = PatternSpec('three_white_soldiers', '红三兵', 3, BULLISH,
                                   'continuation', 'self-built',
                                   _three_white_soldiers)
THREE_BLACK_CROWS = PatternSpec('three_black_crows', '三只乌鸦', 3, BEARISH,
                                'continuation', 'self-built', _three_black_crows)
