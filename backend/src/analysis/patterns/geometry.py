"""形态几何工具（P0-2，方案 §3.2）。

统一收口「影线/实体占比」计算，从机制上消除一字板除零——`ratio` 在分母加
``EPS=0.001``（源项目 hammer.py:17 的 ``.001+`` 惯例；0.001 元远小于 A 股最小
tick 0.01 元，不影响判定）。**注意口径差异**：源项目 doji/star/rain_drop/
morning_star_doji 等用裸 ``(high-low)`` 分母（一字板时 0/0=NaN，比较为 False，
天然不命中），hammer/inverted_hammer/dark_cloud_cover/hanging_man 用 ``.001+``
分母——本库逐形态复刻该差异（对拍红线，禁「顺手统一」）。
"""

from __future__ import annotations

import numpy as np

#: 防除零保护（源项目 .001+ 惯例）
EPS = 1e-3


def as_arrays(df) -> dict:
    """OHLC DataFrame → numpy 数组字典（含前两根 K 的 shift 参照）。

    ``po/pc/ph/pl`` = 前一根，``bpo/bpc/bph/bpl`` = 前两根。首行/次行用
    ``np.roll`` 环绕值——由 ``detect_all`` 按 ``required_count`` 屏蔽，
    锚定日行（最后一行）不受影响。
    """
    def col(name):
        return np.asarray(df[name], dtype=float)

    o, c, h, l = (col('open'), col('close'), col('high'), col('low'))
    return {
        'o': o, 'c': c, 'h': h, 'l': l,
        'po': np.roll(o, 1), 'pc': np.roll(c, 1),
        'ph': np.roll(h, 1), 'pl': np.roll(l, 1),
        'bpo': np.roll(o, 2), 'bpc': np.roll(c, 2),
        'bph': np.roll(h, 2), 'bpl': np.roll(l, 2),
    }


def body(a) -> np.ndarray:
    """实体幅度 |close-open|。"""
    return np.abs(a['c'] - a['o'])


def upper_shadow(a) -> np.ndarray:
    """上影 = high - max(open, close)。"""
    return a['h'] - np.maximum(a['o'], a['c'])


def lower_shadow(a) -> np.ndarray:
    """下影 = min(open, close) - low。"""
    return np.minimum(a['o'], a['c']) - a['l']


def rng(a) -> np.ndarray:
    """全幅 high-low。"""
    return a['h'] - a['l']


def ratio(numer: np.ndarray, denom: np.ndarray) -> np.ndarray:
    """带 EPS 保护的占比（hammer 系口径）。"""
    with np.errstate(divide='ignore', invalid='ignore'):
        return numer / (denom + EPS)


def raw_ratio(numer: np.ndarray, denom: np.ndarray) -> np.ndarray:
    """裸占比（doji 系口径，复刻源项目：一字板 0/0=NaN → 比较为 False）。"""
    with np.errstate(divide='ignore', invalid='ignore'):
        return numer / denom
