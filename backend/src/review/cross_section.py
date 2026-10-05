"""向量化全市场截面判定（复盘报告层方案 §5.2，M1 报告层核心）。

职责：对 ``DataReader.load_kline`` 加载的全市场 60d 窗口面板（多股 × 多日）做
向量化截面判定，输出锚定日（与前一交易日）每股的：
涨停 / 跌停 / 炸板 / 连板数 / 昨日涨停 / 溢价 / 大涨大跌 / 放量倍数 / 新高新低。

**口径红线：与链模块逐股判定逐字节同口径**（§5.7 对拍用例保证）——
涨停/跌停阈值法与护栏复用 ``analysis.limit_up_rules``（同一实现）；
连板数 = 连续 ``up ∧ known`` 日数（含锚定日，cap 同链 max_lookback=60）；
炸板 = 触板 ∧ 未封住（preclose>0 且有 high 才可判）；
昨日涨停 = 同股前一行（前一可得交易日）涨停（停牌缺行天然由 shift 跳过）；
大涨大跌 = ±5% 阈值 + 锚定日有成交；
放量倍数 = 当日量 ÷ 前 N 日均量（基数不含当日、剔除无量行、须满 N 天）；
新高新低 = 收盘严格突破前 N 日收盘区间（不含当日、须满 N 天、当日有成交）。

全部 groupby/向量化实现，禁止逐行 Python 循环（module_base 规范 3）。
"""

from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from ..analysis.limit_up_rules import (
    THRESHOLD_OFFSET,
    annotate_limit_down,
    annotate_limit_up,
    board_of,
)

#: 连板数回看上限（与链 daily_review.yaml 的 consecutive_limit_up.params.max_lookback 一致）
STREAK_LOOKBACK = 60
#: 放量倍数基数窗口（链 volume_surge 默认）
VOLUME_SURGE_WINDOW = 5
#: 新高新低窗口（链 new_high_low 默认/链 YAML 显式 60）
NEW_HIGH_LOW_WINDOW = 60
#: 大涨大跌阈值（链 big_mover 默认）
BIG_MOVE_UP_PCT = 5.0
BIG_MOVE_DOWN_PCT = -5.0


def annotate_panel(df: pd.DataFrame) -> pd.DataFrame:
    """给全市场日K面板追加判定列（涨停/跌停/炸板/连板/昨日涨停/溢价）。

    追加列（前缀 _）：``_board`` ``_limit_pct`` ``_limit_up`` ``_limit_known``
    ``_limit_down`` ``_limit_down_known`` ``_burst`` ``_streak``
    ``_prev_limit_up`` ``_premium``。不修改入参。
    """
    out = df.copy()
    out['_board'] = out['stock_code'].astype(str).map(board_of)

    # 涨停/跌停为逐行判定（阈值法 + 收盘护栏），按板块分组复用链同源实现
    parts = []
    for board, g in out.groupby('_board', sort=False):
        g = annotate_limit_up(g, stock_code='', board=board)
        g = annotate_limit_down(g, stock_code='', board=board)
        parts.append(g)
    out = pd.concat(parts) if parts else out

    # 炸板 = 触板 ∧ 未封住（preclose>0 且 high 可得才可判；封住的不算炸板）
    high = pd.to_numeric(out['high'], errors='coerce')
    preclose = pd.to_numeric(out['preclose'], errors='coerce')
    with np.errstate(divide='ignore', invalid='ignore'):
        high_gain = (high - preclose) / preclose * 100.0
    threshold = out['_limit_pct'] - THRESHOLD_OFFSET
    touched = high_gain.ge(threshold) & preclose.gt(0) & high.notna()
    out['_burst'] = (touched & ~out['_limit_up'] & out['_limit_known']).fillna(False).astype(bool)

    out = out.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
    out = _add_streak(out)
    out = _add_prev_limit_up(out)
    out = _add_rolling_metrics(out)
    return out


def _add_rolling_metrics(out: pd.DataFrame,
                         new_high_low_window: int = NEW_HIGH_LOW_WINDOW,
                         volume_surge_window: int = VOLUME_SURGE_WINDOW) -> pd.DataFrame:
    """逐股滚动窗口指标（须在全面板上计算后再按日切片）：

    - ``_volume_surge`` 放量倍数（volume_surge 同口径：基数不含当日、剔除无量行、须满 N 天）；
    - ``_new_high`` / ``_new_low`` 新高新低（new_high_low 同口径：前 N 日收盘区间，
      严格突破，须满 N 天；当日有成交的前提在截面切片处追加）。
    """
    volume = pd.to_numeric(out['volume'], errors='coerce')
    valid_vol = volume.where(volume.gt(0))
    prev_mean = valid_vol.groupby(out['ts_code']).transform(
        lambda s: s.shift(1).rolling(volume_surge_window, min_periods=volume_surge_window).mean())
    out['_volume_surge'] = (volume / prev_mean).round(4)

    close = pd.to_numeric(out['close'], errors='coerce')
    prev_max = close.groupby(out['ts_code']).transform(
        lambda s: s.shift(1).rolling(new_high_low_window, min_periods=new_high_low_window).max())
    prev_min = close.groupby(out['ts_code']).transform(
        lambda s: s.shift(1).rolling(new_high_low_window, min_periods=new_high_low_window).min())
    out['_new_high'] = close.gt(prev_max).fillna(False).astype(bool)
    out['_new_low'] = close.lt(prev_min).fillna(False).astype(bool)
    return out


def _add_streak(out: pd.DataFrame, lookback: int = STREAK_LOOKBACK) -> pd.DataFrame:
    """连板数：连续 ``up ∧ known`` 日数（含当日）；未涨停/不可判定为 0。

    与链 consecutive_limit_up 同口径：已知不可判定行打断连板；回看窗口内全涨停
    时按窗口长度封顶（链用 max_lookback，此处同值）。
    分组键 = 每股「严格在前的非涨停行数」——连续涨停行共享同键，非涨停行自身
    计入 cumsum 会使其后首根涨停行与它同组（会误 +1）。
    """
    up_known = (out['_limit_up'] & out['_limit_known']).astype(bool)
    non_up = (~up_known).astype(int)
    breakout = non_up.groupby(out['ts_code']).cumsum() - non_up
    streak = up_known.groupby([out['ts_code'], breakout]).cumcount() + 1
    out['_streak'] = streak.where(up_known, 0).astype('int64').clip(upper=lookback)
    return out


def _add_prev_limit_up(out: pd.DataFrame) -> pd.DataFrame:
    """昨日涨停（同股前一行 = 前一可得交易日，链 limit_up_premium 同口径）
    与今日溢价（前日涨停行的当日 change_rate）。"""
    prev_up = out.groupby('ts_code')['_limit_up'].shift(1)
    prev_known = out.groupby('ts_code')['_limit_known'].shift(1)
    prev_up = (prev_up.fillna(False) & prev_known.fillna(False)).astype(bool)
    out['_prev_limit_up'] = prev_up
    out['_premium'] = pd.to_numeric(out['change_rate'], errors='coerce').where(prev_up)
    return out


def suppress_first_day(panel: pd.DataFrame, list_dates: Dict[str, Optional[str]]) -> pd.DataFrame:
    """新股上市首日不判定（与链模块「上市首日不判定」口径对齐，§5.7 对拍前提）。

    - 沪深：按 universe 的 ``list_date`` 匹配首日行（链 ctx.stock.list_date 同源）；
    - 北交所：cod 无上市日（list_date=NULL，§2.5 已声明退化），以名称 ``N`` 前缀
      （A股新股命名惯例）兜底识别首日。

    首日行的涨停/跌停/炸板/连板/昨日涨停判定清零（无涨跌幅限制期间的触板回落
    属正常波动，非炸板）。返回新面板。
    """
    if panel.empty or not list_dates:
        return panel
    mask = panel['ts_code'].map(list_dates).notna() \
        & (panel['trade_date'] == panel['ts_code'].map(list_dates))
    mask = mask | panel['stock_name'].astype(str).str.startswith('N')
    if not mask.any():
        return panel
    out = panel.copy()
    for col in ('_limit_up', '_limit_down', '_burst', '_prev_limit_up'):
        if col in out.columns:
            out.loc[mask, col] = False
    if '_streak' in out.columns:
        out.loc[mask, '_streak'] = 0
    return out


def cross_section_at(panel: pd.DataFrame, trade_date: str,
                     new_high_low_window: int = NEW_HIGH_LOW_WINDOW,
                     volume_surge_window: int = VOLUME_SURGE_WINDOW,
                     up_pct: float = BIG_MOVE_UP_PCT,
                     down_pct: float = BIG_MOVE_DOWN_PCT) -> pd.DataFrame:
    """取指定交易日的每股截面（该日无行的股票即停牌/缺席，天然不出现）。

    追加截面列：``_big_move``（大涨/大跌/否/数据不足）``_traded``。
    返回截面视图（panel 的行切片 + 追加列）。
    """
    out = panel[panel['trade_date'] == str(trade_date)].copy()
    if out.empty:
        return out

    # ---- 当日有成交标记（停牌标记行 volume=0；链各模块以此判 数据不足）----
    volume = pd.to_numeric(out['volume'], errors='coerce')
    change = pd.to_numeric(out['change_rate'], errors='coerce')
    out['_traded'] = volume.gt(0).fillna(False)

    # ---- 大涨大跌（big_mover 同口径：阈值标记；无成交/缺涨跌幅 → 数据不足）----
    big = np.select(
        [out['_traded'] & change.ge(up_pct).fillna(False),
         out['_traded'] & change.le(down_pct).fillna(False)],
        ['大涨', '大跌'], default='否')
    big = np.where(~out['_traded'] | change.isna(), '数据不足', big)
    out['_big_move'] = big

    # 新高新低加当日有成交前提（滚动区间已在 annotate_panel 算好）
    out['_new_high'] = (out['_new_high'] & out['_traded']).astype(bool)
    out['_new_low'] = (out['_new_low'] & out['_traded']).astype(bool)
    out['_volume_surge'] = out['_volume_surge'].where(out['_traded'])
    return out


def sentiment_metrics(cs_today: pd.DataFrame, cs_prev: pd.DataFrame) -> dict:
    """维度2 市场情绪聚合（全部由截面重算，§5.3）。

    Returns:
        {limit_up_count, limit_down_count, burst_count, burst_rate,
         ladder: {高度: 家数}, max_streak, premium_mean, premium_median,
         premium_continued, promotion: {N: 晋级率}, 总样本数}
    """
    limit_up = int(cs_today['_limit_up'].sum()) if not cs_today.empty else 0
    limit_down = int(cs_today['_limit_down'].sum()) if not cs_today.empty else 0
    burst = int(cs_today['_burst'].sum()) if not cs_today.empty else 0
    burst_rate = burst / (limit_up + burst) if (limit_up + burst) > 0 else None

    streaks = cs_today.loc[cs_today['_limit_up'], '_streak'] if not cs_today.empty else pd.Series(dtype='int64')
    ladder = streaks[streaks >= 2].value_counts().sort_index(ascending=False).to_dict()
    ladder = {int(k): int(v) for k, v in ladder.items()}
    max_streak = int(streaks.max()) if len(streaks) else 0

    premium = cs_today['_premium'].dropna() if not cs_today.empty else pd.Series(dtype='float64')
    premium_mean = round(float(premium.mean()), 2) if len(premium) else None
    premium_median = round(float(premium.median()), 2) if len(premium) else None
    # 昨日涨停股今日续涨停单独计数（§5.3）
    prev_up_today = cs_today[cs_today['_prev_limit_up']] if not cs_today.empty else pd.DataFrame()
    premium_continued = int(prev_up_today['_limit_up'].sum()) if not prev_up_today.empty else 0

    # 晋级率：今日 N 板家数 ÷ 昨日 (N-1) 板家数（N=2..空间高度）
    promotion: dict = {}
    if not cs_today.empty and not cs_prev.empty:
        prev_streaks = cs_prev.loc[cs_prev['_limit_up'], '_streak']
        prev_counts = prev_streaks.value_counts()
        for n, today_cnt in ladder.items():
            prev_cnt = int(prev_counts.get(n - 1, 0))
            promotion[n] = {'today': int(today_cnt), 'prev': prev_cnt,
                            'rate': round(today_cnt / prev_cnt, 4) if prev_cnt else None}

    return {
        'limit_up_count': limit_up,
        'limit_down_count': limit_down,
        'burst_count': burst,
        'burst_rate': burst_rate,
        'ladder': ladder,
        'max_streak': max_streak,
        'premium_mean': premium_mean,
        'premium_median': premium_median,
        'premium_count': int(len(premium)),
        'premium_continued': premium_continued,
        'promotion': promotion,
    }
