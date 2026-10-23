"""财务 TTM 纯函数（P0-4，方案 §3.3.2 抽公共）。

与 ``sync/valuation_calc_manager._ttm_frame`` 同口径——该处已改为 import 委托
（一行），本模块是唯一实现（原则 P6：同一逻辑两个实现必漂移）。

口径（累计值 → TTM，A 股财报为累计口径）::

    TTM = cum(L) + cum(A_prev) − cum(S_prev)

- L 为年报（1231）时 TTM = cum(L)；
- L 为季报（0331/0630/0930）时，A_prev = 上年年报、S_prev = 上年同期累计；
- 任一输入缺失/为 NaN → 返回 None（调用方按「数据不足」处理，禁硬造）。

单季换算（扣非年化等场景）::

    single(Q) = cum(Q) − cum(Q_prev_same)
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

import pandas as pd


def _num(x: Any) -> Optional[float]:
    """数值化；NaN/None/不可解析 → None。"""
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def single_quarter(cum_current: Any, cum_prev_same: Any) -> Optional[float]:
    """累计值 → 单季值：``cum(Q) − cum(Q_prev_same)``；缺失返回 None。"""
    cur, prev = _num(cum_current), _num(cum_prev_same)
    if cur is None or prev is None:
        return None
    return cur - prev


def ttm_from_cumulative(latest_cum: Any, latest_stat_date: str,
                        prev_annual_cum: Any = None,
                        prev_same_cum: Any = None) -> Optional[float]:
    """累计口径 → TTM 净利。

    Args:
        latest_cum: 最新报告期累计净利（归母）。
        latest_stat_date: 最新报告期（'YYYYMMDD'，按月日判定 1231=年报）。
        prev_annual_cum: 上年年报累计净利（季报场景用）。
        prev_same_cum: 上年同期累计净利（季报场景用）。
    """
    latest = _num(latest_cum)
    if latest is None:
        return None
    md = str(latest_stat_date)[4:8]
    if md == '1231':
        return latest
    annual, same = _num(prev_annual_cum), _num(prev_same_cum)
    if annual is None or same is None:
        return None
    return latest + annual - same


def ttm_frame(reports: List[Dict[str, Any]]) -> pd.DataFrame:
    """每只股票最近 N 期累计净利 → (TTM 归母净利, 最新净资产, 最新报告期)。

    移植自 ``sync/valuation_calc_manager._ttm_frame``（同口径同列名，行为一致）。
    输入 reports: [{ts_code, stat_date, net_profit, net_assets}, ...]。
    """
    if not reports:
        return pd.DataFrame(columns=['ts_code', 'ttm_profit', 'latest_net_assets',
                                     'latest_stat_date'])
    df = pd.DataFrame(reports)
    df['stat_date'] = df['stat_date'].astype(str)
    df['net_profit'] = pd.to_numeric(df['net_profit'], errors='coerce')
    df['net_assets'] = pd.to_numeric(df.get('net_assets'), errors='coerce')
    df = df.sort_values(['ts_code', 'stat_date'], ascending=[True, False])

    ttm_rows = []
    for ts_code, g in df.groupby('ts_code'):
        rows = g.reset_index(drop=True)
        if rows.empty or pd.isna(rows.loc[0, 'net_profit']):
            continue
        latest = rows.loc[0]
        latest_cum = latest['net_profit']
        latest_md = latest['stat_date'][4:8]
        ttm = None
        if latest_md == '1231':
            ttm = latest_cum
        else:
            prev_annual = rows[(rows['stat_date'].str.endswith('1231'))
                               & (rows['stat_date'] < latest['stat_date'])]
            prev_year = str(int(latest['stat_date'][:4]) - 1)
            prev_same = rows[rows['stat_date'] == f'{prev_year}{latest_md}']
            if not prev_annual.empty and not prev_same.empty \
                    and not pd.isna(prev_annual.iloc[0]['net_profit']) \
                    and not pd.isna(prev_same.iloc[0]['net_profit']):
                ttm = latest_cum + prev_annual.iloc[0]['net_profit'] \
                    - prev_same.iloc[0]['net_profit']
        ttm_rows.append({
            'ts_code': ts_code,
            'ttm_profit': ttm,
            'latest_net_assets': latest['net_assets'],
            'latest_stat_date': latest['stat_date'],
        })
    return pd.DataFrame(ttm_rows)


def ttm_series(reports: List[Dict[str, Any]]) -> pd.DataFrame:
    """每个报告期的 TTM 序列（向量化；CAGR 等历史序列指标的输入）。

    对每个报告期 L 按同一口径计算 TTM（1231 → cum；否则 cum(L)+cum(A_prev)
    −cum(S_prev)），输出 ``[ts_code, stat_date, ttm_profit]``（按 ts_code、
    stat_date 升序；输入不足的期 ttm_profit 为 None，由调用方过滤）。
    """
    if not reports:
        return pd.DataFrame(columns=['ts_code', 'stat_date', 'ttm_profit'])
    df = pd.DataFrame(reports)
    df['stat_date'] = df['stat_date'].astype(str)
    df['net_profit'] = pd.to_numeric(df['net_profit'], errors='coerce')
    df = df[df['net_profit'].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=['ts_code', 'stat_date', 'ttm_profit'])
    df['year'] = df['stat_date'].str[:4]
    df['md'] = df['stat_date'].str[4:8]

    # 上年同期累计（同 ts_code、year-1、同 md）
    prev_same = df[['ts_code', 'year', 'md', 'net_profit']].copy()
    prev_same['year'] = (prev_same['year'].astype(int) + 1).astype(str)
    prev_same = prev_same.rename(columns={'net_profit': 'prev_same_cum'})
    df = df.merge(prev_same, on=['ts_code', 'year', 'md'], how='left')

    # 上年年报累计（同 ts_code、year-1、md=1231）
    annual = df[df['md'] == '1231'][['ts_code', 'year', 'net_profit']].copy()
    annual['year'] = (annual['year'].astype(int) + 1).astype(str)
    annual = annual.rename(columns={'net_profit': 'prev_annual_cum'})
    df = df.merge(annual, on=['ts_code', 'year'], how='left')

    is_annual = df['md'] == '1231'
    df['ttm_profit'] = df['net_profit']
    mask = ~is_annual & df['prev_annual_cum'].notna() & df['prev_same_cum'].notna()
    df.loc[mask, 'ttm_profit'] = (df.loc[mask, 'net_profit']
                                  + df.loc[mask, 'prev_annual_cum']
                                  - df.loc[mask, 'prev_same_cum'])
    df.loc[~is_annual & ~mask, 'ttm_profit'] = None
    return (df[['ts_code', 'stat_date', 'ttm_profit']]
            .sort_values(['ts_code', 'stat_date']).reset_index(drop=True))
