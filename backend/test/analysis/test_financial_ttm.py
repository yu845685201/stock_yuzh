"""TTM 纯函数单测（P0-4 §3.3.2）：单季换算、逐期 TTM、TTM 序列向量化。

ttm_frame 行为由 test/test_valuation_calc.py 的 sync 委托路径保护；
本文件补 ttm_series（CAGR 输入）与单季换算的直接口径。
"""

import pandas as pd

from src.utils.financial_ttm import single_quarter, ttm_from_cumulative, ttm_series


def test_single_quarter():
    assert single_quarter(256.0, 145.0) == 111.0      # Q2 = H1 - Q1
    assert single_quarter(None, 145.0) is None


def test_ttm_from_cumulative_annual():
    """年报期 TTM = cum(L)。"""
    assert ttm_from_cumulative(426.0, '20251231') == 426.0


def test_ttm_from_cumulative_quarterly():
    """季报期 TTM = cum(L) + cum(A_prev) - cum(S_prev)。"""
    assert ttm_from_cumulative(256.0, '20260630', 426.0, 248.0) == 434.0


def test_ttm_from_cumulative_missing():
    assert ttm_from_cumulative(256.0, '20260630', None, 248.0) is None
    assert ttm_from_cumulative(None, '20260630', 426.0, 248.0) is None


def test_ttm_series_vectorized():
    """TTM 序列：逐期计算，与 ttm_from_cumulative 同口径。"""
    reports = [
        {'ts_code': 'sz.000001', 'stat_date': '20231231', 'net_profit': 400.0},
        {'ts_code': 'sz.000001', 'stat_date': '20240630', 'net_profit': 240.0},
        {'ts_code': 'sz.000001', 'stat_date': '20241231', 'net_profit': 426.0},
        {'ts_code': 'sz.000001', 'stat_date': '20250630', 'net_profit': 248.0},
        {'ts_code': 'sz.000001', 'stat_date': '20251231', 'net_profit': 460.0},
        {'ts_code': 'sz.000001', 'stat_date': '20260630', 'net_profit': 256.0},
    ]
    df = ttm_series(reports)
    by_date = dict(zip(df['stat_date'], df['ttm_profit']))
    assert by_date['20231231'] == 400.0                       # 年报=cum
    assert pd.isna(by_date['20240630'])                       # 缺上年年报 → NaN
    assert by_date['20241231'] == 426.0                       # 年报=cum
    assert by_date['20250630'] == 248.0 + 426.0 - 240.0       # 434
    assert by_date['20260630'] == 256.0 + 460.0 - 248.0       # 468


def test_ttm_series_missing_inputs_none():
    """缺上年同期 → 该期 ttm_profit 为 None（不硬造）。"""
    reports = [
        {'ts_code': 'sz.000001', 'stat_date': '20241231', 'net_profit': 426.0},
        {'ts_code': 'sz.000001', 'stat_date': '20250331', 'net_profit': 100.0},  # 缺 2024Q1
    ]
    df = ttm_series(reports)
    by_date = dict(zip(df['stat_date'], df['ttm_profit']))
    assert pd.isna(by_date['20250331'])                       # 缺上年同期 → NaN
    assert by_date['20241231'] == 426.0


def test_ttm_series_empty():
    out = ttm_series([])
    assert list(out.columns) == ['ts_code', 'stat_date', 'ttm_profit']
    assert out.empty
