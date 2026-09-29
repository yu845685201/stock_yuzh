"""PE/PB 自算测试（方案 4.7：TTM 累计口径换算、PB、insufficient_note）"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.sync.valuation_calc_manager import ValuationCalcManager


def make_manager():
    return ValuationCalcManager.__new__(ValuationCalcManager)


def test_ttm_quarterly():
    """TTM = 最新累计 + 上年年报 − 上年同期"""
    mgr = make_manager()
    reports = [
        {'ts_code': 'sz.000001', 'stat_date': '20260630', 'net_profit': 256.0, 'net_assets': 4500.0},
        {'ts_code': 'sz.000001', 'stat_date': '20260331', 'net_profit': 145.0, 'net_assets': 4400.0},
        {'ts_code': 'sz.000001', 'stat_date': '20251231', 'net_profit': 426.0, 'net_assets': 4300.0},
        {'ts_code': 'sz.000001', 'stat_date': '20250630', 'net_profit': 248.0, 'net_assets': 4200.0},
    ]
    df = mgr._ttm_frame(reports)
    assert len(df) == 1
    # TTM = 256 + 426 - 248 = 434
    assert df.iloc[0]['ttm_profit'] == pytest.approx(434.0)
    assert df.iloc[0]['latest_net_assets'] == pytest.approx(4500.0)
    assert df.iloc[0]['latest_stat_date'] == '20260630'


def test_ttm_latest_is_annual():
    """最新期即年报：TTM = 该年报累计"""
    mgr = make_manager()
    reports = [
        {'ts_code': 'sh.600000', 'stat_date': '20251231', 'net_profit': 500.0, 'net_assets': 9000.0},
        {'ts_code': 'sh.600000', 'stat_date': '20250930', 'net_profit': 380.0, 'net_assets': 8800.0},
    ]
    df = mgr._ttm_frame(reports)
    assert df.iloc[0]['ttm_profit'] == pytest.approx(500.0)


def test_ttm_missing_prev_same_period():
    """缺上年同期 -> TTM 不可算（不硬造：None/NaN 均视为不可算）"""
    mgr = make_manager()
    reports = [
        {'ts_code': 'sh.600000', 'stat_date': '20260331', 'net_profit': 100.0, 'net_assets': 500.0},
        {'ts_code': 'sh.600000', 'stat_date': '20251231', 'net_profit': 400.0, 'net_assets': 480.0},
    ]
    df = mgr._ttm_frame(reports)
    val = df.iloc[0]['ttm_profit']
    assert val is None or val != val


def test_compute_full():
    """市值/PE/PB 全链路 + 输入不足标注"""
    mgr = make_manager()
    snapshot = [
        # 有股本有财报
        {'ts_code': 'sz.000001', 'trade_date': '20260925', 'stock_code': '000001',
         'close': 10.0, 'total_share': 100.0, 'amount': 1e6},
        # 缺快照股本，回退 fallback_shares
        {'ts_code': 'sh.600000', 'trade_date': '20260925', 'stock_code': '600000',
         'close': 20.0, 'total_share': None, 'amount': 1e6},
        # 无财报：PE/PB 留空 + note
        {'ts_code': 'sz.300001', 'trade_date': '20260925', 'stock_code': '300001',
         'close': 5.0, 'total_share': 1000.0, 'amount': 1e6},
    ]
    reports = [
        {'ts_code': 'sz.000001', 'stat_date': '20251231', 'net_profit': 100.0, 'net_assets': 250.0},
        {'ts_code': 'sh.600000', 'stat_date': '20251231', 'net_profit': 200.0, 'net_assets': 400.0},
    ]
    fallback = {'sh.600000': 50.0}
    rows = mgr.compute(snapshot, reports, fallback)
    by_code = {r['ts_code']: r for r in rows}

    assert by_code['sz.000001']['total_mv'] == pytest.approx(1000.0)     # 10 × 100
    assert by_code['sz.000001']['pe_ttm'] == pytest.approx(10.0)          # 1000 / 100
    assert by_code['sz.000001']['pb'] == pytest.approx(4.0)               # 1000 / 250

    assert by_code['sh.600000']['total_mv'] == pytest.approx(1000.0)     # 20 × 50（回退股本）
    assert by_code['sh.600000']['pe_ttm'] == pytest.approx(5.0)

    assert by_code['sz.300001']['total_mv'] == pytest.approx(5000.0)
    assert by_code['sz.300001']['pe_ttm'] is None
    assert by_code['sz.300001']['pb'] is None
    assert '缺财报TTM' in by_code['sz.300001']['insufficient_note']
