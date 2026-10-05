"""PE/PB 自算测试（方案 4.7：TTM 累计口径换算、PB、insufficient_note；开发项 A：as-of 回填）"""

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


# ---------- 开发项 A：as-of 历史回填 ----------

class FakeValDB:
    """估值回填的 DB 桩：日历/快照/as-of 取数/行数/upsert 全记录"""

    def __init__(self, calendar, snapshots, reports, shares, counts=None):
        self.calendar = calendar
        self.snapshots = snapshots
        self.reports = reports
        self.shares = shares
        self.counts = counts or {}
        self.asof_calls = []
        self.shares_calls = []
        self.upserted = []
        self.upsert_dates = []

    def fetch_trade_calendar(self, start, end):
        return [{'calendar_date': d} for d in self.calendar if start <= d <= end]

    def fetch_day_kline_snapshot(self, t):
        return self.snapshots.get(t, [])

    def fetch_financial_reports_asof(self, t, periods=8):
        self.asof_calls.append(t)
        return self.reports

    def fetch_total_shares_asof(self, t):
        self.shares_calls.append(t)
        return self.shares

    def fetch_valuation_row_count(self, t):
        return self.counts.get(t, 0)

    def upsert_stock_valuations(self, rows):
        self.upserted.extend(rows)
        self.upsert_dates.extend(sorted({r['trade_date'] for r in rows}))


def _snap(t, code, close=10.0, share=100.0):
    return {'ts_code': code, 'trade_date': t, 'stock_code': code.split('.')[1],
            'close': close, 'total_share': share, 'amount': 1e6}


REPORTS = [{'ts_code': 'sz.000001', 'stat_date': '20251231',
            'net_profit': 100.0, 'net_assets': 250.0}]


def _patch_report(monkeypatch):
    monkeypatch.setattr('src.sync.valuation_calc_manager.write_review_report',
                        lambda *a, **k: None)


def test_execute_single_day_uses_asof_fetch(monkeypatch):
    """单日模式统一走 as-of 取数（当日口径等价），正常计算入库"""
    _patch_report(monkeypatch)
    db = FakeValDB(
        calendar=['2026-09-30'],
        snapshots={'20260930': [_snap('20260930', 'sz.000001', close=10.0, share=100.0)]},
        reports=REPORTS, shares={})
    mgr = make_manager()
    mgr.db = db
    result = mgr.execute(trade_date='20260930')
    assert result['success'] is True
    assert db.asof_calls == ['20260930']
    assert db.shares_calls == ['20260930']
    assert len(db.upserted) == 1
    assert db.upserted[0]['total_mv'] == pytest.approx(1000.0)
    assert db.upserted[0]['pe_ttm'] == pytest.approx(10.0)


def test_backfill_skips_completed_and_no_kline_days(monkeypatch):
    """断点续跑：行数 >= 当日K线×0.8 跳过；无K线日计数跳过；只对缺失日取数计算"""
    _patch_report(monkeypatch)
    db = FakeValDB(
        calendar=['2026-01-05', '2026-01-06', '2026-01-07'],
        snapshots={
            # 0105：无K线（未同步/停牌日） -> days_no_kline
            # 0106：已有 9 行 >= 10×0.8 -> 断点跳过
            '20260106': [_snap('20260106', 'sz.000001')] * 10,
            '20260107': [_snap('20260107', 'sz.000001')],
        },
        reports=REPORTS, shares={}, counts={'20260106': 9})
    mgr = make_manager()
    mgr.db = db
    result = mgr.execute(start_date='20260105', end_date='20260107')
    stats = result['stats']
    assert result['success'] is True
    assert stats['days_total'] == 3
    assert stats['days_no_kline'] == 1
    assert stats['days_skipped_done'] == 1
    assert stats['days_done'] == 1
    assert db.asof_calls == ['20260107']        # 只对缺失日取数
    assert db.upsert_dates == ['20260107']
    assert stats['last_completed_date'] == '20260107'


def test_backfill_dry_run_no_write(monkeypatch):
    """dry-run：不查行数、不写库，统计照常"""
    _patch_report(monkeypatch)
    db = FakeValDB(
        calendar=['2026-01-07'],
        snapshots={'20260107': [_snap('20260107', 'sz.000001')]},
        reports=REPORTS, shares={}, counts={})
    mgr = make_manager()
    mgr.db = db
    result = mgr.execute(start_date='20260107', end_date='20260107', dry_run=True)
    assert result['success'] is True
    assert db.upserted == []
    assert db.asof_calls == ['20260107']
    assert result['stats']['days_done'] == 1


def test_backfill_requires_both_dates(monkeypatch):
    """只给一端：直接拒绝（防误将回填当单日跑）"""
    _patch_report(monkeypatch)
    mgr = make_manager()
    with pytest.raises(ValueError):
        mgr.execute(start_date='20260101')
    with pytest.raises(ValueError):
        mgr.execute(end_date='20260101')
