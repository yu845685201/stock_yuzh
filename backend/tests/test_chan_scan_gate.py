"""P0-7 缠论输入完整性门控单测（scan_stock_anchor expected_bars）。

覆盖四条验收用例：
①窗口删 3 天 → hit 且 reason="缺日3天"（覆盖率 99.4% ≥ 98%）；
②窗口删 15 天 → insufficient（覆盖率 97.1% < 98%）；
③expected_bars=None → 行为与旧版完全一致（兼容性）；
④520 根全 → hit 且 reason=None。

信号命中用 monkeypatch scan_buy_hits 桩替（门控在信号扫描之前/之后的行为
与信号本身无关；真实信号用例见 test_chan_divergence_buy.py 对拍）。
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.chan.scan import ChanHit, scan_stock_anchor

AS_OF = '20261023'


def _df(n_days: int = 520, drop_idx: tuple = ()) -> pd.DataFrame:
    """合成日K：日期自 AS_OF 往回排（引擎只要求末行==锚定日，不校交易日历——
    历语义由 expected_bars 承担）、价格平稳（不产生真实信号）。
    drop_idx 指定要删除的行号模拟缺日。"""
    from datetime import datetime, timedelta
    anchor = datetime.strptime(AS_OF, '%Y%m%d')
    dates = [(anchor - timedelta(days=n)).strftime('%Y%m%d')
             for n in range(n_days - 1, -1, -1)]
    keep = [i for i in range(n_days) if i not in drop_idx]
    df = pd.DataFrame({
        'trade_date': [dates[i] for i in keep],
        'high': 10.0, 'low': 9.9, 'close': 9.95, 'is_st': False,
    })
    return df


def _fake_hit() -> ChanHit:
    return ChanHit(kind='一买', fire_i=1, stop_price=9.0, trigger_low=9.5,
                   zg=10.0, zd=9.0, divergence=0.5, pivot_pid=1, refire=False)


@pytest.fixture()
def fake_signal(monkeypatch):
    """桩掉信号扫描：无论K线形态如何都返回一条一买（隔离门控逻辑）"""
    monkeypatch.setattr('src.chan.scan.scan_buy_hits',
                        lambda high, low, close, fire_i: [_fake_hit()])


def test_missing_3_days_flagged_hit(fake_signal):
    """①删中段 3 天 → hit 且 reason='缺日3天'（≤2% 打标记保留）"""
    df = _df(drop_idx=(200, 201, 202))
    status, hit, reason = scan_stock_anchor(df, AS_OF, expected_bars=520)
    assert status == 'hit'
    assert hit is not None
    assert reason == '缺日3天'


def test_missing_15_days_insufficient(fake_signal):
    """②删中段 15 天 → insufficient（>2% 不判定，防假背驰）"""
    df = _df(drop_idx=tuple(range(200, 215)))
    status, hit, reason = scan_stock_anchor(df, AS_OF, expected_bars=520)
    assert status == 'insufficient'
    assert hit is None
    assert '窗口缺日15天' in reason and '覆盖97%' in reason


def test_expected_bars_none_legacy_behavior(monkeypatch):
    """③expected_bars=None → 与旧版完全一致（无信号 → no/reason=None）"""
    called = {}

    def _no_signal(high, low, close, fire_i):
        called['ran'] = True
        return []
    monkeypatch.setattr('src.chan.scan.scan_buy_hits', _no_signal)
    status, hit, reason = scan_stock_anchor(_df(), AS_OF, expected_bars=None)
    assert (status, hit, reason) == ('no', None, None)
    assert called['ran']


def test_full_window_no_flag(fake_signal):
    """④520 根全 → hit 且 reason=None"""
    status, hit, reason = scan_stock_anchor(_df(520), AS_OF, expected_bars=520)
    assert status == 'hit'
    assert reason is None


def test_young_stock_not_misjudged(fake_signal):
    """次新保护：上市晚（实际 300 根但应含 = 300）→ 不算缺日。
    expected_bars 由调用方按 max(窗口起点, list_date) 起算——本用例直接传 300。"""
    status, hit, reason = scan_stock_anchor(_df(300), AS_OF, expected_bars=300)
    assert status == 'hit'
    assert reason is None


def test_insufficient_gate_precedes_signal_scan(monkeypatch):
    """门控在信号扫描之前：缺日 15 天时信号扫描不应执行（省时且语义清晰）"""
    called = {}
    monkeypatch.setattr('src.chan.scan.scan_buy_hits',
                        lambda *a, **k: called.setdefault('ran', True) or [])
    df = _df(drop_idx=tuple(range(200, 215)))
    status, _, reason = scan_stock_anchor(df, AS_OF, expected_bars=520,
                                          min_coverage=0.98)
    assert status == 'insufficient'
    assert 'ran' not in called


def test_nan_and_nonpositive_still_insufficient(fake_signal):
    """既有前置过滤不回归：NaN / 非正价仍 insufficient"""
    df = _df()
    df.loc[10, 'high'] = np.nan
    status, _, reason = scan_stock_anchor(df, AS_OF, expected_bars=520)
    assert status == 'insufficient' and reason == 'K线含缺失值'
    df2 = _df()
    df2.loc[10, 'low'] = -1.0
    status, _, reason = scan_stock_anchor(df2, AS_OF, expected_bars=520)
    assert status == 'insufficient' and reason == '复权价异常(非正价)'
