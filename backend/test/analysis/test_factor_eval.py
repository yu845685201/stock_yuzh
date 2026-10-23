"""因子评估单测（P1-6 §4.3）：rankic 统计核心 + 前缀不变性对比 + 前瞻收益构造。

验收要点（方案 §4.3）：对已知有短期动量的信号（连板数）RankIC 应显著不为 0——
若为 0 反而是实现错误的自检信号；每日样本 < min_names 不计；禁 inf/NaN。
真实数据端到端见 CLI `analyze rankic` 短周期冒烟（README 实测表）。
"""

import pandas as pd
import pytest

from src.review.factor_eval import (
    SELF_CHECK_SIGNAL, _finite, _nth_trading_day_forward, forward_returns,
    prefix_invariance, rankic,
)


def _long(signal_by_day, fwd_by_day):
    """{date: {code: val}} → MultiIndex Series（长表）。"""
    sig = pd.Series({(d, c): v for d, m in signal_by_day.items()
                     for c, v in m.items()})
    fwd = pd.Series({(d, c): v for d, m in fwd_by_day.items()
                     for c, v in m.items()})
    sig.index = sig.index.set_names(['trade_date', 'ts_code'])
    fwd.index = fwd.index.set_names(['trade_date', 'ts_code'])
    return sig, fwd


class TestRankic:
    def test_perfect_positive(self):
        sig, fwd = _long({'20260101': {'a': 1, 'b': 2, 'c': 3, 'd': 4}},
                         {'20260101': {'a': 0.1, 'b': 0.2, 'c': 0.3, 'd': 0.4}})
        r = rankic(sig, fwd, min_names=4)
        assert r['rank_ic'] == pytest.approx(1.0)
        assert r['n_days'] == 1 and r['n_names'] == 4
        assert r['icir'] is None          # 单日 std 无定义 → None（禁 inf）

    def test_perfect_negative(self):
        sig, fwd = _long({'20260101': {'a': 1, 'b': 2, 'c': 3, 'd': 4}},
                         {'20260101': {'a': 0.4, 'b': 0.3, 'c': 0.2, 'd': 0.1}})
        r = rankic(sig, fwd, min_names=4)
        assert r['rank_ic'] == pytest.approx(-1.0)

    def test_multi_day_icir(self):
        days = {f'2026010{i}': {'a': i, 'b': -i} for i in range(1, 6)}
        fwd = {f'2026010{i}': {'a': 0.01 * i, 'b': -0.01 * i} for i in range(1, 6)}
        sig, f = _long(days, fwd)
        r = rankic(sig, f, min_names=2)
        assert r['n_days'] == 5
        assert r['ic_mean'] == pytest.approx(1.0)
        assert r['icir'] is None          # std=0 → None（禁 inf）

    def test_min_names_skips_day(self):
        sig, fwd = _long({'20260101': {'a': 1, 'b': 2},          # 2 < 5 → 跳过
                          '20260102': {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5}},
                         {'20260101': {'a': 0.1, 'b': 0.2},
                          '20260102': {'a': 0.1, 'b': 0.2, 'c': 0.3, 'd': 0.4, 'e': 0.5}})
        r = rankic(sig, fwd, min_names=5)
        assert r['n_days'] == 1           # 只有第二天计入

    def test_nan_inf_filtered(self):
        sig, fwd = _long({'20260101': {'a': 1, 'b': 2, 'c': 3, 'd': 4}},
                         {'20260101': {'a': 0.1, 'b': float('nan'),
                                       'c': 0.3, 'd': float('inf')}})
        r = rankic(sig, fwd, min_names=4)
        # 有效对 = a/c 两对 → 2 < 4 → 不计（不硬造）
        assert r['n_days'] == 0

    def test_boolean_signal(self):
        """布尔信号（R 系列）按平均秩处理并列——命中组 vs 未命中组。"""
        n = 60
        sig_d = {f's{i}': (1 if i < 10 else 0) for i in range(n)}
        fwd_d = {f's{i}': (0.05 if i < 10 else 0.0) for i in range(n)}
        sig, fwd = _long({'20260101': sig_d}, {'20260101': fwd_d})
        r = rankic(sig, fwd, min_names=50)
        assert r['rank_ic'] > 0            # 命中组收益更高 → 正 IC

    def test_requires_multiindex(self):
        s = pd.Series([1, 2, 3])
        with pytest.raises(ValueError, match='MultiIndex'):
            rankic(s, s)

    def test_empty_input(self):
        idx = pd.MultiIndex.from_tuples([], names=['trade_date', 'ts_code'])
        r = rankic(pd.Series(dtype=float, index=idx),
                   pd.Series(dtype=float, index=idx))
        assert r['n_days'] == 0 and r['rank_ic'] is None


class TestPrefixInvariance:
    def test_identical_ok(self):
        idx = pd.MultiIndex.from_tuples([('20260101', 'a'), ('20260102', 'a')],
                                        names=['trade_date', 'ts_code'])
        a = pd.Series([True, False], index=idx)
        assert prefix_invariance(a, a.copy())['ok'] is True

    def test_diff_detected(self):
        idx = pd.MultiIndex.from_tuples([('20260101', 'a'), ('20260102', 'a')],
                                        names=['trade_date', 'ts_code'])
        a = pd.Series([True, False], index=idx)
        b = pd.Series([False, False], index=idx)
        r = prefix_invariance(a, b)
        assert r['ok'] is False and r['n_diff'] == 1
        assert '20260101' in r['first_diff']

    def test_no_intersection(self):
        idx1 = pd.MultiIndex.from_tuples([('20260101', 'a')],
                                         names=['trade_date', 'ts_code'])
        idx2 = pd.MultiIndex.from_tuples([('20260102', 'a')],
                                         names=['trade_date', 'ts_code'])
        r = prefix_invariance(pd.Series([1], index=idx1),
                              pd.Series([1], index=idx2))
        assert r['ok'] is False and r['n_compared'] == 0

    def test_numeric_nan_tolerance(self):
        idx = pd.MultiIndex.from_tuples([('20260101', 'a')],
                                        names=['trade_date', 'ts_code'])
        a = pd.Series([float('nan')], index=idx)
        b = pd.Series([float('nan')], index=idx)
        assert prefix_invariance(a, b)['ok'] is True   # 双 NaN 视为一致


class TestFinite:
    def test_finite_guard(self):
        assert _finite(None) is None
        assert _finite('abc') is None
        assert _finite(float('inf')) is None
        assert _finite(float('nan')) is None
        assert _finite('3.5') == 3.5


class _StubReader:
    """forward_returns / _nth_trading_day_forward 的 stub。"""

    def __init__(self, calendar, kline):
        self.calendar = calendar      # 升序交易日列表
        self.kline = kline            # DataFrame[trade_date, ts_code, close]

    def _query(self, sql, params=None):
        if 'base_trade_calendar' not in sql:
            raise AssertionError(f'未预期查询: {sql[:60]}')
        as_of, limit = params[0], int(params[1])
        if 'calendar_date >=' in sql:
            rows = [d for d in self.calendar if d >= as_of][:limit]
        else:
            rows = [d for d in self.calendar if d <= as_of][-limit:]
        return [(d,) for d in rows]

    def load_kline(self, columns, as_of, start_date=None, ts_codes=None):
        df = self.kline
        if start_date:
            df = df[df['trade_date'] >= start_date]
        return df[df['trade_date'] <= as_of].reset_index(drop=True)


class TestForwardReturns:
    def test_ratio_and_shift(self):
        cal = [f'2026010{i}' for i in range(1, 10)]
        rows = []
        for d in cal:
            for code, base in (('a', 10.0), ('b', 20.0)):
                rows.append({'trade_date': d, 'ts_code': code,
                             'close': base * (1 + 0.01 * cal.index(d))})
        reader = _StubReader(cal, pd.DataFrame(rows))
        out = forward_returns(reader, '20260101', '20260103', [1, 2])
        assert set(out) == {1, 2}
        f1 = out[1]
        # T=20260101 → T+1=20260102: 10 → 10.1 → +1%
        assert f1.loc[('20260101', 'a')] == pytest.approx(0.01)
        assert f1.loc[('20260101', 'b')] == pytest.approx(0.01)
        # T+2: 10 → 10.2 → +2%
        assert out[2].loc[('20260101', 'a')] == pytest.approx(0.02)

    def test_nth_trading_day_forward(self):
        cal = [f'2026010{i}' for i in range(1, 10)]
        reader = _StubReader(cal, pd.DataFrame())
        assert _nth_trading_day_forward(reader, '20260101', 0) == '20260101'
        assert _nth_trading_day_forward(reader, '20260101', 3) == '20260104'
        assert _nth_trading_day_forward(reader, '20260108', 5) is None  # 日历不足

    def test_empty_kline(self):
        reader = _StubReader(['20260101'], pd.DataFrame(
            columns=['trade_date', 'ts_code', 'close']))
        assert forward_returns(reader, '20260101', '20260101', [1]) == {}
