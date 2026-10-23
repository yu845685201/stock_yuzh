"""指标纯函数库单测（方案 §3.1.5）：0 级函数构造用例 + 2 级指标口径要点。

覆盖：REF/MA/EMA/SMA/STD/SUM/CROSS 语义、MACD 柱×2、KDJ span=5 与 J=3K-2D、
RSI 单调边界（全涨=100/全跌=0）、BOLL 对称、ATR 简单平均口径、OBV 符号与万手
单位、一字板 NaN 语义、WARMUP 表。对拍（vs MyTT）见 test_indicators_parity.py。
"""

import numpy as np
import pandas as pd
import pytest

from src.analysis.indicators import (
    WARMUP, CROSS, EMA, MA, REF, SMA, STD, SUM, adx, atr, boll, kdj, macd, obv,
    rsi,
)
from src.analysis.indicators.tdx_core import COUNT, HHV, LLV, MAX, MIN


# --------------------------------------------------------------- 0 级函数
class TestCoreFunctions:
    def test_ref_shift(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        out = REF(s, 1)
        assert out.iloc[0] is np.nan or pd.isna(out.iloc[0])
        assert out.iloc[1:].tolist() == [1.0, 2.0, 3.0, 4.0]

    def test_ref_zero_is_identity(self):
        s = pd.Series([1.0, 2.0])
        assert REF(s, 0).tolist() == [1.0, 2.0]

    def test_ma_known(self):
        assert MA(pd.Series([1.0, 2.0, 3.0, 4.0, 5.0]), 5).iloc[-1] == 3.0
        assert pd.isna(MA(pd.Series([1.0, 2.0]), 5).iloc[-1])  # warmup 不足

    def test_ema_seeds_first_value(self):
        """adjust=False 以首值播种——EMA 首行=首值（MyTT 同语义）。"""
        out = EMA(pd.Series([10.0, 20.0, 30.0]), 5)
        assert out.iloc[0] == 10.0

    def test_sma_alpha_is_m_over_n(self):
        """中国式 SMA：ewm(alpha=m/n)。SMA([1,2],10,1)[1] = 0.1*2+0.9*1 = 1.1。"""
        out = SMA(pd.Series([1.0, 2.0]), 10, 1)
        assert out.iloc[1] == pytest.approx(1.1)

    def test_std_is_population(self):
        """ddof=0 总体标准差（附录 B 口径）。"""
        s = pd.Series([1.0, 2.0, 3.0])
        assert STD(s, 3).iloc[-1] == pytest.approx(np.std([1.0, 2.0, 3.0]))

    def test_sum_zero_is_cumsum(self):
        assert SUM(pd.Series([1.0, 2.0, 3.0]), 0).tolist() == [1.0, 3.0, 6.0]

    def test_hhv_llv(self):
        s = pd.Series([1.0, 5.0, 3.0, 2.0, 4.0])
        assert HHV(s, 3).iloc[-1] == 4.0
        assert LLV(s, 3).iloc[-1] == 2.0

    def test_cross_golden_at_second_bar(self):
        """金叉=前一根不满足、当前满足；首行恒 False。"""
        s1 = pd.Series([1.0, 2.0, 3.0])
        s2 = pd.Series([2.0, 1.5, 1.8])
        assert CROSS(s1, s2).tolist() == [False, True, False]

    def test_cross_nan_not_cross(self):
        s1 = pd.Series([np.nan, 2.0])
        s2 = pd.Series([2.0, 2.0])
        assert CROSS(s1, s2).tolist() == [False, False]

    def test_max_min_scalar(self):
        """MAX/MIN 支持序列×标量（RSI/ATR 公式依赖，防长度-1 Series 广播错位）。"""
        s = pd.Series([-1.0, 3.0, -5.0])
        assert MAX(s, 0).tolist() == [0.0, 3.0, 0.0]
        assert MIN(s, 0).tolist() == [-1.0, 0.0, -5.0]

    def test_count(self):
        cond = pd.Series([True, False, True, True])
        # rolling(2) 首行样本不足为 NaN
        out = COUNT(cond, 2)
        assert pd.isna(out.iloc[0])
        assert out.iloc[1:].tolist() == [1.0, 1.0, 2.0]


# --------------------------------------------------------------- 2 级指标
def _synthetic(n=140, seed=7):
    """确定性合成日 K（随机漫步 + 固定影线），用于口径断言。"""
    rng = np.random.default_rng(seed)
    close = 10 + np.cumsum(rng.normal(0, 0.3, n))
    close = np.maximum(close, 1.0)
    high = close * (1 + np.abs(rng.normal(0, 0.004, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.004, n)))
    vol = rng.integers(1000, 9000, n).astype(float)
    return close, high, low, vol


class TestIndicators:
    def test_macd_hist_is_dif_minus_dea_times_two(self):
        c, _, _, _ = _synthetic()
        r = macd(c)
        assert r.hist.iloc[-1] == pytest.approx((r.dif.iloc[-1] - r.dea.iloc[-1]) * 2)

    def test_kdj_j_formula(self):
        c, h, l, _ = _synthetic()
        r = kdj(c, h, l)
        assert r.j.iloc[-1] == pytest.approx(3 * r.k.iloc[-1] - 2 * r.d.iloc[-1])

    def test_kdj_k_is_ema_span5(self):
        """K=EMA(RSV, M1*2-1)=EMA(RSV,5)——span=5 而非 rolling-mean（口径红线）。"""
        c, h, l, _ = _synthetic()
        r = kdj(c, h, l)
        rsv = (pd.Series(c) - LLV(pd.Series(l), 9)) / (
            HHV(pd.Series(h), 9) - LLV(pd.Series(l), 9)) * 100
        expected = EMA(rsv, 5)
        assert r.k.iloc[-1] == pytest.approx(expected.iloc[-1])

    def test_rsi_all_up_is_100(self):
        c = np.arange(1.0, 61.0)
        assert rsi(c, 24).iloc[-1] == pytest.approx(100.0)

    def test_rsi_all_down_is_0(self):
        c = np.arange(60.0, 0.0, -1.0)
        assert rsi(c, 24).iloc[-1] == pytest.approx(0.0)

    def test_boll_symmetric(self):
        c, _, _, _ = _synthetic()
        r = boll(c)
        assert r.upper.iloc[-1] - r.mid.iloc[-1] == pytest.approx(
            r.mid.iloc[-1] - r.lower.iloc[-1])

    def test_atr_is_simple_average_of_tr(self):
        """ATR=MA(TR,20) 简单平均——非 Wilder 递推（附录 B 口径）。"""
        c, h, l, _ = _synthetic()
        cs, hs, ls = pd.Series(c), pd.Series(h), pd.Series(l)
        tr = MAX(MAX(hs - ls, (REF(cs, 1) - hs).abs()), (REF(cs, 1) - ls).abs())
        assert atr(c, h, l).iloc[-1] == pytest.approx(tr.rolling(20).mean().iloc[-1])

    def test_atr_nonnegative(self):
        c, h, l, _ = _synthetic()
        assert (atr(c, h, l).dropna() >= 0).all()

    def test_obv_sign_and_unit(self):
        """涨+VOL/跌-VOL/平盘 0，末尾除 10000（万手单位）。"""
        c = np.array([10.0, 11.0, 10.0, 10.0, 12.0])
        v = np.full(5, 100.0)
        out = obv(c, v)
        assert out.tolist() == [0.0, 0.01, 0.0, 0.0, 0.01]

    def test_kdj_one_word_bar_nan(self):
        """一字板区间（H=L=C）RSV 分母为 0 → NaN 向下传播（文档化语义，不设防）。"""
        n = 30
        c = np.full(n, 10.0)
        h = np.full(n, 10.0)
        l = np.full(n, 10.0)
        r = kdj(c, h, l)
        assert pd.isna(r.k.iloc[-1])

    def test_indicators_warmup_nan_prefix(self):
        """rolling 系指标首行为 NaN（warmup 前行）；EMA 系首行=播种值（非 NaN）。"""
        c, h, l, _ = _synthetic()
        assert pd.isna(boll(c).mid.iloc[0])
        assert pd.isna(atr(c, h, l).iloc[0])
        assert pd.isna(kdj(c, h, l).k.iloc[0])
        # EMA(adjust=False) 以首值播种：MACD 首行 = 0（dif=dea=c0），不是 NaN
        assert macd(c).dif.iloc[0] == 0.0


class TestAdx:
    """ADX/DMI（通达信 SUM+MA 口径；MyTT 克隆无此函数，对拍基线为公式本身）。"""

    def _series(self, kind, n=60):
        idx = pd.date_range("20260101", periods=n, freq="B")
        base = pd.Series(np.linspace(10, 20, n), index=idx)
        if kind == "up":
            return base + 0.5, base - 0.5, base
        if kind == "down":
            rev = base.iloc[::-1].reset_index(drop=True)
            rev.index = idx
            return rev + 0.5, rev - 0.5, rev
        # 温和震荡（非平盘：平盘 TR=0 → NaN 是文档化语义，另测）
        osc = pd.Series([10.0 + (0.3 if i % 2 else -0.3) for i in range(n)],
                        index=idx)
        return osc + 0.2, osc - 0.2, osc

    def test_uptrend_max_adx(self):
        """纯上行：MDI=0、ADX=100（趋势强度满分）。"""
        h, l, c = self._series("up")
        pdi, mdi, a = adx(h, l, c)
        assert float(mdi.iloc[-1]) == 0.0
        assert float(a.iloc[-1]) == 100.0
        assert float(pdi.iloc[-1]) > 0

    def test_downtrend_max_adx(self):
        """纯下行：PDI=0、ADX=100。"""
        h, l, c = self._series("down")
        pdi, mdi, a = adx(h, l, c)
        assert float(pdi.iloc[-1]) == 0.0
        assert float(a.iloc[-1]) == 100.0
        assert float(mdi.iloc[-1]) > 0

    def test_oscillating_low_adx(self):
        """温和震荡：ADX 低（<25，趋势弱）。"""
        h, l, c = self._series("osc")
        _, _, a = adx(h, l, c)
        assert not pd.isna(a.iloc[-1])
        assert float(a.iloc[-1]) < 25.0

    def test_flat_tr_zero_nan(self):
        """平盘（TR=0）：PDI/MDI/ADX 为 NaN，不硬造 0（文档化语义）。"""
        idx = pd.date_range("20260101", periods=60, freq="B")
        flat = pd.Series(10.0, index=idx)
        pdi, mdi, a = adx(flat, flat, flat)
        assert pd.isna(pdi.iloc[-1]) and pd.isna(mdi.iloc[-1])
        assert pd.isna(a.iloc[-1])

    def test_nan_warmup(self):
        """warmup（M1+M2-1=19）前行 NaN，不硬造。"""
        h, l, c = self._series("up")
        _, _, a = adx(h, l, c)
        assert a.iloc[:19].isna().all()
        assert not pd.isna(a.iloc[-1])

    def test_adx_is_ma_of_dx(self):
        """ADX = MA(DX, 6)：从 PDI/MDI 反推 DX 再平滑，应与输出一致。"""
        h, l, c = self._series("up")
        pdi, mdi, a = adx(h, l, c)
        dx = (pdi - mdi).abs() / (pdi + mdi) * 100.0
        recomputed = MA(dx, 6)
        assert np.allclose(a.dropna().values,
                           recomputed.dropna().values, equal_nan=True)

    def test_pdi_mdi_nonnegative(self):
        """PDI/MDI 非负（方向运动占比）。"""
        h, l, c = self._series("up")
        pdi, mdi, _ = adx(h, l, c)
        assert (pdi.dropna() >= 0).all() and (mdi.dropna() >= 0).all()


def test_warmup_table():
    """WARMUP 表与模块 max_lookback 声明依据一致（方案 §3.1.2）。"""
    assert WARMUP == {"ma": 60, "macd": 130, "kdj": 60, "rsi": 130,
                      "boll": 60, "atr": 60, "obv": 130, "adx": 30}
