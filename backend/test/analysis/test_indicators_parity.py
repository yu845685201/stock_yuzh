"""指标库对拍测试（方案 §3.1.5）：本库 vs 本地克隆 MyTT（通达信口径基线）。

对拍目标 = 与通达信/同花顺显示一致（MyTT README 自述「一致到小数点后 2 位」），
故末值容差 0.005（MyTT 出口 RD 三位舍入差 ≤0.0005；未包装指标理论上逐位一致）。

数据：
- 合成序列（固定 seed，含一字板/涨跌停/平盘边界日，300 根覆盖全部 warmup）；
- 真实窗口（10 只黄金样本股 × 3 个历史窗口，需数据库；不可用则 skip 不 fail，
  模式同 test_golden_sample）。
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.analysis.indicators import atr, boll, kdj, macd, obv, rsi
from src.analysis.indicators.tdx_core import (
    CROSS, EMA, HHV, LLV, MA, REF, SMA, STD, SUM,
)

# --------------------------------------------------------------- MyTT 克隆导入
# 仓库根 stock_yuzh/ 的上一级（stock_yuzh_base/）下有 refrences/mpquant__MyTT/MyTT.py
_MYTT_CANDIDATES = [
    Path(__file__).resolve().parents[4] / "refrences" / "mpquant__MyTT",
    Path(__file__).resolve().parents[3] / "refrences" / "mpquant__MyTT",
]
_MYTT_DIR = next((p for p in _MYTT_CANDIDATES if (p / "MyTT.py").exists()), None)

if _MYTT_DIR is not None and str(_MYTT_DIR) not in sys.path:
    sys.path.insert(0, str(_MYTT_DIR))

if _MYTT_DIR is None:  # pragma: no cover - 克隆缺失时整体跳过
    pytestmark = pytest.mark.skip(reason="MyTT 克隆缺失（refrences/mpquant__MyTT）",
                                  allow_module_level=True)
else:
    from MyTT import (  # type: ignore
        ATR as mytt_atr, BOLL as mytt_boll, CROSS as mytt_cross, EMA as mytt_ema,
        HHV as mytt_hhv, KDJ as mytt_kdj, LLV as mytt_llv, MACD as mytt_macd,
        MA as mytt_ma, OBV as mytt_obv, REF as mytt_ref, RSI as mytt_rsi,
        SMA as mytt_sma, STD as mytt_std, SUM as mytt_sum,
    )

#: RD 包装指标（MACD/RSI/BOLL）容差：MyTT 出口三位舍入
_RD_ATOL = 0.005
#: 未包装指标（KDJ/ATR/OBV/0 级）容差：同 pandas 算子，理论上逐位一致
_EXACT_ATOL = 1e-9


def _synthetic(n=300, seed=42):
    """确定性合成日 K：随机漫步 + 注入一字板/涨跌停/平盘边界日。"""
    rng = np.random.default_rng(seed)
    close = 10 + np.cumsum(rng.normal(0, 0.3, n))
    close = np.maximum(close, 1.0)
    high = close * (1 + np.abs(rng.normal(0, 0.004, n)))
    low = close * (1 - np.abs(rng.normal(0, 0.004, n)))
    vol = rng.integers(1000, 9000, n).astype(float)
    # 每 ~17 根注入一日一字板（H=L=C，A 股涨跌停场景）
    for i in range(16, n, 17):
        high[i] = low[i] = close[i]
    # 每 ~23 根注入一日 ±10% 跳空
    for i in range(22, n, 23):
        close[i] = round(close[i] * 1.1, 2)
        high[i] = close[i]
        low[i] = close[i] * 0.995
    return close, high, low, vol


def _assert_close(ours, theirs, atol):
    a = np.asarray(ours, dtype=float)
    b = np.asarray(theirs, dtype=float)
    assert a.shape == b.shape, f"长度不一致: {a.shape} vs {b.shape}"
    assert np.allclose(a, b, atol=atol, equal_nan=True), (
        f"末值 ours={a[-1]!r} mytt={b[-1]!r}；最大差="
        f"{np.nanmax(np.abs(a - b)) if a.size else 'n/a'}")


class TestParitySynthetic:
    """合成序列全序列对拍（含边界日）。"""

    def test_core_functions(self):
        c, h, l, v = _synthetic()
        s = pd.Series(c)
        _assert_close(MA(s, 5), mytt_ma(c, 5), _EXACT_ATOL)
        _assert_close(EMA(s, 12), mytt_ema(c, 12), _EXACT_ATOL)
        _assert_close(SMA(s, 24), mytt_sma(c, 24), _EXACT_ATOL)
        _assert_close(REF(s, 1), mytt_ref(c, 1), _EXACT_ATOL)
        _assert_close(HHV(pd.Series(h), 9), mytt_hhv(h, 9), _EXACT_ATOL)
        _assert_close(LLV(pd.Series(l), 9), mytt_llv(l, 9), _EXACT_ATOL)
        _assert_close(STD(s, 20), mytt_std(c, 20), _EXACT_ATOL)
        _assert_close(SUM(s, 5), mytt_sum(c, 5), _EXACT_ATOL)
        _assert_close(SUM(s, 0), mytt_sum(c, 0), _EXACT_ATOL)
        ma5, ma10 = MA(s, 5), MA(s, 10)
        _assert_close(CROSS(ma5, ma10),
                      mytt_cross(np.asarray(ma5), np.asarray(ma10)), _EXACT_ATOL)

    def test_macd(self):
        c, _, _, _ = _synthetic()
        ours = macd(c)
        d, dea, hist = mytt_macd(c)
        _assert_close(ours.dif, d, _RD_ATOL)
        _assert_close(ours.dea, dea, _RD_ATOL)
        _assert_close(ours.hist, hist, _RD_ATOL)

    def test_kdj(self):
        c, h, l, _ = _synthetic()
        ours = kdj(c, h, l)
        k, d, j = mytt_kdj(c, h, l)
        _assert_close(ours.k, k, _EXACT_ATOL)
        _assert_close(ours.d, d, _EXACT_ATOL)
        _assert_close(ours.j, j, _EXACT_ATOL)

    def test_rsi(self):
        c, _, _, _ = _synthetic()
        _assert_close(rsi(c, 24), mytt_rsi(c, 24), _RD_ATOL)
        _assert_close(rsi(c, 6), mytt_rsi(c, 6), _RD_ATOL)

    def test_boll(self):
        c, _, _, _ = _synthetic()
        ours = boll(c)
        up, mid, lo = mytt_boll(c)
        _assert_close(ours.upper, up, _RD_ATOL)
        _assert_close(ours.mid, mid, _RD_ATOL)
        _assert_close(ours.lower, lo, _RD_ATOL)

    def test_atr(self):
        c, h, l, _ = _synthetic()
        _assert_close(atr(c, h, l), mytt_atr(c, h, l), _EXACT_ATOL)

    def test_obv(self):
        c, _, _, v = _synthetic()
        _assert_close(obv(c, v), mytt_obv(c, v), _EXACT_ATOL)


# --------------------------------------------------------------- 真实数据对拍
AS_OF = "20260923"
GOLDEN_STOCKS = ["600418", "600825", "601811", "603636", "000560",
                 "000850", "300750", "688512", "600053", "002935"]


@pytest.mark.skipif(os.getenv("ANALYSIS_GOLDEN") != "1",
                    reason="设 ANALYSIS_GOLDEN=1 开启真实数据对拍（需数据库）")
class TestParityRealData:
    """真实日 K 窗口对拍（黄金样本股 × 3 个窗口）。"""

    def test_real_windows(self):
        from src.analysis.data_reader import DataReader
        from src.analysis.lookback import Lookback
        from src.config import ConfigManager

        reader = DataReader(ConfigManager())
        try:
            universe = {s.stock_code: s for s in reader.load_stock_universe()}
            codes = [universe[c] for c in GOLDEN_STOCKS if c in universe]
            start = reader.resolve_window_start(AS_OF, Lookback.parse("250d"))
            kdf = reader.load_kline(
                columns=["open", "high", "low", "close", "volume"],
                as_of=AS_OF, start_date=start,
                ts_codes=[s.ts_code for s in codes])
        except Exception as exc:  # pragma: no cover - 无数据库环境
            pytest.skip(f"数据库不可用: {exc!r}")
        finally:
            reader.close()

        assert not kdf.empty, "真实窗口加载为空"
        compared = 0
        for _, g in kdf.groupby("ts_code", sort=False):
            g = g.sort_values("trade_date")
            c, h, l, v = (g["close"].to_numpy(dtype=float),
                          g["high"].to_numpy(dtype=float),
                          g["low"].to_numpy(dtype=float),
                          g["volume"].to_numpy(dtype=float))
            ours, (d, dea, hist) = macd(c), mytt_macd(c)
            _assert_close(ours.hist, hist, _RD_ATOL)
            ours_kdj, (k, dd, j) = kdj(c, h, l), mytt_kdj(c, h, l)
            _assert_close(ours_kdj.j, j, _EXACT_ATOL)
            _assert_close(rsi(c, 24), mytt_rsi(c, 24), _RD_ATOL)
            ours_boll, (up, mid, lo) = boll(c), mytt_boll(c)
            _assert_close(ours_boll.mid, mid, _RD_ATOL)
            _assert_close(atr(c, h, l), mytt_atr(c, h, l), _EXACT_ATOL)
            _assert_close(obv(c, v), mytt_obv(c, v), _EXACT_ATOL)
            compared += 1
        assert compared >= 8, f"真实对拍样本不足: {compared}"
