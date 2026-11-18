"""P0-1 指标截面模块单测（方案 §3.1.3，2026-10-18 随增量 C1 落地）。

七个模块共用约定：锚定日必须等于 df 最后一行 trade_date，否则「数据不足」；
空 df / 缺列 / warmup 不足 / 一字板（带宽 0）一律「数据不足」，不抛异常。
指标口径由 indicators 库对拍 MyTT 保证（test_indicators_parity.py），本文件测
模块 wiring、取值域与边界；构造用例覆盖确定性场景（单调涨跌/一字板/V 形），
其余用「取值 ∈ 允许集 + 与库计算值内部一致」断言。
"""

import numpy as np
import pandas as pd
import pytest

from src.analysis.indicators import atr as lib_atr
from src.analysis.indicators import boll as lib_boll
from src.analysis.indicators import macd as lib_macd
from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.atr_pct import AtrPct
from src.analysis.modules.boll_pos import BollPos
from src.analysis.modules.kdj_state import KdjState
from src.analysis.modules.ma_arrangement import MaArrangement
from src.analysis.modules.macd_state import MacdState
from src.analysis.modules.obv_trend import ObvTrend
from src.analysis.modules.rsi_state import RsiState

AS_OF = "20260930"


def _ctx():
    return ChainContext(as_of=AS_OF,
                        stock=StockInfo(ts_code="sh.600001", stock_code="600001",
                                        stock_name="测试股", board="主板"))


def _df(closes, vols=None, start=200):
    """由收盘序列构造日 K（high/low 按 ±0.5% 影线，volume 默认 1000）。"""
    n = len(closes)
    c = np.asarray(closes, dtype=float)
    v = np.asarray(vols if vols is not None else np.full(n, 1000.0), dtype=float)
    dates = pd.date_range("2026-01-01", periods=n, freq="B").strftime("%Y%m%d")
    dates = [d for d in dates][-n:]
    # 保证最后一行是锚定日
    dates[-1] = AS_OF
    return pd.DataFrame({
        "trade_date": dates, "open": c, "high": c * 1.005, "low": c * 0.995,
        "close": c, "volume": v, "change_rate": np.zeros(n), "is_st": 0,
    })


def _rising(n=140):
    return 10 + np.arange(n) * 0.1


def _falling(n=140):
    return 10 + (n - np.arange(n)) * 0.1


ALL_MODULES = [MacdState, KdjState, RsiState, BollPos, AtrPct, ObvTrend,
               MaArrangement]


class TestCommonBoundaries:
    """七模块共用边界：空 df / 锚定日不匹配 / 缺列 → 数据不足。"""

    @pytest.mark.parametrize("mod_cls", ALL_MODULES)
    def test_empty_df(self, mod_cls):
        assert mod_cls().analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT

    @pytest.mark.parametrize("mod_cls", ALL_MODULES)
    def test_anchor_mismatch(self, mod_cls):
        df = _df(_rising())
        df.loc[df.index[-1], "trade_date"] = "20260929"
        assert mod_cls().analyze(df, _ctx()).value == DATA_INSUFFICIENT

    @pytest.mark.parametrize("mod_cls", ALL_MODULES)
    def test_short_series_insufficient(self, mod_cls):
        """短序列（warmup 不足）→ 数据不足，不抛异常。"""
        assert mod_cls().analyze(_df(_rising(3)), _ctx()).value == DATA_INSUFFICIENT


class TestMacdState:
    def test_value_in_domain_and_detail_consistent(self):
        df = _df(_rising())
        res = MacdState().analyze(df, _ctx())
        assert res.value in {"金叉且柱放大", "金叉", "死叉且柱缩小", "死叉",
                             "零轴上多头", "零轴下空头"}
        r = lib_macd(pd.to_numeric(df["close"]))
        assert res.detail["hist"] == round(float(r.hist.iloc[-1]), 4)

    def test_rising_ends_above_zero(self):
        """单调上升：DIF>0 且通常无当日交叉 → 零轴上多头（或金叉类）。"""
        res = MacdState().analyze(_df(_rising()), _ctx())
        assert res.value in {"零轴上多头", "金叉", "金叉且柱放大"}


class TestKdjState:
    def test_format_and_flags_consistent(self):
        res = KdjState().analyze(_df(_rising()), _ctx())
        if res.value == "否":
            return
        assert res.value.startswith("K=") and "D=" in res.value and "J=" in res.value
        for flag in res.detail["flags"]:
            assert flag in res.value

    def test_all_up_overbought(self):
        """阴跌 60 日后末端 10 日急涨（收于锚定日）：RSV 触顶使 K 快速上行、
        D 滞后 → J=3K-2D > 100 超买（J>100 是上升瞬态，故急涨须止于锚定日）。"""
        closes = list(np.linspace(100, 50, 60)) + list(np.linspace(50, 100, 10))
        res = KdjState().analyze(_df(closes), _ctx())
        assert res.value != DATA_INSUFFICIENT
        assert "超买" in res.value


class TestRsiState:
    def test_all_up_overbought(self):
        res = RsiState().analyze(_df(_rising(160)), _ctx())
        assert res.value.startswith("RSI6=")
        assert "超买(RSI6>80)" in res.value

    def test_all_down_oversold(self):
        res = RsiState().analyze(_df(_falling(160)), _ctx())
        assert "超卖(RSI6<20)" in res.value

    def test_thresholds_configurable(self):
        mod = RsiState({"overbought": 99.0, "oversold": 1.0})
        res = mod.analyze(_df(_rising(160)), _ctx())
        assert "超买(RSI6>99)" in res.value


class TestBollPos:
    def test_value_matches_independent_computation(self):
        df = _df(_rising())
        res = BollPos().analyze(df, _ctx())
        r = lib_boll(pd.to_numeric(df["close"]))
        width = float(r.upper.iloc[-1]) - float(r.lower.iloc[-1])
        # %B 口径：(close-LOWER)/(UPPER-LOWER)，中轨=50
        expected = (float(df["close"].iloc[-1]) - float(r.lower.iloc[-1])) / width * 100
        assert res.value == pytest.approx(round(expected, 1))
        assert res.detail["bandwidth"] is not None

    def test_one_word_bar_insufficient(self):
        """一字板（H=L=C）→ 带宽 0 → 数据不足。"""
        df = _df([10.0] * 80)
        assert BollPos().analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_oscillating_near_mid(self):
        """围绕 10 窄幅振荡且末根收于均值 → 收盘贴近中轨，位置在 50 附近。"""
        closes = list(10 + 0.02 * np.sin(np.arange(140) / 3.0))
        closes[-1] = 10.0
        res = BollPos().analyze(_df(closes), _ctx())
        assert res.value != DATA_INSUFFICIENT
        assert 30 <= res.value <= 70


class TestAtrPct:
    def test_value_matches_independent_computation(self):
        df = _df(_rising())
        res = AtrPct().analyze(df, _ctx())
        c = pd.to_numeric(df["close"])
        expected = float(lib_atr(c, df["high"], df["low"], 20).iloc[-1]) \
            / float(c.iloc[-1]) * 100
        assert res.value == pytest.approx(round(expected, 2))

    def test_positive(self):
        res = AtrPct().analyze(_df(_rising()), _ctx())
        assert res.value > 0


class TestObvTrend:
    def test_monotonic_up_is_price_up_volume_up(self):
        res = ObvTrend().analyze(_df(_rising()), _ctx())
        assert res.value == "价升量增"
        assert res.detail["price_dir"] == "up" and res.detail["obv_dir"] == "up"

    def test_monotonic_down_is_price_down_volume_down(self):
        res = ObvTrend().analyze(_df(_falling()), _ctx())
        assert res.value == "价跌量减"

    def test_price_up_obv_down(self):
        """价 5 日涨但 OBV 5 日降（末 5 日净流出），且收盘非 20 日新高 → 价升量缩。"""
        closes = [100.0] * 130 + [110, 109, 108, 107, 105]
        res = ObvTrend().analyze(_df(closes), _ctx())
        assert res.value == "价升量缩"

    def test_divergence_watch(self):
        """价升量缩 + 收盘创 20 日新高 → 背离待察（顶背离信号）。"""
        closes = [100.0] * 130 + [110, 109, 108, 107, 111]
        res = ObvTrend().analyze(_df(closes), _ctx())
        assert res.value == "背离待察"

    def test_flat_is_no(self):
        res = ObvTrend().analyze(_df([10.0] * 140), _ctx())
        assert res.value == "否"


class TestMaArrangement:
    def test_monotonic_up_bull(self):
        res = MaArrangement().analyze(_df(_rising(160)), _ctx())
        assert res.value == "多头排列(MA5>10>20>60)"

    def test_monotonic_down_bear(self):
        res = MaArrangement().analyze(_df(_falling(160)), _ctx())
        assert res.value == "空头排列(MA5<10<20<60)"

    def test_flat_entangled(self):
        res = MaArrangement().analyze(_df([10.0] * 140), _ctx())
        assert res.value == "纠缠"

    def test_equal_ma5_ma10_not_strict_order(self):
        """平价防护：前 5 日和恰等于后 5 日和 → MA5 与 MA10 数学相等，多空严格序
        不成立 → 纠缠（20261027 实盘 002182/002488：rolling 浮点误差曾把相等
        翻成空头排列，链/面板互斥翻车）。"""
        first5 = [11.12, 10.7, 11.01, 10.98, 10.88]    # 和 = 54.69
        last5 = [11.13, 10.94, 11.11, 11.08, 10.43]    # 和 = 54.69
        res = MaArrangement().analyze(_df([10.0] * 130 + first5 + last5), _ctx())
        assert res.value == "纠缠"
        # detail 的 MA 值应严格相等（同精度输出）
        assert res.detail["ma5"] == res.detail["ma10"]

    def test_cross_detail_consistency(self):
        """若命中「收盘上穿MA60」，则当日 close>MA60 且前一日 close≤MA60。"""
        df = _df(list(np.concatenate([np.linspace(100, 50, 100),
                                      np.linspace(51, 80, 40)])))
        res = MaArrangement().analyze(df, _ctx())
        if res.value == "收盘上穿MA60":
            assert res.detail["close"] > res.detail["ma60"]
        else:
            assert res.value in {"多头排列(MA5>10>20>60)",
                                 "空头排列(MA5<10<20<60)", "纠缠"}
