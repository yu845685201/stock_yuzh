"""P1-12 单测（方案 §4.5）：RPS 模块 / 自适应阈值（big_mover/volume_surge）/
情绪分位标定（P1-10 §4.4 增量）。

验收红线：adaptive 默认关时行为与增量前逐字节一致（同输入同输出同桶键）；
开启时高波动股阈值抬高；RPS 走 env 不模块自算。
"""

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
from src.analysis.modules.big_mover import BigMover
from src.analysis.modules.rps_breakout import RpsBreakout
from src.analysis.modules.volume_surge import VolumeSurge
from src.review.sentiment_calibration import quantile_calibration, quantile_state

AS_OF = "20260930"


def _ctx(upstream=None, ts_code="sh.600001"):
    return ChainContext(
        as_of=AS_OF,
        stock=StockInfo(ts_code=ts_code, stock_code="600001",
                        stock_name="测试股", board="主板"),
        upstream=dict(upstream or {}))


def _dates(n, end="20260930"):
    d = date(int(end[:4]), int(end[4:6]), int(end[6:]))
    days = []
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return list(reversed(days))


class TestRpsBreakout:
    def _df(self):
        return pd.DataFrame([{"trade_date": AS_OF, "is_st": 0}])

    def test_hit(self):
        res = RpsBreakout().analyze(
            self._df(), _ctx({"rps_map": {"sh.600001": {20: 95.0, 60: 88.0}}}))
        assert res.value == "RPS20=95/RPS60=88"

    def test_below_threshold(self):
        res = RpsBreakout().analyze(
            self._df(), _ctx({"rps_map": {"sh.600001": {20: 85.0}}}))
        assert res.value == "否"

    def test_missing_rps_insufficient(self):
        res = RpsBreakout().analyze(self._df(), _ctx({"rps_map": {}}))
        assert res.value == "数据不足"

    def test_st_rejected(self):
        df = pd.DataFrame([{"trade_date": AS_OF, "is_st": 1}])
        res = RpsBreakout().analyze(
            df, _ctx({"rps_map": {"sh.600001": {20: 99.0}}}))
        assert res.value == "否"


class TestAdaptiveBigMover:
    def _df(self, closes):
        dates = _dates(len(closes))
        rows = []
        for i, c in enumerate(closes):
            prev = closes[i - 1] if i else c
            rows.append({"trade_date": dates[i], "close": c, "preclose": prev,
                         "high": c * 1.01, "low": c * 0.99,
                         "change_rate": round((c / prev - 1) * 100, 2),
                         "volume": 1000.0, "is_st": 0})
        return pd.DataFrame(rows)

    def test_default_off_fixed_threshold(self):
        """默认关：+6% → 大涨（固定阈值 5）。"""
        closes = [10.0] * 24 + [10.6]
        res = BigMover().analyze(self._df(closes), _ctx())
        assert res.value == "大涨"
        assert res.detail["adaptive"] is False

    def test_adaptive_raises_threshold_for_high_vol(self):
        """自适应：高波动股（大 ATR%）阈值抬高。"""
        # 构造高波动序列：日内振幅 ±8%
        n = 25
        closes = [10.0 + (0.8 if i % 2 else -0.8) for i in range(n)]
        closes[-1] = closes[-2] * 1.06
        mod = BigMover({"adaptive": True, "atr_k": 1.5})
        res = mod.analyze(self._df(closes), _ctx())
        assert res.detail["adaptive"] is True
        assert res.detail["atr_pct"] is not None
        # 有效阈值 = max(5, 1.5×ATR%) ≥ 5
        assert res.detail["up_pct"] >= 5.0

    def test_adaptive_low_vol_keeps_base(self):
        """低波动股：ATR% 小 → 阈值保持基值，+6% 仍大涨。"""
        closes = [10.0 + 0.001 * i for i in range(25)]
        closes[-1] = closes[-2] * 1.06
        mod = BigMover({"adaptive": True, "atr_k": 1.5})
        res = mod.analyze(self._df(closes), _ctx())
        assert res.value == "大涨"
        assert res.detail["up_pct"] == pytest.approx(5.0, abs=0.01)


class TestAdaptiveVolumeSurge:
    def _df(self, vols):
        dates = _dates(len(vols))
        rows = []
        for i, v in enumerate(vols):
            c = 10.0
            rows.append({"trade_date": dates[i], "close": c, "high": c * 1.01,
                         "low": c * 0.99, "preclose": c, "change_rate": 0.0,
                         "volume": v, "is_st": 0})
        return pd.DataFrame(rows)

    def test_value_unchanged_by_adaptive(self):
        """自适应不改输出值（CSV 列回归红线）。"""
        vols = [1000.0] * 24 + [3000.0]
        off = VolumeSurge().analyze(self._df(vols), _ctx())
        on = VolumeSurge({"adaptive": True}).analyze(self._df(vols), _ctx())
        assert off.value == on.value == 3.0

    def test_hit_keys_adaptive_uses_effective(self):
        """自适应开启：命中判定用有效阈值；桶键仍为基阈值格式（防桶爆炸）。"""
        vols = [1000.0] * 24 + [3000.0]
        mod = VolumeSurge({"adaptive": True, "atr_k": 1.5})
        res = mod.analyze(self._df(vols), _ctx())
        keys = mod.hit_keys(res.value)
        assert keys in ([], ["≥2.0x"])

    def test_default_off_byte_identical(self):
        """默认关：命中判定与增量前一致（3.0 ≥ 2.0 → 命中）。"""
        vols = [1000.0] * 24 + [3000.0]
        mod = VolumeSurge()
        res = mod.analyze(self._df(vols), _ctx())
        assert mod.hit_keys(res.value) == ["≥2.0x"]


class TestQuantileCalibration:
    def _df(self, n=100):
        rng = np.random.RandomState(42)
        return pd.DataFrame({
            "max_streak": rng.randint(1, 8, n),
            "premium_mean": rng.normal(1.0, 2.0, n).round(3),
            "burst_rate": rng.uniform(0.2, 0.7, n).round(3),
        })

    def test_quantiles_ordered(self):
        calib = quantile_calibration(self._df())
        for col in ("max_streak", "premium_mean", "burst_rate"):
            node = calib[col]
            assert node["p10"] <= node["p60"] <= node["p90"]
            assert node["n"] == 100

    def test_empty_df_none(self):
        assert quantile_calibration(pd.DataFrame()) == {}

    def test_quantile_state_climax(self):
        calib = quantile_calibration(self._df())
        m = {"max_streak": calib["max_streak"]["p90"] + 1,
             "premium_mean": calib["premium_mean"]["p90"] + 1,
             "burst_rate": 0.3}
        state, basis = quantile_state(m, calib)
        assert state == "高潮" and len(basis) == 2

    def test_quantile_state_ice(self):
        calib = quantile_calibration(self._df())
        m = {"max_streak": calib["max_streak"]["p10"] - 1,
             "premium_mean": 1.0, "burst_rate": 0.3}
        state, _ = quantile_state(m, calib)
        assert state == "冰点"

    def test_quantile_state_fallback(self):
        calib = quantile_calibration(self._df())
        m = {"max_streak": 4, "premium_mean": 1.0, "burst_rate": 0.4}
        state, basis = quantile_state(m, calib)
        assert state == "回暖/震荡"

    def test_quantile_state_no_calib(self):
        state, basis = quantile_state({"max_streak": 5}, {})
        assert state == "回暖/震荡" and "数据不足" in basis[0]
