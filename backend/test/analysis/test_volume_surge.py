"""M7 volume_surge 单测：常规倍数、基数不含当日、窗口不足、无量行剔除、边界。"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.volume_surge import VolumeSurge


def _ctx(as_of="20260923"):
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(ts_code="sh.600375", stock_code="600375", stock_name="测试股", board="主板"),
    )


def _series(vols, start="20260916"):
    """生成连续自然日序列（仅作日期递增用，非真实交易日历）。"""
    return pd.DataFrame({
        "trade_date": [str(int(start) + i) for i in range(len(vols))],
        "volume": vols,
    })


class TestVolumeSurge:
    def setup_method(self):
        self.mod = VolumeSurge()

    def test_ratio_excludes_anchor_from_base(self):
        """基数 = 前 5 日均量，不含当日。"""
        df = _series([100.0, 100.0, 100.0, 100.0, 100.0, 500.0])
        assert self.mod.analyze(df, _ctx(as_of="20260921")).value == 5.0

    def test_ratio_below_one(self):
        """缩量 < 1。"""
        df = _series([200.0, 200.0, 200.0, 200.0, 200.0, 100.0])
        assert self.mod.analyze(df, _ctx(as_of="20260921")).value == 0.5

    def test_normal_window(self):
        df = _series([100.0, 120.0, 80.0, 150.0, 50.0, 300.0])  # 均量 100
        assert self.mod.analyze(df, _ctx(as_of="20260921")).value == 3.0

    def test_insufficient_history(self):
        """前 5 日不足 → 数据不足（不降窗口）。"""
        df = _series([100.0, 100.0, 100.0, 200.0])
        assert self.mod.analyze(df, _ctx(as_of="20260919")).value == DATA_INSUFFICIENT

    def test_zero_volume_in_base_window_insufficient(self):
        """基数窗口含无量行（停牌标记）→ 有效天数不足 → 数据不足。"""
        df = _series([100.0, 100.0, 0.0, 100.0, 100.0, 300.0])
        assert self.mod.analyze(df, _ctx(as_of="20260921")).value == DATA_INSUFFICIENT

    def test_anchor_zero_volume_insufficient(self):
        """锚定日无量（停牌标记行）→ 数据不足。"""
        df = _series([100.0, 100.0, 100.0, 100.0, 100.0, 0.0])
        assert self.mod.analyze(df, _ctx(as_of="20260921")).value == DATA_INSUFFICIENT

    def test_window_param_override(self):
        mod = VolumeSurge({"window": 2})
        df = _series([100.0, 300.0, 100.0, 100.0, 100.0, 600.0])
        # 前 2 日均量 = (100+100)/2 = 100
        assert mod.analyze(df, _ctx(as_of="20260921")).value == 6.0

    def test_illegal_window(self):
        mod = VolumeSurge({"window": 0})
        df = _series([100.0] * 6)
        assert mod.analyze(df, _ctx(as_of="20260921")).value == DATA_INSUFFICIENT

    def test_unknown_param_rejected(self):
        with pytest.raises(ValueError):
            VolumeSurge({"foo": 1})

    def test_anchor_date_mismatch_is_insufficient(self):
        df = _series([100.0] * 6)
        assert self.mod.analyze(df, _ctx(as_of="20260930")).value == DATA_INSUFFICIENT

    def test_empty_is_insufficient(self):
        assert self.mod.analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT

    def test_detail_carries_base(self):
        df = _series([100.0, 120.0, 80.0, 150.0, 50.0, 300.0])
        res = self.mod.analyze(df, _ctx(as_of="20260921"))
        assert res.detail["base_volume"] == 100.0
        assert res.detail["window"] == 5
        assert res.detail["window_start"] == "20260916"
