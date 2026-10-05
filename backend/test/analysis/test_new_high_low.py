"""M8 new_high_low 单测：新高、新低、持平为否、窗口不足、边界与参数。"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.new_high_low import NewHighLow


def _ctx(as_of="20260923"):
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(ts_code="sh.600375", stock_code="600375", stock_name="测试股", board="主板"),
    )


def _series(closes, as_of="20260923"):
    """以 as_of 为末行（锚定日）向前按自然日回推生成日期序列（模块只依赖日期有序性）。"""
    end = datetime.strptime(as_of, "%Y%m%d").date()
    n = len(closes)
    dates = [(end - timedelta(days=n - 1 - i)).strftime("%Y%m%d") for i in range(n)]
    return pd.DataFrame({"trade_date": dates, "close": closes, "volume": [100.0] * n})


class TestNewHighLow:
    def setup_method(self):
        self.mod = NewHighLow()

    def test_new_high(self):
        closes = [10.0] * 59 + [10.5, 10.6]
        assert self.mod.analyze(_series(closes), _ctx()).value == "新高"

    def test_new_low(self):
        closes = [10.0] * 59 + [9.5, 9.4]
        assert self.mod.analyze(_series(closes), _ctx()).value == "新低"

    def test_equal_prev_max_is_no(self):
        """持平前高不算创新高（严格大于）。"""
        closes = [10.0] * 59 + [10.5, 10.5]
        assert self.mod.analyze(_series(closes), _ctx()).value == "否"

    def test_inside_range_is_no(self):
        closes = [10.0] * 59 + [10.5, 10.2]
        assert self.mod.analyze(_series(closes), _ctx()).value == "否"

    def test_base_window_excludes_anchor(self):
        """基准为前 60 日，不含当日：prev_max 为前 60 日最大值。"""
        closes = [10.0] * 60 + [12.0]
        res = self.mod.analyze(_series(closes), _ctx())
        assert res.value == "新高"
        assert res.detail["prev_max"] == 10.0

    def test_insufficient_history(self):
        """前 60 日不足（次新）→ 数据不足，不降窗口。"""
        closes = [10.0] * 40 + [11.0]
        assert self.mod.analyze(_series(closes), _ctx()).value == DATA_INSUFFICIENT

    def test_anchor_suspension_marker_insufficient(self):
        """锚定日为停牌标记行（volume=0，close 复制昨收）→ 数据不足。"""
        closes = [10.0] * 61
        df = _series(closes)
        df.loc[df.index[-1], "volume"] = 0.0
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_window_param_override(self):
        mod = NewHighLow({"window": 5})
        closes = [10.0, 10.0, 10.0, 10.0, 10.0, 11.0]
        assert mod.analyze(_series(closes), _ctx()).value == "新高"

    def test_illegal_window(self):
        mod = NewHighLow({"window": 0})
        closes = [10.0] * 10
        assert mod.analyze(_series(closes), _ctx()).value == DATA_INSUFFICIENT

    def test_unknown_param_rejected(self):
        with pytest.raises(ValueError):
            NewHighLow({"foo": 1})

    def test_anchor_date_mismatch_is_insufficient(self):
        closes = [10.0] * 61
        assert self.mod.analyze(_series(closes), _ctx(as_of="20260930")).value == DATA_INSUFFICIENT

    def test_empty_is_insufficient(self):
        assert self.mod.analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT

    def test_detail_carries_range(self):
        closes = [10.0] * 59 + [9.5, 10.6]
        res = self.mod.analyze(_series(closes), _ctx())
        assert res.detail["prev_max"] == 10.0
        assert res.detail["prev_min"] == 9.5
        assert res.detail["window"] == 60
