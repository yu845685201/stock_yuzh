"""M6 big_mover 单测：大涨、大跌、否、参数覆盖、参数非法、无成交。"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.big_mover import BigMover


def _ctx(as_of="20260923", list_date=None):
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(
            ts_code="sh.600375", stock_code="600375", stock_name="测试股",
            list_date=list_date, board="主板",
        ),
    )


def _df(rows):
    return pd.DataFrame(rows)


class TestBigMover:
    def setup_method(self):
        self.mod = BigMover()

    def test_big_up(self):
        df = _df([{"trade_date": "20260923", "change_rate": 6.5, "volume": 100.0}])
        assert self.mod.analyze(df, _ctx()).value == "大涨"

    def test_big_down(self):
        df = _df([{"trade_date": "20260923", "change_rate": -7.2, "volume": 100.0}])
        assert self.mod.analyze(df, _ctx()).value == "大跌"

    def test_normal_is_no(self):
        df = _df([{"trade_date": "20260923", "change_rate": 4.99, "volume": 100.0}])
        assert self.mod.analyze(df, _ctx()).value == "否"
        df2 = _df([{"trade_date": "20260923", "change_rate": -5.0, "volume": 100.0}])
        assert self.mod.analyze(df2, _ctx()).value == "大跌"  # 边界：恰为阈值计入

    def test_threshold_boundary_inclusive(self):
        df = _df([{"trade_date": "20260923", "change_rate": 5.0, "volume": 100.0}])
        assert self.mod.analyze(df, _ctx()).value == "大涨"

    def test_first_listing_day_still_judged(self):
        """新股上市首日不跳过：首日暴涨同样是异动事实。"""
        df = _df([{"trade_date": "20260923", "change_rate": 44.0, "volume": 100.0}])
        assert self.mod.analyze(df, _ctx(list_date="20260923")).value == "大涨"

    def test_params_override(self):
        mod = BigMover({"up_pct": 9.0, "down_pct": -9.0})
        df = _df([{"trade_date": "20260923", "change_rate": 6.5, "volume": 100.0}])
        assert mod.analyze(df, _ctx()).value == "否"

    def test_unknown_param_rejected(self):
        with pytest.raises(ValueError):
            BigMover({"foo": 1})

    def test_illegal_params_insufficient(self):
        mod = BigMover({"up_pct": -5.0, "down_pct": 5.0})
        df = _df([{"trade_date": "20260923", "change_rate": 6.0, "volume": 100.0}])
        assert mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_no_volume_is_insufficient(self):
        df = _df([{"trade_date": "20260923", "change_rate": 0.0, "volume": 0.0}])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_missing_change_rate_is_insufficient(self):
        df = _df([{"trade_date": "20260923", "change_rate": None, "volume": 100.0}])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_anchor_date_mismatch_is_insufficient(self):
        df = _df([{"trade_date": "20260918", "change_rate": 6.0, "volume": 100.0}])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_empty_is_insufficient(self):
        assert self.mod.analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT
