"""M1 limit_up_today 单测：正常涨停、未涨停、上市首日、制度前、数据不足。"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.limit_up_today import LimitUpToday


def _ctx(as_of="20260923", list_date=None, board="主板", stock_code="600375"):
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(
            ts_code="sh.600375", stock_code=stock_code, stock_name="测试股",
            list_date=list_date, board=board,
        ),
    )


def _df(rows):
    return pd.DataFrame(rows)


class TestLimitUpToday:
    def setup_method(self):
        self.mod = LimitUpToday()

    def test_limit_up(self):
        df = _df([
            {"trade_date": "20260922", "change_rate": 1.0, "close": 10.0, "high": 10.1, "is_st": 0, "preclose": 9.9},
            {"trade_date": "20260923", "change_rate": 10.0, "close": 11.0, "high": 11.0, "is_st": 0, "preclose": 10.0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "是"

    def test_not_limit_up(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": 9.89, "close": 10.99, "high": 11.0, "is_st": 0, "preclose": 10.0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "否"

    def test_first_listing_day_is_insufficient(self):
        """上市首日不判定（§3.9）。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": 44.0, "close": 14.4, "high": 14.4, "is_st": 0, "preclose": 10.0},
        ])
        ctx = _ctx(list_date="20260923")
        assert self.mod.analyze(df, ctx).value == DATA_INSUFFICIENT

    def test_before_limit_system_is_insufficient(self):
        df = _df([
            {"trade_date": "19960102", "change_rate": 30.0, "close": 13.0, "high": 13.0, "is_st": 0, "preclose": 10.0},
        ])
        ctx = _ctx(as_of="19960102")
        assert self.mod.analyze(df, ctx).value == DATA_INSUFFICIENT

    def test_anchor_date_mismatch_is_insufficient(self):
        """停牌导致锚定日无 K 线 → 数据不足，不能用旧数据冒充当日。"""
        df = _df([
            {"trade_date": "20260918", "change_rate": 10.0, "close": 11.0, "high": 11.0, "is_st": 0, "preclose": 10.0},
        ])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_empty_is_insufficient(self):
        assert self.mod.analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT

    def test_st_limit_up(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": 5.0, "close": 10.5, "high": 10.5, "is_st": 1, "preclose": 10.0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "是"

    def test_detail_carries_evidence(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": 10.0, "close": 11.0, "high": 11.0, "is_st": 0, "preclose": 10.0},
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.detail["limit_pct"] == 10.0
        assert res.detail["change_rate"] == 10.0
