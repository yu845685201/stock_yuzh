"""M3 limit_down_today 单测：正常跌停、未跌停、ST、上市首日、制度前、数据不足。"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.limit_down_today import LimitDownToday


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


class TestLimitDownToday:
    def setup_method(self):
        self.mod = LimitDownToday()

    def test_limit_down(self):
        df = _df([
            {"trade_date": "20260922", "change_rate": -1.0, "close": 9.9, "low": 9.8, "is_st": 0},
            {"trade_date": "20260923", "change_rate": -10.0, "close": 9.0, "low": 9.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "是"

    def test_not_limit_down(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": -9.89, "close": 9.01, "low": 9.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "否"

    def test_close_not_low_is_not_limit_down(self):
        """跌幅达标但收盘不是当日最低价 → 护栏判否。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": -10.02, "close": 9.0, "low": 8.8, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "否"

    def test_st_limit_down(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": -5.0, "close": 9.5, "low": 9.5, "is_st": 1},
        ])
        assert self.mod.analyze(df, _ctx()).value == "是"

    def test_gem_20pct(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": -19.95, "close": 8.0, "low": 8.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx(board="创业板", stock_code="300750")).value == "是"

    def test_first_listing_day_is_insufficient(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": -30.0, "close": 7.0, "low": 7.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx(list_date="20260923")).value == DATA_INSUFFICIENT

    def test_before_limit_system_is_insufficient(self):
        df = _df([
            {"trade_date": "19960102", "change_rate": -15.0, "close": 8.5, "low": 8.5, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx(as_of="19960102")).value == DATA_INSUFFICIENT

    def test_anchor_date_mismatch_is_insufficient(self):
        df = _df([
            {"trade_date": "20260918", "change_rate": -10.0, "close": 9.0, "low": 9.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_empty_is_insufficient(self):
        assert self.mod.analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT

    def test_detail_carries_evidence(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": -10.0, "close": 9.0, "low": 9.0, "is_st": 0},
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.detail["limit_pct"] == 10.0
        assert res.detail["change_rate"] == -10.0
        assert res.detail["low"] == 9.0
