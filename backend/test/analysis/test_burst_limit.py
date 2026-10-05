"""M4 burst_limit 单测：触板未封、封住不算炸板、未触板、缺 preclose、边界阈值。"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.burst_limit import BurstLimit


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


class TestBurstLimit:
    def setup_method(self):
        self.mod = BurstLimit()

    def test_touched_but_not_sealed(self):
        """盘中触及涨停价（high 涨幅 ≥ 9.90）但收盘未封住 → 是。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": 5.0, "close": 10.5, "high": 11.0,
             "preclose": 10.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "是"

    def test_sealed_is_not_burst(self):
        """收于涨停（封住）不算炸板。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "preclose": 10.0, "is_st": 0},
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.value == "否"
        assert res.detail["sealed"] is True

    def test_not_touched(self):
        """盘中最高涨幅未达阈值且未封住 → 否。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": 5.0, "close": 10.5, "high": 10.8,
             "preclose": 10.0, "is_st": 0},
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.value == "否"
        assert res.detail["touched"] is False

    def test_high_gain_below_threshold_by_epsilon(self):
        """最高涨幅 9.89%（阈值 9.90）→ 未触板。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": 3.0, "close": 10.3, "high": 10.989,
             "preclose": 10.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "否"

    def test_open_at_limit_then_fall_counts_as_burst(self):
        """一字开板后回落：开盘即涨停价（high=涨停价）但收盘回落 → 是。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": 2.0, "close": 10.2, "high": 11.0,
             "preclose": 10.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "是"

    def test_gem_20pct_threshold(self):
        """创业板触板阈值 19.90。"""
        df = _df([
            {"trade_date": "20260923", "change_rate": 10.0, "close": 11.0, "high": 12.0,
             "preclose": 10.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx(board="创业板", stock_code="300750")).value == "是"

    def test_missing_preclose_is_insufficient(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": 5.0, "close": 10.5, "high": 11.0,
             "preclose": None, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_first_listing_day_is_insufficient(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": 44.0, "close": 14.4, "high": 14.4,
             "preclose": 10.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx(list_date="20260923")).value == DATA_INSUFFICIENT

    def test_anchor_date_mismatch_is_insufficient(self):
        df = _df([
            {"trade_date": "20260918", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "preclose": 10.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_empty_is_insufficient(self):
        assert self.mod.analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT

    def test_detail_carries_evidence(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": 5.0, "close": 10.5, "high": 11.0,
             "preclose": 10.0, "is_st": 0},
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.detail["high_gain"] == 10.0
        assert res.detail["limit_pct"] == 10.0
        assert res.detail["sealed"] is False
