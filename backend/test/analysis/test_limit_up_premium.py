"""M5 limit_up_premium 单测：昨日涨停有溢价、未涨停为否、不可判定、无成交、边界。"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.limit_up_premium import LimitUpPremium


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


class TestLimitUpPremium:
    def setup_method(self):
        self.mod = LimitUpPremium()

    def test_prev_limit_up_positive_premium(self):
        df = _df([
            {"trade_date": "20260922", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "volume": 1000.0, "is_st": 0},
            {"trade_date": "20260923", "change_rate": 3.5, "close": 11.385, "high": 11.5,
             "volume": 900.0, "is_st": 0},
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.value == 3.5
        assert res.detail["prev_date"] == "20260922"

    def test_prev_limit_up_negative_premium(self):
        """昨日涨停今日大跌 → 负溢价（亏钱效应）。"""
        df = _df([
            {"trade_date": "20260922", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "volume": 1000.0, "is_st": 0},
            {"trade_date": "20260923", "change_rate": -5.2, "close": 10.428, "high": 10.9,
             "volume": 900.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == -5.2

    def test_prev_not_limit_up_is_no(self):
        df = _df([
            {"trade_date": "20260922", "change_rate": 5.0, "close": 10.5, "high": 10.6,
             "volume": 1000.0, "is_st": 0},
            {"trade_date": "20260923", "change_rate": 3.0, "close": 10.815, "high": 11.0,
             "volume": 900.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "否"

    def test_st_prev_limit_up(self):
        df = _df([
            {"trade_date": "20260922", "change_rate": 5.0, "close": 10.5, "high": 10.5,
             "volume": 1000.0, "is_st": 1},
            {"trade_date": "20260923", "change_rate": 1.8, "close": 10.689, "high": 10.8,
             "volume": 900.0, "is_st": 1},
        ])
        assert self.mod.analyze(df, _ctx()).value == 1.8

    def test_suspension_gap_uses_previous_available_day(self):
        """锚定日前一交易日停牌（缺行）→ 以前一可得交易日为基准。"""
        df = _df([
            {"trade_date": "20260918", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "volume": 1000.0, "is_st": 0},
            # 0919~0922 停牌缺行
            {"trade_date": "20260923", "change_rate": -1.0, "close": 10.89, "high": 11.1,
             "volume": 800.0, "is_st": 0},
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.value == -1.0
        assert res.detail["prev_date"] == "20260918"

    def test_prev_day_suspension_marker_is_not_limit_up(self):
        """前一交易日为停牌标记行（volume=0、change_rate=0）→ 未涨停 → 否。"""
        df = _df([
            {"trade_date": "20260922", "change_rate": 0.0, "close": 11.0, "high": 11.0,
             "volume": 0.0, "is_st": 0},
            {"trade_date": "20260923", "change_rate": 2.0, "close": 11.22, "high": 11.3,
             "volume": 900.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == "否"

    def test_anchor_no_volume_is_insufficient(self):
        """锚定日为停牌标记行 → 数据不足。"""
        df = _df([
            {"trade_date": "20260922", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "volume": 1000.0, "is_st": 0},
            {"trade_date": "20260923", "change_rate": 0.0, "close": 11.0, "high": 11.0,
             "volume": 0.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_only_one_row_is_insufficient(self):
        df = _df([
            {"trade_date": "20260923", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "volume": 1000.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_prev_before_limit_system_is_insufficient(self):
        df = _df([
            {"trade_date": "19960102", "change_rate": 30.0, "close": 13.0, "high": 13.0,
             "volume": 1000.0, "is_st": 0},
            {"trade_date": "19960103", "change_rate": 5.0, "close": 13.65, "high": 13.8,
             "volume": 900.0, "is_st": 0},
        ])
        res = self.mod.analyze(df, _ctx(as_of="19960103"))
        assert res.value == DATA_INSUFFICIENT

    def test_anchor_date_mismatch_is_insufficient(self):
        df = _df([
            {"trade_date": "20260918", "change_rate": 10.0, "close": 11.0, "high": 11.0,
             "volume": 1000.0, "is_st": 0},
        ])
        assert self.mod.analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_empty_is_insufficient(self):
        assert self.mod.analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT
