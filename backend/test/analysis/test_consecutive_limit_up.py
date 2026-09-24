"""M2 consecutive_limit_up 单测：连板计数、停牌缺口、次新、max_lookback 截断。"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.consecutive_limit_up import ConsecutiveLimitUp


def _ctx(as_of="20260923", list_date=None, board="主板", stock_code="600375"):
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(
            ts_code="sh.600375", stock_code=stock_code, stock_name="测试股",
            list_date=list_date, board=board,
        ),
    )


def _bar(date, cr, price, is_st=0):
    """构造一根 K 线：close == high 表示收于当日最高价。"""
    return {"trade_date": date, "change_rate": cr, "close": price, "high": price,
            "is_st": is_st, "preclose": price}


class TestConsecutiveLimitUp:
    def setup_method(self):
        self.mod = ConsecutiveLimitUp()

    def test_anchor_not_limit_up_returns_zero(self):
        df = pd.DataFrame([_bar("20260922", 10.0, 11.0), _bar("20260923", 1.0, 11.11)])
        assert self.mod.analyze(df, _ctx()).value == 0

    def test_single_limit_up(self):
        df = pd.DataFrame([_bar("20260921", -2.0, 10.0), _bar("20260922", 0.5, 10.05),
                           _bar("20260923", 10.0, 11.055)])
        assert self.mod.analyze(df, _ctx()).value == 1

    def test_four_consecutive(self):
        df = pd.DataFrame([
            _bar("20260917", -1.0, 8.0),
            _bar("20260918", 10.0, 8.8),
            _bar("20260921", 10.0, 9.68),
            _bar("20260922", 10.0, 10.648),
            _bar("20260923", 10.0, 11.7128),
        ])
        res = self.mod.analyze(df, _ctx())
        assert res.value == 4
        assert res.detail["truncated"] is False

    def test_streak_broken_by_non_limit_day(self):
        df = pd.DataFrame([
            _bar("20260918", 10.0, 8.8),
            _bar("20260921", 3.0, 9.064),   # 中断
            _bar("20260922", 10.0, 9.97),
            _bar("20260923", 10.0, 10.967),
        ])
        assert self.mod.analyze(df, _ctx()).value == 2

    def test_suspension_gap_does_not_break_streak(self):
        """停牌造成的缺行按可得行计算（§3.9 边界）。"""
        df = pd.DataFrame([
            _bar("20260911", 10.0, 8.8),
            # 20260914~20260917 停牌（无行）
            _bar("20260918", 10.0, 9.68),
            _bar("20260923", 10.0, 10.648),
        ])
        assert self.mod.analyze(df, _ctx()).value == 3

    def test_next_new_stock_insufficient_when_only_one_bar(self):
        """次新股：锚定日涨停但之前无任何交易日 → 数据不足。"""
        df = pd.DataFrame([_bar("20260923", 10.0, 11.0)])
        ctx = _ctx(list_date="20260922")
        assert self.mod.analyze(df, ctx).value == DATA_INSUFFICIENT

    def test_first_listing_day_insufficient(self):
        df = pd.DataFrame([_bar("20260922", 1.0, 10.0), _bar("20260923", 10.0, 11.0)])
        ctx = _ctx(list_date="20260923")
        assert self.mod.analyze(df, ctx).value == DATA_INSUFFICIENT

    def test_max_lookback_truncates_streak(self):
        rows = [_bar("20260101", -1.0, 5.0)]
        price = 5.0
        for i in range(30):
            price *= 1.1
            rows.append(_bar(f"202602{i + 1:02d}" if i < 9 else f"202603{i:02d}", 10.0, price))
        mod = ConsecutiveLimitUp({"max_lookback": 5})
        res = mod.analyze(pd.DataFrame(rows), _ctx(as_of=rows[-1]["trade_date"]))
        assert res.value == 5
        assert res.detail["truncated"] is True

    def test_before_limit_system_insufficient(self):
        df = pd.DataFrame([_bar("19960102", 30.0, 13.0), _bar("19960103", 30.0, 16.9)])
        ctx = _ctx(as_of="19960103")
        assert self.mod.analyze(df, ctx).value == DATA_INSUFFICIENT

    def test_unknown_param_rejected(self):
        with pytest.raises(ValueError):
            ConsecutiveLimitUp({"max_lookback": 10, "typo": 1})

    def test_default_params_declared(self):
        assert ConsecutiveLimitUp().param("max_lookback") == 60
        assert ConsecutiveLimitUp().effective_lookback() == 60
