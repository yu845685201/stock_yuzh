"""涨停规则单测：板块识别、制度分段阈值、向量化判定。"""

import pandas as pd
import pytest

from src.analysis.limit_up_rules import (
    BOARD_BSE,
    BOARD_GEM,
    BOARD_MAIN,
    BOARD_STAR,
    BOARD_UNKNOWN,
    annotate_limit_up,
    board_of,
    limit_pct,
)


class TestBoardOf:
    @pytest.mark.parametrize(
        "code,expected",
        [
            ("600375", BOARD_MAIN),
            ("000001", BOARD_MAIN),
            ("002594", BOARD_MAIN),
            ("300750", BOARD_GEM),
            ("688981", BOARD_STAR),
            ("830799", BOARD_BSE),
            ("430418", BOARD_BSE),
            ("920002", BOARD_BSE),
            ("510300", BOARD_UNKNOWN),  # ETF，不在股票池
            ("", BOARD_UNKNOWN),
        ],
    )
    def test_board_of(self, code, expected):
        assert board_of(code) == expected


class TestLimitPct:
    def test_main_board_before_limit_system(self):
        assert limit_pct(BOARD_MAIN, "19961215", False) is None

    def test_main_board_and_st(self):
        assert limit_pct(BOARD_MAIN, "19961216", False) == 10.0
        assert limit_pct(BOARD_MAIN, "20260923", True) == 5.0

    def test_gem_switch_to_20pct(self):
        assert limit_pct(BOARD_GEM, "20200823", False) == 10.0
        assert limit_pct(BOARD_GEM, "20200824", False) == 20.0
        # 创业板注册制后 ST 不再享受 5% 档
        assert limit_pct(BOARD_GEM, "20200824", True) == 20.0
        # 开板前
        assert limit_pct(BOARD_GEM, "20091029", False) is None

    def test_star_and_bse(self):
        assert limit_pct(BOARD_STAR, "20190721", False) is None
        assert limit_pct(BOARD_STAR, "20190722", False) == 20.0
        assert limit_pct(BOARD_BSE, "20211114", False) is None
        assert limit_pct(BOARD_BSE, "20211115", False) == 30.0


def _frame(rows):
    return pd.DataFrame(rows)


class TestAnnotateLimitUp:
    def test_main_board_threshold_with_guard(self):
        df = _frame(
            [
                # 涨停：+10.00%，收于最高价
                {"trade_date": "20260922", "change_rate": 10.0, "close": 11.0, "high": 11.0, "is_st": 0},
                # 未涨停：+9.89%，未达 9.90 阈值
                {"trade_date": "20260923", "change_rate": 9.89, "close": 10.99, "high": 11.0, "is_st": 0},
            ]
        )
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is True
        assert bool(out.loc[1, "_limit_up"]) is False

    def test_close_not_high_is_not_limit_up(self):
        """涨幅达标但收盘价不是当日最高价 → 不可能收于涨停价，判否（护栏）。"""
        df = _frame(
            [{"trade_date": "20260923", "change_rate": 10.02, "close": 11.0, "high": 11.5, "is_st": 0}]
        )
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is False

    def test_st_uses_5pct_threshold(self):
        df = _frame(
            [
                {"trade_date": "20260923", "change_rate": 4.95, "close": 10.49, "high": 10.49, "is_st": 1},
                {"trade_date": "20260922", "change_rate": 4.85, "close": 10.39, "high": 10.49, "is_st": 1},
            ]
        )
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is True      # 5% 档达标（阈值 4.90）
        assert bool(out.loc[1, "_limit_up"]) is False     # 4.85 未达阈值

    def test_above_cap_still_counts_as_limit_up_by_threshold(self):
        """阈值法语义：只要涨幅 ≥ 阈值即判定涨停，不设上限。

        涨幅突破制度上限意味着该行数据已被预检标记为「前复权断裂」（R3），
        此处仍按阈值法判定，偏向「识别出涨停」，由存疑列负责提示数据可信度。
        """
        df = _frame(
            [{"trade_date": "20260923", "change_rate": 9.95, "close": 10.99, "high": 10.99, "is_st": 1}]
        )
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is True
        assert float(out.loc[0, "_limit_pct"]) == 5.0

    def test_before_limit_system_is_unknown(self):
        df = _frame(
            [{"trade_date": "19960102", "change_rate": 15.0, "close": 11.0, "high": 11.0, "is_st": 0}]
        )
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_known"]) is False
        assert bool(out.loc[0, "_limit_up"]) is False

    def test_missing_change_rate_is_unknown(self):
        df = _frame(
            [{"trade_date": "20260923", "change_rate": None, "close": 11.0, "high": 11.0, "is_st": 0}]
        )
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_known"]) is False

    def test_high_missing_does_not_block_detection(self):
        """high 缺失时不启用护栏，退化为纯阈值法。"""
        df = _frame(
            [{"trade_date": "20260923", "change_rate": 10.0, "close": 11.0, "high": None, "is_st": 0}]
        )
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is True

    def test_gem_20pct(self):
        df = _frame(
            [
                {"trade_date": "20260923", "change_rate": 19.95, "close": 12.0, "high": 12.0, "is_st": 0},
                {"trade_date": "20260922", "change_rate": 10.05, "close": 11.0, "high": 11.0, "is_st": 0},
            ]
        )
        out = annotate_limit_up(df, "300750", BOARD_GEM)
        assert bool(out.loc[0, "_limit_up"]) is True
        assert bool(out.loc[1, "_limit_up"]) is False
