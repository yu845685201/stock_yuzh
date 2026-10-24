"""涨停/跌停规则单测：板块识别、制度分段阈值、向量化判定。"""

import pandas as pd
import pytest

from src.analysis.limit_up_rules import (
    BOARD_BSE,
    BOARD_GEM,
    BOARD_MAIN,
    BOARD_STAR,
    BOARD_UNKNOWN,
    annotate_limit_down,
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


class TestAnnotateLimitDown:
    def test_main_board_threshold_with_guard(self):
        df = _frame(
            [
                # 跌停：-10.00%，收于最低价
                {"trade_date": "20260922", "change_rate": -10.0, "close": 9.0, "low": 9.0, "is_st": 0},
                # 未跌停：-9.89%，未达 -9.90 阈值
                {"trade_date": "20260923", "change_rate": -9.89, "close": 9.01, "low": 9.0, "is_st": 0},
            ]
        )
        out = annotate_limit_down(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_down"]) is True
        assert bool(out.loc[1, "_limit_down"]) is False

    def test_close_not_low_is_not_limit_down(self):
        """跌幅达标但收盘价不是当日最低价 → 不可能收于跌停价，判否（护栏）。"""
        df = _frame(
            [{"trade_date": "20260923", "change_rate": -10.02, "close": 9.0, "low": 8.8, "is_st": 0}]
        )
        out = annotate_limit_down(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_down"]) is False

    def test_st_uses_5pct_threshold(self):
        df = _frame(
            [
                {"trade_date": "20260923", "change_rate": -5.0, "close": 9.5, "low": 9.5, "is_st": 1},
                {"trade_date": "20260922", "change_rate": -4.85, "close": 9.61, "low": 9.5, "is_st": 1},
            ]
        )
        out = annotate_limit_down(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_down"]) is True      # 5% 档达标（阈值 -4.90）
        assert bool(out.loc[1, "_limit_down"]) is False     # -4.85 未达阈值

    def test_gem_20pct(self):
        df = _frame(
            [
                {"trade_date": "20260923", "change_rate": -19.95, "close": 8.0, "low": 8.0, "is_st": 0},
                {"trade_date": "20260922", "change_rate": -10.05, "close": 9.0, "low": 8.8, "is_st": 0},
            ]
        )
        out = annotate_limit_down(df, "300750", BOARD_GEM)
        assert bool(out.loc[0, "_limit_down"]) is True
        assert bool(out.loc[1, "_limit_down"]) is False

    def test_before_limit_system_is_unknown(self):
        df = _frame(
            [{"trade_date": "19960102", "change_rate": -15.0, "close": 9.0, "low": 9.0, "is_st": 0}]
        )
        out = annotate_limit_down(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_down_known"]) is False
        assert bool(out.loc[0, "_limit_down"]) is False

    def test_missing_change_rate_is_unknown(self):
        df = _frame(
            [{"trade_date": "20260923", "change_rate": None, "close": 9.0, "low": 9.0, "is_st": 0}]
        )
        out = annotate_limit_down(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_down_known"]) is False

    def test_low_missing_does_not_block_detection(self):
        """low 缺失时不启用护栏，退化为纯阈值法。"""
        df = _frame(
            [{"trade_date": "20260923", "change_rate": -10.0, "close": 9.0, "low": None, "is_st": 0}]
        )
        out = annotate_limit_down(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_down"]) is True

    def test_does_not_modify_input(self):
        df = _frame(
            [{"trade_date": "20260923", "change_rate": -10.0, "close": 9.0, "low": 9.0, "is_st": 0}]
        )
        annotate_limit_down(df, "600375", BOARD_MAIN)
        assert "_limit_down" not in df.columns


# ---------------------------------------------------------------- P0-1/P0-2（V2.0 2026-10-24）

class TestDynamicThreshold:
    """P0-1 价格量级动态阈值：低价股涨停补判 + raw_close 回退。"""

    @staticmethod
    def _row(change_rate, close, high, is_st=0, raw_close=None, preclose=None):
        r = {"trade_date": "20261023", "change_rate": change_rate,
             "close": close, "high": high, "is_st": is_st}
        if raw_close is not None:
            r["raw_close"] = raw_close
        if preclose is not None:
            r["preclose"] = preclose
        return r

    def test_low_price_main_board_hit(self):
        """低价主板 2.04→2.24 = +9.80%（真实涨停）→ 动态阈值 9.755 命中；
        次一个可达 tick 2.23 = +9.31% 不误判"""
        df = _frame([
            self._row(9.8039, 2.24, 2.24, raw_close=2.24),   # 涨停（raw=close，最新日口径）
            self._row(9.31, 2.23, 2.23, raw_close=2.23),     # 未涨停
        ])
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is True
        assert bool(out.loc[0, "_limit_exact"]) is True
        assert bool(out.loc[1, "_limit_up"]) is False

    def test_low_price_st_5pct(self):
        """ST 低价 1.02→1.07 = +4.90%（5% 档真实涨停）"""
        df = _frame([self._row(4.902, 1.07, 1.07, is_st=1, raw_close=1.07)])
        out = annotate_limit_up(df, "600001", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is True

    def test_low_price_gem_20cm(self):
        """创业板低价 2.00→2.40 = +20%（20cm 真实涨停）；2.39=+19.5% 不判"""
        df = _frame([
            self._row(20.0, 2.40, 2.40, raw_close=2.40),
            self._row(19.5, 2.39, 2.39, raw_close=2.39),
        ])
        out = annotate_limit_up(df, "300001", BOARD_GEM)
        assert bool(out.loc[0, "_limit_up"]) is True
        assert bool(out.loc[1, "_limit_up"]) is False

    def test_low_price_bse_30pct(self):
        """北交所低价 2.00→2.60 = +30% 真实涨停"""
        df = _frame([self._row(30.0, 2.60, 2.60, raw_close=2.60)])
        out = annotate_limit_up(df, "920001", BOARD_BSE)
        assert bool(out.loc[0, "_limit_up"]) is True

    def test_raw_close_missing_falls_back(self):
        """raw_close 缺失 → 回退固定下浮 0.10 并标 _limit_exact=False（口径注记随行）"""
        df = _frame([
            self._row(9.95, 10.95, 10.95),                    # 无 raw_close 列 → 固定阈值 9.90 命中
        ])
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is True
        assert bool(out.loc[0, "_limit_exact"]) is False

    def test_down_mirror_low_price(self):
        """跌停镜像：低价 2.04→1.84 = -9.80% 真实跌停命中"""
        df = _frame([{"trade_date": "20261023", "change_rate": -9.8039,
                      "close": 1.84, "low": 1.84, "is_st": 0, "raw_close": 1.84}])
        out = annotate_limit_down(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_down"]) is True

    def test_guard_still_rejects(self):
        """护栏不回归：涨幅达标但 close<high → 不判"""
        df = _frame([self._row(9.9, 10.90, 11.0, raw_close=10.90)])
        out = annotate_limit_up(df, "600375", BOARD_MAIN)
        assert bool(out.loc[0, "_limit_up"]) is False


class TestNewStockNoLimitWindow:
    """P0-2 注册制次新无涨跌幅窗口：前 5 交易日（北交所首日）不判定。"""

    @staticmethod
    def _panel(dates, board_code, list_date, change=25.0, close_high=True):
        rows = []
        for d in dates:
            r = {"trade_date": d, "change_rate": change,
                 "close": 10.0 if close_high else 9.0,
                 "high": 10.0 if close_high else 10.0, "is_st": 0,
                 "raw_close": 10.0}
            rows.append(r)
        return pd.DataFrame(rows), board_code, list_date

    def test_registration_day3_not_limit(self):
        """注册制（创业板）上市第 3 日 +25% → 不判涨停、_limit_known=False"""
        dates = ["20260921", "20260922", "20260923", "20260924", "20260925"]
        df, _, _ = self._panel(dates, "300001", "20260921")
        out = annotate_limit_up(df, "300001", BOARD_GEM, list_date="20260921")
        # 前 5 日全部 NaN（无限制）
        assert not out["_limit_known"].any()
        assert not out["_limit_up"].any()

    def test_day6_recovers(self):
        """第 6 日起恢复判定（10%→创业板 20% 上限，+25% 达标且收最高 → 涨停）"""
        dates = ["20260921", "20260922", "20260923", "20260924", "20260925",
                 "20260928", "20260929"]
        df, _, _ = self._panel(dates, "300001", "20260921", change=25.0)
        out = annotate_limit_up(df, "300001", BOARD_GEM, list_date="20260921")
        assert bool(out.loc[5, "_limit_known"]) is True      # 第 6 日 20260928
        assert bool(out.loc[5, "_limit_up"]) is True         # 25% ≥ 19.9 且收最高
        assert bool(out.loc[6, "_limit_known"]) is True

    def test_bse_first_day_only(self):
        """北交所仅首日无限制（次日起 30% 判定恢复）"""
        dates = ["20261116", "20261117", "20261118"]
        df, _, _ = self._panel(dates, "920001", "20261116", change=31.0)
        out = annotate_limit_up(df, "920001", BOARD_BSE, list_date="20261116")
        assert not bool(out.loc[0, "_limit_known"])          # 首日
        assert bool(out.loc[1, "_limit_known"]) is True      # 次日恢复
        assert bool(out.loc[1, "_limit_up"]) is True         # 31% ≥ 29.9（+护栏）

    def test_old_stock_window_not_misflagged(self):
        """老股滚窗：list_date 远早于窗口首日 → 窗口内不标无限制（不误清零）"""
        dates = ["20260921", "20260922", "20260923"]
        df, _, _ = self._panel(dates, "600000", "20200101", change=10.0)
        out = annotate_limit_up(df, "600000", BOARD_MAIN, list_date="20200101")
        assert out["_limit_known"].all()

    def test_window_start_within_tolerance(self):
        """窗口起点与上市日差距在容差内（≤7 自然日）→ 窗口内前几日仍标无限制"""
        dates = ["20260924", "20260925", "20260928"]   # 上市 20260921，缺前 3 行
        df, _, _ = self._panel(dates, "300001", "20260921", change=25.0)
        out = annotate_limit_up(df, "300001", BOARD_GEM, list_date="20260921")
        # 首行 20260924 距上市 3 天 ≤7 容差 → 按行序第 1 行起算，仍在 5 日窗口
        assert not bool(out.loc[0, "_limit_known"])

    def test_pre_registration_listing_unaffected(self):
        """注册制前上市（如 2019 创业板）不适用 5 日窗口"""
        dates = ["20190902", "20190903", "20190904", "20190905", "20190906"]
        df, _, _ = self._panel(dates, "300001", "20190902", change=10.0)
        out = annotate_limit_up(df, "300001", BOARD_GEM, list_date="20190902")
        assert out["_limit_known"].all()   # 首日由 suppress/模块层另行处理

    def test_no_list_date_unchanged(self):
        """未传 list_date → 行为与旧版完全一致（兼容性）"""
        df, _, _ = self._panel(["20260923"], "300001", None, change=25.0)
        out = annotate_limit_up(df, "300001", BOARD_GEM)
        assert bool(out.loc[0, "_limit_known"]) is True
        assert bool(out.loc[0, "_limit_up"]) is True
