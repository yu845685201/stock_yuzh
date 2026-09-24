"""数据预检单测（不连库：覆写交易日历来源）。

覆盖：
- 前复权断裂检测（涨跌幅突破制度上限）
- 缺行分流：孤立单日缺行 → 存疑；连续 ≥2 交易日缺行 → 疑似停牌（不计存疑）
- missing_day_mode = all / off 的行为
- 存疑原因串截断
"""

import pandas as pd
import pytest

from src.analysis.data_reader import (
    MISSING_MODE_ALL,
    MISSING_MODE_GAP,
    MISSING_MODE_OFF,
    DataReader,
)
from src.analysis.module_base import StockInfo


TRADING_DAYS = [f"2026090{i}" for i in range(1, 10)]  # 20260901..20260909


class StubReader(DataReader):
    """覆写交易日历来源，避开数据库。"""

    def __init__(self, trading_days=TRADING_DAYS):
        super().__init__(config_manager=None)
        self._days = trading_days

    def _load_trading_days(self, as_of, start_date=None):
        return list(self._days)


def _kdf(rows):
    df = pd.DataFrame(rows)
    df["ts_code"] = "sh.600375"
    df["stock_code"] = "600375"
    return df


def _bar(date, cr=1.0, close=10.0, high=10.0, is_st=0):
    return {
        "trade_date": date, "change_rate": cr, "close": close, "high": high, "is_st": is_st,
        "preclose": close,
    }


UNIVERSE = [StockInfo(ts_code="sh.600375", stock_code="600375", stock_name="测试股",
                      list_date="20200101", board="主板")]


class TestPrecheckBreak:
    def test_break_over_cap_detected(self):
        rows = [_bar(d) for d in TRADING_DAYS]
        rows[4]["change_rate"] = 20.0   # 主板上限 10% → 断裂
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0],
                                    universe=UNIVERSE)
        reasons = res.suspects["sh.600375"]
        assert any("前复权断裂" in r for r in reasons)
        assert "20260905" in reasons[0]
        assert res.break_count == 1

    def test_within_cap_and_tolerance_not_flagged(self):
        rows = [_bar(d) for d in TRADING_DAYS]
        rows[4]["change_rate"] = 10.5   # 10% + 0.6 容差 = 10.6，未超
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0],
                                    universe=UNIVERSE)
        assert "sh.600375" not in res.suspects

    def test_st_cap_is_5pct(self):
        rows = [_bar(d, is_st=1) for d in TRADING_DAYS]
        rows[4]["change_rate"] = 6.0    # ST 上限 5% + 0.6 → 断裂
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0],
                                    universe=UNIVERSE)
        assert any("前复权断裂" in r for r in res.suspects["sh.600375"])


class TestPrecheckMissingDay:
    def test_isolated_single_gap_is_suspect(self):
        rows = [_bar(d) for d in TRADING_DAYS if d != "20260905"]
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0],
                                    universe=UNIVERSE)
        assert res.suspects["sh.600375"] == ["20260905缺日(上一交易日20260904)"]
        assert res.missing_count == 1
        assert res.suspended == {}

    def test_consecutive_run_is_treated_as_suspension(self):
        rows = [_bar(d) for d in TRADING_DAYS if d not in {"20260904", "20260905", "20260906"}]
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0],
                                    universe=UNIVERSE)
        assert res.suspects == {}
        assert res.suspended["sh.600375"] == ["20260904起连续3个交易日无K线"]

    def test_mode_all_flags_suspension_too(self):
        rows = [_bar(d) for d in TRADING_DAYS if d not in {"20260904", "20260905"}]
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0],
                                    universe=UNIVERSE, missing_day_mode=MISSING_MODE_ALL)
        assert res.suspects["sh.600375"] == [
            "20260904缺日(上一交易日20260903)",
            "20260905缺日(上一交易日20260904)",
        ]
        assert res.missing_count == 2
        assert res.suspended == {}

    def test_mode_off_skips_check(self):
        rows = [_bar(d) for d in TRADING_DAYS if d != "20260905"]
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0],
                                    universe=UNIVERSE, missing_day_mode=MISSING_MODE_OFF)
        assert res.suspects == {}

    def test_illegal_mode_rejected(self):
        with pytest.raises(ValueError, match="missing_day_mode"):
            StubReader().precheck(_kdf([_bar("20260909")]), as_of="20260909", start_date=TRADING_DAYS[0],
                                  missing_day_mode="bogus")

    def test_gap_before_listing_not_flagged(self):
        """上市前无数据不算缺行。"""
        rows = [_bar(d) for d in TRADING_DAYS[3:]]
        uni = [StockInfo(ts_code="sh.600375", stock_code="600375", stock_name="次新",
                         list_date=TRADING_DAYS[3], board="主板")]
        res = StubReader().precheck(_kdf(rows), as_of="20260909", start_date=TRADING_DAYS[0], universe=uni)
        assert res.suspects == {}


class TestPrecheckLimits:
    def test_reason_string_truncated(self):
        days = TRADING_DAYS + ["20260910", "20260911"]
        rows = [_bar(d) for d in days if d not in {"20260902", "20260904", "20260906", "20260908", "20260910"}]
        # 全部为孤立单日缺行
        res = StubReader(days).precheck(_kdf(rows), as_of="20260911", start_date=days[0],
                                        universe=UNIVERSE, reason_limit=3)
        reasons = res.suspects["sh.600375"]
        assert len(reasons) == 4
        assert reasons[-1].startswith("...共5处")

    def test_empty_frame(self):
        res = StubReader().precheck(pd.DataFrame(), as_of="20260909")
        assert res.suspects == {}
        assert res.break_count == 0
