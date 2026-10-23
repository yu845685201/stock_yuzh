"""策略截面模块单测（P1-7 §4.6 C）：ma_cross_volume / turtle_breakout /
narrow_flag 三策略的构造用例与边界。

rps_breakout 未在本增量实现：其截面排名机制（env a_share_rps）归属 P1-12
（§4.5）——避免两处 RPS 实现漂移（原则 P6），方案原文亦许可「可先用涨跌幅
排名实现再换 P1-12 的 RPS 机制」，本增量选择直接归属 P1-12。
"""

from datetime import date, timedelta

import pandas as pd

from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
from src.analysis.modules.ma_cross_volume import MaCrossVolume
from src.analysis.modules.narrow_flag import NarrowFlag
from src.analysis.modules.turtle_breakout import TurtleBreakout

AS_OF = "20260930"


def _ctx(upstream=None, ts_code="sh.600001"):
    return ChainContext(
        as_of=AS_OF,
        stock=StockInfo(ts_code=ts_code, stock_code="600001",
                        stock_name="测试股", board="主板"),
        upstream=dict(upstream or {}))


def _dates(n, end="20260930"):
    d = date(int(end[:4]), int(end[4:6]), int(end[6:]))
    days = []
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return list(reversed(days))


def _base_row(i, d, **kw):
    row = {"ts_code": "sh.600001", "trade_date": d, "open": 10.0,
           "high": 10.2, "low": 9.8, "close": 10.0, "preclose": 10.0,
           "change_rate": 0.0, "volume": 1000.0, "amount": 10000.0,
           "is_st": 0, "float_share": 5e8}
    row.update(kw)
    return row


class TestMaCrossVolume:
    def _make(self, closes, volume_surge=3.0, is_st=0):
        dates = _dates(len(closes))
        rows = []
        for i, c in enumerate(closes):
            prev = closes[i - 1] if i else c
            rows.append(_base_row(i, dates[i], close=c, preclose=prev,
                                  change_rate=round((c / prev - 1) * 100, 2),
                                  is_st=is_st))
        return pd.DataFrame(rows)

    def test_golden_cross_with_volume(self):
        # 24 日横盘（MA5=MA20）+ 末日单跳 → 金叉恰在锚定日
        closes = [10.0] * 24 + [11.0]
        res = MaCrossVolume().analyze(
            self._make(closes), _ctx({"放量倍数": ModuleResult("3.0")}))
        assert res.value.startswith("金叉/放量")
        assert "3.0x" in res.value

    def test_no_cross(self):
        closes = [10.0 + 0.001 * i for i in range(25)]   # 极平稳上行无新叉
        res = MaCrossVolume().analyze(
            self._make(closes), _ctx({"放量倍数": ModuleResult("3.0")}))
        assert res.value == "否"

    def test_cross_but_low_volume(self):
        closes = [10.0] * 24 + [11.0]
        res = MaCrossVolume().analyze(
            self._make(closes), _ctx({"放量倍数": ModuleResult("1.5")}))
        assert res.value == "否"                        # 金叉但放量不足

    def test_st_rejected(self):
        closes = [10.0] * 24 + [11.0]
        res = MaCrossVolume().analyze(
            self._make(closes, is_st=1),
            _ctx({"放量倍数": ModuleResult("3.0")}))
        assert res.value == "否"

    def test_missing_predecessor(self):
        res = MaCrossVolume().analyze(self._make([10.0] * 25), _ctx())
        assert res.value == "数据不足"


class TestTurtleBreakout:
    def _make(self, closes, opens=None, float_share=5e8):
        dates = _dates(len(closes))
        rows = []
        for i, c in enumerate(closes):
            o = opens[i] if opens else c
            prev = closes[i - 1] if i else c
            rows.append(_base_row(i, dates[i], close=c, open=o, preclose=prev,
                                  high=max(c, o) + 0.1, low=min(c, o) - 0.1,
                                  change_rate=round((c / prev - 1) * 100, 2),
                                  float_share=float_share))
        return pd.DataFrame(rows)

    def test_breakout_with_mv(self):
        closes = [10.0] * 24 + [11.0]                   # 突破 21 日高点
        res = TurtleBreakout().analyze(self._make(closes), _ctx())
        assert res.value.startswith("突破/")
        assert "亿" in res.value
        assert res.detail["float_mv_yi"] == 55.0        # 11 × 5e8 / 1e8

    def test_no_breakout(self):
        closes = [10.0 + 0.001 * i for i in range(25)]
        res = TurtleBreakout().analyze(self._make(closes), _ctx())
        assert res.value == "否"

    def test_high_open_low_close_rejected(self):
        # 突破但高开低走（开盘 +8%，收盘 10.7 < 开盘 10.8）→ 剔除
        closes = [10.0] * 24 + [10.7]
        opens = [10.0] * 24 + [10.8]
        res = TurtleBreakout().analyze(self._make(closes, opens), _ctx())
        assert res.value == "否"

    def test_mv_missing_still_hits(self):
        closes = [10.0] * 24 + [11.0]
        res = TurtleBreakout().analyze(
            self._make(closes, float_share=float("nan")), _ctx())
        assert res.value == "突破/市值缺失"


class TestNarrowFlag:
    def _make(self, closes, change_rates):
        dates = _dates(len(closes))
        rows = []
        for i, c in enumerate(closes):
            prev = closes[i - 1] if i else c
            rows.append(_base_row(i, dates[i], close=c, preclose=prev,
                                  change_rate=change_rates[i]))
        return pd.DataFrame(rows)

    def test_flag_hit(self):
        # 旗杆 +9.9%（第 10 日），随后 4 日窄幅整理（回撤 <6%），末日收复
        closes = [10.0] * 9 + [11.0, 10.9, 10.8, 10.85, 11.05]
        chg = [0.0] * 9 + [9.9, -0.9, -0.9, 0.5, 1.9]
        res = NarrowFlag().analyze(self._make(closes, chg), _ctx())
        assert res.value.startswith("旗形/杆9.9%/面4日")

    def test_no_pole(self):
        closes = [10.0 + 0.1 * i for i in range(15)]
        chg = [0.5] * 15
        res = NarrowFlag().analyze(self._make(closes, chg), _ctx())
        assert res.value == "否"

    def test_wide_flag_rejected(self):
        # 旗面回撤 >6% → 非窄旗
        closes = [10.0] * 9 + [11.0, 10.0, 9.5, 9.6, 11.05]
        chg = [0.0] * 9 + [9.9, -9.1, -5.0, 1.1, 15.1]
        res = NarrowFlag().analyze(self._make(closes, chg), _ctx())
        assert res.value == "否"

    def test_not_reclaimed_rejected(self):
        # 未收复旗杆高点 → 否
        closes = [10.0] * 9 + [11.0, 10.9, 10.8, 10.7, 10.85]
        chg = [0.0] * 9 + [9.9, -0.9, -0.9, -0.9, 1.4]
        res = NarrowFlag().analyze(self._make(closes, chg), _ctx())
        assert res.value == "否"
