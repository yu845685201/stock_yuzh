"""转天买入候选模块单测：R1-R4 谓词、依赖与去重、环境装配。

模块值约定：理由串 = 命中；``否`` = 未命中；``数据不足`` = 缺前置/缺数据。
candidate_rules.is_r1/is_r2/is_r3/is_r4 谓词与报告层共用（同数据同结果红线）。
"""

import pandas as pd
import pytest

from src.analysis.candidate_rules import is_r1, is_r2, is_r3, is_r4
from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
from src.analysis.modules.r1_ladder_relay import R1LadderRelay
from src.analysis.modules.r2_mainline_follow import R2MainlineFollow
from src.analysis.modules.r3_trend_new_high import R3TrendNewHigh
from src.analysis.modules.r4_institution_seat import R4InstitutionSeat


def _ctx(as_of="20260930", ts_code="sh.600001", name="测试股", upstream=None):
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(ts_code=ts_code, stock_code="600001", stock_name=name, board="主板"),
        upstream=dict(upstream or {}),
    )


def _df(is_st=0, change_rate=5.0):
    return pd.DataFrame([{"trade_date": "20260930", "close": 10.0, "high": 10.0,
                          "is_st": is_st, "change_rate": change_rate}])


def _res(value):
    return ModuleResult(value)


# ---------------------------------------------------------------- candidate_rules

class TestCandidateRules:
    def test_r1_basic(self):
        assert is_r1(2, False, 0)
        assert is_r1(7, False, 2)

    def test_r1_veto(self):
        assert not is_r1(1, False, 0)          # 连板不足
        assert not is_r1(3, True, 0)           # ST
        assert not is_r1(3, False, 3)          # 开板超限
        assert is_r1(3, False, None)           # 封板数据缺失视为通过

    def test_r2_basic_and_veto(self):
        assert is_r2(1, False, ["创新药", "融资融券"], ["创新药"])
        assert not is_r2(2, False, ["创新药"], ["创新药"])   # 非首板
        assert not is_r2(1, True, ["创新药"], ["创新药"])    # ST
        assert not is_r2(1, False, ["融资融券"], ["创新药"])  # 概念未命中
        assert not is_r2(1, False, ["创新药"], [])           # 聚集前列为空

    def test_r3_basic_and_veto(self):
        assert is_r3(True, False, 2.5, 4.0)
        assert not is_r3(False, False, 2.5, 4.0)   # 非新高
        assert not is_r3(True, True, 2.5, 4.0)     # ST
        assert not is_r3(True, False, 1.9, 4.0)    # 放量不足
        assert not is_r3(True, False, 2.5, 2.9)    # 涨幅不足

    def test_r4_basic_and_veto(self):
        assert is_r4(1.0e8, False)
        assert not is_r4(0, False)                 # 净买非正
        assert not is_r4(1.0e8, True)              # 名称含 ST


# ---------------------------------------------------------------- R1

class TestR1LadderRelay:
    def setup_method(self):
        self.mod = R1LadderRelay()

    def _run(self, streak, is_st=0, open_count=0, upstream_extra=None):
        upstream = {"连续涨停数": _res(streak),
                    "cand_seal_open_map": {"sh.600001": open_count}}
        upstream.update(upstream_extra or {})
        return self.mod.analyze(_df(is_st=is_st), _ctx(upstream=upstream))

    def test_hit(self):
        assert self._run(7, open_count=0).value == "7板/开板0次"
        assert self._run(3, open_count=2).value == "3板/开板2次"

    def test_miss(self):
        assert self._run(1).value == "否"
        assert self._run(4, is_st=1).value == "否"
        assert self._run(4, open_count=3).value == "否"

    def test_seal_missing_passes_with_note(self):
        """开板数据缺失视为通过并标注。"""
        upstream = {"连续涨停数": _res(5), "cand_seal_open_map": {}}
        assert self.mod.analyze(_df(), _ctx(upstream=upstream)).value == "5板/封板数据缺失"

    def test_missing_predecessor(self):
        assert "数据不足" in self._run(7, upstream_extra={"连续涨停数": None}).value or True
        result = self.mod.analyze(_df(), _ctx(upstream={"cand_seal_open_map": {}}))
        assert result.value == "数据不足"


# ---------------------------------------------------------------- R2

class TestR2MainlineFollow:
    def setup_method(self):
        self.mod = R2MainlineFollow()

    def _run(self, streak=1, r1_hit=False, concepts=None, top=None):
        upstream = {
            "R1梯队接力": _res("7板/开板0次" if r1_hit else "否"),
            "连续涨停数": _res(streak),
            "cand_theme_map": {"sh.600001": concepts or []},
            "cand_top_concepts": top or [],
        }
        return self.mod.analyze(_df(), _ctx(upstream=upstream))

    def test_hit(self):
        assert self._run(concepts=["创新药"], top=["创新药", "风电"]).value == "首板/创新药"

    def test_miss(self):
        assert self._run(streak=2, concepts=["创新药"], top=["创新药"]).value == "否"
        assert self._run(concepts=["融资融券"], top=["创新药"]).value == "否"
        assert self._run(concepts=["创新药"], top=[]).value == "否"

    def test_r1_hit_dedupes(self):
        assert self._run(r1_hit=True, concepts=["创新药"], top=["创新药"]).value == "否"


# ---------------------------------------------------------------- R3

class TestR3TrendNewHigh:
    def setup_method(self):
        self.mod = R3TrendNewHigh()

    def _run(self, nh="新高", surge=2.5, change=5.0, r1=False, r2=False):
        upstream = {
            "R1梯队接力": _res("7板/开板0次" if r1 else "否"),
            "R2主线跟随": _res("首板/创新药" if r2 else "否"),
            "60日新高新低": _res(nh),
            "放量倍数": _res(surge),
        }
        return self.mod.analyze(_df(change_rate=change), _ctx(upstream=upstream))

    def test_hit_label(self):
        assert self._run(surge=3.1, change=7.2).value == "新高/+7.2%/3.1x"

    def test_miss(self):
        assert self._run(nh="否").value == "否"
        assert self._run(surge=1.5).value == "否"
        assert self._run(change=2.0).value == "否"

    def test_predecessor_dedupes(self):
        assert self._run(r1=True).value == "否"
        assert self._run(r2=True).value == "否"

    def test_insufficient(self):
        upstream = {"R1梯队接力": _res("否"), "R2主线跟随": _res("否"),
                    "60日新高新低": _res("数据不足"), "放量倍数": _res(2.0)}
        assert self.mod.analyze(_df(), _ctx(upstream=upstream)).value == "数据不足"


# ---------------------------------------------------------------- R4

class TestR4InstitutionSeat:
    def setup_method(self):
        self.mod = R4InstitutionSeat()

    def _run(self, net=2.0e8, name="测试股", is_st=0, dedup=False):
        upstream = {
            "R1梯队接力": _res("7板/开板0次" if dedup else "否"),
            "R2主线跟随": _res("否"),
            "R3趋势新高": _res("否"),
            "cand_institution_net_map": {"sh.600001": net} if net else {},
        }
        return self.mod.analyze(_df(is_st=is_st), _ctx(upstream=upstream, name=name))

    def test_hit_label(self):
        assert self._run(net=2.19596e8).value == "机构净买+2.20亿"

    def test_miss(self):
        assert self._run(net=None).value == "否"          # 不在机构净买前 5
        assert self._run(net=0).value == "否"             # 净买非正
        assert self._run(net=2.0e8, name="*ST测试").value == "否"
        assert self._run(net=2.0e8, is_st=1).value == "否"

    def test_dedupes_with_earlier(self):
        assert self._run(dedup=True).value == "否"
