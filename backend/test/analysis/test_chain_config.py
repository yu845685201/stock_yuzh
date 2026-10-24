"""链配置与模块注册表单测：无默认链约束、未知链/模块/参数报错、README 登记校验。"""

import pytest

from src.analysis.chain_config import ChainConfig, ModuleSpec, list_chains, load_chain, parse_modules_option
from src.analysis.module_registry import build, load_registry, registration_warnings


class TestRegistry:
    def test_two_modules_discovered(self):
        registry = load_registry()
        assert "limit_up_today" in registry
        assert "consecutive_limit_up" in registry

    def test_build_unknown_module(self):
        with pytest.raises(ValueError, match="未知分析模块"):
            build("no_such_module")

    def test_build_unknown_param(self):
        with pytest.raises(ValueError, match="不支持参数"):
            build("consecutive_limit_up", {"not_a_param": 1})

    def test_all_modules_registered_in_readme(self):
        assert registration_warnings() == []


class TestChainConfig:
    def test_load_limit_up_scan(self):
        chain = load_chain("limit_up_scan")
        assert chain.chain == "limit_up_scan"
        assert [s.name_en for s in chain.modules] == ["limit_up_today", "consecutive_limit_up"]
        assert chain.modules[1].params == {"max_lookback": 60}

    def test_load_daily_review(self):
        """每日复盘链：24 模块（量价资金 2026-10-06；R1-R4 候选 2026-10-18 §2.4；
        P0-1 指标七件套 2026-10-18 §3.1；P0-2 形态 2026-10-20 §3.2；
        P0-4 估值摘要 2026-10-20 §3.3；P1-11 涨停归因 2026-10-20 §3.4）、
        60 交易日窗口、双 env 装配。"""
        chain = load_chain("daily_review")
        assert chain.chain == "daily_review"
        assert chain.env_list() == ["a_share_candidates", "a_share_valuation"]
        assert [s.name_en for s in chain.modules] == [
            "limit_up_today",
            "consecutive_limit_up",
            "limit_down_today",
            "burst_limit",
            "limit_up_premium",
            "big_mover",
            "volume_surge",
            "volume_trend",
            "new_high_low",
            "money_trend",
            "r1_ladder_relay",
            "r2_mainline_follow",
            "r3_trend_new_high",
            "r4_institution_seat",
            "macd_state",
            "kdj_state",
            "rsi_state",
            "boll_pos",
            "atr_pct",
            "obv_trend",
            "ma_arrangement",
            "candle_pattern",
            "valuation_digest",
            "limit_up_attribution",
            "chan_divergence_buy",
        ]
        assert chain.env_list() == ["a_share_candidates", "a_share_valuation"]
        assert chain.lookback.is_all is False
        assert str(chain.lookback) == "60d"
        assert chain.modules[1].params == {"max_lookback": 60}
        # new_high_low（60日窗口）与 money_trend（无参数）两模块 params
        assert chain.modules[8].params == {"window": 60}
        # 末位缠论模块 P0-7 门控参数（与 config chan.min_coverage 同口径）
        assert chain.modules[-1].params == {"min_coverage": 0.98}

    def test_list_chains_contains_daily_review(self):
        assert "daily_review" in [c.chain for c in list_chains()]

    def test_unknown_chain(self):
        with pytest.raises(ValueError, match="未知分析链"):
            load_chain("does_not_exist")

    def test_empty_chain_name(self):
        with pytest.raises(ValueError):
            load_chain("")

    def test_parse_modules_option(self):
        specs = parse_modules_option("limit_up_today, consecutive_limit_up")
        assert [s.name_en for s in specs] == ["limit_up_today", "consecutive_limit_up"]

    def test_parse_modules_option_empty(self):
        with pytest.raises(ValueError):
            parse_modules_option(" , ")

    def test_list_chains(self):
        assert "limit_up_scan" in [c.chain for c in list_chains()]

    def test_chain_config_repr_via_specs(self):
        chain = ChainConfig(chain="adhoc", modules=[ModuleSpec(name_en="limit_up_today")])
        assert chain.modules[0].params == {}
