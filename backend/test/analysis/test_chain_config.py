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
