"""分析窗口（Lookback）单测：写法解析、日历回溯、链 YAML 配置与默认值。"""

import shutil
from pathlib import Path

import pytest
import yaml

from src.analysis.chain_config import ChainConfig, load_chain
from src.analysis.lookback import ALL, DAYS, DEFAULT_SPEC, MONTHS, YEARS, Lookback

BACKEND = Path(__file__).resolve().parents[2]


@pytest.fixture()
def chains_dir():
    """把临时链目录放在项目内（backend/tmp/），避免依赖系统临时目录。"""
    path = BACKEND / "tmp" / "pytest_chains"
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


class TestParse:
    @pytest.mark.parametrize(
        "spec,kind,value",
        [
            ("1y", YEARS, 1),
            ("2y", YEARS, 2),
            ("6m", MONTHS, 6),
            ("3m", MONTHS, 3),
            ("250d", DAYS, 250),
            ("120D", DAYS, 120),
            ("250", DAYS, 250),
            (250, DAYS, 250),
            ("0", ALL, 0),
            ("all", ALL, 0),
            ("ALL", ALL, 0),
            ("全历史", ALL, 0),
        ],
    )
    def test_parse_valid(self, spec, kind, value):
        lb = Lookback.parse(spec)
        assert (lb.kind, lb.value) == (kind, value)

    @pytest.mark.parametrize("bad", ["", None, "abc", "1x", "-1y", "y", "1y2m"])
    def test_parse_invalid(self, bad):
        with pytest.raises(ValueError):
            Lookback.parse(bad)

    def test_default_is_one_year(self):
        lb = Lookback.default()
        assert lb.kind == YEARS and lb.value == 1
        assert DEFAULT_SPEC == "1y"

    def test_str_roundtrip(self):
        assert str(Lookback.parse("1y")) == "1y"
        assert str(Lookback.parse("all")) == "all"


class TestCalendarStart:
    def test_one_year_back(self):
        assert Lookback.parse("1y").calendar_start("20260923") == "20250923"

    def test_two_years_back_leap_day(self):
        # 2024-02-29 往前 2 个自然年 → 2022-02-28（relativedelta 处理）
        assert Lookback.parse("2y").calendar_start("20240229") == "20220228"

    def test_six_months_back(self):
        assert Lookback.parse("6m").calendar_start("20260923") == "20260323"

    def test_month_back_clamps_day(self):
        # 3/31 往前 1 个月 → 2/28（无 2/31）
        assert Lookback.parse("1m").calendar_start("20260331") == "20260228"

    def test_days_and_all_need_calendar(self):
        assert Lookback.parse("250d").calendar_start("20260923") is None
        assert Lookback.parse("0").calendar_start("20260923") is None


class TestChainLookbackConfig:
    def test_shipped_chain_uses_one_year(self):
        chain = load_chain("limit_up_scan")
        assert str(chain.lookback) == "1y"

    def test_chain_without_lookback_falls_back_to_default(self, chains_dir):
        (chains_dir / "t.yaml").write_text(
            yaml.safe_dump({"chain": "t", "modules": [{"name": "limit_up_today"}]}),
            encoding="utf-8",
        )
        chain = load_chain("t", chains_dir=chains_dir)
        assert str(chain.lookback) == "1y"

    def test_chain_lookback_override(self, chains_dir):
        (chains_dir / "t.yaml").write_text(
            yaml.safe_dump({"chain": "t", "lookback": "3y", "modules": [{"name": "limit_up_today"}]}),
            encoding="utf-8",
        )
        assert str(load_chain("t", chains_dir=chains_dir).lookback) == "3y"

    def test_chain_lookback_invalid_fails_at_load(self, chains_dir):
        (chains_dir / "t.yaml").write_text(
            yaml.safe_dump({"chain": "t", "lookback": "1x", "modules": [{"name": "limit_up_today"}]}),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="非法 lookback"):
            load_chain("t", chains_dir=chains_dir)

    def test_chain_lookback_all(self, chains_dir):
        (chains_dir / "t.yaml").write_text(
            yaml.safe_dump({"chain": "t", "lookback": "0", "modules": [{"name": "limit_up_today"}]}),
            encoding="utf-8",
        )
        assert load_chain("t", chains_dir=chains_dir).lookback.is_all

    def test_adhoc_chain_default(self):
        assert ChainConfig(chain="adhoc").lookback.kind == YEARS
