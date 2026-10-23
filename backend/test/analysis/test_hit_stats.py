"""命中统计口径规格测试（方案 §2.5，2026-10-18 随候选链合入实施）。

覆盖五类口径：默认（布尔/数值>0，行为不变）、档位分桶（大涨/大跌等）、
阈值（放量倍数，可配置）、有符号（溢价正负）、样本数（连续比率）、
理由串（R 系列/money_trend）；另测 flatten_hits 压平规则。
"""

import pytest

from src.analysis.chain_engine import flatten_hits
from src.analysis.module_base import DATA_INSUFFICIENT, ModuleResult
from src.analysis.modules.ah_premium import AhPremium
from src.analysis.modules.big_mover import BigMover
from src.analysis.modules.chg_5d import Chg5d
from src.analysis.modules.consecutive_limit_up import ConsecutiveLimitUp
from src.analysis.modules.limit_up_premium import LimitUpPremium
from src.analysis.modules.limit_up_today import LimitUpToday
from src.analysis.modules.money_trend import MoneyTrend
from src.analysis.modules.new_high_low import NewHighLow
from src.analysis.modules.position_60d import Position60d
from src.analysis.modules.r1_ladder_relay import R1LadderRelay
from src.analysis.modules.volume_surge import VolumeSurge
from src.analysis.modules.volume_trend import VolumeTrend


class TestDefaultRuleUnchanged:
    """未声明口径的模块：行为与历史一致（布尔/「是」/数值>0）。"""

    def test_boolean_modules(self):
        mod = LimitUpToday()
        assert mod.hit_keys("是") == ["命中"]
        assert mod.hit_keys("否") == []
        assert mod.hit_keys(DATA_INSUFFICIENT) == []

    def test_numeric_positive(self):
        mod = ConsecutiveLimitUp()
        assert mod.hit_keys(3) == ["命中"]
        assert mod.hit_keys(0) == []          # 未涨停 → 连板数 0，不计
        assert mod.hit_keys(DATA_INSUFFICIENT) == []

    def test_unrecognized_string_not_hit(self):
        """未声明 hit_buckets 的模块遇到档位串：不计（历史行为）。"""
        mod = ConsecutiveLimitUp()
        assert mod.hit_keys("某档位") == []


class TestTierBuckets:
    """档位分桶：方向/档位分别计数（用户 2026-10-17 确认）。"""

    def test_big_mover_two_directions(self):
        mod = BigMover()
        assert mod.hit_keys("大涨") == ["大涨"]
        assert mod.hit_keys("大跌") == ["大跌"]
        assert mod.hit_keys("否") == []

    def test_new_high_low_two_directions(self):
        mod = NewHighLow()
        assert mod.hit_keys("新高") == ["新高"]
        assert mod.hit_keys("新低") == ["新低"]
        assert mod.hit_keys("否") == []

    def test_volume_trend_surge_tiers_only(self):
        mod = VolumeTrend()
        assert mod.hit_keys("连续放量") == ["连续放量"]
        assert mod.hit_keys("放量") == ["放量"]
        assert mod.hit_keys("温和") == []      # 非异动态不计
        assert mod.hit_keys("缩量") == []


class TestThresholdRule:
    """放量倍数阈值：默认 2.0，可经 params 覆盖（链 YAML 同机制）。"""

    def test_default_threshold(self):
        mod = VolumeSurge()
        assert mod.hit_keys(2.0) == ["≥2.0x"]   # 边界含等号
        assert mod.hit_keys(1.99) == []
        assert mod.hit_keys(DATA_INSUFFICIENT) == []

    def test_threshold_configurable(self):
        mod = VolumeSurge({"window": 5, "hit_threshold": 3.0})
        assert mod.hit_keys(2.5) == []
        assert mod.hit_keys(3.0) == ["≥3.0x"]


class TestSignedRule:
    """溢价正负分桶（用户确认：晋级成功/失败两面都统计）。"""

    def test_positive_and_negative(self):
        mod = LimitUpPremium()
        assert mod.hit_keys(1.25) == ["正"]
        assert mod.hit_keys(-0.8) == ["负"]
        assert mod.hit_keys(0.0) == []          # 零溢价不计入任何方向
        assert mod.hit_keys("否") == []
        assert mod.hit_keys(DATA_INSUFFICIENT) == []


class TestSampleRule:
    """连续比率：不计命中，可算样本数（数值即可算，含负值）。"""

    @pytest.mark.parametrize("mod_cls", [Chg5d, Position60d, AhPremium])
    def test_numeric_is_sample(self, mod_cls):
        mod = mod_cls()
        assert mod.hit_keys(3.5) == ["可算"]
        assert mod.hit_keys(-12.0) == ["可算"]   # 负值同样是可算样本
        assert mod.hit_keys(0.0) == ["可算"]
        assert mod.hit_keys(DATA_INSUFFICIENT) == []

    def test_non_numeric_states_not_sample(self):
        """AH 专列的非数值态（无A股映射/汇率待补/未对齐）不算可算样本。"""
        mod = AhPremium()
        assert mod.hit_keys("无A股映射") == []
        assert mod.hit_keys("汇率待补") == []
        assert mod.hit_keys("未对齐") == []


class TestReasonStringRule:
    """理由串类：非「否/数据不足」即命中（R 系列 + money_trend）。"""

    @pytest.mark.parametrize("mod_cls", [R1LadderRelay, MoneyTrend])
    def test_reason_string_counts(self, mod_cls):
        mod = mod_cls()
        assert mod.hit_keys("7板/开板0次") == ["命中"]
        assert mod.hit_keys("净流入1.20亿/连3天") == ["命中"]
        assert mod.hit_keys("否") == []
        assert mod.hit_keys(DATA_INSUFFICIENT) == []


class TestFlattenHits:
    """跨股累计桶 → 展示结构压平。"""

    def test_single_hit_bucket_flattens_to_int(self):
        assert flatten_hits({"是否涨停": {"命中": 56}}) == {"是否涨停": 56}

    def test_empty_bucket_is_zero(self):
        assert flatten_hits({"大涨大跌": {}}) == {"大涨大跌": 0}

    def test_multi_bucket_kept_as_dict(self):
        buckets = {"大涨大跌": {"大涨": 147, "大跌": 130},
                   "昨日涨停今日溢价": {"正": 28, "负": 28},
                   "60日位置%": {"可算": 5415}}
        assert flatten_hits(buckets) == buckets

    def test_mixed_modules(self):
        out = flatten_hits({"是否涨停": {"命中": 56},
                            "量能趋势": {"连续放量": 217, "放量": 205},
                            "放量倍数": {"≥2.0x": 163}})
        assert out["是否涨停"] == 56
        assert out["量能趋势"] == {"连续放量": 217, "放量": 205}
        assert out["放量倍数"] == {"≥2.0x": 163}
