"""双份 SKILL.md 同步 + prompt 预算（P0-5，方案 §4.2）。

- 同步：用户级（生效副本）与 workspace 留档副本逐字节一致——防「改了生效副本
  忘了留档」的历史问题复发（2026-09-24 实测过 skill 加载范围教训）；
- 第三副本：workspace 级 ``.zcode/skills``（ZCode 以 stock_yuzh_base 为工作区时
  加载）存在即须与正本一致——2026-10-25 发现该副本停在 P2 前版本（缺
  check_digest_numbers.py）无人纳管，补全方案 C-4 纳入看守；
- 预算：SKILL.md 字节数记录基线；增量 >25% 测试失败（InvestSkill 25% token
  纪律——提示词膨胀会稀释关键纪律的注意力）。
"""

import json
from pathlib import Path

import pytest

_USER_SKILL = Path.home() / ".workbuddy" / "skills" / "stock-analysis-chain" / "SKILL.md"
_WS_SKILL = (Path(__file__).resolve().parents[4] / ".workbuddy" / "skills" /
             "stock-analysis-chain" / "SKILL.md")
_ZCODE_SKILL = (Path(__file__).resolve().parents[4] / ".zcode" / "skills" /
                "stock-analysis-chain" / "SKILL.md")
_ZCODE_CHECK = (_ZCODE_SKILL.parent / "scripts" / "check_digest_numbers.py")
_BUDGET = Path(__file__).resolve().parent / "prompt_budget.json"

pytestmark = pytest.mark.skipif(not _USER_SKILL.exists(), reason="用户级 SKILL.md 不存在")


class TestZcodeCopyInSync:
    """第三副本看守（C-4，2026-10-25）：存在即须一致、有 SKILL.md 就必须有校验脚本。"""

    def test_zcode_copy_byte_identical_if_present(self):
        """.zcode 副本存在即须与正本逐字节一致——防 P2 式漏同步复发"""
        if not _ZCODE_SKILL.exists():
            pytest.skip(".zcode 副本不存在（已删除方案）")
        a = _USER_SKILL.read_bytes()
        b = _ZCODE_SKILL.read_bytes()
        assert a == b, (
            f".zcode 第三副本漂移（正本 {len(a)}B vs .zcode {len(b)}B）——"
            "P2 式漏同步：改技能后须同步 stock_yuzh_base/.zcode/skills/ 副本")

    def test_zcode_check_script_present_if_skill_present(self):
        """.zcode 有 SKILL.md 就必须有 check_digest_numbers.py——本次教训
        （旧副本停在 P2 前、缺结构校验交付门脚本）"""
        if not _ZCODE_SKILL.exists():
            pytest.skip(".zcode 副本不存在（已删除方案）")
        assert _ZCODE_CHECK.exists(), (
            ".zcode 副本缺 scripts/check_digest_numbers.py——结构校验交付门不可用，"
            "请从正本同步 scripts/ 全部文件")


class TestCopiesInSync:
    def test_two_copies_byte_identical(self):
        """两份 SKILL.md 逐字节一致（改提示词必须同步两份）。"""
        if not _WS_SKILL.exists():
            pytest.skip("workspace 留档副本不存在")
        a = _USER_SKILL.read_bytes()
        b = _WS_SKILL.read_bytes()
        assert a == b, (
            f"两份 SKILL.md 不一致（用户级 {len(a)}B vs workspace {len(b)}B）——"
            "改提示词后必须 cp 同步，防留档副本漂移")


class TestPromptBudget:
    def test_budget_file_valid(self):
        data = json.loads(_BUDGET.read_text(encoding="utf-8"))
        assert data["max_growth_pct"] == 25
        assert data["baseline_bytes"] > 0

    def test_growth_within_25pct(self):
        """当前字节数不超基线 +25%（超了说明提示词膨胀，先精简再改）。"""
        data = json.loads(_BUDGET.read_text(encoding="utf-8"))
        current = _USER_SKILL.stat().st_size
        growth = (current - data["baseline_bytes"]) / data["baseline_bytes"] * 100
        assert growth <= data["max_growth_pct"], (
            f"SKILL.md 字节数 {current} 超基线 {data['baseline_bytes']} "
            f"+{growth:.1f}%（门槛 +{data['max_growth_pct']}%）——提示词膨胀，"
            "请精简或经用户确认后更新 prompt_budget.json 基线")
