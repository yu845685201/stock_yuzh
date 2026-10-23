"""golden 回归（P0-5，方案 §4.2）：结构契约 + 数字白名单（复用 P0-3 校验器）。

golden fixture 为人工校准过的解读文本（golden_20260923_note.md）——「校准」
的实操定义 = 全部数字可溯源到 golden CSV（校验器零 WARN）+ 结构契约齐全。
"""

import sys
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parent / "fixtures"
GOLDEN_NOTE = FIXTURES / "golden_20260923_note.md"
GOLDEN_CSV = FIXTURES / "golden_20260923.csv"

_SCRIPT = Path.home() / ".workbuddy" / "skills" / "stock-analysis-chain" / \
    "scripts" / "check_digest_numbers.py"

pytestmark = pytest.mark.skipif(
    not (GOLDEN_NOTE.exists() and GOLDEN_CSV.exists() and _SCRIPT.exists()),
    reason="golden fixture 或校验脚本缺失")

if _SCRIPT.exists():
    sys.path.insert(0, str(_SCRIPT.parent))
    import check_digest_numbers as cdn  # noqa: E402


class TestGoldenStructure:
    """结构契约：必备章节在、免责声明在、表格优先。"""

    def test_sections_present(self):
        text = GOLDEN_NOTE.read_text(encoding="utf-8")
        for section in ("## 综合研判", "## 数据质量点名"):
            assert section in text, f"golden note 缺章节: {section}"

    def test_disclaimer_present(self):
        assert "免责声明" in GOLDEN_NOTE.read_text(encoding="utf-8")

    def test_table_first(self):
        """可枚举对照内容用表格（候选名单必须是表）。"""
        text = GOLDEN_NOTE.read_text(encoding="utf-8")
        assert "| 代码 | 名称 | 连板 | 归因 |" in text

    def test_observation_not_recommendation(self):
        """候选措辞纪律：观察名单非推荐。"""
        assert "观察名单，非推荐" in GOLDEN_NOTE.read_text(encoding="utf-8")


class TestGoldenNumbers:
    """数字白名单：golden note 全部数字接地（校验器零 WARN）。"""

    def test_all_numbers_grounded(self):
        white = cdn._csv_whitelist(GOLDEN_CSV)
        warns = cdn.check_note(GOLDEN_NOTE, white)
        assert warns == [], f"golden note 有未接地数字: {warns[:5]}"

    def test_inference_marked(self):
        """推断句带【推断】前缀。"""
        assert "【推断】" in GOLDEN_NOTE.read_text(encoding="utf-8")
