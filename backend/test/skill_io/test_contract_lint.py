"""SKILL.md 契约 lint（P0-5，方案 §4.2）：必备章节/纪律缺失即测试失败。

平移 InvestSkill `check-skill-contract.js` 思路：SKILL.md 必含——数据时点与
口径标注、存疑逐一点名、免责声明、表格优先原则、综合研判三段结构、防幻觉
三件套规则；缺一项 fail（防「改提示词改丢了纪律」）。
"""

from pathlib import Path

import pytest

_SKILL = Path.home() / ".workbuddy" / "skills" / "stock-analysis-chain" / "SKILL.md"

pytestmark = pytest.mark.skipif(not _SKILL.exists(), reason="SKILL.md 不存在")

#: (契约项, 必须命中的文本片段)
_REQUIRED = [
    ("数据时点与口径标注", ["数据可用时点表", "口径"]),
    ("存疑逐一点名", ["是否存疑=是", "逐一点名"]),
    ("免责声明", ["免责声明", "不构成投资建议"]),
    ("表格优先原则", ["表格优先原则"]),
    ("综合研判三段结构", ["综合研判", "市场状态一句话定位", "多维证据交叉表",
                          "精选操作池"]),
    ("防幻觉三件套", ["防幻觉三件套", "数字接地", "推断标注", "编号可查",
                      "check_digest_numbers"]),
]


def _text() -> str:
    return _SKILL.read_text(encoding="utf-8")


class TestContractLint:
    @pytest.mark.parametrize("item,fragments", _REQUIRED,
                             ids=[x[0] for x in _REQUIRED])
    def test_required_section_present(self, item, fragments):
        text = _text()
        missing = [f for f in fragments if f not in text]
        assert not missing, f"SKILL.md 缺契约项「{item}」片段: {missing}"

    def test_frontmatter_valid(self):
        """frontmatter 必备字段（name/description）——skill 加载依赖。"""
        text = _text()
        assert text.startswith("---"), "SKILL.md 必须以 frontmatter 开头"
        head = text.split("---", 2)[1]
        assert "name:" in head and "description:" in head

    def test_no_default_chain_claim(self):
        """无默认链纪律仍在（防回退到隐式默认链）。"""
        assert "无默认链" in _text()
