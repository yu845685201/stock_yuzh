"""变异测试（P0-5，方案 §4.2）：三个变异 CSV 各配「agent 解读时必须点名的不一致」
预期清单。

形态：以人工标注的期望行为清单回归——**不接 LLM live eval**（无 API key 预算，
方案显式声明为取舍）。每条变异断言：① 变异确实改了数据（防 fixture 失效）；
② 期望行为清单非空且含关键措辞（防预期清单被掏空）。
"""

import csv
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parent / "fixtures"

#: 变异 → 期望行为清单（人工标注；LLM live eval 为后续可选）
_MUTATIONS = {
    "mutate_limitup_count.csv": [
        "必须出现「重算涨停家数与 CSV 不一致」类表述（golden note 的涨停家数 "
        "与变异 CSV 差 1）",
        "点名差异来源：是否涨停列被改动（是→否）",
    ],
    "mutate_suspect_flag.csv": [
        "必须点名「存疑股被标记为否」——golden note 的存疑家数与变异 CSV 不一致",
        "指出该行存疑原因被清空，数据质量结论不可照抄 golden",
    ],
    "mutate_break_reason.csv": [
        "必须点名「前复权断裂原因缺失」——note 引用断裂打折但 CSV 无该原因",
        "不得把该行当正常数据解读（断裂行结论打折纪律仍适用）",
    ],
}


def _read(path: Path):
    with path.open(encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


class TestMutations:
    @pytest.mark.parametrize("name", list(_MUTATIONS))
    def test_mutation_fixture_differs_from_golden(self, name):
        """变异确实改了数据（与 golden 不同）——防 fixture 失效静默通过。"""
        golden = _read(FIXTURES / "golden_20260923.csv")
        mutated = _read(FIXTURES / name)
        assert len(golden) == len(mutated), "变异不应改变行数"
        diff = [i for i, (g, m) in enumerate(zip(golden, mutated)) if g != m]
        assert diff, f"{name} 与 golden 无差异（fixture 失效）"

    @pytest.mark.parametrize("name", list(_MUTATIONS))
    def test_expected_behavior_list_nonempty(self, name):
        """期望行为清单非空且含关键措辞（防预期清单被掏空）。"""
        expectations = _MUTATIONS[name]
        assert expectations, f"{name} 缺期望行为清单"
        joined = " ".join(expectations)
        assert "点名" in joined or "必须" in joined

    def test_limitup_count_mutation_semantics(self):
        """变异①语义：恰好一只涨停股被改为非涨停。"""
        golden = _read(FIXTURES / "golden_20260923.csv")
        mutated = _read(FIXTURES / "mutate_limitup_count.csv")
        g_up = sum(1 for r in golden if r["是否涨停"] == "是")
        m_up = sum(1 for r in mutated if r["是否涨停"] == "是")
        assert g_up - m_up == 1

    def test_suspect_flag_mutation_semantics(self):
        """变异②语义：恰好一只存疑股被改为否且原因清空。"""
        golden = _read(FIXTURES / "golden_20260923.csv")
        mutated = _read(FIXTURES / "mutate_suspect_flag.csv")
        g_sus = sum(1 for r in golden if r["是否存疑"] == "是")
        m_sus = sum(1 for r in mutated if r["是否存疑"] == "是")
        assert g_sus - m_sus == 1
        flipped = [m for g, m in zip(golden, mutated)
                   if g["是否存疑"] == "是" and m["是否存疑"] == "否"]
        assert flipped and not flipped[0]["存疑原因"]

    def test_break_reason_mutation_semantics(self):
        """变异③语义：至少一只票的断裂原因被删除（或原因清空）。"""
        golden = _read(FIXTURES / "golden_20260923.csv")
        mutated = _read(FIXTURES / "mutate_break_reason.csv")
        changed = [m for g, m in zip(golden, mutated)
                   if "前复权断裂" in (g.get("存疑原因") or "")
                   and "前复权断裂" not in (m.get("存疑原因") or "")]
        if not changed:
            changed = [m for g, m in zip(golden, mutated)
                       if (g.get("存疑原因") or "") and not (m.get("存疑原因") or "")]
        assert changed, "变异③未删除任何断裂原因"
