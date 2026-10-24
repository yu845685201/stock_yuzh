"""check_digest_numbers 校验脚本单测（P0-3 §4.1.2）。

覆盖：白名单构建（CSV/meta）、五类数字形状、来源括注豁免、结构性数字豁免、
注记行豁免、WARN 清单结构、归一化。免网络免数据库。
"""

import json
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

_SCRIPT = Path.home() / ".workbuddy" / "skills" / "stock-analysis-chain" / \
    "scripts" / "check_digest_numbers.py"

pytestmark = pytest.mark.skipif(not _SCRIPT.exists(),
                                reason="check_digest_numbers.py 不存在（skill 未安装）")

sys.path.insert(0, str(_SCRIPT.parent))
import check_digest_numbers as cdn  # noqa: E402


@pytest.fixture
def workdir():
    """沙箱内 pytest tmp_path 不可用（pytest-of-root mkdir EEXIST），
    改用 tempfile.mkdtemp 并自行清理。"""
    d = Path(tempfile.mkdtemp(prefix="cdn_test_"))
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def csv_file(workdir):
    p = workdir / "daily_review_20260930.csv"
    p.write_text(
        "股票代码,股票名称,是否涨停,放量倍数,涨幅%\n"
        "600000,浦发银行,是,2.01,9.99\n"
        "600001,测试股,否,1.08,-5.02\n", encoding="utf-8-sig")
    return p


@pytest.fixture
def note_file(workdir):
    def _make(text):
        p = workdir / "daily_review_20260930_digest_note.md"
        p.write_text(text, encoding="utf-8")
        return p
    return _make


class TestWhitelist:
    def test_csv_values(self, csv_file):
        white = cdn._csv_whitelist(csv_file)
        assert "600000" in white and "9.99" in white and "2.01" in white
        assert "-5.02" in white and "1.08" in white

    def test_csv_thousands_separator(self, workdir):
        p = workdir / "t.csv"
        p.write_text("a\n13,860\n", encoding="utf-8-sig")
        assert "13860" in cdn._csv_whitelist(p)

    def test_meta_values(self, workdir):
        p = workdir / "meta.json"
        p.write_text(json.dumps({"stock_count": 5820, "duration_seconds": 61.2}),
                     encoding="utf-8")
        white = cdn._meta_whitelist(p)
        assert "5820" in white and "61.2" in white

    def test_meta_invalid(self, workdir):
        p = workdir / "bad.json"
        p.write_text("{not json", encoding="utf-8")
        assert cdn._meta_whitelist(p) == set()


class TestCheckNote:
    def test_grounded_number_no_warn(self, csv_file, note_file):
        note = note_file("今日涨停 1 家，放量倍数 2.01，涨幅 9.99%。")
        warns = cdn.check_note(note, cdn._csv_whitelist(csv_file))
        # "1"（家）不在 CSV → WARN；"2.01"/"9.99" 在白名单 → 不告警
        nums = [w["num"] for w in warns]
        assert "2.01" not in nums and "9.99" not in nums
        assert any("1" in n for n in nums)

    def test_inference_annotation_exempt(self, csv_file, note_file):
        note = note_file("预计明日情绪延续（推断：涨停约 7 家）。")
        warns = cdn.check_note(note, cdn._csv_whitelist(csv_file))
        assert warns == []

    def test_source_annotation_exempt(self, csv_file, note_file):
        note = note_file("量能趋势连续放量（CSV:量能趋势=连续放量）计 217 日。")
        warns = cdn.check_note(note, cdn._csv_whitelist(csv_file))
        assert warns == []

    def test_structural_exempt(self, csv_file, note_file):
        note = note_file("详见 ev-20260930-3 与 §4.1；第 2 节。")
        warns = cdn.check_note(note, cdn._csv_whitelist(csv_file))
        assert warns == []

    def test_note_line_exempt(self, csv_file, note_file):
        note = note_file("> 口径：样本 999 只，剔除异常值。\n正文无数字。")
        warns = cdn.check_note(note, cdn._csv_whitelist(csv_file))
        assert warns == []

    def test_warn_structure(self, csv_file, note_file):
        note = note_file("明日预计成交额 888 亿元。")
        warns = cdn.check_note(note, cdn._csv_whitelist(csv_file))
        assert len(warns) == 1
        assert warns[0]["line"] == "1"
        assert "888" in warns[0]["num"]
        assert "成交额" in warns[0]["context"]

    def test_percent_and_multiple_shapes(self, csv_file, note_file):
        """百分比/倍数形状均能被提取并与白名单比对（value 为归一化数字）。"""
        note = note_file("放量 2.5x，占比 12.5%。")
        warns = cdn.check_note(note, cdn._csv_whitelist(csv_file))
        assert {w["value"] for w in warns} == {"2.5", "12.5"}
        assert {w["num"] for w in warns} == {"2.5x", "12.5%"}


class TestNormalize:
    def test_strip_trailing_zeros(self):
        assert cdn._normalize("147.0") == "147"
        assert cdn._normalize("9.99") == "9.99"
        assert cdn._normalize("13,860") == "13860"
        assert cdn._normalize("0.50") == "0.5"


# ---------------------------------------------------------------- P2-5 结构规则库

_CFG = """
review:
  sentiment_calibrated: true
  sentiment_rules:
    ice: {max_streak_lte: 2, burst_rate_gte: 0.5}
    ebb: {premium_mean_lte: 0}
    climax: {max_streak_gte: 5, premium_mean_gte: 4.0}
"""

_GOOD_DIGEST = """
<details><summary>口径字典（点击展开）</summary><table>…</table></details>
<p class="note">口径：背驰入选 |Σhist₂|/|Σhist₁| &lt; 1 即背驰，本表按强度升序取前 10。</p>
<table><thead><tr><th>判据</th></tr></thead><tbody>
<tr><td>高潮</td><td>高度≥5 且 溢价≥4.0%</td><td>…</td><td>否</td></tr>
<tr><td>冰点</td><td>高度≤2 或 炸板率≥0.5</td><td>…</td><td>否</td></tr>
<tr><td>退潮</td><td>溢价均值≤0%</td><td>…</td><td>否</td></tr>
</tbody></table>
"""

_GOOD_NOTE = "缠论候选×存疑清单交叉：命中 0 只。\n\n精选观察池（非推荐）\n"


class TestStructuralRules:
    def test_all_green(self, workdir, csv_file):
        note = workdir / "n.md"
        note.write_text(_GOOD_NOTE, encoding="utf-8")
        digest = workdir / "d.html"
        digest.write_text(_GOOD_DIGEST, encoding="utf-8")
        cfg = workdir / "config.yaml"
        cfg.write_text(_CFG, encoding="utf-8")
        assert cdn.run_structural_rules(note, digest, cfg) == []

    def test_missing_glossary_fails(self, workdir):
        note = workdir / "n.md"
        note.write_text(_GOOD_NOTE, encoding="utf-8")
        digest = workdir / "d.html"
        digest.write_text("<html>无字典</html>", encoding="utf-8")
        cfg = workdir / "config.yaml"
        cfg.write_text(_CFG, encoding="utf-8")
        fails = cdn.run_structural_rules(note, digest, cfg)
        assert any('glossary' in f for f in fails)
        assert any('criteria_cfg' in f for f in fails)
        assert any('chan_criteria' in f for f in fails)

    def test_note_rules(self, workdir):
        note = workdir / "n.md"
        note.write_text("正文没有交叉披露，也没有观察池表头。", encoding="utf-8")
        digest = workdir / "d.html"
        digest.write_text(_GOOD_DIGEST, encoding="utf-8")
        fails = cdn.run_structural_rules(note, digest, None)
        assert any('chan_cross' in f for f in fails)
        assert any('pool_style' in f for f in fails)
        assert not any('glossary' in f for f in fails)   # digest 规则通过
        # digest 提供但 config 缺失 → criteria_cfg 报需要 --config（流程纪律保证完整入参）
        assert any('criteria_cfg' in f and '--config' in f for f in fails)

    def test_criteria_cfg_threshold_mismatch(self, workdir):
        note = workdir / "n.md"
        note.write_text(_GOOD_NOTE, encoding="utf-8")
        digest = workdir / "d.html"
        # 判据表阈值写成 6（config 为 5）→ 必须失败
        digest.write_text(_GOOD_DIGEST.replace("高度≥5", "高度≥6").replace(
            "<details><summary>口径字典（点击展开）</summary><table>…</table></details>", "")
            + "<details>口径字典</details>" + "|Σhist₂|/|Σhist₁| &lt; 1 即背驰",
            encoding="utf-8")
        cfg = workdir / "config.yaml"
        cfg.write_text(_CFG, encoding="utf-8")
        fails = cdn.run_structural_rules(note, digest, cfg)
        assert any('criteria_cfg' in f for f in fails)

    def test_main_exit_code_hard_fail(self, workdir, csv_file):
        """结构规则失败 → 退出码 1（不受 --strict 影响，硬失败阻断发布）"""
        note = workdir / "n.md"
        note.write_text("无结构要素。", encoding="utf-8")
        digest = workdir / "d.html"
        digest.write_text("<html>缺字典</html>", encoding="utf-8")
        rc = cdn.main(['--note', str(note), '--csv', str(csv_file),
                       '--digest', str(digest)])
        assert rc == 1

    def test_main_skip_without_digest(self, csv_file, note_file):
        """未提供 --digest → 结构规则整体跳过，行为与旧版一致（WARN 级）"""
        note = note_file("今日涨停 1 家。")
        rc = cdn.main(['--note', str(note), '--csv', str(csv_file)])
        assert rc == 0

    def test_main_green_full_args(self, workdir, csv_file):
        """完整入参全绿 → 0"""
        note = workdir / "n.md"
        note.write_text(_GOOD_NOTE, encoding="utf-8")
        digest = workdir / "d.html"
        digest.write_text(_GOOD_DIGEST, encoding="utf-8")
        cfg = workdir / "config.yaml"
        cfg.write_text(_CFG, encoding="utf-8")
        rc = cdn.main(['--note', str(note), '--csv', str(csv_file),
                       '--digest', str(digest), '--config', str(cfg)])
        assert rc == 0
