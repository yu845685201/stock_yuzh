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

    def test_new_citation_format_exempt_w2(self, csv_file, note_file):
        """W2：（六维§X）/（图文§X）新括注即接地；（报告§X）旧格式继续接受（历史兼容）"""
        note = note_file("溢价 3.38%（六维§二）；高度 6 板（图文§二）；"
                         "涨停 63 家（报告§四）。")
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


class TestImplLeak:
    def test_impl_leak_w6(self, note_file):
        """W6/P2-8：正文 config 键名与时钟时间戳 WARN；表格行/> 注记行豁免；每行一条"""
        note = note_file("接缝由 config review.chan_pool_linkage 决定。\n"
                         "| 项 | 说明 |\n|---|---|\n| 链重跑 | 21:35 seal 收紧 |\n"
                         "> 注记：21:35 在引用行豁免。\n"
                         "正文裸时钟 21:35 命中。\n")
        leaks = cdn.check_impl_leak(note)
        assert [lk['line'] for lk in leaks] == ['1', '6']
        assert any('config' in lk['num'] for lk in leaks)
        assert any(lk['num'] == '21:35' for lk in leaks)

    def test_impl_leak_market_time_exempt_w6(self, note_file):
        """W6 实施修正：行情时刻词紧邻的时钟数字是行情事实（首封 14:30），不告警"""
        note = note_file("鸣志电器首封 14:30、开板 3 次收盘未封住。\n"
                         "21:35 链重跑后命中收敛。\n")
        leaks = cdn.check_impl_leak(note)
        assert [lk['line'] for lk in leaks] == ['2']

    def test_impl_leak_nonblocking_and_strict(self, csv_file, note_file):
        """W6：语汇 WARN 默认不阻断（0），--strict 时 1；全绿路径文案含 P2-8"""
        note = note_file("正文提到 config review.xxx 键名。")
        assert cdn.main(['--note', str(note), '--csv', str(csv_file)]) == 0
        assert cdn.main(['--note', str(note), '--csv', str(csv_file),
                         '--strict']) == 1
        clean = note_file("正文无实现细节，数字全部接地。")
        assert cdn.main(['--note', str(clean), '--csv', str(csv_file)]) == 0


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

_GOOD_NOTE = ("缠论候选×存疑清单交叉：命中 0 只。\n\n精选观察池（非推荐）\n\n"
              "**主干：测试主干。**\n\n- **基准倾向**：测试倾向\n")


class TestStructuralRules:
    def test_all_green(self, workdir, csv_file):
        note = workdir / "n.md"
        note.write_text(_GOOD_NOTE, encoding="utf-8")
        digest = workdir / "d.html"
        digest.write_text(_GOOD_DIGEST, encoding="utf-8")
        cfg = workdir / "config.yaml"
        cfg.write_text(_CFG, encoding="utf-8")
        report = workdir / "r.md"          # V4.3：规则⑧⑨需要六维 md
        report.write_text("指数 ADX 闸门：full——弱档判定未生效", encoding="utf-8")
        assert cdn.run_structural_rules(note, digest, cfg, report) == []

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
        """完整入参全绿 → 0（V4.3 起完整入参含 --report）"""
        note = workdir / "n.md"
        note.write_text(_GOOD_NOTE, encoding="utf-8")
        digest = workdir / "d.html"
        digest.write_text(_GOOD_DIGEST, encoding="utf-8")
        cfg = workdir / "config.yaml"
        cfg.write_text(_CFG, encoding="utf-8")
        report = workdir / "r.md"
        report.write_text("指数 ADX 闸门：full——弱档判定未生效", encoding="utf-8")
        rc = cdn.main(['--note', str(note), '--csv', str(csv_file),
                       '--digest', str(digest), '--config', str(cfg),
                       '--report', str(report)])
        assert rc == 0


class TestTldrRules:
    """T1 速览框提取契约（结构规则 ⑥note_trunk ⑦note_bias，硬失败）"""

    def test_note_trunk_bias_rules(self, workdir):
        digest = workdir / "d.html"
        digest.write_text(_GOOD_DIGEST, encoding="utf-8")
        cfg = workdir / "config.yaml"
        cfg.write_text(_CFG, encoding="utf-8")
        report = workdir / "r.md"          # V4.3：规则⑧⑨需要六维 md
        report.write_text("指数 ADX 闸门：full——弱档判定未生效", encoding="utf-8")
        # 缺契约行 → ⑥⑦ 硬失败
        note = workdir / "n.md"
        note.write_text("缠论候选×存疑清单交叉：命中 0 只。\n"
                        "精选观察池（非推荐）\n", encoding="utf-8")
        fails = cdn.run_structural_rules(note, digest, cfg, report)
        assert any('note_trunk' in f for f in fails)
        assert any('note_bias' in f for f in fails)
        # 含契约行（列表前缀形态同样算数）→ 通过
        note.write_text(_GOOD_NOTE, encoding="utf-8")
        assert cdn.run_structural_rules(note, digest, cfg, report) == []

    def test_main_exit_code_missing_trunk(self, workdir, csv_file):
        """缺主干行 → 退出码 1（硬失败阻断发布）"""
        digest = workdir / "d.html"
        digest.write_text(_GOOD_DIGEST, encoding="utf-8")
        note = workdir / "n.md"
        note.write_text(_GOOD_NOTE.replace("**主干：测试主干。**\n\n", ""),
                        encoding="utf-8")
        rc = cdn.main(['--note', str(note), '--csv', str(csv_file),
                       '--digest', str(digest)])
        assert rc == 1


class TestT5Warns:
    """T5 骨架兜底四条 WARN：工单号/嵌套括注/免责/池表行序（WARN 保险丝不阻断）"""

    def test_ticket_leak(self, note_file):
        """T5.4：X/P/S/A 系列工单号命中（含表格行）；合法词不误伤"""
        note = note_file(
            "X2 修复后对账一致，P4-8 离线回测落地。\n"
            "| 项 | 说明 |\n|---|---|\n| 对账 | P0-7 门控后 15 只 |\n")
        hits = cdn.check_ticket_leak(note)
        nums = [h['num'] for h in hits]
        assert 'X2' in nums and 'P4-8' in nums and 'P0-7' in nums
        legal = note_file("P1-P3 情景三档；R3 趋势新高；A50 与 CHA50CFD；"
                          "2026-10-26 起生效；V4 验点；S3 已收口。\n")
        assert cdn.check_ticket_leak(legal) == []

    def test_nested_paren(self, note_file):
        """T5.1：正文行全角括号嵌套命中；表格/引用行豁免；单层括注不误伤"""
        note = note_file(
            "情绪：回暖/震荡（高潮未命中：溢价 3.38%<4.0%（六维§二））。\n"
            "| 项 | 说明（来源（嵌套）） |\n"
            "> 引用行（外层（内层））豁免。\n"
            "单层括注（六维§二）不命中。\n")
        hits = cdn.check_nested_paren(note)
        assert [h['line'] for h in hits] == ['1']

    def test_disclaimer(self, note_file):
        """T5.5：note 含「免责声明」（含 > 引用行）命中；无免责不命中"""
        note = note_file("> 免责声明：以上不构成投资建议。\n正文干净。\n")
        hits = cdn.check_disclaimer(note)
        assert len(hits) == 1 and hits[0]['line'] == '1'
        clean = note_file("正文无免责，页脚单一出处。\n")
        assert cdn.check_disclaimer(clean) == []

    def test_pool_order_and_csv_subset(self, workdir, csv_file, note_file):
        """T5.2：档内评分升序命中；证据数升序命中；正序不命中；代码不在 CSV 命中"""
        bad = note_file(
            "| 股票 | 类别（证据/评分） |\n|---|---|\n"
            "| 浦发银行 600000 | R2（4 证，42.9 分，六维§六） |\n"
            "| 测试股 600001 | R2（4 证，49.1 分，六维§六） |\n")
        hits = cdn.check_pool_order(bad, csv_file)
        assert any(h['value'] == '池序逆行' and '49.1' in h['num'] for h in hits)
        bad2 = note_file(
            "| 股票 | 类别（证据/评分） |\n|---|---|\n"
            "| 浦发银行 600000 | R2（3 证，50.0 分，六维§六） |\n"
            "| 测试股 600001 | R2（4 证，60.0 分，六维§六） |\n")
        assert any(h['value'] == '池序逆行'
                   for h in cdn.check_pool_order(bad2, csv_file))
        good = note_file(
            "| 股票 | 类别（证据/评分） |\n|---|---|\n"
            "| 测试股 600001 | R2（4 证，49.1 分，六维§六） |\n"
            "| 浦发银行 600000 | R2（4 证，42.9 分，六维§六） |\n"
            "| 浦发银行 600000 | R2（3 证，71.3 分，六维§六） |\n")
        assert cdn.check_pool_order(good, csv_file) == []
        outsider = note_file(
            "| 股票 | 类别（证据/评分） |\n|---|---|\n"
            "| 幽灵股 999999 | R2（4 证，50.0 分，六维§六） |\n")
        assert any(h['value'] == '池内代码不在 CSV'
                   for h in cdn.check_pool_order(outsider, csv_file))
        # 行数 <2 或无池表 → 不校验
        assert cdn.check_pool_order(note_file("无池表。"), csv_file) == []

    def test_t5_warns_nonblocking_and_strict(self, csv_file, note_file):
        """T5 WARN 默认退 0，--strict 退 1"""
        note = note_file("正文提到 X2 修复。")
        assert cdn.main(['--note', str(note), '--csv', str(csv_file)]) == 0
        assert cdn.main(['--note', str(note), '--csv', str(csv_file),
                         '--strict']) == 1


# ---------------------------------------------------------------- V4.3 规则⑧⑨

class TestRule8CitedGrounded:
    """规则⑧：来源括注（六维§X）数字必须真在六维 md 命中（20261023
    「MA60 3881 断链引用」漏网根因——挂引用即免检）。"""

    def test_ghost_number_hard_fail(self):
        """编造数字挂引用 → 硬失败（3881 不在只含 MA20 的 md 中）"""
        note = "机制：收盘 3690 低于 MA60 3881（六维§六）。"
        ok, why = cdn._check_cited_grounded(note, "收盘 3690 vs MA20 3848。")
        assert not ok and '3881' in why and 'L1' in why

    def test_truncation_variant_passes(self):
        """note 写 3881、md 写 3881.43 → 截断变体命中放行"""
        note = "收盘低于 MA60 3881（六维§六），下行。"
        ok, _ = cdn._check_cited_grounded(note, "MA60 3881.43 且下行（V4.3 A1）。")
        assert ok

    def test_rounding_variant_passes(self):
        """note 写 3.4、md 写 3.38 → 1 位四舍五入变体命中"""
        note = "溢价均值 3.4%（六维§二）。"
        ok, _ = cdn._check_cited_grounded(note, "溢价均值 3.38%。")
        assert ok

    def test_computed_annotation_exempt(self):
        """（计算：…）诚实标注的派生值豁免（如差 0.62pct）"""
        note = "差 0.62pct（计算：4.0−3.38，六维§二）。"
        ok, _ = cdn._check_cited_grounded(note, "溢价 3.38% / 阈值 4.0%。")
        assert ok

    def test_small_count_exempt(self):
        """<100 无小数整数（家数/序号）不参与换源校验（误报源收窄）"""
        note = "R1 弱档否决明细：12 只连板股（六维§六）。"
        ok, _ = cdn._check_cited_grounded(note, "弱档判定生效。")
        assert ok

    def test_missing_report_fails(self):
        ok, why = cdn._check_cited_grounded("任意", None)
        assert not ok and '--report' in why


class TestRule9GateSemantics:
    """规则⑨：note 闸门/否决归因不得与六维报告状态矛盾。"""

    def test_veto_claim_with_marker_passes(self):
        note = "R1 梯队接力今日零开放——差异根因是弱档否决闸门在报告层。"
        md = ("候选开放度：回暖/震荡：R2-R4 开放（R1 因弱档否决不开放）；"
              "指数 ADX 闸门：full；MA60 3881↓ vs 收盘 3690——弱档判定生效"
              "（R1 不开放；触发源=MA60 简化规则）")
        ok, _ = cdn._check_gate_semantics(note, md)
        assert ok

    def test_veto_claim_without_marker_fails(self):
        """note 写「闸门关闭」而 md full 未生效 → 硬失败（20261023 假想错误）"""
        note = "指数趋势闸门关闭：本轮不出买入候选。"
        md = "指数 ADX 闸门：full（ADX 59.67↑）——弱档判定未生效"
        ok, why = cdn._check_gate_semantics(note, md)
        assert not ok and 'L1' in why

    def test_open_claim_contra_fails(self):
        """note 声称四类候选全开而 md 标注 R1 弱档否决 → 矛盾"""
        note = "候选开放度：回暖/震荡：四类候选全开。"
        md = "候选开放度：回暖/震荡：R2-R4 开放（R1 因弱档否决不开放）"
        ok, why = cdn._check_gate_semantics(note, md)
        assert not ok

    def test_candidate_veto_not_gate_claim(self):
        """「一票否决出池」（候选级 VETO）不触发闸门语义比对"""
        note = "利亚德、歌尔股份（业绩暴雷一票否决出池，当日 VETO 共 31 只）。"
        md = "指数 ADX 闸门：full——弱档判定未生效"
        ok, _ = cdn._check_gate_semantics(note, md)
        assert ok

    def test_open_claim_without_contra_passes(self):
        note = "候选开放度：回暖/震荡：四类候选全开；ADX 全开放。"
        md = "候选开放度：回暖/震荡：四类候选全开；ADX 全开放"
        ok, _ = cdn._check_gate_semantics(note, md)
        assert ok


class TestReportWiring:
    """--report 接线：缺失判失败防绕过；兄弟文件自动推导。"""

    def test_digest_without_report_fails_89(self, workdir, csv_file, note_file):
        """--digest 提供而 report 推导不出 → 规则⑧⑨报参缺失（防绕过）"""
        digest = workdir / 'd.html'
        digest.write_text('<details>口径字典</details>', encoding='utf-8')
        note = note_file("**主干：测试。**\n**基准倾向**：跟随。\n")
        fails = cdn.run_structural_rules(note, digest, None, None)
        assert any('cited_grounded' in f and '--report' in f for f in fails)
        assert any('gate_semantics' in f for f in fails)

    def test_sibling_report_autoderive(self, workdir, note_file):
        """note 同目录存在 daily_review_<X>.md → _derive_report_path 命中"""
        note = note_file("正文。")
        note.rename(workdir / 'daily_review_20261023_digest_note.md')
        note = workdir / 'daily_review_20261023_digest_note.md'
        (workdir / 'daily_review_20261023.md').write_text('MA60 3881.43',
                                                          encoding='utf-8')
        assert cdn._derive_report_path(note) ==             workdir / 'daily_review_20261023.md'

    def test_sibling_missing_returns_none(self, workdir, note_file):
        """命名合约定但兄弟文件缺失 → None（交规则⑧⑨报缺失）"""
        note = note_file("正文。")
        note.rename(workdir / 'daily_review_20261024_digest_note.md')
        assert cdn._derive_report_path(
            workdir / 'daily_review_20261024_digest_note.md') is None
        assert cdn._derive_report_path(note_file("任意名。")) is None
