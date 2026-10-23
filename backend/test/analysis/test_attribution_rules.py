"""归因规则单测（P1-11，方案 §3.4.1 验收）：关键词正反、18 类映射、极性、
pick 优先级、空窗口、覆盖断代。模块层见 test_limit_up_attribution.py。
"""

import pytest

from src.analysis.attribution_rules import (
    ANN_TYPE_MAP, COVERAGE_CUTOFF, FALLBACK_LABEL, KEYWORDS, classify,
    coverage_note, match_keyword, pick_attribution,
)


class TestKeywords:
    def test_count_19(self):
        assert len(KEYWORDS) == 19
        assert len(set(KEYWORDS)) == 19

    @pytest.mark.parametrize("title,kw", [
        ('关于中标某项目暨签订合同的公告', '中标'),
        ('控股股东签署战略合作协议', '战略合作'),
        ('关于收购XX公司100%股权的公告', '收购'),
        ('2026年半年度业绩预增公告', '预增'),
        ('关于回购股份方案的公告', '回购'),
        ('关于获得政府补贴的公告', '补贴'),
        ('关于终止重大资产重组的公告', '重组'),
        ('关于非公开发行股票（定增）的公告', '定增'),
        ('关于股票复牌的提示性公告', '复牌'),
    ])
    def test_keyword_hit(self, title, kw):
        assert match_keyword(title) == kw

    @pytest.mark.parametrize("title", [
        '关于召开2026年第一次临时股东大会的通知',
        '关于举行年度业绩说明会的公告',
        '关于监事辞职的公告',
        '',
        None,
    ])
    def test_keyword_miss(self, title):
        assert match_keyword(title) is None

    def test_long_keyword_first(self):
        """「中标候选人」先于「中标」命中（长词优先）。"""
        assert match_keyword('关于项目中标候选人公示的公告') == '中标候选人'


class TestClassify:
    def test_keyword_priority_over_type(self):
        """关键词优先于类型：类型为定期报告但标题有「中标」→ 题材催化。"""
        label, kind = classify('关于中标的公告', '定期报告')
        assert label == '题材催化(中标)' and kind == '利好'

    @pytest.mark.parametrize("ann_type,label,kind", [
        ('业绩预告', '业绩预增', '利好'),
        ('重大事项', '重大事项', '利好'),
        ('回购', '回购', '利好'),
        ('分红送转', '分红送转', '利好'),
        ('解禁', '解禁', '利空'),
        ('监管问询', '监管问询', '利空'),
        ('风险警示', '风险警示', '利空'),
        ('诉讼仲裁', '诉讼仲裁', '利空'),
        ('定期报告', '定期报告', '中性'),
        ('公司治理', '公司治理', '中性'),
    ])
    def test_type_mapping(self, ann_type, label, kind):
        assert classify('某公告', ann_type) == (label, kind)

    def test_all_18_types_mapped(self):
        """库内 18 类全部有映射（漏映射 → '其他' 兜底但单测点名）。"""
        assert len(ANN_TYPE_MAP) == 18

    def test_unknown_type_fallback(self):
        label, kind = classify('某公告', '不存在的类型')
        assert label == '其他' and kind == '中性'


class TestPickAttribution:
    def test_empty_announcements(self):
        label, kind, src = pick_attribution([])
        assert label == FALLBACK_LABEL and kind == '无' and src is None

    def test_keyword_beats_type(self):
        anns = [
            {'ann_id': 2, 'ann_date': '20260930', 'ann_type': '重大事项', 'title': '关于对外投资项目的公告'},
            {'ann_id': 1, 'ann_date': '20260929', 'ann_type': '重大事项', 'title': '关于中标的公告'},
        ]
        label, kind, src = pick_attribution(anns)
        assert label == '题材催化(中标)' and kind == '利好'
        assert src['ann_id'] == 1

    def test_latest_among_same_priority(self):
        """同为利好类型（标题无关键词）→ 取窗口内最新。"""
        anns = [
            {'ann_id': 1, 'ann_date': '20260929', 'ann_type': '股权激励', 'title': '关于股权激励计划的公告'},
            {'ann_id': 2, 'ann_date': '20260930', 'ann_type': '募集资金', 'title': '关于募集资金使用进展的公告'},
        ]
        label, _, src = pick_attribution(anns)
        assert src['ann_id'] == 2          # 同优先级取最新

    def test_no_bulls_fallback(self):
        """仅利空/中性公告 → 题材炒作或跟风上涨（利好=0 显式结论）。"""
        anns = [
            {'ann_id': 1, 'ann_date': '20260930', 'ann_type': '解禁', 'title': '限售股上市流通'},
            {'ann_id': 2, 'ann_date': '20260929', 'ann_type': '定期报告', 'title': '第三季度报告'},
        ]
        label, kind, src = pick_attribution(anns)
        assert label == FALLBACK_LABEL and kind == '无' and src is None

    def test_bearish_type_never_attribution(self):
        """利空类型（解禁）不得成为涨停利好归因。"""
        anns = [{'ann_id': 1, 'ann_date': '20260930', 'ann_type': '解禁',
                 'title': '关于限售股上市流通的提示性公告'}]
        label, _, _ = pick_attribution(anns)
        assert label == FALLBACK_LABEL


class TestCoverageNote:
    def test_before_cutoff(self):
        assert coverage_note('20260901') is True
        assert coverage_note('20250101') is True

    def test_on_and_after_cutoff(self):
        assert coverage_note(COVERAGE_CUTOFF) is False
        assert coverage_note('20260930') is False
