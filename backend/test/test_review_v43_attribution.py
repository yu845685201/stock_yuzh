"""V4.3 A1/A2 单测：弱档否决可见化（gate_note 随行 MA60/弱档判定）与
R1 空池三态归因（弱档否决/规则筛除/池缺失）。

背景：20261023 实况——gate=full、market_weak=True（MA60 简化规则触发），
R1 被静默否决而报告渲染"四类候选全开"，自相矛盾；note 撰写方只能自行推断。

模式沿用 test_review_p0_fixes.py：stub agg，不触网不落库。
"""
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.analysis.candidate_rules import is_r1, r1_reject_reason  # noqa: E402
from src.review.sections import tomorrow_plan  # noqa: E402


def _text(blocks):
    return ''.join(str(b.get('text', '')) for b in blocks)


def _agg(market_weak, gate='full', r1_filtered=None, r1_pool=None):
    """stub agg：sentiment=回暖/震荡 + adx_gate 按参注入（10-23 实况形状）。"""
    adx = {'gate': gate, 'index': 'sh.000001', 'adx': 59.67, 'adx_rising': True,
           'close': 3690.48, 'ma20': 3848.12, 'ma60': 3881.43,
           'market_weak': market_weak}
    if gate == 'closed' and not market_weak:
        adx = {'gate': 'closed', 'reason': 'ADX/MA20 warmup 不足'}
    return {'sentiment': {'data': {'cycle': {'position': '回暖/震荡'}}},
            'candidates': {'available': True, 'adx_gate': adx,
                           'R1_filtered': r1_filtered or [],
                           'R1': r1_pool or [], 'R2': [], 'R3': [], 'R4': [],
                           'vetoed': [], 'vetoed_count': 0}}


_FILTERED_WEAK = [
    {'ts_code': 'a', 'code': '1', 'name': '华阳智能', 'streak': 2,
     'open_count': 1, 'reason': 'weak_veto'},
    {'ts_code': 'b', 'code': '2', 'name': '上海九百', 'streak': 2,
     'open_count': 2, 'reason': 'weak_veto'},
    {'ts_code': 'c', 'code': '3', 'name': '睿能科技', 'streak': 2,
     'open_count': 0, 'reason': 'weak_veto'},
]


class TestA1WeakVetoRendering:
    """A1：弱档判定随行渲染（ma60/market_weak 在 adx_info 已算出，此前零渲染）。"""

    def test_market_weak_full_gate_visible(self):
        """10-23 实况：full + market_weak → MA60/弱档判定生效随行 + 开放度行
        R1 剔除说明 + 弱档否决明细行。"""
        text = _text(tomorrow_plan._buy_candidates(
            _agg(True, 'full', _FILTERED_WEAK), {}))
        assert '指数 ADX 闸门：full' in text
        assert 'MA60 3881↓ vs 收盘 3690' in text
        assert '弱档判定生效（R1 不开放；触发源=MA60 简化规则）' in text
        assert 'R2-R4 开放（R1 因弱档否决不开放）；ADX 全开放——P1-10：收盘<MA60 且 MA60 下行' in text
        assert '四类候选全开' not in text
        assert 'R1 弱档否决明细：3 只连板股整类不开放' in text
        assert '华阳智能（2板/开板1次）' in text
        # 3881 从此在六维 md 可检索（A3 规则⑧的前置）
        assert '3881' in text

    def test_market_weak_closed_gate_source_adx(self):
        """closed 档弱档触发源=ADX 闸门 closed；MA60 方向未知不标 ↓。"""
        text = _text(tomorrow_plan._buy_candidates(
            _agg(True, 'closed'), {}))
        assert '触发源=ADX 闸门 closed' in text
        assert 'MA60 3881↓' not in text
        assert '本轮不出买入候选' in text

    def test_not_weak_full_gate_unchanged(self):
        """非弱档 full：弱档判定未生效随行；无 R1 剔除说明、无否决明细。"""
        text = _text(tomorrow_plan._buy_candidates(_agg(False, 'full'), {}))
        assert '弱档判定未生效' in text
        assert '四类候选全开' in text
        assert 'R1 因弱档否决不开放' not in text
        assert 'R1 弱档否决明细' not in text

    def test_ma60_missing_degrades_silently(self):
        """ma60 缺失（数据不足路径）→ weak_note 整段不渲染，不硬造。"""
        agg = _agg(False, 'full')
        agg['candidates']['adx_gate'] = {
            'gate': 'full', 'index': 'sh.000001', 'adx': 30.0,
            'adx_rising': True, 'close': 3690.0, 'ma20': 3700.0,
            'market_weak': False}
        text = _text(tomorrow_plan._buy_candidates(agg, {}))
        assert '弱档判定' not in text


class TestA2R1EmptyThreeStates:
    """A2：R1 空池三态归因（替代旧两态混写文案）。"""

    def test_reopen_over_state(self):
        """规则筛除态：开板超限计数 + 高位板更严档计数 + 前 3 点名。"""
        filtered = [
            {'ts_code': 'x', 'code': '9', 'name': '甲股', 'streak': 3,
             'open_count': 4, 'reason': 'reopen_over'},
            {'ts_code': 'y', 'code': '8', 'name': '乙股', 'streak': 6,
             'open_count': 3, 'reason': 'high_reopen_over'},
        ]
        agg = _agg(False, 'full', filtered)
        agg['candidates']['R2'] = [{'code': '002812', 'name': '恩捷股份',
                                    'label': '首板/专精特新', 'amount': 1.66e9,
                                    'evidence': ['资金', '聚集'],
                                    'evidence_count': 2, 'score': 71.3}]
        text = _text(tomorrow_plan._buy_candidates(agg, {}))
        assert '无入选——2 只连板股均未过封板规则：开板超限 2 只' in text
        assert '甲股（开板4次）' in text
        assert '其中高位板更严档 1 只' in text

    def test_pool_missing_state(self):
        """池缺失态：R1 空池给出三态新文案（R2-R4 空池按设计保留旧文案）。"""
        text = _text(tomorrow_plan._buy_candidates(_agg(False, 'full'), {}))
        assert '无入选（池数据缺失：当日无连板≥2 截面）' in text

    def test_weak_veto_rows_not_rendered_as_pool(self):
        """弱档日 R1 被剔出 open_types：池节不出现 R1 标题，明细行走 note 块。"""
        text = _text(tomorrow_plan._buy_candidates(
            _agg(True, 'full', _FILTERED_WEAK), {}))
        assert '梯队接力候选（0 只' not in text
        assert 'R1 弱档否决明细' in text


class TestR1RejectReasonParity:
    """r1_reject_reason 与 is_r1 判定对拍（单一出处防漂移）。"""

    @pytest.mark.parametrize('streak,is_st,oc,mw', [
        (2, False, 0, False), (2, False, 3, False), (6, False, 2, False),
        (6, False, 1, False), (1, False, 0, False), (3, True, 0, False),
        (2, False, None, False), (0, False, 0, True), (6, False, 0, True),
        (None, False, 0, False),
    ])
    def test_parity(self, streak, is_st, oc, mw):
        assert is_r1(streak, is_st, oc, market_weak=mw) == \
            (r1_reject_reason(streak, is_st, oc, market_weak=mw) is None)

    def test_reason_values(self):
        assert r1_reject_reason(1, False, 0) == 'streak_low'
        assert r1_reject_reason(3, True, 0) == 'st'
        assert r1_reject_reason(2, False, 3) == 'reopen_over'
        assert r1_reject_reason(6, False, 2) == 'high_reopen_over'
        assert r1_reject_reason(6, False, 1) is None
        assert r1_reject_reason(2, False, 0, market_weak=True) == 'weak_veto'
