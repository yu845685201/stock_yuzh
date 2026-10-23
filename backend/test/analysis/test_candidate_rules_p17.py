"""P1-7 判据增强单测（方案 §4.6）：A 分层 / B VETO / D 五维评分 / E ADX 闸门。

覆盖新增谓词函数与阈值语义；R 模块侧集成（理由串分层标签、反高开低走、
聚集强度）由 test_candidate_modules 与链实跑覆盖。
"""

import pytest

from src.analysis.candidate_rules import (
    DEFAULT_SCORE_WEIGHTS, VETO_SCORE, adx_gate, is_r2, is_r3, is_vetoed,
    r1_tier, score_candidate, veto_reasons,
)


class TestVeto:
    def test_no_veto_empty(self):
        assert veto_reasons() == []

    def test_st_veto(self):
        assert 'ST/*ST' in veto_reasons(is_st=True)

    def test_profit_bomb_veto(self):
        """业绩暴雷：净利同比 ≤ 阈值（含等号）。"""
        assert any('业绩暴雷' in r for r in
                   veto_reasons(profit_yoy=-60.0))
        assert any('业绩暴雷' in r for r in
                   veto_reasons(profit_yoy=-50.0))       # 等号即暴雷
        assert veto_reasons(profit_yoy=-49.0) == []      # 差 1pct 不否决

    def test_profit_missing_no_veto(self):
        """缺失不过否决（不硬造）。"""
        assert veto_reasons(profit_yoy=None) == []

    def test_reduction_ann_veto(self):
        assert '大股东减持' in veto_reasons(has_reduction_ann=True)

    def test_lhb_sell1_veto(self):
        reasons = veto_reasons(lhb_sell1_pct=35.0)
        assert any('卖一占比' in r for r in reasons)
        assert veto_reasons(lhb_sell1_pct=30.0) == []     # 等号不否决（>才毙）

    def test_high_board_volume_yin_veto(self):
        """高位板 + 放量长阴：连板 ≥6 且 放量 ≥2 且 跌幅 ≥3%。"""
        assert '高位板放量长阴' in veto_reasons(
            streak=7, volume_surge=2.5, change_rate=-3.0)
        assert veto_reasons(                          # 低位板不适用
            streak=3, volume_surge=2.5, change_rate=-5.0) == []
        assert veto_reasons(                          # 缩量长阴不适用
            streak=7, volume_surge=1.5, change_rate=-5.0) == []
        assert veto_reasons(                          # 长阴但非跌 3% 不适用
            streak=7, volume_surge=2.5, change_rate=-2.0) == []

    def test_is_vetoed(self):
        assert is_vetoed(is_st=True) is True
        assert is_vetoed() is False
        assert VETO_SCORE == -100


class TestScore:
    def test_weighted_average(self):
        r = score_candidate({'tech': 80, 'money': 60})
        # (0.35*80 + 0.35*60) / 0.7 = 70
        assert r['score'] == pytest.approx(70.0)
        assert set(r['applied_weights']) == {'tech', 'money'}
        assert set(r['missing_dims']) == {'fundamental', 'sector', 'event'}

    def test_all_dims(self):
        dims = {'tech': 100, 'money': 100, 'fundamental': 100,
                'sector': 100, 'event': 100}
        assert score_candidate(dims)['score'] == 100.0

    def test_missing_normalization(self):
        """缺维按剩余维权重归一化（不硬造 0）。"""
        r = score_candidate({'tech': 50, 'money': None})
        assert r['score'] == pytest.approx(50.0)

    def test_all_missing_none(self):
        r = score_candidate({'tech': None, 'money': None})
        assert r['score'] is None

    def test_custom_weights_override(self):
        r = score_candidate({'tech': 80, 'money': 60},
                            weights={'tech': 1.0, 'money': 0.0})
        assert r['score'] == pytest.approx(80.0)

    def test_default_weights_sum_one(self):
        assert sum(DEFAULT_SCORE_WEIGHTS.values()) == pytest.approx(1.0)


class TestAdxGate:
    def test_full_open(self):
        assert adx_gate(30.0, True, 3500.0, 3400.0) == 'full'

    def test_half_open(self):
        assert adx_gate(15.0, True, 3500.0, 3400.0) == 'half'

    def test_closed_variants(self):
        assert adx_gate(20.0, True, 3500.0, 3400.0) == 'closed'   # 中档震荡
        assert adx_gate(30.0, False, 3500.0, 3400.0) == 'closed'  # 强但下行
        assert adx_gate(15.0, True, 3300.0, 3400.0) == 'closed'   # 低于 MA20

    def test_missing_inputs_closed(self):
        """输入缺失 → closed（禁默认开放）。"""
        assert adx_gate(None, True, 3500.0, 3400.0) == 'closed'
        assert adx_gate(30.0, True, None, 3400.0) == 'closed'

    def test_boundary_values(self):
        assert adx_gate(25.0, True, 3500.0, 3400.0) == 'closed'   # 25 不 >25
        assert adx_gate(18.0, True, 3500.0, 3400.0) == 'closed'   # 18 不 <18


class TestR3AntiHighOpen:
    """反高开低走（P1-7 A）：开盘涨幅 >5% 且 收盘<开盘 → 剔除。"""

    def test_high_open_low_close_rejected(self):
        assert not is_r3(True, False, 2.5, 4.0, open_gain=7.0,
                         close_lt_open=True)

    def test_high_open_but_close_above_open_ok(self):
        assert is_r3(True, False, 2.5, 4.0, open_gain=7.0,
                     close_lt_open=False)

    def test_low_open_gain_not_filtered(self):
        assert is_r3(True, False, 2.5, 4.0, open_gain=3.0,
                     close_lt_open=True)

    def test_missing_inputs_no_filter(self):
        """open_gain 缺失不过滤（不硬造）。"""
        assert is_r3(True, False, 2.5, 4.0, open_gain=None,
                     close_lt_open=True)


class TestR2ClusterStrength:
    """聚集强度阈值（P1-7 A）：聚集家数 < r2_cluster_min 不通过。"""

    def test_cluster_below_min_rejected(self):
        assert not is_r2(1, False, ['创新药'], ['创新药'],
                         cluster_count=1)

    def test_cluster_at_min_ok(self):
        assert is_r2(1, False, ['创新药'], ['创新药'], cluster_count=2)

    def test_cluster_missing_no_filter(self):
        """家数缺失不过滤（不硬造）。"""
        assert is_r2(1, False, ['创新药'], ['创新药'], cluster_count=None)
