"""估值 calc 函数库单测（P0-4，方案 §3.3.1/§3.3.2 验收口径）。

覆盖：7 个 calc 函数的三态（ok/not_meaningful/insufficient）、inf/NaN 守卫、
calculation_id 确定性与参数规范化、display 格式化。TTM 纯函数口径由
test/test_valuation_calc.py 与 test_financial_ttm.py 保护。
"""

import math

import pytest

from src.review.valuation_calc import (
    INSUFFICIENT, NOT_MEANINGFUL, OK, calculation_id, format_pe, format_pct,
    format_years, forward_vs_ttm_judgement, pe_deducted_annualized,
    pe_digestion_years, peg, percentile_rank, profit_cagr_hist,
)


class TestPeAnnualized:
    def test_ok_归母单季年化(self):
        r = pe_deducted_annualized(25.0, 1000.0)   # 1000/(25*4)=10x
        assert r.status == OK and r.value == pytest.approx(10.0)
        assert '非扣非' in r.reason                  # 降级口径显式标注

    def test_loss_not_meaningful(self):
        r = pe_deducted_annualized(-5.0, 1000.0)
        assert r.status == NOT_MEANINGFUL and r.value is None
        assert '亏损' in r.reason

    def test_missing_insufficient(self):
        assert pe_deducted_annualized(None, 1000.0).status == INSUFFICIENT
        assert pe_deducted_annualized(25.0, None).status == INSUFFICIENT


class TestProfitCagrHist:
    def test_ok(self):
        # 100 → 200 用 2 年 → CAGR ≈ 41.42%
        r = profit_cagr_hist([100.0, 150.0, 200.0], years=2)
        assert r.status == OK
        assert r.value == pytest.approx(41.42, abs=0.01)
        assert math.isfinite(r.value)

    def test_insufficient_points(self):
        assert profit_cagr_hist([100.0, 200.0], years=2).status == INSUFFICIENT
        assert profit_cagr_hist([], years=2).status == INSUFFICIENT

    def test_insufficient_first_nonpositive(self):
        assert profit_cagr_hist([0.0, 100.0, 200.0], years=2).status == INSUFFICIENT


class TestPeDigestionYears:
    def test_ok(self):
        # PE 31.25 → 锚 25，CAGR 25%：ln(31.25/25)/ln(1.25) = 1.0 年
        r = pe_digestion_years(31.25, 25.0, 25.0)
        assert r.status == OK
        assert r.value == pytest.approx(1.0)

    def test_negative_growth_not_meaningful(self):
        r = pe_digestion_years(31.25, 25.0, -10.0)
        assert r.status == NOT_MEANINGFUL
        assert r.value is None and '负增长' in r.reason

    def test_already_below_anchor(self):
        """PE 20 < 锚 25（已比锚定价便宜）→ 0 年，无需消化。"""
        r = pe_digestion_years(20.0, 25.0, 10.0)
        assert r.status == OK and r.value == 0.0

    def test_zero_cagr_no_inf(self):
        r = pe_digestion_years(20.0, 25.0, 0.0)
        assert r.status == NOT_MEANINGFUL
        assert r.value is None                      # 禁 inf


class TestForwardVsTtm:
    def test_forward_missing_insufficient(self):
        r = forward_vs_ttm_judgement(None, 18.0, ttm_yoy_pct=12.5)
        assert r.status == INSUFFICIENT
        assert '前瞻数据未获取' in r.reason
        assert '+12.5%' in r.reason                  # TTM 同比侧就绪

    def test_forward_present_bands(self):
        near = forward_vs_ttm_judgement(18.5, 18.0)
        assert near.status == OK and '基本一致' in near.reason
        down = forward_vs_ttm_judgement(15.0, 18.0)
        assert '预期下修' in down.reason
        up = forward_vs_ttm_judgement(21.0, 18.0)
        assert '预期上修' in up.reason


class TestPercentileRank:
    def test_ok(self):
        hist = [float(x) for x in range(1, 101)]   # 1..100
        r = percentile_rank(50.0, hist)
        assert r.status == OK and r.value == pytest.approx(50.0)

    def test_insufficient_samples(self):
        r = percentile_rank(50.0, [10.0, 20.0], min_samples=20)
        assert r.status == INSUFFICIENT and '样本不足' in r.reason

    def test_nonpositive_value_not_meaningful(self):
        r = percentile_rank(-1.0, [float(x) for x in range(1, 101)])
        assert r.status == NOT_MEANINGFUL

    def test_history_nonpositive_filtered(self):
        hist = [-5.0] * 30 + [float(x) for x in range(1, 21)]
        r = percentile_rank(10.0, hist)             # 负值剔除后 20 样本
        assert r.status == OK


class TestPeg:
    def test_ok(self):
        r = peg(20.0, 25.0)
        assert r.status == OK and r.value == pytest.approx(0.8)
        assert 'PEG 低≠安全' in r.reason

    def test_negative_growth(self):
        assert peg(20.0, -5.0).status == NOT_MEANINGFUL
        assert peg(20.0, None).status == INSUFFICIENT


class TestCalculationId:
    def test_deterministic(self):
        a = calculation_id('peg', {'pe': 20.0, 'cagr': 25.0})
        b = calculation_id('peg', {'pe': 20.0, 'cagr': 25.0})
        assert a == b and len(a) == 16

    def test_int_float_same_id(self):
        """参数规范化：1 与 1.0 同 id（原则 P7 同日重算一致）。"""
        assert calculation_id('f', {'x': 1}) == calculation_id('f', {'x': 1.0})

    def test_different_params_different_id(self):
        assert calculation_id('f', {'x': 1}) != calculation_id('f', {'x': 2})


class TestDisplay:
    def test_formats(self):
        assert format_pe(18.234) == '18.2x'
        assert format_pe(None) == '—' and format_pe(-1.0) == '—'
        assert format_pct(31.6) == '32%'
        assert format_years(2.14) == '2.1年'
        assert format_years(float('inf')) == '—'   # inf 守卫
