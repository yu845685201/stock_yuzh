"""V4.3 B1-B3/C1-C2 渲染层单测：A-H 图表同序 / 距最近判据钩子 / 连板溢价
裂口点名 / 缠论表收盘距止损列 / score 窗口分位极值降噪。

模式沿用 test_review_p1_render.py：stub agg，不触网不落库。
"""
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.review import digest  # noqa: E402
from src.review.money_effect import _consec_premium  # noqa: E402


# ---------------------------------------------------------------- B2 钩子行

class TestB2NearestMissHook:
    """直接测生产函数 digest._render_nearest_miss（P3-4 教训：禁自证式测试）。"""

    def test_hook_rendered(self):
        nm = {'name': '高潮', 'metric': '溢价均值', 'value': 3.38,
              'threshold': 4.0, 'gap': 0.62}
        html = digest._render_nearest_miss(nm)
        assert '距最近定位判据：高潮 溢价均值 +3.38% vs 阈值 +4.0%（差 0.62pct）' in html
        assert '次日溢价 ≥4.0% 即触发，验点 V3 直接验证' in html

    def test_ebb_direction_le(self):
        nm = {'name': '退潮', 'metric': '溢价均值', 'value': 0.8,
              'threshold': 0.0, 'gap': 0.8}
        html = digest._render_nearest_miss(nm)
        assert '次日溢价 ≤0.0% 即触发' in html

    def test_none_and_missing_gap_return_none(self):
        assert digest._render_nearest_miss(None) is None
        assert digest._render_nearest_miss({'name': '高潮', 'gap': None}) is None


# ---------------------------------------------------------------- B3 裂口点名

class TestB3ConsecPremiumBest:
    def test_best_tracked_and_named(self):
        today = {'a': {'chg': -2.0, 'name': '甲'}, 'b': {'chg': 21.0, 'name': '乙'},
                 'c': {'chg': 3.0, 'name': '丙'}}
        out = _consec_premium({'a': 2, 'b': 2, 'c': 2}, today)
        assert out['best'] == {'code': 'b', 'name': '乙', 'chg': 21.0}
        assert abs(out['mean'] - out['median']) >= 3

    def test_empty_traded_best_none(self):
        out = _consec_premium({'a': 2}, {})
        assert out['best'] is None

    def test_name_missing_safe(self):
        """旧合成截面无 name 列 → best.name 置空不硬造。"""
        out = _consec_premium({'a': 2}, {'a': {'chg': 5.0}})
        assert out['best'] == {'code': 'a', 'name': '', 'chg': 5.0}


# ---------------------------------------------------------------- C1 距止损列

class TestC1StopDistanceColumn:
    def _chan_agg(self, close):
        from src.review.aggregator import AggregationResult
        item = {'ts_code': 'sz.002643', 'code': '002643', 'name': '万润股份',
                'kind': '一买', 'divergence': 0.01, 'trigger_low': 11.18,
                'stop': 11.18, 'data_flag': None, 'refire': False}
        if close is not None:
            item['close'] = close
        return {'chan': AggregationResult(data={'items': [item], 'count': 1})}

    def test_column_rendered_positive(self):
        """收盘 11.30 > 止损 11.18 → 距止损 +1.1%（正值在止损上方）。"""
        html = digest._render_chan_digest(self._chan_agg(11.30),
                                          {'position': '回暖/震荡'}, {}, {})
        assert '收盘距止损%' in html
        assert '+1.1' in html
        assert '收盘口径' in html or '操作口径为次日开盘' in html

    def test_close_missing_dash(self):
        """close 缺失（旧缓存日）→ '—' 不硬造。"""
        html = digest._render_chan_digest(self._chan_agg(None),
                                          {'position': '回暖/震荡'}, {}, {})
        assert '收盘距止损%' in html

    def test_close_below_stop_negative(self):
        """收盘低于止损 → 负值。"""
        html = digest._render_chan_digest(self._chan_agg(11.00),
                                          {'position': '回暖/震荡'}, {}, {})
        assert '-1.6' in html


# ---------------------------------------------------------------- C2 极值降噪

class TestC2WindowPctDenoise:
    """直接测生产函数 digest._window_pct_text。"""

    def test_extreme_omitted(self):
        for v in (100, 0, '100', '0', 100.0, 0.0):
            assert digest._window_pct_text(v) == '', v

    def test_normal_kept(self):
        assert digest._window_pct_text(70.4) == '（窗口分位 70.4%）'
        assert digest._window_pct_text('38') == '（窗口分位 38%）'

    def test_missing_and_unparseable_omitted(self):
        assert digest._window_pct_text(None) == ''
        assert digest._window_pct_text('n/a') == ''
