"""V2.0 优化步骤 3（P1 渲染层）单测：判据对照表 / 口径字典 / A50 分级 /
资金锚质量标签 / 缠论 digest 节 / V4 验点 / 情绪 criteria 与 trial 水印。

模式沿用 test_review_phase3.py：stub reader + 合成 agg，不触网不落库。
"""
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.review.aggregator import ReviewAggregator
from src.review.digest import (_GLOSSARY_ROWS, _anchor_conf_note,
                               _chan_linkage_state, _render_chan_digest,
                               _render_glossary)
from src.review.sections import market_env, tomorrow_plan


# ---------------------------------------------------------------- P1-1 / P1-4

class _EmptyReader:
    def _query(self, sql, params=None):
        return []


def _agg_cfg(calibrated):
    cfg = {
        'review': {
            'sentiment_calibrated': calibrated,
            'sentiment_rules': {
                'climax': {'max_streak_gte': 5, 'premium_mean_gte': 4.0},
                'ice': {'max_streak_lte': 2, 'burst_rate_gte': 0.5},
                'ebb': {'premium_mean_lte': 0},
            },
        },
    }
    return ReviewAggregator(_EmptyReader(), cfg)


class TestSentimentCriteria:
    def test_criteria_structured_and_not_hit(self):
        """10-23 案例：高度6 命中高潮半条件、溢价距 4% 差 0.62pct → 全部未命中"""
        out = _agg_cfg(True)._sentiment_cycle(
            {'max_streak': 6, 'burst_rate': 0.22, 'premium_mean': 3.38}, '20261023')
        names = [c['name'] for c in out['criteria']]
        assert names == ['高潮', '冰点', '退潮', '高潮·第二证据（独立）']
        climax = out['criteria'][0]
        assert '高度≥5' in climax['expr'] and '溢价≥4.0%' in climax['expr']
        assert '高度6' in climax['values'] and '溢价3.38%' in climax['values']
        assert all(not c['hit'] for c in out['criteria'])
        assert out['position'] == '回暖/震荡'

    def test_criteria_hit_marks(self):
        out = _agg_cfg(True)._sentiment_cycle(
            {'max_streak': 6, 'burst_rate': 0.2, 'premium_mean': 4.5}, '20261023')
        assert out['position'] == '高潮'
        assert out['criteria'][0]['hit'] is True
        assert out['criteria'][1]['hit'] is False

    def test_climax_second_evidence_breadth(self):
        """P4-5（B8）：第 4 行=涨跌家数比独立证据；climax 命中时 basis 带确认/未确认"""
        out = _agg_cfg(True)._sentiment_cycle(
            {'max_streak': 6, 'burst_rate': 0.2, 'premium_mean': 4.5,
             'up_count': 3000, 'down_count': 1000}, '20261023')
        sec = out['criteria'][3]
        assert sec['hit'] is True and '比3.0' in sec['values']
        assert '涨跌家数比≥2.0' in sec['expr']
        assert any('确认' in b and '未确认' not in b for b in out['basis'])
        out2 = _agg_cfg(True)._sentiment_cycle(
            {'max_streak': 6, 'burst_rate': 0.2, 'premium_mean': 4.5,
             'up_count': 1200, 'down_count': 2600}, '20261023')
        assert out2['criteria'][3]['hit'] is False
        assert any('未确认' in b for b in out2['basis'])
        # 无涨跌家数（缺数据）：不硬造，值显示 —，hit False
        out3 = _agg_cfg(True)._sentiment_cycle(
            {'max_streak': 6, 'burst_rate': 0.2, 'premium_mean': 4.5}, '20261023')
        assert out3['criteria'][3]['hit'] is False and '比—' in out3['criteria'][3]['values']

    def test_trial_flag_two_states(self):
        """P1-4：trial 键随 sentiment_calibrated 开关两态（水印消费恢复）"""
        assert _agg_cfg(False)._sentiment_cycle(
            {'max_streak': 3, 'burst_rate': 0.2, 'premium_mean': 1.0})['trial'] is True
        assert _agg_cfg(True)._sentiment_cycle(
            {'max_streak': 3, 'burst_rate': 0.2, 'premium_mean': 1.0})['trial'] is False


# ---------------------------------------------------------------- P1-2

class TestGlossary:
    def test_glossary_covers_key_terms(self):
        html = _render_glossary()
        assert html.startswith('<details>') and '口径字典' in html
        terms = [t for t, _ in _GLOSSARY_ROWS]
        for need in ('涨停家数', '炸板率', '溢价均值', 'R1-R4', '弱档否决',
                     '一买（缠论）', '背驰强度', '中枢'):
            assert any(need in t for t in terms), need
        # 缠论条目含入选口径与历史统计出处（2026-10-25 门控保留样本修订后数字）
        chan_rows = ' '.join(d for t, d in _GLOSSARY_ROWS if '缠论' in t or '背驰' in t)
        assert '+6.79%' in chan_rows and '次日开盘' in chan_rows
        # A50 快照口径披露（P1-5 采集侧评估收口，附录 C.5）
        assert any('A50' in t for t in terms)


# ---------------------------------------------------------------- P1-5

class TestA50Stale:
    def test_snapshot_stale_days(self):
        f = market_env._snapshot_stale_days
        assert f('20261006', '20261023') == 17
        assert f('2026-10-06', '20261023') == 17
        assert f('20261024', '20261023') == -1
        assert f('最新可得', '20261023') is None
        assert f(None, None) is None

    @staticmethod
    def _env(snap_date):
        return {'available': True, 'data': {
            'as_of': '20261023',
            'indices': [], 'overseas_indices': [], 'ah_mapping': {},
            'overseas': [{'name': '富时A50期货', 'date': snap_date,
                          'close': 13800.0, 'change_pct': 1.2},
                         {'name': '离岸人民币', 'date': snap_date,
                          'close': 7.13, 'change_pct': -0.1}],
        }}

    def test_fresh_snapshot_unchanged(self):
        text = '\n'.join(str(b) for b in market_env.build(self._env('20261021'), {'available': True, 'data': {}}))
        assert '累计值非隔夜' not in text

    def test_stale_4_days_annotated(self):
        text = '\n'.join(str(b) for b in market_env.build(self._env('20261018'), {'available': True, 'data': {}}))
        assert '累计值非隔夜' in text and '快照距锚定日 5 天' in text

    def test_stale_17_days_column_hidden(self):
        text = '\n'.join(str(b) for b in market_env.build(self._env('20261006'), {'available': True, 'data': {}}))
        assert '快照过期 17 天' in text and '整列停显' in text
        assert '涨跌幅%' not in text.split('汇率与其他')[1].split('note')[0]

    def test_future_snapshot_annotated(self):
        """历史重跑：快照晚于锚定日 → 涨跌幅为快照日口径标注"""
        text = '\n'.join(str(b) for b in market_env.build(self._env('20261024'), {'available': True, 'data': {}}))
        assert '快照日期晚于锚定日' in text


# ---------------------------------------------------------------- P1-6

class TestCapitalAnchorTags:
    @staticmethod
    def _ca(hkex_date='20261023', etf_date='20261023'):
        return {'available': True, 'data': {
            'hkex_date': hkex_date, 'north_total_yi': 3131.8, 'south_net_yi': 45.2,
            'etf': {'date': etf_date, 'prev_date': '20261022', 'chg_yi_fen': -1.2},
            'macro': None, 'option': None, 'north_detail': None,
            'anchor_tags': {'south_net': '实测', 'north_total': '代理',
                            'etf': '代理', 'macro': '实测', 'option': '实测',
                            'north_detail': '停旧值'},
            'confidence': '中',
        }}

    def test_tags_rendered_with_confidence(self):
        text = '\n'.join(str(b) for b in market_env.build(
            {'available': True, 'data': {}}, {'available': True, 'data': {}},
            capital_anchors=self._ca()))
        assert '质量' in text and '代理' in text and '实测' in text
        assert '综合置信度：中' in text

    def test_stale_tag_overrides(self):
        """聚合器：快照日期 < as_of → 停旧值覆盖；置信度降低"""
        class _R:
            def _query(self, sql, params=None):
                s = str(sql)
                if 'hkex_connect_daily' in s:
                    return [('north_sse', 100000, 60000, 55000),
                            ('south_sse', 100000, 60000, 55000)]
                if 'etf_scale_daily' in s:
                    return [('20261020', 5e9), ('20261019', 5.1e9)]
                return []
        agg = ReviewAggregator(_R(), {'review': {}})
        data = agg._capital_anchors('20261023').data
        # hkex 查询按锚定日精确取（缺行整项消失）；etf 取 <= as_of 最新 → 停旧值
        assert data['etf']['date'] == '20261020'
        assert data['anchor_tags']['etf'] == '停旧值'
        assert data['anchor_tags']['south_net'] == '实测'
        assert data['confidence'] == '低'

    def test_fresh_tags_confidence_medium(self):
        """当日快照：hkex=实测、etf=代理（北向总额代理）→ 置信度中"""
        class _R:
            def _query(self, sql, params=None):
                s = str(sql)
                if 'hkex_connect_daily' in s:
                    return [('north_sse', 100000, 60000, 55000),
                            ('south_sse', 100000, 60000, 55000)]
                if 'etf_scale_daily' in s:
                    return [('20261023', 5e9), ('20261022', 5.1e9)]
                return []
        agg = ReviewAggregator(_R(), {'review': {}})
        data = agg._capital_anchors('20261023').data
        assert data['anchor_tags']['south_net'] == '实测'
        assert data['anchor_tags']['north_total'] == '代理'
        assert data['confidence'] == '中'


# ---------------------------------------------------------------- P1-8

class TestChanDigestSection:
    @staticmethod
    def _chan_agg():
        items = [
            {'ts_code': 'sh.600331', 'code': '600331', 'name': '宏达股份',
             'kind': '一买', 'divergence': 0.06, 'trigger_low': 10.63,
             'stop': 10.63, 'data_flag': '缺日1天', 'suspect_reason': 'X缺日',
             'refire': False},
            {'ts_code': 'sz.002643', 'code': '002643', 'name': '万润股份',
             'kind': '一买', 'divergence': 0.01, 'trigger_low': 11.18,
             'stop': 11.18, 'data_flag': None, 'refire': False},
        ]
        from src.review.aggregator import AggregationResult
        return {'chan': AggregationResult(data={
            'items': items, 'count': 2, 'window_start': '20240701',
            'suspect_overlap': {'available': True, 'suspect_total': 1194,
                                'hit_count': 1, 'hits': []}})}

    def test_table_headers_and_marks(self):
        html = _render_chan_digest(self._chan_agg(),
                                   {'position': '回暖/震荡'}, {}, {})
        assert '六、缠论背驰候选' in html
        for h in ('代码', '名称', '信号', '背驰强度', '行业', '触发低点(止损)', '数据标记'):
            assert h in html
        assert '缺日1天；链存疑' in html
        assert '—' in html
        assert '行业' in html and '宏达股份' in html

    def test_linkage_state_rendered(self):
        html = _render_chan_digest(self._chan_agg(),
                                   {'position': '回暖/震荡'}, {}, {})
        assert '弱化观察' in html
        html2 = _render_chan_digest(self._chan_agg(),
                                    {'position': '冰点'}, {}, {})
        assert '升主关注' in html2

    def test_suspect_overlap_and_glossary_note(self):
        html = _render_chan_digest(self._chan_agg(),
                                   {'position': '回暖/震荡'}, {}, {})
        assert '缠论候选×存疑清单交叉：命中 1 只' in html
        assert '全链存疑股票 1194 只' in html
        assert '|Σhist₂|/|Σhist₁| &lt; 1 即背驰' in html
        assert '短笔柱面积' in html
        assert '+6.79%' in html          # HIST_NOTE 随行（2026-10-25 门控保留样本修订）

    def test_industry_column_from_map(self):
        html = _render_chan_digest(self._chan_agg(),
                                   {'position': '回暖/震荡'},
                                   {'sh.600331': '有色金属'}, {})
        assert '有色金属' in html

    def test_unavailable_returns_none(self):
        from src.review.aggregator import AggregationResult
        assert _render_chan_digest({'chan': AggregationResult(available=False,
                                                              reason='x')},
                                   {}, {}, {}) is None
        assert _render_chan_digest({}, {}, {}, {}) is None

    def test_linkage_mapping(self):
        assert _chan_linkage_state('回暖/震荡', {}) == '弱化观察'
        assert _chan_linkage_state('冰点', {}) == '升主关注'
        assert _chan_linkage_state('退潮', {}) == '升主关注'
        assert _chan_linkage_state('高潮', {}) == '仅记录'
        assert _chan_linkage_state('', {}) == '弱化观察'   # 兜底

    def test_v4_row(self):
        agg = dict(self._chan_agg())
        agg['market_env'] = {'available': True, 'data': {}}
        agg['sentiment'] = {'available': True, 'data': {
            'metrics': {'premium_mean': 1.3, 'limit_up_count': 83}}}
        blocks = tomorrow_plan.build(agg)
        text = '\n'.join(str(b) for b in blocks)
        assert 'V4 缠论池' in text and '15 只' not in text and '2 只' in text
        assert '次日开盘相对全市场中位超额' in text

    def test_v4_absent_without_chan(self):
        agg = {'market_env': {'available': True, 'data': {}},
               'sentiment': {'available': True, 'data': {
                   'metrics': {'premium_mean': 1.3}}}}
        blocks = tomorrow_plan.build(agg)
        text = '\n'.join(str(b) for b in blocks)
        assert 'V4 缠论池' not in text


class TestDigestAnchorConfNote:
    """C-3（2026-10-25 补全）：digest 资金锚置信度行——只渲染聚合层推导结果。"""

    def test_renders_confidence_with_tags(self):
        ca = {'available': True, 'data': {
            'anchor_tags': {'south_net': '实测', 'north_total': '代理',
                            'etf': '代理', 'macro': '实测', 'option': '实测',
                            'north_detail': '停旧值'},
            'confidence': '中'}}
        note = _anchor_conf_note(ca)
        assert note is not None
        assert '综合 中' in note
        assert '北向成交额=代理' in note and '南向净买入=实测' in note and 'ETF份额=代理' in note
        assert '停旧值=快照日期早于锚定日' in note

    def test_missing_confidence_returns_none(self):
        ca = {'available': True, 'data': {'anchor_tags': {'north_total': '代理'}}}
        assert _anchor_conf_note(ca) is None

    def test_missing_tags_or_none_returns_none(self):
        assert _anchor_conf_note({'available': True, 'data': {'confidence': '中'}}) is None
        assert _anchor_conf_note({'available': False, 'data': {}}) is None
        assert _anchor_conf_note(None) is None
