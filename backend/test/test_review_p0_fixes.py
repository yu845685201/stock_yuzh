"""V2.0 优化步骤 1 四项小修单测（P0-3/P0-5/P0-4/P0-6）。

P0-3 资金锚源编排（B7）：hkex_connect/etf_scale 并入 DEFAULT_SOURCES；
P0-5 滚动统计同日双计（C8）：trade_date < as_of + current 单独补位；
P0-4 VETO 财务前视偏差（C3）：业绩查询加披露日约束；
P0-6 行业归属不确定（C2）：industry_map 稳定排序 + 缓存共用。

模式沿用 test_review_phase3.py / test_review_sync_all.py：stub reader，
不触网不落库。
"""
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pandas as pd
import pytest

from src.review.aggregator import ReviewAggregator
from src.review.verifier import POOL_KEYS, rolling_stats
from src.sync.review_sync_orchestrator import (ALL_SOURCES, DEFAULT_SOURCES,
                                               ReviewSyncOrchestrator)


# ------------------------------------------------------------------ P0-3

class TestP03Sources:
    def test_default_sources_contain_capital_anchors(self):
        """资金锚两源必须在默认集（否则 sync-review-all 永不执行，B7）"""
        assert 'hkex_connect' in DEFAULT_SOURCES
        assert 'etf_scale' in DEFAULT_SOURCES
        assert 'hkex_connect' in ALL_SOURCES and 'etf_scale' in ALL_SOURCES

    def test_explicit_source_name_accepted(self):
        """--sources hkex_connect 不再报"未知源名"（B7）"""
        requested = ReviewSyncOrchestrator._parse_sources('hkex_connect')
        assert requested == ['hkex_connect']
        # all → DEFAULT_SOURCES（语义不变）
        assert ReviewSyncOrchestrator._parse_sources('all') == list(DEFAULT_SOURCES)

    def test_unknown_source_still_rejected(self):
        with pytest.raises(ValueError):
            ReviewSyncOrchestrator._parse_sources('no_such_source')


# ------------------------------------------------------------------ P0-5

class _DateFilterReader:
    """按 SQL 中的比较符与 as_of 参数过滤 review_verification_daily 行，
    模拟真实 DB 的 WHERE trade_date 语义（单列 payload 行，其余查询返回空）。"""

    def __init__(self, rows):
        self.rows = rows          # [(trade_date, payload)]
        self.queries = []

    def _query(self, sql, params=None):
        s = str(sql)
        self.queries.append(s)
        if 'review_verification_daily' in s:
            as_of = str(params[0])
            if '< %s' in s or '<%s' in s:
                kept = [p for d, p in self.rows if str(d) < as_of]
            else:
                kept = [p for d, p in self.rows if str(d) <= as_of]
            return [(p,) for p in kept]      # SELECT payload → 单列
        return []


def _payload(excess=None):
    pool = {'traded': 2}
    if excess is not None:
        pool['excess_median'] = excess
    return {'candidates': {'pools': {POOL_KEYS[0]: pool}},
            'scenario': {}, 'verify_points': []}


class TestP05RollingNoDoubleCount:
    def test_same_day_payload_not_counted_twice(self):
        """当日核验已落库 + current 补位 → 只计一次（C8 双计修复）"""
        rows = [('20260929', _payload(excess=5.0)),   # 当日已落库
                ('20260928', _payload(excess=1.0)),
                ('20260927', _payload(excess=-1.0))]
        reader = _DateFilterReader(rows)
        current = _payload(excess=5.0)
        out = rolling_stats('20260929', current, reader)
        assert out is not None
        assert out['days'] == 3                        # 29/28/27，29 不重复（旧实现=4）
        pool = out['pools'][POOL_KEYS[0]]
        assert pool['days'] == 3                       # 3 个核验日各计一次
        assert 'review_verification_daily' in reader.queries[0]
        assert '<' in reader.queries[0]                # 严格小于

    def test_rerun_twice_identical(self):
        """同 as_of 连续两次 rolling_stats 输出相等（统一验收纪律）"""
        rows = [('20260929', _payload(excess=5.0)),
                ('20260928', _payload(excess=1.0))]
        r1 = rolling_stats('20260929', _payload(excess=5.0), _DateFilterReader(rows))
        r2 = rolling_stats('20260929', _payload(excess=5.0), _DateFilterReader(rows))
        assert r1 == r2


# ------------------------------------------------------------------ P0-4

class _VetoDB:
    """捕获 veto 查询 SQL/params，返回预置行（模拟 PG 已按披露约束过滤）。"""

    def __init__(self, profit_rows):
        self.profit_rows = profit_rows
        self.captured = []

    def _query(self, sql, params=None):
        s = str(sql)
        self.captured.append((s, params))
        if 'base_financial_report' in s:
            return self.profit_rows
        return []          # announcement_daily 无减持


class TestP04VetoNoLookahead:
    def test_query_has_disclosure_constraint(self):
        """SQL 必须含披露日约束且绑定 as_of（C3：未来公告不得否决历史候选）"""
        agg = ReviewAggregator(_VetoDB([]), {'review': {}})
        agg._candidate_veto_map('20261023', {})
        sql, params = agg.db.captured[0]
        assert 'disclosure_date' in sql
        assert 'COALESCE' in sql
        assert params == ('20261023',)

    def test_future_disclosure_row_not_returned(self):
        """披露日 > as_of 的行不进入 veto_map（stub 模拟 PG 过滤后不返回该行）；
        披露日 <= as_of 的暴雷行正常否决"""
        rows = [('sh.600000', -51.0)]                  # 已披露的暴雷行
        agg = ReviewAggregator(_VetoDB(rows), {'review': {}})
        out = agg._candidate_veto_map('20261023', {'veto_profit_yoy': -50})
        assert 'sh.600000' in out

    def test_rerun_twice_identical(self):
        rows = [('sh.600000', -51.0), ('sz.000001', -80.0)]
        a1 = ReviewAggregator(_VetoDB(rows), {'review': {}})._candidate_veto_map(
            '20261023', {'veto_profit_yoy': -50})
        a2 = ReviewAggregator(_VetoDB(rows), {'review': {}})._candidate_veto_map(
            '20261023', {'veto_profit_yoy': -50})
        assert a1 == a2


# ------------------------------------------------------------------ P0-6

class _RoutingDB:
    """按表名路由的 stub：money_flow / 行业映射 / his_kline_day 计数。"""

    def __init__(self, mf_rows, ind_rows, traded=10):
        self.mf_rows = mf_rows
        self.ind_rows = ind_rows
        self.traded = traded
        self.ind_query_count = 0

    def _query(self, sql, params=None):
        s = str(sql)
        if 'sector_concept_map' in s:
            self.ind_query_count += 1
            return self.ind_rows
        if 'his_kline_day' in s:
            return [(self.traded,)]
        if 'money_flow_daily' in s:
            return self.mf_rows
        return []


class TestP06IndustryMapDeterministic:
    IND = [('sh.600000', '银行', 'sw_2021'),         # sw_2021（SQL 排序后首行）
           ('sh.600000', '房地产', 'sina_hy'),       # sina_hy 兜底行（应被忽略）
           ('sz.000001', '银行', 'sw_2021')]
    MF = [('sh.600000', 2e8), ('sz.000001', -1e8)]

    def test_first_row_wins_sw_priority(self):
        agg = ReviewAggregator(_RoutingDB(self.MF, self.IND), {'review': {}})
        m = agg.industry_map('20261023')
        assert m['sh.600000'] == '银行'
        assert m['sz.000001'] == '银行'

    def test_cache_same_as_of(self):
        """同 as_of 复用缓存（只查一次库）；换 as_of 重新查询"""
        db = _RoutingDB(self.MF, self.IND)
        agg = ReviewAggregator(db, {'review': {}})
        agg.industry_map('20261023')
        agg.industry_map('20261023')
        assert db.ind_query_count == 1
        agg.industry_map('20261024')
        assert db.ind_query_count == 2

    def test_sector_money_rerun_identical(self):
        """_sector_money 同数据连跑两次逐值一致（同数据同结果）"""
        def run():
            agg = ReviewAggregator(_RoutingDB(self.MF, self.IND), {'review': {}})
            return agg._sector_money('20261023').data
        assert run() == run()

    def test_sector_money_uses_shared_map(self):
        """行业榜净额按共用归属聚合：600000(银行,+2亿)+000001(银行,-1亿)→银行+1亿；
        sw 成分板不打概念标（评价-3）"""
        agg = ReviewAggregator(_RoutingDB(self.MF, self.IND), {'review': {}})
        data = agg._sector_money('20261023').data
        top = {r['industry']: r['net_yi'] for r in data['industry_top']}
        assert top.get('银行') == 1.0                    # (2e8-1e8)/1e8
        assert '房地产' not in top                       # 兜底行不参与
        assert all(not r.get('is_concept') for r in data['industry_top'])

    def test_source_map_and_concept_board(self):
        """来源缓存随行（sina_hy 兜底股可识别）；纯 sina 成分板打概念标"""
        ind = self.IND + [('sz.000009', '次新股', 'sina_hy')]
        mf = self.MF + [('sz.000009', 5e8)]
        agg = ReviewAggregator(_RoutingDB(mf, ind), {'review': {}})
        src = agg.industry_source_map('20261023')
        assert src['sh.600000'] == 'sw_2021'
        data = agg._sector_money('20261023').data
        flags = {r['industry']: r.get('is_concept') for r in data['industry_top']}
        assert flags.get('次新股') is True               # 纯 sina 成分 → 概念板
        assert flags.get('银行') is False


# ---------------------------------------------------------------- P3-3 / P3-4（步骤 7）

class TestP03ErrorVisibility:
    def test_module_exception_returns_error_token(self):
        """P3-3（C6）：模块异常 → 「错误」值 + 原因在 detail（不再吞成数据不足）"""
        from src.analysis.module_base import DATA_ERROR, ChainContext, StockInfo
        from src.analysis.chain_engine import ChainEngine

        class BoomModule:
            name = '爆模块'
            name_en = 'boom'

            def hit_keys(self, value):
                return []

            def analyze(self, df, ctx):
                raise RuntimeError('注入异常')

        engine = ChainEngine.__new__(ChainEngine)
        stock = StockInfo('sh.600000', '600000', '测试股')
        ctx_expected = {'window_trading_days': None, 'window_start': None}
        results = engine._run_modules([BoomModule()], pd.DataFrame(), '20261023',
                                      stock, [], {}, **ctx_expected)
        assert results['爆模块'].value == DATA_ERROR
        assert '注入异常' in results['爆模块'].detail['reason']

    def test_error_not_counted_as_hit(self):
        """「错误」与「数据不足」同样不计命中桶（hit_keys 口径）"""
        from src.analysis.module_base import AnalysisModule, DATA_ERROR
        assert DATA_ERROR in AnalysisModule._NO_HIT_STRINGS


class TestP34R2NextDefault:
    """P3-4（C9）真修复测试：直接测生产函数 hit_concept_top（2026-10-25 补全，
    首轮为自证式测试未触生产代码，已删除）。"""

    def test_hit_concept_takes_cluster_order_first(self):
        """聚集序最前者优先（top_concepts 顺序即家数降序）"""
        from src.analysis.modules.r2_mainline_follow import hit_concept_top
        assert hit_concept_top(['专精特新', '重组概念'], ['重组概念', '专精特新']) == '专精特新'

    def test_empty_intersection_returns_none(self):
        """空交集 → None 不抛 StopIteration（C9 防御本体）"""
        from src.analysis.modules.r2_mainline_follow import hit_concept_top
        assert hit_concept_top(['专精特新'], ['不相关概念']) is None

    def test_empty_inputs_return_none(self):
        """空输入两侧均安全"""
        from src.analysis.modules.r2_mainline_follow import hit_concept_top
        assert hit_concept_top([], ['x']) is None
        assert hit_concept_top(['x'], []) is None


# ------------------------------------------------------------------ P4-4

class TestP44SentimentFallback:
    """P4-4（B6）：情绪定位异常/未知时候选兜底从"四类候选全开"改为"全关+人工复核"。

    _buy_candidates 的 openness 匹配原为 else 大筐——'试用版判据待校准'/空定位
    落入"回暖/震荡：四类候选全开"（异常时向激进方向兜底，方向性错误）。
    """

    @staticmethod
    def _agg(position):
        return {'candidates': {'available': True},
                'sentiment': {'data': {'cycle': {'position': position}}}}

    @staticmethod
    def _text(blocks):
        return ''.join(str(b.get('text', '')) for b in blocks)

    def test_anomaly_position_closes_all(self):
        from src.review.sections import tomorrow_plan
        text = self._text(tomorrow_plan._buy_candidates(
            self._agg('试用版判据待校准'), {}))
        assert '情绪定位异常' in text and '转人工复核' in text

    def test_empty_position_closes_all(self):
        from src.review.sections import tomorrow_plan
        text = self._text(tomorrow_plan._buy_candidates(self._agg(''), {}))
        assert '转人工复核' in text

    def test_normal_position_passes_through(self):
        """回暖/震荡 仍走全开路径（不落入异常分支）——经 ADX 缺省 closed 正常返回"""
        from src.review.sections import tomorrow_plan
        text = self._text(tomorrow_plan._buy_candidates(
            self._agg('回暖/震荡'), {}))
        assert '情绪定位异常' not in text
        assert '指数趋势闸门关闭' in text

    def test_bingdian_still_closed(self):
        """冰点行为保持：不出候选（回归保护）"""
        from src.review.sections import tomorrow_plan
        text = self._text(tomorrow_plan._buy_candidates(self._agg('冰点'), {}))
        assert '本轮不出买入候选' in text and '情绪定位异常' not in text


# ------------------------------------------------------------------ P4-6

class TestP46LhbTimeGate:
    """P4-6（B10）：龙虎榜 17:30 发布时点门控——当日盘后早跑不引用未发布数据。"""

    def test_same_day_before_publish_time_blocked(self):
        from src.review.aggregator import lhb_published
        from datetime import datetime
        assert lhb_published('20261023', now=datetime(2026, 10, 23, 16, 0)) is False

    def test_same_day_after_publish_time_allowed(self):
        from src.review.aggregator import lhb_published
        from datetime import datetime
        assert lhb_published('20261023', now=datetime(2026, 10, 23, 17, 31)) is True

    def test_historical_date_always_allowed(self):
        from src.review.aggregator import lhb_published
        from datetime import datetime
        assert lhb_published('20261022', now=datetime(2026, 10, 23, 10, 0)) is True

    def test_aggregate_lhb_gated_before_publish(self):
        """早跑时 _aggregate_lhb 不触库直接停显（reason 带门控说明）"""
        from src.review.aggregator import ReviewAggregator
        from datetime import datetime as _dt

        class _BoomDB:
            def _query(self, sql, params=None):
                raise AssertionError('门控生效时不得触库')

        out = ReviewAggregator(_BoomDB(), {'review': {}})._aggregate_lhb(
            _dt.now().strftime('%Y%m%d'))
        assert out.available is False
        assert '17:30' in out.reason and 'P4-6' in out.reason

    def test_lhb_codes_empty_when_gated(self):
        from src.review.aggregator import ReviewAggregator

        class _BoomDB:
            def _query(self, sql, params=None):
                raise AssertionError('门控生效时不得触库')

        agg = ReviewAggregator(_BoomDB(), {'review': {}})
        from datetime import datetime as _dt
        assert agg._lhb_codes_of_day(_dt.now().strftime('%Y%m%d')) == set()


# ------------------------------------------------------------------ P4-8

class TestP48RPoolPrior:
    """P4-8（评二-7）：R 系列候选先验随行——核验前向积累为准，无积累如实注明。"""

    @staticmethod
    def _agg(pools):
        from src.review.aggregator import AggregationResult
        return {'verification': AggregationResult(data={'rolling': {'pools': pools}})}

    def test_prior_with_stats(self):
        from src.review.sections import tomorrow_plan
        text = tomorrow_plan._r_pool_prior(self._agg(
            {'R1': {'days': 5, 'avg_excess': 1.2, 'win_rate': 0.6}}), 'R1')
        assert '近 5 个核验日' in text and '+1.20pct' in text and '60%' in text
        assert '历史统计非预测' in text

    def test_prior_untracked_pool(self):
        from src.review.sections import tomorrow_plan
        text = tomorrow_plan._r_pool_prior(self._agg({'R2': {'days': 0}}), 'R2')
        assert '先验：未统计' in text and '无离线同构回测' in text

    def test_prior_no_verification(self):
        from src.review.sections import tomorrow_plan
        assert '未统计' in tomorrow_plan._r_pool_prior({}, 'R3')
        assert '未统计' in tomorrow_plan._r_pool_prior(
            {'verification': None}, 'R4')

    def test_buy_candidates_embed_prior(self):
        """候选节每类块带先验注（有积累引用积累，无积累注明未统计）"""
        from src.review.sections import tomorrow_plan
        agg = {'candidates': {'available': True},
               'sentiment': {'data': {'cycle': {'position': '冰点'}}},
               'verification': self._agg({'R1': {'days': 3}})['verification']}
        blocks = tomorrow_plan._buy_candidates(agg, {})
        text = ''.join(str(b.get('text', '')) for b in blocks)
        # 冰点不出候选，但异常分支之外不触发先验渲染；换回暖/震荡+ADX closed 同理不出
        assert 'R1 先验' not in text
        agg2 = {'candidates': {'available': True,
                               'adx_gate': {'gate': 'full', 'adx': 25,
                                            'adx_rising': True, 'close': 4000,
                                            'ma20': 3900, 'index': '上证指数'},
                               'R1': [], 'R2': [], 'R3': [], 'R4': []},
                'sentiment': {'data': {'cycle': {'position': '回暖/震荡'}}},
                'verification': self._agg({'R1': {'days': 3}})['verification']}
        blocks2 = tomorrow_plan._buy_candidates(agg2, {})
        text2 = ''.join(str(b.get('text', '')) for b in blocks2)
        assert 'R1 先验（核验体系前向积累，近 3 个核验日）' in text2
