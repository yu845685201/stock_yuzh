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

        # now 注入 16:00（不依赖墙钟——17:30 后跑原写法会翻转为放行）
        _n = _dt.now()
        out = ReviewAggregator(_BoomDB(), {'review': {}})._aggregate_lhb(
            _n.strftime('%Y%m%d'), now=_dt(_n.year, _n.month, _n.day, 16, 0))
        assert out.available is False
        assert '17:30' in out.reason and 'P4-6' in out.reason

    def test_lhb_codes_empty_when_gated(self):
        from src.review.aggregator import ReviewAggregator

        class _BoomDB:
            def _query(self, sql, params=None):
                raise AssertionError('门控生效时不得触库')

        agg = ReviewAggregator(_BoomDB(), {'review': {}})
        from datetime import datetime as _dt
        _n = _dt.now()
        assert agg._lhb_codes_of_day(_n.strftime('%Y%m%d'),
                                     now=_dt(_n.year, _n.month, _n.day, 16, 0)) == set()


# ------------------------------------------------------------------ P4-8

class TestP48RPoolPrior:
    """P4-8（评二-7）：R 系列候选先验随行——离线回测优先、前向核验补充，皆无如实注明。

    测试经 monkeypatch 隔离 data/priors JSON（数据文件不入库，测试不得依赖）。
    """

    @staticmethod
    def _agg(pools):
        from src.review.aggregator import AggregationResult
        return {'verification': AggregationResult(data={'rolling': {'pools': pools}})}

    @staticmethod
    def _no_offline(monkeypatch):
        from src.review.sections import tomorrow_plan
        monkeypatch.setattr(tomorrow_plan, '_offline_prior', lambda: {})

    def test_prior_with_stats(self, monkeypatch):
        self._no_offline(monkeypatch)
        from src.review.sections import tomorrow_plan
        text = tomorrow_plan._r_pool_prior(self._agg(
            {'R1': {'days': 5, 'avg_excess': 1.2, 'win_rate': 0.6}}), 'R1')
        assert '近 5 个核验日' in text and '+1.20pct' in text
        assert '历史统计非预测' in text

    def test_prior_offline_first(self, monkeypatch):
        """离线回测在场时优先呈现（R3 全历史/R1 近一年），前向核验并列补充"""
        from src.review.sections import tomorrow_plan
        monkeypatch.setattr(tomorrow_plan, '_offline_prior', lambda: {
            'R3': {'window': '20150105~20260930', 'days': 2700, 'signals': 12000,
                   'avg_excess_median': 0.85, 'win_rate': 0.52}})
        text = tomorrow_plan._r_pool_prior(self._agg(
            {'R3': {'days': 12, 'avg_excess': 1.5}}), 'R3')
        assert '离线同构回测 20150105~20260930' in text
        assert '+0.85pct' in text and '52%' in text
        assert '前向核验近 12 个核验日' in text

    def test_prior_untracked_pool(self, monkeypatch):
        self._no_offline(monkeypatch)
        from src.review.sections import tomorrow_plan
        text = tomorrow_plan._r_pool_prior(self._agg({'R2': {'days': 0}}), 'R2')
        assert '先验：未统计' in text and '前向核验' in text

    def test_prior_no_verification(self, monkeypatch):
        self._no_offline(monkeypatch)
        from src.review.sections import tomorrow_plan
        assert '未统计' in tomorrow_plan._r_pool_prior({}, 'R3')
        assert '未统计' in tomorrow_plan._r_pool_prior(
            {'verification': None}, 'R4')

    def test_buy_candidates_embed_prior(self, monkeypatch):
        """候选节每类块带先验注（有积累引用积累，无积累注明未统计）"""
        self._no_offline(monkeypatch)
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
        assert 'R1 先验：前向核验近 3 个核验日' in text2


# ------------------------------------------------------------------ P4-1

class TestP41CandidateVetoShared:
    """P4-1（B3）：VETO 单一出处 + 卖一占比接线 + 链侧入口。

    load_candidate_veto_map 为报告层与链侧 env 装配共用（两入口否决清单
    一致）；卖一占比 = SUM(卖1)/SUM(卖席)，受 lhb_published（P4-6）门控。
    """

    class _VetoDB3:
        """按表名分发预置行的 stub（捕获查询供断言）。"""

        def __init__(self, sell1_rows=None, now=None):
            self.sell1_rows = sell1_rows or []
            self.now = now
            self.queries = []

        def _query(self, sql, params=None):
            s = str(sql)
            self.queries.append(s)
            if 'base_financial_report' in s:
                return []                      # 无暴雷
            if 'announcement_daily' in s:
                return []                      # 无减持
            if 'lhb_detail' in s:
                return self.sell1_rows
            return []

    def test_sell1_veto_wired(self):
        from src.analysis.candidate_veto import load_candidate_veto_map
        db = self._VetoDB3(sell1_rows=[('sh.600519', 45.0), ('sz.000001', 12.0)])
        out = load_candidate_veto_map(db, '20261023', {})
        assert 'sh.600519' in out
        assert any('卖一占比45%超限' in r for r in out['sh.600519'])
        assert 'sz.000001' not in out          # 未超阈不出池

    def test_sell1_reason_combines_with_profit(self):
        """卖一占比与业绩暴雷并存时理由合并（veto_reasons 单一谓词源）"""
        from src.analysis.candidate_veto import load_candidate_veto_map

        class _DB(self._VetoDB3):
            def _query(self, sql, params=None):
                s = str(sql)
                if 'base_financial_report' in s:
                    return [('sh.600519', -80.0)]
                return super()._query(sql, params)

        out = load_candidate_veto_map(_DB(sell1_rows=[('sh.600519', 45.0)]),
                                      '20261023', {'veto_profit_yoy': -50.0})
        joined = '；'.join(out['sh.600519'])
        assert '业绩暴雷' in joined and '卖一占比45%超限' in joined

    def test_sell1_skipped_before_publish_time(self):
        """P4-6 门控联动：当日未到 17:30 时不发 lhb_detail 查询、不产生卖一否决"""
        from src.analysis.candidate_veto import load_candidate_veto_map
        from datetime import datetime
        db = self._VetoDB3(sell1_rows=[('sh.600519', 45.0)],
                           now=datetime(2026, 10, 23, 16, 0))
        out = load_candidate_veto_map(db, '20261023', {}, now=db.now)
        assert out == {}
        assert not any('lhb_detail' in q for q in db.queries)

    def test_report_layer_delegates(self):
        """报告层 _candidate_veto_map 委托同一装载器（SQL 形状保持 C3 约束）"""
        agg = ReviewAggregator(self._VetoDB3(), {'review': {}})
        out = agg._candidate_veto_map('20261023', {})
        assert out == {}
        assert any('disclosure_date' in q for q in agg.db.queries)
        assert any('COALESCE' in q for q in agg.db.queries)

    def test_chain_r4_veto_gate(self):
        """链侧入口：R4 模块命中 veto_map → 否（与报告层出池一致）"""
        from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
        from src.analysis.modules.r4_institution_seat import R4InstitutionSeat
        base_up = {'R1梯队接力': ModuleResult('否'),
                   'R2主线跟随': ModuleResult('否'),
                   'R3趋势新高': ModuleResult('否'),
                   'cand_institution_net_map': {'sh.600000': 2.0e8}}
        stock = StockInfo(ts_code='sh.600000', stock_code='600000',
                          stock_name='浦发银行')
        vetoed = dict(base_up, cand_veto_map={'sh.600000': ['大股东减持']})
        assert R4InstitutionSeat().analyze(None, ChainContext(
            as_of='20261023', stock=stock, upstream=vetoed)).value == '否'
        clean = R4InstitutionSeat().analyze(None, ChainContext(
            as_of='20261023', stock=stock, upstream=base_up))
        assert '机构净买' in str(clean.value)


# ------------------------------------------------------------------ P4-2

class TestP42FiveDims:
    """P4-2（B9）：五维评分补全——基本面（PEG 锚）/事件（归因极性锚）接线。"""

    def test_cand_score_all_five_dims(self):
        """五维齐备时按设计权重加权（资金 0.35 恢复名义含义，不再被归一化放大）"""
        from src.review.aggregator import _cand_score
        from src.analysis.candidate_rules import score_candidate
        dims = {'money': 50.0, 'tech': 50.0, 'sector': 50.0,
                'fundamental': 90.0, 'event': 80.0}
        res = score_candidate(dims)
        assert set(res['applied_weights']) == {
            'money', 'tech', 'sector', 'fundamental', 'event'}
        expected = (0.35 * 50 + 0.35 * 50 + 0.10 * 60 + 0.10 * 90 + 0.10 * 80)
        assert abs(_cand_score(1e8, 2.0, 3, 90.0, 80.0) - expected / 1.0) < 0.01
        # 仅三维时（旧行为回归保护）：资金有效权重被放大
        old = score_candidate({'money': 50.0, 'tech': 50.0, 'sector': 50.0})
        assert abs(old['applied_weights']['money'] / sum(
            old['applied_weights'].values()) - 0.4375) < 1e-6

    def test_fundamental_dim_peg_anchor(self):
        from types import SimpleNamespace
        from src.review.aggregator import fundamental_dim
        assert fundamental_dim(SimpleNamespace(peg_value=0.8)) == 90.0
        assert fundamental_dim(SimpleNamespace(peg_value=1.5)) == 70.0
        assert fundamental_dim(SimpleNamespace(peg_value=2.5)) == 50.0
        assert fundamental_dim(SimpleNamespace(peg_value=5.0)) == 30.0
        assert fundamental_dim(SimpleNamespace(peg_value=None)) is None
        assert fundamental_dim(SimpleNamespace(peg_value=-1.0)) is None
        assert fundamental_dim(None) is None

    def test_event_dim_polarity_anchor(self):
        from src.review.aggregator import event_dim
        assert event_dim(None) is None
        assert event_dim([]) is None
        assert event_dim([{'ann_date': '20261023', 'ann_type': '中标',
                           'title': '公司中标重大项目'}]) == 80.0
        assert event_dim([{'ann_date': '20261023', 'ann_type': '日常公告',
                           'title': '董事会会议决议'}]) == 10.0  # 无利好显式结论


# ------------------------------------------------------------------ P4-3

class TestP43SurvivorPool:
    """P4-3（B4）：股票池 as-of 感知 + 幸存者偏差量化披露。"""

    class _PoolDB:
        def __init__(self):
            self.queries = []

        def _query(self, sql, params=None):
            s = str(sql)
            self.queries.append((s, params))
            if 'survivor' in s or 'list_status' in s and 'FILTER' in s:
                return [(137, 0)]
            return [('sh.600000', '600000', '浦发银行', '19991110')]

    def test_universe_asof_aware_where(self):
        """as_of 给定时池查询含 delist_date > as_of 的退市股分支；缺省行为不变"""
        from src.analysis.data_reader import DataReader
        db = self._PoolDB()
        reader = DataReader.__new__(DataReader)
        reader._query = db._query
        reader.load_stock_universe(as_of='20261023')
        sql, params = db.queries[-1]
        assert "list_status = 'D'" in sql and 'delist_date > %s' in sql
        assert params == ('20261023',)
        reader.load_stock_universe()
        sql2, params2 = db.queries[-1]
        assert "list_status = 'L'" in sql2 and params2 is None

    def test_survivor_stats_shape(self):
        from src.analysis.data_reader import DataReader
        db = self._PoolDB()
        reader = DataReader.__new__(DataReader)
        reader._query = db._query
        stats = reader.survivor_pool_stats('20261023')
        assert stats == {'delisted_alive': 137, 'with_kline': 0}


# ------------------------------------------------------------------ 反抽板 veto

class TestHighBoardLongyinRecent:
    """P4-1 语义修订：近 N 日高位板放量长阴后的涨停入池即否决（反抽板陷阱）。"""

    def test_rebound_after_blowoff_vetoed(self):
        """6 连板 → 次日放量长阴（-4%、2.5x）→ 第三日再涨停 → veto（窗口=lookback+1=4 日）"""
        from src.analysis.candidate_rules import high_board_longyin_recent
        streaks = [5, 6, 0, 1]        # .../高位板日/长阴日/反抽板日
        changes = [10.0, 10.0, -4.0, 10.0]
        surges = [2.0, 2.0, 2.5, 2.0]
        out = high_board_longyin_recent(streaks, changes, surges)
        assert out is not None and '反抽板风险' in out

    def test_no_volume_no_veto(self):
        """长阴日未放量（1.2x）→ 不否决（条件缺一不可）"""
        from src.analysis.candidate_rules import high_board_longyin_recent
        out = high_board_longyin_recent([6, 0, 1], [10.0, -4.0, 10.0],
                                        [2.0, 1.2, 2.0])
        assert out is None

    def test_low_board_longyin_no_veto(self):
        """长阴前一日非高位板（streak 2）→ 不否决"""
        from src.analysis.candidate_rules import high_board_longyin_recent
        out = high_board_longyin_recent([2, 0, 1], [10.0, -4.0, 10.0],
                                        [2.0, 2.5, 2.0])
        assert out is None

    def test_insufficient_window_returns_none(self):
        from src.analysis.candidate_rules import high_board_longyin_recent
        assert high_board_longyin_recent([6], [10.0], [2.0]) is None
        assert high_board_longyin_recent([], [], []) is None

    def test_chain_r1_vetoed_by_longyin(self):
        """链侧入口：R1 模块命中反抽板窗口 → 否"""
        from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
        from src.analysis.modules.r1_ladder_relay import R1LadderRelay
        # 8 日序列：0..4 五板(10%)、第6日高位板(10%, streak6)、第7日长阴(-4%)、第8日反抽板
        rows = []
        for i, (cr, vol) in enumerate([(10.0, 100), (10.0, 100), (10.0, 100),
                                       (10.0, 100), (10.0, 100), (10.0, 100),
                                       (-4.0, 400), (10.0, 200)]):
            rows.append({'ts_code': 'sh.600000', 'trade_date': f'2026101{i + 6}'
                         if i + 6 < 10 else f'2026102{i + 6 - 4}',
                         'change_rate': cr, 'close': 10.0 + cr * 0.1,
                         'high': 10.0 + cr * 0.1, 'is_st': False, 'volume': vol,
                         'preclose': 10.0})
        df = pd.DataFrame(rows)
        stock = StockInfo(ts_code='sh.600000', stock_code='600000', stock_name='测试',
                          board='主板')
        streak_res = ModuleResult(6, {'streak': 6})
        ctx = ChainContext(as_of=str(df['trade_date'].iloc[-1]), stock=stock,
                           upstream={'连续涨停数': streak_res,
                                     'cand_seal_open_map': {}})
        out = R1LadderRelay().analyze(df, ctx)
        assert out.value == '否'


# ------------------------------------------------------------------ X2 (V4.0-A2)

class TestX2R1RawSeedDedup:
    """X2：弱档日 raw R1 命中播种 used——R3/R4 与链去重口径一致。

    _build_candidates 首批直测（hermetic）：合成截面 + 实例级 monkeypatch
    隔离全部 IO 触点（adx 闸门/概念 map/VETO map/估值/公告；money_flow 走
    _EmptyReader 返回 []）。R2 与 R1 恒无交集（is_r2 强制首板 streak==1），
    真实交集在 R3/R4。
    """

    _COLS = ['ts_code', 'stock_code', 'stock_name', 'is_st', '_limit_up',
             '_streak', '_new_high', '_volume_surge', 'change_rate', 'amount',
             'open', 'preclose', 'close']

    @staticmethod
    def _aggregator(monkeypatch, market_weak):
        class _EmptyReader:
            def _query(self, sql, params=None):
                return []

        agg = ReviewAggregator(_EmptyReader(), {'review': {}})
        monkeypatch.setattr(agg, '_index_adx_gate',
                            lambda as_of: {'market_weak': market_weak,
                                           'gate': 'closed' if market_weak else 'full',
                                           'adx': 15.0, 'adx_rising': False,
                                           'close': 3600.0, 'ma20': 3700.0,
                                           'ma60': 3800.0, 'index': '上证指数',
                                           'as_of': as_of})
        monkeypatch.setattr(agg, '_concept_map', lambda as_of: {})
        monkeypatch.setattr(agg, '_candidate_veto_map', lambda *a, **k: {})
        monkeypatch.setattr(agg, '_valuation_map_of_day', lambda as_of: {})
        monkeypatch.setattr(agg, '_ann_map_of_day', lambda as_of: {})
        return agg

    @staticmethod
    def _row(ts, name, streak, new_high):
        return {'ts_code': ts, 'stock_code': ts.split('.')[-1], 'stock_name': name,
                'is_st': False, '_limit_up': True, '_streak': streak,
                '_new_high': new_high, '_volume_surge': 3.0, 'change_rate': 6.0,
                'amount': 5e8, 'open': 10.0, 'preclose': 10.0,
                'close': 10.0 * (1 + streak * 0.1)}

    def _cs(self, rows):
        return pd.DataFrame(rows, columns=self._COLS)

    def test_weak_day_r3_excludes_raw_r1_hit(self, monkeypatch):
        """核心修复：弱档日 R1=0（闸门不变），R1 原命中不入 R3（与链口径一致）"""
        agg = self._aggregator(monkeypatch, market_weak=True)
        cs = self._cs([self._row('sz.301502', '华阳智能', 2, True)])
        out = agg._build_candidates(cs, '20261023', [], {}, [])
        assert out['R1'] == []
        assert all(r['ts_code'] != 'sz.301502' for r in out['R3'])

    def test_strong_day_regression_locked(self, monkeypatch):
        """回归锁定：非弱档日原优先级行为不变——该股在 R1、不在 R3"""
        agg = self._aggregator(monkeypatch, market_weak=False)
        cs = self._cs([self._row('sz.301502', '华阳智能', 2, True)])
        out = agg._build_candidates(cs, '20261023', [], {}, [])
        assert [r['ts_code'] for r in out['R1']] == ['sz.301502']
        assert all(r['ts_code'] != 'sz.301502' for r in out['R3'])

    def test_weak_day_r4_excludes_raw_r1_hit(self, monkeypatch):
        """R4 同构（链 r4 对 upstream R1 去重）：弱档日 R1 原命中不入 R4；
        纯机构股主路径不受影响"""
        agg = self._aggregator(monkeypatch, market_weak=True)
        cs = self._cs([self._row('sz.301502', '华阳智能', 2, False)])
        inst = [{'ts_code': 'sz.301502', 'name': '华阳智能', 'net': 5e8},
                {'ts_code': 'sh.600000', 'name': '浦发银行', 'net': 3e8}]
        out = agg._build_candidates(cs, '20261023', [], {}, inst)
        r4_codes = [r['ts_code'] for r in out['R4']]
        assert 'sz.301502' not in r4_codes
        assert 'sh.600000' in r4_codes
