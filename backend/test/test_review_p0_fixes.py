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
    IND = [('sh.600000', '银行'),          # sw_2021（SQL 排序后首行）
           ('sh.600000', '房地产'),        # sina_hy 兜底行（应被忽略）
           ('sz.000001', '银行')]
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
        """行业榜净额按共用归属聚合：600000(银行,+2亿)+000001(银行,-1亿)→银行+1亿"""
        agg = ReviewAggregator(_RoutingDB(self.MF, self.IND), {'review': {}})
        data = agg._sector_money('20261023').data
        top = {r['industry']: r['net_yi'] for r in data['industry_top']}
        assert top.get('银行') == 1.0                    # (2e8-1e8)/1e8
        assert '房地产' not in top                       # 兜底行不参与
