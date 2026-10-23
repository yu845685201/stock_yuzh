"""涨停归因模块单测（P1-11 §3.4）：前置模块依赖、非涨停、利好/无利好、
覆盖断代、env 未装配。装配层 stub 覆盖见 test_attribution_env.py。
"""

import pandas as pd

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, ModuleResult, StockInfo
from src.analysis.modules.limit_up_attribution import LimitUpAttribution

AS_OF = "20260930"


def _ctx(ts_code="sh.600000", limit_up=None, ann_map=None):
    upstream = {}
    if limit_up is not None:
        upstream['是否涨停'] = ModuleResult(limit_up)
    if ann_map is not None:
        upstream['cand_announcement_map'] = ann_map
    return ChainContext(as_of=AS_OF,
                        stock=StockInfo(ts_code=ts_code, stock_code="600000",
                                        stock_name="测试股", board="主板"),
                        upstream=upstream)


def _ann(ann_id, date, ann_type, title):
    return {'ann_id': ann_id, 'ann_date': date, 'ann_type': ann_type,
            'title': title}


class TestLimitUpAttribution:
    def test_limit_up_with_keyword(self):
        res = LimitUpAttribution().analyze(
            pd.DataFrame(), _ctx(limit_up='是', ann_map={
                'sh.600000': [_ann(1, '20260930', '其他', '关于中标的公告')]}))
        assert res.value == '题材催化(中标)'
        assert res.detail['kind'] == '利好'
        assert res.detail['source']['ann_id'] == 1

    def test_limit_up_with_type_only(self):
        """类型映射（标题无关键词）：业绩预告 → 业绩预增。"""
        res = LimitUpAttribution().analyze(
            pd.DataFrame(), _ctx(limit_up='是', ann_map={
                'sh.600000': [_ann(1, '20260930', '业绩预告', '2026年年度业绩预告')]}))
        assert res.value == '业绩预增'

    def test_limit_up_no_bulls_fallback(self):
        res = LimitUpAttribution().analyze(
            pd.DataFrame(), _ctx(limit_up='是', ann_map={
                'sh.600000': [_ann(1, '20260930', '解禁', '限售股上市流通')]}))
        assert res.value == '题材炒作或跟风上涨'
        assert res.detail['kind'] == '无'

    def test_limit_up_no_announcements(self):
        res = LimitUpAttribution().analyze(
            pd.DataFrame(), _ctx(limit_up='是', ann_map={}))
        assert res.value == '题材炒作或跟风上涨'

    def test_not_limit_up(self):
        res = LimitUpAttribution().analyze(
            pd.DataFrame(), _ctx(limit_up='否', ann_map={}))
        assert res.value == '否'
        assert res.detail['limit_up'] is False

    def test_missing_prerequisite(self):
        """前置模块未运行 → 数据不足（禁静默当非涨停）。"""
        res = LimitUpAttribution().analyze(pd.DataFrame(), _ctx())
        assert res.value == DATA_INSUFFICIENT
        assert '未运行' in res.detail['reason']

    def test_env_not_assembled(self):
        res = LimitUpAttribution().analyze(
            pd.DataFrame(), _ctx(limit_up='是'))
        assert res.value == DATA_INSUFFICIENT
        assert '未装配' in res.detail['reason']

    def test_coverage_cutoff_note(self):
        """锚定日早于断代日 → 输出附「公告覆盖不全」。"""
        ctx = _ctx(limit_up='是', ann_map={
            'sh.600000': [_ann(1, '20260901', '其他', '关于中标的公告')]})
        ctx.as_of = '20260901'
        res = LimitUpAttribution().analyze(pd.DataFrame(), ctx)
        assert res.value == '题材催化(中标)(公告覆盖不全)'
        assert res.detail['coverage_full'] is False

    def test_full_coverage_no_note(self):
        res = LimitUpAttribution().analyze(
            pd.DataFrame(), _ctx(limit_up='是', ann_map={
                'sh.600000': [_ann(1, '20260930', '其他', '关于中标的公告')]}))
        assert '(公告覆盖不全)' not in res.value
