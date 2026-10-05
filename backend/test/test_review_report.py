"""复盘报告层单测（§5.7）：渲染、章节纯函数、生成器组装与逐字节稳定性。

聚合器六维指标的正确性由 cross_section 对拍用例保证（test/analysis/
test_cross_section_parity.py），本文件验证「聚合结果 → 章节块 → 文本」链路。
"""
import json

import pytest

from src.review.generator import ReviewReportGenerator
from src.review.renderer import blocks_to_html, blocks_to_markdown, missing_note
from src.review.sections import (
    conclusion,
    fundamentals,
    market_env,
    sector_mainline,
    sentiment as sentiment_section,
    stock_movers,
)


# ------------------------------------------------------------------ 渲染

class TestRenderer:
    def _blocks(self):
        return [
            {'type': 'heading', 'level': 2, 'text': '标题&<符号>'},
            {'type': 'para', 'text': '段落 文本'},
            {'type': 'bullets', 'items': ['项一', '项二']},
            {'type': 'table', 'headers': ['列A', '列B'],
             'rows': [[1.0, 'x'], [None, float('nan')]]},
            {'type': 'note', 'text': '口径说明'},
            missing_note('某某数据', '表空'),
        ]

    def test_markdown_render(self):
        md = blocks_to_markdown(self._blocks())
        assert '## 标题&<符号>' in md
        assert '| 列A | 列B |' in md
        assert '|---|---|' in md
        assert '| 1 | x |' in md
        assert '| — | — |' in md          # None/NaN → —
        assert '> 口径说明' in md
        assert '> ⏳ 待补：某某数据（表空）' in md

    def test_html_render_escapes(self):
        html = blocks_to_html('标题', self._blocks())
        assert '<h2>标题&amp;&lt;符号&gt;</h2>' in html
        assert '<table>' in html and '<th>列A</th>' in html
        assert '<td>—</td>' in html
        assert 'nth-child(even)' in html  # 斑马纹 CSS 内联
        assert html.startswith('<!DOCTYPE html>')

    def test_unknown_block_raises(self):
        with pytest.raises(ValueError):
            blocks_to_markdown([{'type': 'unknown'}])


# ------------------------------------------------------------------ 章节

def _sentiment_agg(available=True):
    data = {
        'limit_up_count': 62, 'limit_down_count': 3, 'burst_count': 18,
        'burst_rate': 0.225, 'ladder': {3: 2, 2: 9}, 'max_streak': 3,
        'premium_mean': 2.34, 'premium_median': 1.5, 'premium_count': 58,
        'premium_continued': 11,
        'promotion': {2: {'today': 9, 'prev': 62, 'rate': 0.145},
                      3: {'today': 2, 'prev': 9, 'rate': 0.2222}},
        'cycle': {'position': '回暖/震荡', 'basis': ['未命中判据'], 'trial': True},
    }
    return {'available': available, 'reason': '' if available else '截面为空', 'data': data}


def _env_agg():
    return {'available': True, 'data': {
        'indices': [{'name': '上证指数', 'change_rate': 0.52, 'close': 3830.45,
                     'position_pct': 71.2}],
        'turnover': {'today_total': 1.8e12, 'prev_total': 1.6e12, 'delta': 2e11,
                     'delta_pct': 12.5,
                     'by_market': {'sh': {'today': 8e11, 'prev': 7e11},
                                   'sz': {'today': 1e12, 'prev': 9e11}}},
        'advance_decline': {'up': 3200, 'down': 1800, 'flat': 200, 'total': 5200},
        'overseas': [{'date': '20260929', 'name': '道琼斯', 'close': 42100.0,
                      'change_pct': -0.4}],
    }}


def _movers_agg():
    row = {'code': '920982', 'name': '锦波生物', 'change_rate': 10.0, 'streak': 3,
           'volume_surge': 2.5, 'amount': 2.9e8, 'is_st': False}
    return {'available': True, 'data': {
        'limit_up': [row] * 3, 'limit_up_count': 62,
        'limit_down': [], 'limit_down_count': 0,
        'burst': [dict(row, streak=0)], 'burst_count': 18,
        'surge_up': [row], 'new_high': [row], 'new_low': [],
        'cross_check': {'ok': True, 'csv_limit_up': 62, 'recomputed_limit_up': 62},
    }}


class TestSections:
    def test_market_env_tables(self):
        blocks = market_env.build(_env_agg(), _sentiment_agg())
        text = blocks_to_markdown(blocks)
        assert '一、大盘环境' in text
        assert '上证指数' in text and '71%' in text
        assert '较昨增加 2,000 亿' in text or '较昨增加 2000 亿' in text
        assert '涨停 62 家' in text

    def test_sentiment_ladder_and_promotion(self):
        text = blocks_to_markdown(sentiment_section.build(_sentiment_agg()))
        assert '空间板高度 3 板' in text
        assert '| 3 板 | 2 |' in text
        assert '22.5%' in text
        assert '+2.34%' in text
        assert '| 2→3 板 | 2 | 9 | 22%' in text
        assert '判据试用版' in text

    def test_movers_detail_tables(self):
        text = blocks_to_markdown(stock_movers.build(_movers_agg()))
        assert '涨停明细（62 家' in text
        assert '| 920982 | 锦波生物 | 10 | 3 | 2.5 | 29000 | 否 |' in text
        assert '今日无跌停' in text

    def test_movers_cross_check_warning(self):
        agg = _movers_agg()
        agg['data']['cross_check'] = {'ok': False, 'csv_limit_up': 60,
                                      'recomputed_limit_up': 62}
        text = blocks_to_markdown(stock_movers.build(agg))
        assert '交叉校验不一致' in text

    def test_sector_missing_note(self):
        text = blocks_to_markdown(sector_mainline.build(
            {'available': False, 'reason': '映射表为空'}))
        assert '⏳ 待补：板块数据（映射表为空）' in text

    def test_sector_mainline_candidate(self):
        agg = {'available': True, 'data': {
            'industry_today': [{'industry': '半导体', 'mean_change': 3.2, 'stocks': 80},
                               {'industry': '证券', 'mean_change': 2.1, 'stocks': 40}],
            'industry_5d': [{'industry': '半导体', 'mean_change': 8.1, 'stocks': 80},
                            {'industry': '白酒', 'mean_change': 4.0, 'stocks': 20}],
            'limit_up_clusters': [{'concept': '半导体', 'count': 12, 'share': 0.15},
                                  {'concept': '光模块', 'count': 9, 'share': 0.3}],
        }}
        text = blocks_to_markdown(sector_mainline.build(agg))
        assert '行业涨跌榜（今日，等权）' in text
        assert '半导体' in text and '高低切换观察' in text
        assert '主线候选' in text and '半导体（5 日榜前列 ∧ 涨停聚集前列）' in text

    def test_fundamentals_partial_missing(self):
        agg = {'available': True, 'data': {'disclosure': {
            'next_5': 120, 'next_10': 260, 'actual_today': 45}}}
        text = blocks_to_markdown(fundamentals.build(agg))
        assert '未来 5 个交易日预约披露：120 家' in text
        assert '⏳ 待补：当日业绩预告' in text
        assert '⏳ 待补：个股估值' in text

    def test_conclusion_includes_disclaimer(self):
        agg = {'sentiment': _sentiment_agg(),
               'sector': {'available': False, 'reason': '空', 'data': {}},
               'fundamentals': {'available': False, 'reason': '空', 'data': {}}}
        text = blocks_to_markdown(conclusion.build(agg))
        assert '情绪周期定位' in text and '回暖/震荡' in text
        assert '风险提示' in text
        assert '免责声明：本报告由免费公开数据自动生成' in text


# ------------------------------------------------------------------ 生成器

class FakeConfigManager:
    def __init__(self, config):
        self._config = config

    def load_config(self):
        return self._config


class TestGeneratorBlocks:
    def _agg(self):
        return {
            'as_of': '20260929', 'prev_date': '20260928',
            'window_start': '20220601', 'panel_rows': 310000, 'stock_count': 5560,
            'market_env': _env_agg(),
            'sentiment': _sentiment_agg(),
            'sector': {'available': False, 'reason': 'sector_concept_map 为空', 'data': {}},
            'movers': _movers_agg(),
            'fundamentals': {'available': False, 'reason': '各表为空', 'data': {}},
        }

    def _generator(self):
        gen = ReviewReportGenerator.__new__(ReviewReportGenerator)
        gen.config = {'review': {'output_dir': '/tmp/review_test_out'}}
        return gen

    def test_render_blocks_full(self):
        blocks = ReviewReportGenerator._render_blocks(self._agg(), {})
        text = blocks_to_markdown(blocks)
        assert '每日复盘报告（锚定日 20260929）' in text
        assert '三、板块主线' in text and '⏳ 待补：板块数据' in text
        assert '五、业绩估值' in text and '待补' in text
        assert '✓ 交叉校验：重算涨停家数 62 与链 CSV 一致' in text
        assert '免责声明' in text

    def test_byte_stable_for_same_agg(self):
        """同锚定日重复渲染逐字节一致（§5.4，正文无运行时间戳）"""
        agg = self._agg()
        t1 = blocks_to_markdown(ReviewReportGenerator._render_blocks(agg, {}))
        t2 = blocks_to_markdown(ReviewReportGenerator._render_blocks(dict(agg), {}))
        assert t1 == t2
        assert '20260930' not in t1  # 无生成时刻泄漏（样例锚定日 2026-09-29）


class TestDevDBlocks:
    """开发项 D：五类数据子块渲染（有数据 / 空表两态）"""

    # ---- D1 两融 ----

    @staticmethod
    def _margin_agg(available=True):
        if not available:
            return {'available': False, 'reason': 'margin_trade_daily 无数据'}
        return {'available': True, 'data': {
            'series': [{'date': '20260925', 'rzye': 2.55e12},
                       {'date': '20260928', 'rzye': 2.552e12}],
            'latest_date': '20260928', 'latest_rzye': 2.552e12,
            'delta': 2e9, 'trend': '↑', 'partial_dates': ['20260930'],
        }}

    def test_margin_block_rendered(self):
        text = blocks_to_markdown(market_env.build(_env_agg(), _sentiment_agg(),
                                                   self._margin_agg()))
        assert '两融余额' in text
        assert '25,520 亿' in text          # 2.552e12/1e8 = 25520
        assert '较上一数据日增加 20.0 亿' in text
        assert '6 日趋势 ↑' in text
        assert '20260930' in text           # 单边日标注
        assert '数据日期 T-1' in text

    def test_margin_block_missing(self):
        text = blocks_to_markdown(market_env.build(_env_agg(), _sentiment_agg(),
                                                   {'available': False, 'reason': 'margin_trade_daily 无数据'}))
        assert '⏳ 待补：两融数据（margin_trade_daily 无数据）' in text

    # ---- D2 龙虎榜 ----

    @staticmethod
    def _lhb_agg(available=True):
        if not available:
            return {'available': False, 'reason': 'lhb_stock_daily 当日无数据'}
        return {'available': True, 'data': {
            'top': [{'ts_code': 'sh.600825', 'name': '新华传媒', 'reason': '涨幅偏离累计达20%',
                     'buy': 1.29e8, 'sell': 0.37e8, 'net': 0.92e8, 'sec_tx_amount': 2.92e8}],
            'institution': [{'ts_code': 'sz.301218', 'name': '华是科技',
                             'net': 2.2e8, 'buy': 5.87e8, 'sell': 3.68e8}],
        }}

    def test_lhb_block_rendered(self):
        text = blocks_to_markdown(stock_movers.build(_movers_agg(), self._lhb_agg()))
        assert '龙虎榜净买额 Top 10' in text
        assert '新华传媒' in text and '涨幅偏离累计达20%' in text
        assert '| sh.600825 | 新华传媒 | 涨幅偏离累计达20% | 1.29 | 0.37 | 0.92 | 2.92 |' in text
        assert '机构净买(亿)' in text and '华是科技' in text
        assert '前五席位合计' in text and '禁止跨原因求和' in text

    def test_lhb_block_missing(self):
        text = blocks_to_markdown(stock_movers.build(
            _movers_agg(), {'available': False, 'reason': '当日无数据'}))
        assert '⏳ 待补：龙虎榜（当日无数据）' in text

    def test_movers_detail_marks_lhb_and_money_flow(self):
        agg = _movers_agg()
        agg['data']['limit_up'][0]['lhb'] = '是'
        agg['data']['limit_up'][0]['big_net_wan'] = 1234
        text = blocks_to_markdown(stock_movers.build(agg, self._lhb_agg()))
        assert '| 榜 | 大单净流入(万) |' in text
        assert '| 是 | 1234 |' in text

    # ---- D3 资金流 ----

    @staticmethod
    def _mf_agg(available=True):
        if not available:
            return {'available': False, 'reason': 'money_flow_daily 当日无数据'}
        return {'available': True, 'data': {
            'inflow_top': [{'ts_code': 'sz.000002', 'name': '万科A',
                            'big_net': 1.25834e9, 'total_amt': 5.158e9}],
            'outflow_top': [{'ts_code': 'sz.000001', 'name': '平安银行',
                             'big_net': -8.9e8, 'total_amt': 4.0e9}],
        }}

    def test_money_flow_block_rendered(self):
        text = blocks_to_markdown(stock_movers.build(_movers_agg(), None, self._mf_agg()))
        assert '大单净流入 / 净流出 Top 10' in text
        assert '125834' in text            # 1.25834e9/1e4
        assert '-89000' in text
        assert '异动子集近似口径' in text

    def test_money_flow_block_missing(self):
        text = blocks_to_markdown(stock_movers.build(
            _movers_agg(), None, {'available': False, 'reason': '当日无数据'}))
        assert '⏳ 待补：分笔资金流（当日无数据）' in text

    # ---- D4 解禁 ----

    @staticmethod
    def _unlock_agg(available=True):
        if not available:
            return {'available': False, 'reason': 'share_unlock_calendar 窗口内无已披露记录'}
        return {'available': True, 'data': {
            'window_end': '20261020',
            'items': [{'ts_code': 'sz.003013', 'name': '地铁设计', 'unlock_date': '20260930',
                       'shares': 9126785.0, 'ratio': 2.235166, 'type': '重组限售'},
                      {'ts_code': 'sh.603363', 'name': '傲农生物', 'unlock_date': '20260930',
                       'shares': None, 'ratio': None, 'type': '限售股上市流通'}],
        }}

    def test_unlock_block_rendered_sorted_by_date(self):
        from src.review.sections import conclusion as conclusion_section
        agg = {'sentiment': _sentiment_agg(), 'fundamentals': {'available': False, 'data': {}},
               'unlock': self._unlock_agg()}
        text = blocks_to_markdown(conclusion_section.build(agg))
        assert '未来 10 个交易日解禁日历' in text
        assert '| 20260930 | sz.003013 | 地铁设计 | 0.09 | 2.24 | 重组限售 |' in text
        assert '非完整前瞻' in text
        # 未解析出股数 -> 占比留空
        assert '| 20260930 | sh.603363 | 傲农生物 | — | — |' in text

    def test_unlock_block_missing(self):
        from src.review.sections import conclusion as conclusion_section
        agg = {'sentiment': _sentiment_agg(), 'fundamentals': {'available': False, 'data': {}},
               'unlock': {'available': False, 'reason': '窗口内无已披露记录'}}
        text = blocks_to_markdown(conclusion_section.build(agg))
        assert '⏳ 待补：解禁日历（窗口内无已披露记录）' in text

    # ---- D5 公告 ----

    @staticmethod
    def _ann_agg(available=True):
        if not available:
            return {'available': False, 'reason': '当日无非"其他"分类公告'}
        return {'available': True, 'data': {
            'counts': {'定期报告': 2, '分红送转': 1},
            'samples': {'定期报告': [{'ts_code': 'sh.600000', 'name': '浦发银行',
                                    'title': '2026年年度报告'}],
                        '分红送转': [{'ts_code': 'sz.000002', 'name': '万科Ａ',
                                    'title': '关于2026年半年度权益分派实施的公告'}]},
            'total': 3,
        }}

    def test_announcement_block_rendered(self):
        text = blocks_to_markdown(fundamentals.build({'available': True, 'data': {}}, self._ann_agg()))
        assert '当日公告摘要（分类 2 类，共 3 条）' in text
        assert '定期报告: 2 条' in text
        assert '2026年年度报告' in text
        assert '标题（截断 40 字）' in text

    def test_announcement_block_missing(self):
        text = blocks_to_markdown(fundamentals.build(
            {'available': True, 'data': {}}, {'available': False, 'reason': '当日无分类公告'}))
        assert '⏳ 待补：公告摘要（当日无分类公告）' in text


class TestThemeAttribution:
    """E 题材自算归因：强概念聚集 / 仅概念普涨 / 无候选 / 黑名单剔除"""

    @staticmethod
    def _cs():
        import pandas as pd
        rows = []
        # 涨停股 3 只 + 普通成员若干
        for code, chg, up in [('sz.000001', 10.0, True), ('sz.000004', 10.1, True),
                              ('sz.000009', 3.0, False), ('sz.000010', 3.5, False),
                              ('sz.000011', 2.5, False), ('sz.000002', 10.0, True),
                              ('sz.000003', 10.0, True)]:
            rows.append({'ts_code': code, 'change_rate': chg, '_limit_up': up})
        for i in range(9):   # C概念 的 9 个普涨成员（压低等权涨幅）
            rows.append({'ts_code': f'sz.0001{i}0', 'change_rate': 0.5, '_limit_up': False})
        return pd.DataFrame(rows)

    MAP = {
        'sz.000001': ['A概念', '融资融券'],
        'sz.000004': ['A概念'],
        'sz.000009': ['A概念'],
        'sz.000002': ['B概念'],
        'sz.000010': ['B概念'],
        'sz.000011': ['B概念'],
        'sz.000003': ['C概念'],
        **{f'sz.0001{i}0': ['C概念'] for i in range(9)},
    }

    def test_three_cases(self):
        from src.review.aggregator import ReviewAggregator
        out = ReviewAggregator._theme_candidates(self._cs(), self.MAP,
                                                 exclude=['融资融券'])
        # A概念：2 只涨停（≥2）→ 强概念聚集
        assert out['sz.000001'] == 'A概念'          # 黑名单'融资融券'被剔除
        assert out['sz.000004'] == 'A概念'
        # B概念：1 只涨停但成分等权涨幅 (10+3.5+2.5)/3=5.33 ≥2 → 仅概念普涨
        assert out['sz.000002'] == 'B概念'
        # C概念：1 只涨停且等权 (10+0.5*9)/10=1.45 <2 → 无候选
        assert 'sz.000003' not in out

    def test_theme_column_rendered_with_note(self):
        agg = _movers_agg()
        agg['data']['limit_up'][0]['theme'] = 'A概念'
        text = blocks_to_markdown(stock_movers.build(agg))
        assert '题材候选' in text
        assert '推断口径' in text


class TestSentimentCalibratedWatermark:
    """情绪判据定稿开关（config review.sentiment_calibrated）：trial 水印条件展示"""

    @staticmethod
    def _sentiment(trial):
        agg = _sentiment_agg()
        agg['data']['cycle'] = {'position': '回暖/震荡', 'basis': ['未命中'],
                                'trial': trial}
        return agg

    def test_watermark_shown_when_trial(self):
        text = blocks_to_markdown(sentiment_section.build(self._sentiment(True)))
        assert '判据试用版' in text

    def test_watermark_absent_when_calibrated(self):
        text = blocks_to_markdown(sentiment_section.build(self._sentiment(False)))
        assert '判据试用版' not in text

    def test_conclusion_heading_suffix(self):
        from src.review.sections import conclusion as conclusion_section
        agg = {'sentiment': self._sentiment(False),
               'fundamentals': {'available': False, 'data': {}}}
        text = blocks_to_markdown(conclusion_section.build(agg))
        assert '判据定稿口径见复盘框架.MD §3' in text
        assert '判据试用版' not in text
