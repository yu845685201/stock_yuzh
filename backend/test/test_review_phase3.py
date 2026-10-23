"""四期第三阶段单测：情绪周期走向 / 梯队断层 / 封板时段 / 题材集中度与事件树 /
昨日核验章节与新闻章节渲染（aggregator 模块函数 + stub reader + 章节纯函数）。"""
import pandas as pd
import pytest

from src.review.aggregator import (_fmt_hhmm, _ladder_gap,
                                   _theme_concentration, ReviewAggregator)
from src.review.sections import market_env, sector_mainline, yesterday_review


# ------------------------------------------------------------------ 梯队断层

def test_ladder_gap_detect():
    """7 板与 2 板之间缺 3-6 → 断层；连续 → 完整；空梯队 → None。"""
    gap = _ladder_gap({7: 1, 2: 6})
    assert gap == {'min': 2, 'max': 7, 'gaps': [3, 4, 5, 6]}
    ok = _ladder_gap({3: 2, 2: 5})
    assert ok['gaps'] == []
    assert _ladder_gap({}) is None
    assert _ladder_gap({1: 10}) is None    # 首板不算梯队档位


def test_fmt_hhmm():
    assert _fmt_hhmm(92500) == '09:25'
    assert _fmt_hhmm(140105) == '14:01'


# ------------------------------------------------------------------ 集中度

def test_theme_concentration():
    rows = [(f'60000{i}', True) for i in range(6)]
    cs = pd.DataFrame(rows, columns=['stock_code', '_limit_up'])
    cs['_limit_up'] = cs['_limit_up'].astype(bool)
    conc = _theme_concentration(
        [{'concept': '创新药', 'count': 3}, {'concept': '融资融券', 'count': 6}],
        cs, exclude=['融资融券'])
    assert conc['concept'] == '创新药'      # 黑名单概念不做头部
    assert conc['rate'] == 0.5
    assert _theme_concentration([], cs) is None


# ------------------------------------------------------------------ 周期走向

class _StubReader:
    def __init__(self, rows):
        self._rows = rows

    def _query(self, sql, params=None):
        return self._rows


def _agg(reader_rows):
    return ReviewAggregator(_StubReader(reader_rows), {'review': {}})


def test_cycle_trend_window_score():
    """10 日窗口归一：涨停家数递增（炸板率/高度持平）→ 走向上行。

    stub 行序模拟 SQL ``ORDER BY trade_date DESC``（降序返回）。"""
    asc = [(f'202609{(i + 1):02d}' if i < 9 else '20260930', lu, 0.3, 5)
           for i, lu in enumerate([20, 25, 30, 40, 50, 55, 60, 70, 80, 90])]
    agg = _agg(list(reversed(asc)))
    trend = agg._sentiment_cycle_trend('20260930',
                                       {'limit_up_count': 90, 'burst_rate': 0.3,
                                        'max_streak': 5})
    assert trend['complete'] is True
    assert trend['direction'] == '上行'
    assert trend['days_since_low'] == 9      # 单调走强：低点在窗口首日，今天距低点第 9 天
    assert trend['window_pct'] == 100
    assert trend['scores'][-1]['date'] == '20260930'


def test_cycle_trend_uses_today_metrics_when_no_table_row():
    """锚定日无表行 → 用当日重算值补位（表行 9 天 + 当日；stub 降序）。"""
    asc = [(f'202609{i:02d}', 50, 0.3, 4) for i in range(1, 10)]
    agg = _agg(list(reversed(asc)))
    trend = agg._sentiment_cycle_trend('20260930',
                                       {'limit_up_count': 90, 'burst_rate': 0.1,
                                        'max_streak': 6})
    assert trend['window'] == 10
    assert trend['scores'][-1]['date'] == '20260930'


def test_seal_time_dist_buckets():
    """首封五时段分桶 + 一次未炸 + 尾盘封比例。"""
    today = pd.DataFrame({
        'ts_code': [f'sh.60000{i}' for i in range(4)],
        'trade_date': ['20260930'] * 4,
        '_limit_up': [True] * 4,
    })
    seal_rows = [
        ('sh.600000', '092500', 0),   # 集合竞价封
        ('sh.600001', '104500', 0),   # 上午盘
        ('sh.600002', '143000', 2),   # 尾盘 + 炸过 2 次
        ('sh.600003', '150000', 1),   # 收盘封（尾盘档）
    ]
    agg = _agg(seal_rows)
    dist = agg._seal_time_dist('20260930', today)
    assert dist['parsed'] == 4 and dist['covered'] == 4
    buckets = {b['bucket']: b['count'] for b in dist['buckets']}
    assert buckets['09:25-09:35'] == 1
    assert buckets['10:00-11:30'] == 1
    assert buckets['14:00-15:00'] == 2
    assert dist['never_broken_rate'] == 0.5
    assert dist['late_seal_rate'] == 0.5
    assert agg._seal_time_dist('20260930', pd.DataFrame()) is None


# ------------------------------------------------------------------ 题材事件树

def test_theme_tree_rows():
    """事件树行：昨→今家数（扩散/收敛）、昨日题材股今日表现、首封范围。"""
    cs_today = pd.DataFrame({
        'ts_code': ['sh.600001', 'sh.600002', 'sh.600003'],
        'trade_date': ['20260930'] * 3,
        'change_rate': [10.0, 5.0, -2.0],
        '_limit_up': [True, False, False],
        '_burst': [False, False, False],
        '_streak': [2, 0, 0],
    })
    cs_prev = pd.DataFrame({
        'ts_code': ['sh.600001', 'sh.600003'],
        'trade_date': ['20260929'] * 2,
        'change_rate': [10.0, 10.0],
        '_limit_up': [True, True],
        '_burst': [False, False],
        '_streak': [1, 1],
    })
    clusters = [{'concept': '创新药', 'count': 1, 'share': 0.1}]
    concept_map = {'sh.600001': ['创新药'], 'sh.600003': ['创新药']}
    agg = _agg([('sh.600001', '093000', 0), ('sh.600002', '140000', 1)])
    tree = agg._theme_tree(cs_today, cs_prev, '20260930', clusters, concept_map)
    assert len(tree) == 1
    row = tree[0]
    assert row['prev_up'] == 2 and row['today_up'] == 1
    assert row['flow'] == '收敛'
    assert row['relimit'] == 1
    assert row['prev_mean'] == 4.0 and row['red_rate'] == 0.5
    assert row['seal_first'] == '09:30' and row['seal_last'] == '09:30'
    assert row['max_streak'] == 2


# ------------------------------------------------------------------ 章节渲染

def test_yesterday_review_blocks_no_record():
    blocks = yesterday_review.build(
        {'available': False, 'reason': '无 20260929 预测记录（核验体系 2026-10-07 起逐日积累）',
         'data': {}})
    text = ''.join(str(b.get('text', '')) for b in blocks)
    assert '〇、昨日核验' in blocks[0]['text']
    assert '⏳ 待补' in text and '逐日积累' in text


def test_yesterday_review_blocks_full():
    data = {
        'prev_date': '20260929',
        'scenario': {'status': '记录', 'gap_pct': 1.5,
                     'moves': [{'name': '恒生指数', 'accum': -1.96,
                                'base_close': 24613, 'last_close': 24131},
                               {'name': '纳斯达克', 'accum': 2.75,
                                'base_close': 26861, 'last_close': 27600}],
                     'triggered': 'P1', 'expectation': '低开',
                     'open_gap': -0.42, 'fulfilled': '成立'},
        'verify_points': [
            {'key': 'V1 量能', 'base': '14327 亿 ±20%', 'actual': '15500 亿（1.08，温和）',
             'verdict': '记录'},
            {'key': 'V2 梯队', 'base': '高度股 新华传媒（7 板）晋级',
             'actual': '今日 -10.00%，跌停', 'verdict': '不成立'},
        ],
        'candidates': {'market_median': 0.3, 'market_limit_rate': 0.01,
                       'pools': {'R1': {'count': 2, 'traded': 2, 'mean': -4.0,
                                        'median': -4.0, 'win_rate': 0.0,
                                        'limit_up_n': 0, 'burst_n': 0, 'down_n': 1,
                                        'excess_median': -4.3,
                                        'rows': [{'code': '600001', 'name': '甲',
                                                  'chg': 2.0, 'limit_up': True,
                                                  'burst': False, 'limit_down': False,
                                                  'label': '7板'},
                                                 {'code': '600002', 'name': '乙',
                                                  'chg': -10.0, 'limit_up': False,
                                                  'burst': False, 'limit_down': True,
                                                  'label': '5板'}]},
                                 'R2': {'count': 0}, 'R3': {'count': 0},
                                 'R4': {'count': 0}}},
        'rolling': {'days': 4,
                    'pools': {'R1': {'days': 4, 'avg_excess': 1.1, 'win_rate': 0.75,
                                     'limit_rate': 0.2, 'mkt_limit_rate': 0.01}},
                    'scenario': {'P1': {'n': 3, 'ok': 2}}, 'v2': {'n': 4, 'ok': 3}},
    }
    blocks = yesterday_review.build({'available': True, 'data': data})
    import json as _json
    joined = _json.dumps(blocks, ensure_ascii=False, default=str)
    assert any(b.get('text') == '〇、昨日核验（预期 vs 实际）' for b in blocks)
    assert 'P1' in joined and '低开预期成立' in joined.replace(' ', '')
    assert '不成立' in joined
    assert '-4.30pct' in joined
    assert '滚动战绩' in joined and '晋级 3 次' in joined
    # 表格块存在
    assert any(b.get('type') == 'table' for b in blocks)


def test_market_env_news_blocks():
    news = {'data': {'items': [
        {'time': '2026-10-07 09:31:00', 'title': '央行开展逆回购', 'importance': 1},
        {'time': '2026-10-07 10:02:00', 'title': '某板块利好', 'importance': 0},
    ]}}
    blocks = market_env._news_blocks(news)
    assert blocks[0]['text'] == '市场要闻（新浪 7x24，重要优先）'
    assert any('央行开展逆回购' in str(b) for b in blocks)
    assert market_env._news_blocks(None) == []


def test_sector_mainline_theme_tree_blocks():
    tree = [{'concept': '创新药', 'prev_up': 2, 'today_up': 4, 'flow': '扩散',
             'prev_mean': 3.2, 'red_rate': 0.8, 'relimit': 1, 'burst': 1,
             'max_streak': 3, 'seal_first': '09:31', 'seal_last': '14:05'}]
    blocks = sector_mainline._theme_tree_blocks(tree)
    assert blocks[0]['text'] == '题材事件树（聚集前列概念跨日复盘）'
    row = blocks[1]['rows'][0]
    assert '2 → 4' in row[1] and '扩散' in row[1]
    assert row[5] == '3 板'
    assert sector_mainline._theme_tree_blocks([]) == []
