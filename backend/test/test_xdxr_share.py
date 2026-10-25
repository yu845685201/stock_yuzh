"""XDXR 股本采集单测（baostock 下线换源 20261025）：事件→行生成纯函数

口径要点（见 xdxr_share_manager 模块注释）：
- category 1（除权除息）股本字段全 0 不产生行；2-10 才有效
- 单位万股→股（×1e4）；未变动一侧（0）延续前值
- 基线行 = 首事件前值 @ list_date（缺失回退首事件日；前值缺失回退后值）
"""

import pytest

from src.sync.xdxr_share_manager import (
    SHARE_EVENT_CATEGORIES,
    xdxr_events_to_rows,
)


def _ev(date, cat, qian=0.0, hou=0.0, panqian=0.0, panhou=0.0, **kw):
    e = {'date': date, 'category': cat, 'category_name': kw.get('name', ''),
         'qianzongguben': qian, 'houzongguben': hou,
         'panqianliutong': panqian, 'panhouliutong': panhou}
    e.update(kw)
    return e


def test_share_categories_range():
    assert SHARE_EVENT_CATEGORIES == frozenset(range(2, 11))
    assert 1 not in SHARE_EVENT_CATEGORIES


def test_category1_dropped_and_wan_to_share():
    """category 1 无股本字段不产生行；category 5 万股正确换算为股"""
    events = [
        _ev('20020725', 1, name='除权除息'),                       # 弃用
        _ev('20020726', 2, qian=25000, hou=27500, panqian=7150, panhou=7865),
    ]
    rows = xdxr_events_to_rows('sh.600519', '600519', '贵州茅台', events, list_date='2001-08-27')
    # 基线行 + 事件行
    assert [r['stat_date'] for r in rows] == ['20010827', '20020726']
    assert rows[0]['total_share'] == 25000 * 10000   # 2.5 亿股
    assert rows[0]['float_share'] == 7150 * 10000
    assert rows[1]['total_share'] == 27500 * 10000
    assert rows[1]['disclosure_date'] == '20020726'  # 事件日=生效日=披露日口径


def test_baseline_fallback_when_no_list_date():
    """list_date 缺失时基线行落在首事件日"""
    rows = xdxr_events_to_rows('sz.000001', '000001', '平安银行',
                               [_ev('19910403', 5, qian=1000, hou=1500, panqian=800, panhou=1200)])
    assert len(rows) == 2
    assert rows[0]['stat_date'] == '19910403'
    assert rows[0]['total_share'] == 1000 * 10000


def test_baseline_fallback_when_qian_missing():
    """首事件前值缺失 → 基线行回退首事件后值（防 join 落空优先于前值可得）"""
    rows = xdxr_events_to_rows('sh.600000', '600000', '浦发银行',
                               [_ev('19991110', 6, hou=24100, panhou=3200)])
    assert rows[0]['total_share'] == 24100 * 10000
    assert rows[1]['total_share'] == 24100 * 10000


def test_unchanged_side_carries_forward():
    """事件未变动的一侧（字段 0）延续前值，保证每行双侧完整"""
    events = [
        _ev('20100101', 5, qian=1000, hou=1000, panqian=600, panhou=600),
        _ev('20120101', 5, qian=1000, hou=2000, panqian=600, panhou=0),  # 仅总股本变动
    ]
    rows = xdxr_events_to_rows('sz.000002', '000002', '万科A', events, list_date='19911223')
    assert rows[-1]['total_share'] == 2000 * 10000
    assert rows[-1]['float_share'] == 600 * 10000  # 延续前值


def test_events_sorted_by_date_and_empty():
    """事件乱序输入按日期升序；空事件/全 category1 返回空"""
    events = [
        _ev('20150101', 5, hou=3000, panhou=2000),
        _ev('20100101', 5, hou=2000, panhou=1000),
    ]
    rows = xdxr_events_to_rows('sh.600030', '600030', '中信证券', events, list_date='20030706')
    dates = [r['stat_date'] for r in rows]
    assert dates == sorted(dates)
    assert xdxr_events_to_rows('sh.1', '1', 'x', []) == []
    assert xdxr_events_to_rows('sh.1', '1', 'x', [_ev('20200101', 1)]) == []


def test_list_date_later_than_first_event_clamped():
    """list_date 晚于首事件日（脏数据）时基线行收紧到首事件日，避免倒挂"""
    rows = xdxr_events_to_rows('sh.600006', '600006', 'x',
                               [_ev('19900101', 5, qian=100, hou=200, panqian=50, panhou=80)],
                               list_date='19950101')
    assert rows[0]['stat_date'] == '19900101'
    assert rows[0]['total_share'] == 100 * 10000


def test_dashed_list_date_normalized():
    rows = xdxr_events_to_rows('sh.600007', '600007', 'x',
                               [_ev('20000101', 5, qian=100, hou=150, panqian=60, panhou=90)],
                               list_date='1997-06-01')
    assert rows[0]['stat_date'] == '19970601'
