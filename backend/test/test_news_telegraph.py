"""市场要闻采集单测（四期方案 §4；news_telegraph_manager.parse_feed 纯函数）。"""
import pytest

from src.sync.news_telegraph_manager import parse_feed


def _payload(items):
    return {'result': {'data': {'feed': {'list': items}}}}


def test_parse_feed_basic():
    rows = parse_feed(_payload([
        {'id': 101, 'create_time': '2026-10-07 09:31:00',
         'rich_text': '【重大发布】央行开展逆回购操作',
         'tag': [{'id': '5', 'name': '市场'}], 'is_focus': 1},
        {'id': '102', 'create_time': '2026-10-07 10:02:00',
         'rich_text': '某板块盘中拉升，无标题前缀条目', 'tag': [], 'is_focus': 0},
    ]))
    assert len(rows) == 2
    first = rows[0]
    assert first['news_id'] == '101'
    assert first['title'] == '重大发布'                 # 【】前缀提取
    assert first['trade_date'] == '20261007'
    assert first['importance'] == 1                     # is_focus=1
    assert first['subject'] == '市场'
    assert first['content'].startswith('【重大发布】')
    second = rows[1]
    assert second['title'] == '某板块盘中拉升，无标题前缀条目'[:60]
    assert second['importance'] == 0


def test_parse_feed_focus_tag_and_drops_invalid():
    """焦点 tag 也算重要；无 id/无正文行丢弃；缺时间行 trade_date=None。"""
    rows = parse_feed(_payload([
        {'id': 103, 'create_time': '2026-10-07 11:00:00',
         'rich_text': '【焦点】指数异动', 'tag': [{'name': '焦点'}]},
        {'id': None, 'rich_text': '无 id 丢弃'},
        {'id': 104, 'rich_text': ''},                    # 空正文丢弃
        {'id': 105, 'create_time': '', 'rich_text': '缺时间行'},
    ]))
    assert len(rows) == 2
    assert rows[0]['importance'] == 1
    assert rows[1]['news_id'] == '105'
    assert rows[1]['trade_date'] is None


def test_parse_feed_structure_error():
    """响应结构变更 → ValueError（绝不让异常行静默丢弃）。"""
    with pytest.raises(ValueError):
        parse_feed({'errno': 'x'})
    with pytest.raises(ValueError):
        parse_feed({})
