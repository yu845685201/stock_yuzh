"""新浪板块源解析测试（行业/概念清单 var 提取、成分翻页、代码转换）"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.sina_sector_source import (
    NODE_PAGE_SIZE,
    SinaSectorSource,
)


# ---------- 清单 var 解析 ----------

def _industry_payload() -> bytes:
    payload = {
        'new_blhy': 'new_blhy,玻璃行业,19,16.52,-0.90,-5.18,895747499,16621841643,sh600293,...',
        'new_dlhy': 'new_dlhy,电力行业,62,8.27,-0.08,-1.01,2255995409,20520618108,sh600886,...',
    }
    text = ('var S_Finance_bankuai_sinaindustry = ' + json.dumps(payload, ensure_ascii=False) + ';')
    return text.encode('gbk')


def test_parse_industry_var():
    nodes = SinaSectorSource.parse_sina_var(_industry_payload(), 'S_Finance_bankuai_sinaindustry')
    assert set(nodes) == {'new_blhy', 'new_dlhy'}
    assert nodes['new_blhy']['name'] == '玻璃行业'
    assert nodes['new_blhy']['count'] == 19
    assert nodes['new_dlhy']['count'] == 62


def test_parse_var_structure_change_returns_empty():
    """结构变更（找不到 var）必须返回空 dict，由调用方判空报错——不允许静默 0 行"""
    assert SinaSectorSource.parse_sina_var(b'var something_else = {};', 'S_Finance_bankuai') == {}
    assert SinaSectorSource.parse_sina_var(b'\xb7\xc7\xb7\xa8\xd2\xb3\xc3\xe6', 'S_Finance_bankuai') == {}


def test_parse_concept_var_fields():
    payload = {'gn_hwqc': 'gn_hwqc,华为汽车,97,24.06,-0.98,-3.92,1821151356,32010633988,sz002232,...'}
    text = ('var S_Finance_bankuai_class = ' + json.dumps(payload, ensure_ascii=False) + ';')
    nodes = SinaSectorSource.parse_sina_var(text.encode('gbk'), 'S_Finance_bankuai_class')
    assert nodes['gn_hwqc']['name'] == '华为汽车'
    assert nodes['gn_hwqc']['count'] == 97


# ---------- 代码转换 ----------

def test_symbol_to_ts_code():
    assert SinaSectorSource.symbol_to_ts_code('sh600176') == 'sh.600176'
    assert SinaSectorSource.symbol_to_ts_code('SZ000001') == 'sz.000001'
    assert SinaSectorSource.symbol_to_ts_code('bj430047') == 'bj.430047'
    assert SinaSectorSource.symbol_to_ts_code('600176') is None
    assert SinaSectorSource.symbol_to_ts_code('hk00700') is None
    assert SinaSectorSource.symbol_to_ts_code('') is None


# ---------- 成分翻页 ----------

def _member_row(symbol, name='某股'):
    return {'symbol': symbol, 'code': symbol[2:], 'name': name}


def test_fetch_node_members_paging_until_empty(monkeypatch):
    source = SinaSectorSource({'review_sync': {'sina_sector_interval': 0}})
    pages = [
        [_member_row('sh600176'), _member_row('sz000012')],
        [_member_row('sh603021')],
        [],  # 末页空 -> 终止
    ]
    calls = []

    def fake_get_json(url, params=None, **kwargs):
        calls.append(params['page'])
        return pages[params['page'] - 1]

    monkeypatch.setattr(source, 'get_json', fake_get_json)
    members = source.fetch_node_members('new_blhy')

    assert calls == [1, 2, 3]
    assert [m['ts_code'] for m in members] == ['sh.600176', 'sz.000012', 'sh.603021']
    assert members[0]['code6'] == '600176'


def test_fetch_node_members_skips_unparseable(monkeypatch):
    source = SinaSectorSource({'review_sync': {'sina_sector_interval': 0}})
    monkeypatch.setattr(source, 'get_json',
                        lambda url, params=None, **kw: (
                            [_member_row('sh600176'), _member_row('hk00700'),
                             {'symbol': '', 'name': 'x'}]
                            if params['page'] == 1 else []))
    members = source.fetch_node_members('new_x')
    assert [m['ts_code'] for m in members] == ['sh.600176']


def test_node_page_size_is_verified_value():
    """单页条数用 2026-09-28 实测值 40，防止随手改成未验证参数"""
    assert NODE_PAGE_SIZE == 40
