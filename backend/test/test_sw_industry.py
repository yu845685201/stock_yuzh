"""申万行业分类源与行业 manager 增强口径测试（sw_2021）"""

import io
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pandas as pd

from src import sync as _sync_pkg  # noqa: F401  确保 backend 在 sys.path
from src.data_sources.sw_source import (
    SwSectorSource,
    _code_cell_to_ts_code,
    _parse_classification_xlsx,
)
from src.sync import sector_sync_base
from src.sync.industry_manager import IndustryManager

CONFIG = {
    'review_sync': {
        'sina_sector_interval': 0,
        'sw_interval': 0,
        'industry': {'enabled': True, 'sw_enabled': True},
        'concept': {'enabled': True},
    },
}


class FakeConfigManager:
    def __init__(self, config):
        self._config = config

    def load_config(self):
        return self._config

    def get(self, key, default=None):
        return default

    def get_database_config(self):
        return {}


class FakeDB:
    def __init__(self, active=None):
        # active: {source: {concept_code: [ts_code]}}（模拟真实 DAO 按 source 过滤后的扁平返回）
        self.active = active or {}
        self.concepts = []
        self.maps = []

    def ensure_review_tables(self):
        pass

    def fetch_concept_active_codes(self, source=None):
        return dict(self.active.get(source) or {})

    def upsert_concepts(self, rows):
        self.concepts.extend(rows)

    def upsert_concept_maps(self, rows):
        self.maps.extend(rows)


class FakeSinaSource:
    def __init__(self, nodes=None, members=None):
        self._nodes = nodes if nodes is not None else {
            'new_blhy': {'name': '玻璃行业', 'count': 2}}
        self._members = members if members is not None else {
            'new_blhy': [{'ts_code': 'sh.600176', 'code6': '600176', 'name': '中国巨石'},
                         {'ts_code': 'sz.000012', 'code6': '000012', 'name': '南玻A'}]}
        self.request_interval = 0

    def fetch_industry_list(self):
        return self._nodes

    def fetch_node_members(self, node, expected_count=None):
        return list(self._members.get(node, []))


class FakeSwSource:
    def __init__(self, names=None, members=None, fail_names=()):
        self._names = names or []
        self._members = members or {}
        self._fail = set(fail_names)

    def fetch_industry_names(self):
        return list(self._names)

    def fetch_industry_members(self, name):
        if name in self._fail:
            raise ValueError(f'申万分类表结构变更: {name}')
        return list(self._members.get(name, []))


# ---------- xlsx 解析 ----------

def _xlsx_bytes(rows):
    df = pd.DataFrame(rows, columns=['行业名称', '股票代码', '股票名称', '计入日期'])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return buf.getvalue()


def test_parse_classification_xlsx_and_codes():
    content = _xlsx_bytes([
        ['农林牧渔', 19, '深粮控股', '2021-07-30'],
        ['农林牧渔', 600598, '北大荒', '2021-07-30'],
        ['农林牧渔', 832000, '某北交所', '2025-01-01'],
        ['农林牧渔', None, '缺代码', ''],
    ])
    df = _parse_classification_xlsx(content)
    codes = [(_code_cell_to_ts_code(r, lambda c: c)) for r in df['股票代码']]
    assert codes[:3] == ['000019', '600598', '832000']
    assert codes[3] is None


def test_parse_xlsx_structure_change():
    bad = _xlsx_bytes([])  # 空表
    try:
        _parse_classification_xlsx(bad)
        raise AssertionError('空表应当抛结构变更')
    except ValueError:
        pass


def test_code_cell_garbage():
    assert _code_cell_to_ts_code('abc', lambda c: c) is None
    assert _code_cell_to_ts_code(float('nan'), lambda c: c) is None
    assert _code_cell_to_ts_code('sh600176', lambda c: c) is None  # 含字母且非纯 6 位数字


# ---------- 行业 manager 增强口径 ----------

def test_industry_with_sw_extra_snapshot(monkeypatch):
    db = FakeDB(active={'sw_2021': {'农林牧渔': ['sz.000019']}})
    monkeypatch.setattr(sector_sync_base, 'DatabaseConnection', lambda cm: db)
    manager = IndustryManager(FakeConfigManager(CONFIG))
    manager.source = FakeSinaSource()
    manager.sw_source = FakeSwSource(
        names=['农林牧渔', '电子'],
        members={'农林牧渔': ['sz.000019', 'sh.600598'], '电子': ['sz.000100']},
    )
    result = manager.execute()

    assert result['success'] is True
    stats = result['stats']
    assert stats['sw_names'] == 2
    assert stats['sw_members'] == 3
    assert stats['sw_new'] == 2          # 000019 已在册 -> 只 600598/000100 新进
    sw_concepts = [c for c in db.concepts if c['source'] == 'sw_2021']
    assert {c['concept_code'] for c in sw_concepts} == {'农林牧渔', '电子'}
    assert any(m['concept_code'] == '电子' and m['in_date'] is not None for m in db.maps)


def test_sw_failure_does_not_break_primary(monkeypatch):
    """增强口径失败只记 errors，主口径结果保持成功（数据已入库）"""
    db = FakeDB()
    monkeypatch.setattr(sector_sync_base, 'DatabaseConnection', lambda cm: db)
    manager = IndustryManager(FakeConfigManager(CONFIG))
    manager.source = FakeSinaSource()
    manager.sw_source = FakeSwSource(names=['农林牧渔'], fail_names=('农林牧渔',))
    result = manager.execute()

    assert result['success'] is True
    assert any('增强口径失败' in e for e in result['errors'])
    assert db.concepts  # 主口径已入库
    assert all(c['source'] == 'sina_hy' for c in db.concepts)


def test_sw_disabled_by_config(monkeypatch):
    config = {'review_sync': {'industry': {'enabled': True, 'sw_enabled': False}}}
    db = FakeDB()
    monkeypatch.setattr(sector_sync_base, 'DatabaseConnection', lambda cm: db)
    manager = IndustryManager(FakeConfigManager(config))
    manager.source = FakeSinaSource()
    assert manager.sw_source is None
    result = manager.execute()
    assert result['success'] is True
    assert 'sw_names' not in result['stats']


# --------------------------------------------------------------- P0-3 SSL 降级
import requests
import pytest

from src.data_sources.http_collector_base import HttpCollectorBase


def _no_retry(fn, attempts=1, backoff=None, retry_on=(), on_error=None):
    """测试替身：去掉指数退避重试，SSLError 直达降级分支"""
    return fn()


class TestSwSslFallback:
    """verify 优先级：certifi 默认 → SSLError 降级手工 bundle → 仍失败抛可操作错误"""

    def _make_source(self):
        src = SwSectorSource(CONFIG)
        src._bundle_path = '/tmp/nonexistent-bundle.pem'  # 隔离真实 bundle 文件
        src.request_interval = 0
        return src

    def test_ssl_error_downgrades_to_bundle(self, monkeypatch):
        src = self._make_source()
        seen = []

        def fake_attempt(self_inner, method, url, **kwargs):
            seen.append(self_inner.verify)
            if self_inner.verify is None:
                raise requests.exceptions.SSLError('unable to get local issuer certificate')
            return 'ok'

        monkeypatch.setattr(HttpCollectorBase, '_attempt', fake_attempt)
        monkeypatch.setattr('src.data_sources.http_collector_base.run_with_retry', _no_retry)
        assert src._request('GET', 'https://www.swsresearch.com/x') == 'ok'
        assert seen == [None, src._bundle_path]
        assert src.verify == src._bundle_path  # 降级后保持，后续请求直连 bundle

    def test_double_ssl_failure_actionable_error(self, monkeypatch):
        src = self._make_source()
        src.verify = src._bundle_path  # 已在 bundle 路径上失败

        def fake_attempt(self_inner, method, url, **kwargs):
            raise requests.exceptions.SSLError('bad bundle')

        monkeypatch.setattr(HttpCollectorBase, '_attempt', fake_attempt)
        monkeypatch.setattr('src.data_sources.http_collector_base.run_with_retry', _no_retry)
        with pytest.raises(RuntimeError, match='pip install -U certifi'):
            src._request('GET', 'https://www.swsresearch.com/x')

    def test_no_downgrade_without_ssl_error(self, monkeypatch):
        src = self._make_source()

        def fake_attempt(self_inner, method, url, **kwargs):
            return 'ok'

        monkeypatch.setattr(HttpCollectorBase, '_attempt', fake_attempt)
        monkeypatch.setattr('src.data_sources.http_collector_base.run_with_retry', _no_retry)
        assert src._request('GET', 'https://www.swsresearch.com/x') == 'ok'
        assert src.verify is None  # certifi 默认未被替换


# ---------- 开发项 G：申万单请求全量管道 ----------

_SWCLASS_HTML = """<html><body><table>
<tr><th>行业名称</th><th>股票代码</th><th>股票名称</th><th>起始日期</th><th>结束日期</th></tr>
<tr><td>农林牧渔</td><td>19</td><td>仙坛股份</td><td>2021/7/30 0:00:00</td><td></td></tr>
<tr><td>农林牧渔</td><td>204319</td><td>退市</td><td>2021-01-01 0:00:00</td><td>2022/5/27 0:00:00</td></tr>
</table></body></html>"""


def test_parse_swclass_html():
    """GBK HTML 表格解析：列名定位、日期归一、退市股带结束日期"""
    from src.data_sources.sw_source import _parse_swclass_html, _norm_sw_date
    rows = _parse_swclass_html(_SWCLASS_HTML.encode('gbk'))
    assert len(rows) == 2
    assert rows[0]['industry'] == '农林牧渔'
    assert rows[0]['code'] == '19'
    assert rows[0]['in_date'] == '2021/7/30 0:00:00'
    assert rows[0]['out_date'] == ''
    assert rows[1]['out_date'] == '2022/5/27 0:00:00'
    # SwClass 实测日期格式（斜杠+时间）归一为 yyyyMMdd
    assert _norm_sw_date('2021/7/30 0:00:00') == '20210730'
    assert _norm_sw_date('2021-12-10') == '20211210'
    assert _norm_sw_date('20211230') == '20211230'
    assert _norm_sw_date('') == '' and _norm_sw_date(None) == ''
    assert _norm_sw_date('garbage') == ''


def test_fetch_all_classification_rejects_small_payload():
    """解析行数 <1000 → 结构变更拒绝（防把首页 HTML 当数据入库）"""
    from src.data_sources.sw_source import SwSectorSource
    src = SwSectorSource({'review_sync': {'sw_interval': 0}})
    calls = []

    def fake_get_content(url):
        calls.append(url)
        return b'<html><table><tr><td>x</td></tr></table></html>'

    src.get_content = fake_get_content
    import pytest
    with pytest.raises(ValueError):
        src.fetch_all_classification()
    assert calls and calls[0].endswith('all_trade_classification_download/')  # 尾斜杠


def test_industry_manager_fallback_to_per_industry(monkeypatch):
    """单请求路径失败 → 回退逐行业下载（现行路径保留兜底）"""
    import src.sync.industry_manager as im_mod
    import src.sync.sector_sync_base as base_mod
    from src.sync.industry_manager import IndustryManager

    class FakeSwSource:
        def fetch_all_classification(self):
            raise RuntimeError('boom')

        def fetch_industry_names(self):
            return ['农林牧渔']

        def fetch_industry_members(self, name):
            return ['sz.000019']

    class FakeSnapDB:
        def ensure_review_tables(self):
            pass

        def fetch_concept_active_codes(self, source):
            return {}

        def upsert_concepts(self, rows):
            return len(rows)

        def upsert_concept_maps(self, rows):
            return len(rows)

        def upsert_concept_maps_explicit(self, rows):
            raise AssertionError('回退路径不应走显式日期 upsert')

    class FakeCM:
        def load_config(self):
            return {'review_sync': {'industry': {'enabled': True, 'sw_enabled': True}}}

    class FakeSinaSource:
        request_interval = 0.0

        def fetch_industry_list(self):
            return {'881101': {'name': '农林牧渔', 'count': None}}

        def fetch_node_members(self, node, expected_count=None):
            return [{'ts_code': 'sz.000019'}]

    monkeypatch.setattr(base_mod, 'DatabaseConnection', lambda cm: FakeSnapDB())
    monkeypatch.setattr(base_mod, 'SinaSectorSource', lambda cfg: FakeSinaSource())
    monkeypatch.setattr(base_mod, 'write_review_report', lambda *a, **k: None)
    m = IndustryManager(FakeCM())
    m.sw_source = FakeSwSource()
    m.source = FakeSinaSource()
    result = m.execute()
    stats = result['stats']
    assert stats.get('sw_mode') == 'per_industry_fallback'
    assert result['success'] is True
