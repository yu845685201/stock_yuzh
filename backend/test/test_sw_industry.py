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
