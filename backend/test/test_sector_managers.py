"""行业/概念板块 manager 测试（快照 diff、禁用路径、清单为空保护）"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.sync import sector_sync_base
from src.sync.concept_manager import ConceptManager
from src.sync.industry_manager import IndustryManager

CONFIG = {
    'review_sync': {
        'sina_sector_interval': 0,
        'industry': {'enabled': True},
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
        self.active = active or {}
        self.concepts = []
        self.maps = []

    def ensure_review_tables(self):
        pass

    def fetch_concept_active_codes(self, source=None):
        return self.active

    def upsert_concepts(self, rows):
        self.concepts.extend(rows)

    def upsert_concept_maps(self, rows):
        self.maps.extend(rows)


class FakeSource:
    def __init__(self, nodes=None, members=None):
        self._nodes = nodes or {}
        self._members = members or {}
        self.member_calls = []
        self.request_interval = 0

    def fetch_industry_list(self):
        return self._nodes

    def fetch_concept_list(self):
        return self._nodes

    def fetch_node_members(self, node, expected_count=None):
        self.member_calls.append(node)
        return list(self._members.get(node, []))


def _build(manager_cls, fake_source, active=None, monkeypatch=None):
    db = FakeDB(active)
    if monkeypatch is not None:
        monkeypatch.setattr(sector_sync_base, 'DatabaseConnection', lambda cm: db)
    manager = manager_cls(FakeConfigManager(CONFIG))
    manager.source = fake_source
    return manager, db


NODES = {
    'new_blhy': {'name': '玻璃行业', 'count': 3},
    'new_dlhy': {'name': '电力行业', 'count': 2},
}
MEMBERS = {
    'new_blhy': [
        {'ts_code': 'sh.600176', 'code6': '600176', 'name': '中国巨石'},
        {'ts_code': 'sz.000012', 'code6': '000012', 'name': '南玻A'},
        {'ts_code': 'sh.603021', 'code6': '603021', 'name': '*ST华鹏'},
    ],
    'new_dlhy': [
        {'ts_code': 'sh.600886', 'code6': '600886', 'name': '国投电力'},
        {'ts_code': 'sz.000027', 'code6': '000027', 'name': '深圳能源'},
    ],
}


def test_industry_full_snapshot(monkeypatch):
    """首次全量：全部成分 in_date=刷新日；清单与成分都入库"""
    source = FakeSource(NODES, MEMBERS)
    manager, db = _build(IndustryManager, source, active={}, monkeypatch=monkeypatch)
    result = manager.execute()

    assert result['success'] is True
    assert result['blocked'] is False
    assert db.concepts and len(db.concepts) == 2
    assert all(c['source'] == 'sina_hy' for c in db.concepts)
    assert len(db.maps) == 5
    assert all(m['in_date'] == db.maps[0]['in_date'] and m['out_date'] is None for m in db.maps)
    stats = result['stats']
    assert stats['nodes_total'] == 2 and stats['nodes_ok'] == 2
    assert stats['members_active'] == 5 and stats['members_new'] == 5 and stats['members_out'] == 0


def test_concept_snapshot_diff_marks_out(monkeypatch):
    """快照 diff：留存行不动（in_date 传 None），新进 in_date=今日，退出 out_date=今日"""
    today = __import__('datetime').datetime.now().strftime('%Y%m%d')
    source = FakeSource({'gn_a': {'name': '华为汽车', 'count': 3}},
                        {'gn_a': [
                            {'ts_code': 'sh.600006', 'code6': '600006', 'name': '东风股份'},
                            {'ts_code': 'sz.002232', 'code6': '002232', 'name': '启明信息'},
                        ]})
    active = {'gn_a': ['sh.600006', 'sz.000001']}  # 000001 本次缺席 -> 退出；600006 留存
    manager, db = _build(ConceptManager, source, active=active, monkeypatch=monkeypatch)
    result = manager.execute()

    assert result['success'] is True
    by_key = {(m['concept_code'], m['ts_code']): m for m in db.maps}
    assert by_key[('gn_a', 'sh.600006')]['in_date'] is None      # 留存：不覆盖首次日期
    assert by_key[('gn_a', 'sh.600006')]['out_date'] is None
    assert by_key[('gn_a', 'sz.002232')]['in_date'] == today     # 新进
    assert by_key[('gn_a', 'sz.000001')]['out_date'] == today    # 退出留痕
    stats = result['stats']
    assert stats['members_new'] == 1 and stats['members_out'] == 1
    assert stats['members_active'] == 2


def test_dry_run_writes_nothing(monkeypatch):
    source = FakeSource(NODES, MEMBERS)
    manager, db = _build(IndustryManager, source, active={}, monkeypatch=monkeypatch)
    result = manager.execute(dry_run=True)
    assert result['success'] is True
    assert db.concepts == [] and db.maps == []


def test_empty_node_list_fails_without_writes(monkeypatch):
    """清单解析为空（结构变更）必须失败且不落库——数据真实性原则"""
    source = FakeSource({}, {})
    manager, db = _build(IndustryManager, source, active={}, monkeypatch=monkeypatch)
    result = manager.execute()
    assert result['success'] is False
    assert not result['blocked']
    assert any('清单' in e for e in result['errors'])
    assert db.concepts == [] and db.maps == []


def test_disabled_short_circuits(monkeypatch):
    config = {'review_sync': {'industry': {'enabled': False}}}
    monkeypatch.setattr(sector_sync_base, 'DatabaseConnection', lambda cm: FakeDB())
    manager = IndustryManager(FakeConfigManager(config))
    result = manager.execute()
    assert result['skipped_disabled'] is True
    assert result['success'] is False


def test_shrink_guard_blocks_truncated_node(monkeypatch):
    """截断防护：节点实抓较库内在册异常缩减(>30%)且重抓无效 -> 整轮失败不落库
    （2026-09-29 盘中实测：瞬时空响应把 247 只大节点截成一页 40 只）"""
    calls = {'n': 0}

    class ShrinkSource(FakeSource):
        def fetch_node_members(self, node, expected_count=None):
            calls['n'] += 1
            # 每次都只返回首只（截断态），覆盖首抓/重抓各轮
            return [dict(m) for m in self._members.get(node, [])[:1]]

    source = ShrinkSource(NODES, MEMBERS)
    active = {node: [m['ts_code'] for m in MEMBERS[node]] for node in MEMBERS}
    manager, db = _build(IndustryManager, source, active=active, monkeypatch=monkeypatch)
    result = manager.execute()

    assert result['success'] is False
    assert not result['blocked']
    assert any('异常缩减' in e for e in result['errors'])
    assert db.concepts == [] and db.maps == []  # 整轮不落库
