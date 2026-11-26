"""第三方数据采集限流优化第一批测试（方案 20261120）

覆盖：
- §1 拒绝分类结构化信号（429/403/400/blocked_page + Retry-After + headers 合并）
- §2 请求级准入域门（冷却拦截/共享节拍推进/端点级暂停/即时冷却登记）
- §3 明确被拒即停（ETF 循环、LHB 席位缺口、两融沪深拆分）
- §4.3 子项缺口（missing_scope 合并、sub_key、seats-only 补采）
- 新浪板块节点验收门控与复核预算

全部离线：HTTP 层用 FakeSession，DB 层用假实现，不向真实上游发请求。
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.domain_gate import (
    DomainGate,
    build_endpoint_tag,
    domain_for_host,
    endpoint_domain,
    parse_retry_after,
)
from src.data_sources.errors import ReviewBlockedError
from src.database.dao_collection_retry import _merge_missing_scope


# ============================================================ §1 HTTP 基类分类

class FakeResponse:
    def __init__(self, status_code=200, content=b'', json_data=None, headers=None):
        self.status_code = status_code
        self.content = content if isinstance(content, bytes) else content.encode('utf-8')
        self.encoding = 'utf-8'
        self.headers = headers or {}
        self._json_data = json_data

    def json(self):
        if self._json_data is None:
            raise ValueError('no json')
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f'HTTP {self.status_code}')

    @property
    def text(self):
        return self.content.decode(self.encoding, errors='replace')


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({'method': method, 'url': url, **kwargs})
        if len(self.responses) == 1:
            return self.responses[0]
        return self.responses.pop(0)


def _collector(monkeypatch, responses, interval=0.0):
    from src.data_sources.http_collector_base import HttpCollectorBase
    collector = HttpCollectorBase(
        {'review_sync': {'retry_attempts': 3, 'blocked_cooldown_seconds': 77,
                         'default_timeout': [1, 2]}},
        'test', interval_seconds=interval)
    session = FakeSession(responses)
    monkeypatch.setattr('src.data_sources.http_collector_base._session_for',
                        lambda url: session)
    return collector, session


def test_429_parses_retry_after(monkeypatch):
    collector, session = _collector(
        monkeypatch, [FakeResponse(status_code=429, headers={'Retry-After': '120'})])
    with pytest.raises(ReviewBlockedError) as ei:
        collector.get_json('http://example.com/api')
    assert len(session.calls) == 1  # 明确拒绝不重试
    assert ei.value.kind == 'rate_limited'
    assert ei.value.retry_after == 120
    assert ei.value.cooldown_seconds == 120  # 服务端等待时间 > 本地 77s 时取服务端


def test_403_400_classified(monkeypatch):
    collector, _ = _collector(monkeypatch, [FakeResponse(status_code=403, content='x')])
    with pytest.raises(ReviewBlockedError) as ei:
        collector.get_json('http://example.com/api')
    assert ei.value.kind == 'forbidden'
    collector, _ = _collector(monkeypatch, [FakeResponse(status_code=400, content='x')])
    with pytest.raises(ReviewBlockedError) as ei:
        collector.get_json('http://example.com/api')
    assert ei.value.kind == 'bad_request'
    assert ei.value.cooldown_seconds == 77


def test_5xx_with_retry_after_not_retried(monkeypatch):
    collector, session = _collector(
        monkeypatch, [FakeResponse(status_code=503, headers={'Retry-After': '30'})])
    with pytest.raises(ReviewBlockedError) as ei:
        collector.get_json('http://example.com/api')
    assert len(session.calls) == 1  # 服务端明确要求等待：不再重试
    assert ei.value.kind == 'rate_limited'


def test_headers_merged_not_replaced(monkeypatch):
    collector, session = _collector(monkeypatch, [FakeResponse(status_code=200, json_data={})])
    collector.get_json('http://example.com/api', headers={'Referer': 'http://r.example/'})
    sent = session.calls[0]['headers']
    # 单次 headers 与默认 headers 合并，显式覆盖项优先（修复只传 Referer 丢 UA）
    assert sent['Referer'] == 'http://r.example/'
    assert 'User-Agent' in sent and 'Accept' in sent


# ============================================================ §2 域门

def test_build_endpoint_tag_and_hosts():
    tag = build_endpoint_tag('https://www.szse.cn/api/report/ShowReport/data',
                             params={'CATALOGID': '1842_detal', 'PAGENO': 3})
    assert tag.startswith('ShowReport/data#')
    assert '1842_detal' in tag
    assert domain_for_host('WWW.SZSE.CN') == 'szse'
    assert domain_for_host('example.com') is None
    assert parse_retry_after('15') == 15
    assert parse_retry_after(None) is None


class _GateCursor:
    """按 SQL 关键字分派的假游标（只覆盖 domain_gate 实际使用的语句）"""

    def __init__(self, state):
        self.state = state
        self._result = None

    def execute(self, sql, params=None):
        s = ' '.join(sql.split())
        if s.startswith('INSERT INTO sync_domain_cooldown (domain) VALUES'):
            self.state.setdefault(params[0], {'blocked_until': None, 'next_send_at': None})
        elif s.startswith('SELECT blocked_until, next_send_at, slot_owner, slot_until'):
            row = self.state.get(params[0])
            self._result = ((row.get('blocked_until'), row.get('next_send_at'),
                             None, None) if row else (None, None, None, None))
        elif s.startswith('SELECT blocked_until FROM sync_domain_cooldown'):
            row = self.state.get(params[0])
            self._result = ((row.get('blocked_until'),) if row else (None,))
        elif s.startswith('UPDATE sync_domain_cooldown SET next_send_at'):
            row = self.state.setdefault(params[3],
                                        {'blocked_until': None, 'next_send_at': None})
            row['next_send_at'] = params[0]
        elif s.startswith('UPDATE sync_domain_cooldown SET slot_owner = NULL'):
            pass  # 时隙释放：状态无关紧要
        elif s.startswith('INSERT INTO sync_domain_cooldown (domain, blocked_until'):
            row = self.state.setdefault(params[0],
                                        {'blocked_until': None, 'next_send_at': None})
            if row.get('blocked_until') is None or params[1] > row['blocked_until']:
                row['blocked_until'] = params[1]
        else:
            raise AssertionError(f'未预期的 SQL: {s[:80]}')

    def fetchone(self):
        return self._result

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeGateDB:
    """域门假数据库（含 get_connection 事务语义与 execute_query）"""

    def __init__(self):
        self.state = {}
        self.commits = 0

    def get_connection(self):
        db = self

        class _Conn:
            def cursor(self, *a, **k):
                return _GateCursor(db.state)

            def commit(self):
                db.commits += 1

            def rollback(self):
                pass

        class _Ctx:
            def __enter__(self):
                return _Conn()

            def __exit__(self, *a):
                return False

        return _Ctx()

    def execute_query(self, sql, params=None):
        best = None
        for d in (params or []):
            row = self.state.get(d)
            if row and row.get('blocked_until') and (best is None
                                                     or row['blocked_until'] > best):
                best = row['blocked_until']
        return [{'mx': best}] if best else [{'mx': None}]


def _gate(state=None):
    db = FakeGateDB()
    db.state = state or {}
    gate = DomainGate(enabled=True, db_factory=lambda: db)
    return gate, db


def test_gate_acquire_advances_beat_and_denies_cooldown():
    now = datetime.now(timezone.utc)
    gate, db = _gate({'szse': {'blocked_until': now + timedelta(seconds=600),
                               'next_send_at': None}})
    url = 'https://www.szse.cn/api/report/ShowReport/data?x=1'
    with pytest.raises(ReviewBlockedError) as ei:
        gate.acquire(url, min_interval=1.0, endpoint='ShowReport/data')
    assert ei.value.kind == 'cooldown_active'  # 请求未发出
    # 健康域正常放行并推进共享节拍
    gate2, db2 = _gate()
    gate2.acquire(url, min_interval=1.0)
    assert db2.state['szse']['next_send_at'] is not None


def test_gate_honors_legacy_exchange_cooldown():
    now = datetime.now(timezone.utc)
    gate, _ = _gate({'exchange': {'blocked_until': now + timedelta(seconds=300),
                                  'next_send_at': None}})
    with pytest.raises(ReviewBlockedError):
        gate.acquire('https://www.szse.cn/api/report/ShowReport/data')


def test_gate_report_429_registers_host_cooldown():
    gate, db = _gate()
    url = 'https://www.szse.cn/api/report/ShowReport/data'
    gate.acquire(url, min_interval=0.0)
    gate.report(url, endpoint='ShowReport/data', kind='rate_limited',
                retry_after=60, ok=False)
    until = db.state['szse']['blocked_until']
    assert until > datetime.now(timezone.utc) + timedelta(seconds=1700)


def test_gate_report_403_endpoint_pause_then_host_upgrade():
    gate, db = _gate()
    base = 'https://www.szse.cn/api/report/ShowReport'
    ep1 = 'ShowReport/data#1837_xxpl'
    ep2 = 'ShowReport/data#1842_detal'
    gate.acquire(f'{base}/data', endpoint=ep1)
    gate.report(f'{base}/data', endpoint=ep1, kind='forbidden', ok=False)
    ekey = endpoint_domain('szse', ep1)
    assert db.state[ekey]['blocked_until'] > datetime.now(timezone.utc)
    assert 'szse' not in db.state or db.state['szse'].get('blocked_until') is None
    # 同轮第二个不同端点被拒 → 升级主机级
    gate.acquire(f'{base}/data', endpoint=ep2)
    gate.report(f'{base}/data', endpoint=ep2, kind='forbidden', ok=False)
    assert db.state['szse']['blocked_until'] > datetime.now(timezone.utc)


def test_gate_report_400_endpoint_only_no_host():
    gate, db = _gate()
    url = 'https://www.szse.cn/api/report/ShowReport/data'
    gate.acquire(url, endpoint='ShowReport/data')
    gate.report(url, endpoint='ShowReport/data', kind='bad_request', ok=False)
    assert 'szse' not in db.state or db.state['szse'].get('blocked_until') is None
    ekey = endpoint_domain('szse', 'ShowReport/data')
    assert db.state[ekey]['blocked_until'] > datetime.now(timezone.utc)


def test_gate_remaining_for_url():
    now = datetime.now(timezone.utc)
    gate, _ = _gate({'cninfo_query': {'blocked_until': now + timedelta(seconds=90),
                                      'next_send_at': None}})
    assert 0 < gate.remaining_for_url('http://www.cninfo.com.cn/new/hisAnnouncement/query') <= 90
    assert gate.remaining_for_url('http://example.com/other') == 0


# ============================================================ §4.3 子项缺口合并

def test_merge_missing_scope_list_union_and_scalar_overwrite():
    merged = _merge_missing_scope(
        {'detail': '旧', 'rows': 1, 'seats': [['a', 'r1']]},
        {'detail': '新', 'seats': [['a', 'r1'], ['b', 'r2']]})
    assert merged['detail'] == '新'          # 标量新值覆盖
    assert merged['rows'] == 1              # 保留旧有键
    assert merged['seats'] == [['a', 'r1'], ['b', 'r2']]  # 列表并集去重


def test_merge_missing_scope_truncates_huge_lists():
    old = [[str(i)] for i in range(5)]
    new = [[str(i)] for i in range(2100)]
    merged = _merge_missing_scope({'seats': old}, {'seats': new})
    assert len(merged['seats']) <= 2001  # 截断 + 1 条溢出标记
    assert any('_truncated' in str(x) for x in merged['seats'])


class _CaptureCursor:
    def __init__(self, existing):
        self.existing = existing
        self.insert_params = None

    def execute(self, sql, params=None):
        s = ' '.join(sql.split())
        if s.startswith('SELECT missing_scope FROM sync_collection_retry'):
            self.select_key = params[0]
        else:
            self.insert_params = params

    def fetchone(self):
        return (self.existing,) if self.existing is not None else None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _CaptureConn:
    def __init__(self, existing=None):
        self.cursor_obj = _CaptureCursor(existing)

    def cursor(self):
        return self.cursor_obj


def test_register_retry_tx_sub_key():
    from src.database.dao_collection_retry import CollectionRetryDaoMixin
    mixin = CollectionRetryDaoMixin()
    conn = _CaptureConn()
    mixin.register_retry_tx(conn, dataset='lhb', target_date='20261120',
                            sub_key='seats', action='SCOPE_RETRY',
                            missing_scope={'seats': [['000001', '涨幅偏离']]})
    assert conn.cursor_obj.insert_params['retry_key'] == 'lhb|CN|20261120|seats'
    # 二次登记同键：列表并集
    conn2 = _CaptureConn(existing={'seats': [['000001', '涨幅偏离']]})
    mixin.register_retry_tx(conn2, dataset='lhb', target_date='20261120',
                            sub_key='seats', missing_scope={'seats': [['000002', '换手']]})
    import json
    merged = json.loads(conn2.cursor_obj.insert_params['missing_scope'])
    assert merged['seats'] == [['000001', '涨幅偏离'], ['000002', '换手']]


# ============================================================ §3 被拒即停（ETF / LHB / margin）

def _etf_manager(monkeypatch, sse_fail_at=2):
    """构造 ETF 管理器：第 sse_fail_at 次沪市请求抛 ReviewBlockedError"""
    from src.sync import etf_scale_manager as m
    from src.sync.etf_scale_manager import EtfScaleManager
    calls = {'sse': 0}

    class FakeDB:
        def upsert_etf_scale(self, rows):
            return len(rows)

        def execute_query(self, sql, params=None):
            return [{'mx': None}]

    mgr = EtfScaleManager.__new__(EtfScaleManager)
    mgr.db = FakeDB()
    calls['mgr'] = mgr

    def fake_fetch_sse(ymd):
        calls['sse'] += 1
        if calls['sse'] >= sse_fail_at:
            raise ReviewBlockedError('[sse_etf] HTTP 403')
        return [{'fund_code': '510300'}]

    mgr.fetch_sse = fake_fetch_sse
    return mgr, calls


def test_etf_stops_on_blocked(monkeypatch):
    mgr, calls = _etf_manager(monkeypatch)
    r = mgr.execute(start_date='20261110', end_date='20261120', write_report=False)
    assert r['blocked'] is True
    assert r['success'] is False
    assert calls['sse'] == 2  # 明确被拒后不再继续逐日请求（旧行为会打满 11 天）
    assert any('风控急停' in e for e in r['errors'])


def _lhb_manager(source, db):
    from src.sync.lhb_manager import LhbManager
    m = LhbManager.__new__(LhbManager)
    m.source = source
    m.db = db
    return m


_SZSE_ROWS = [
    {'zqdm': '000001', 'dqrq': '2026-11-20', 'plyy': '日涨幅偏离值达7%的证券',
     'bz': 'ZBDM=0902', 'zqjc': '平安银行', 'cjje': '10.0'},
    {'zqdm': '000002', 'dqrq': '2026-11-20', 'plyy': '日换手率达20%的证券',
     'bz': 'ZBDM=0900', 'zqjc': '万科A', 'cjje': '20.0'},
    {'zqdm': '000003', 'dqrq': '2026-11-20', 'plyy': '日内振幅值达15%的证券',
     'bz': 'ZBDM=0901', 'zqjc': 'X股', 'cjje': '30.0'},
]


class SeatBlockedSource:
    """首条席位请求即被拒：验证停止后续席位请求并记满缺口"""

    def __init__(self):
        self.seat_calls = []

    def fetch_szse_lhb(self, trade_date):
        return _SZSE_ROWS

    def fetch_szse_lhb_seats(self, ann_date, code, zb_dm):
        self.seat_calls.append(code)
        raise ReviewBlockedError('[sse_szse] HTTP 403')

    def fetch_sse_lhb_summary(self, trade_date):
        return []


class GapDB:
    def __init__(self):
        self.retries = []
        self.details = []
        self.completed = []

    def upsert_lhb_stocks(self, rows):
        return len(rows)

    def upsert_lhb_details(self, rows):
        self.details.extend(rows)
        return len(rows)

    def register_retry(self, **kw):
        self.retries.append(kw)

    def complete_scope_retry(self, dataset, target_date, sub_key):
        self.completed.append((dataset, target_date, sub_key))
        return True

    def scope_retry_missing_scope(self, dataset, target_date, sub_key):
        return self.pending_scope

    def fetch_lhb_row_count(self, d):
        return self.row_count

    def fetch_trade_calendar(self, start, end):
        return []

    row_count = 0
    pending_scope = None


def test_lhb_seats_stop_and_register_gap():
    src = SeatBlockedSource()
    db = GapDB()
    m = _lhb_manager(src, db)
    r = m.execute(trade_date='20261120', write_report=False)
    # 席位端点被拒：停止后续席位请求，缺口即时登记（"榜单已取，席位待补"）——
    # 榜单行完整，不整日急停（余量由子项补采按缺口找回）
    assert r['blocked'] is False
    assert r['success'] is True
    assert len(src.seat_calls) == 1  # 明确被拒后停止后续席位请求
    assert len(r['stats']['seat_gaps']) == 3  # 余下行全部记缺口
    assert db.retries and db.retries[0]['sub_key'] == 'seats'
    assert db.retries[0]['action'] == 'SCOPE_RETRY'
    assert len(db.retries[0]['missing_scope']['seats']) == 3


def _seat_source_ok():
    class SeatOkSource:
        def __init__(self):
            self.calls = []

        def fetch_szse_lhb_seats(self, ann_date, code, zb_dm):
            self.calls.append(code)
            return [{'zsmc': '机构专用', 'mmlb': '买1', 'mrje': '100.0'}]

    return SeatOkSource()


def test_lhb_backfill_seats_recovers_gaps():
    db = GapDB()
    src = _seat_source_ok()
    m = _lhb_manager(src, db)
    gaps = [{'ann_date': '20261120', 'code': '000001', 'zb_dm': '0902',
             'ts_code': 'sz.000001', 'reason': '日涨幅偏离'}]
    r = m.backfill_seats('20261120', gaps, suppress_report=True)
    assert r['success'] is True
    assert r['stats']['recovered'] == 1
    assert len(db.details) == 1
    assert ('lhb', '20261120', 'seats') in db.completed  # 全部找回即结清待办


def test_lhb_backfill_seats_blocked_stops():
    db = GapDB()
    src = SeatBlockedSource()
    m = _lhb_manager(src, db)
    gaps = [{'ann_date': '20261120', 'code': '000001', 'zb_dm': '0902',
             'ts_code': 'sz.000001', 'reason': 'x'},
            {'ann_date': '20261120', 'code': '000002', 'zb_dm': '0900',
             'ts_code': 'sz.000002', 'reason': 'y'}]
    r = m.backfill_seats('20261120', gaps, suppress_report=True)
    assert r['blocked'] is True
    assert len(src.calls if hasattr(src, 'calls') else src.seat_calls) == 1
    assert r['stats']['recovered'] == 0
    assert ('lhb', '20261120', 'seats') not in db.completed


def test_margin_per_exchange_partial_commit():
    """单所被拒：已入库另一所保留（2026-11-19 深市 2119 行整批为零的教训）"""
    from src.sync.margin_manager import MarginManager
    from src.data_sources.http_collector_base import ReviewBlockedError as RBE
    upserted = []

    class DB:
        def upsert_margin_trades(self, rows):
            upserted.append(list(rows))
            return len(rows)

    m = MarginManager.__new__(MarginManager)
    m.db = DB()
    m.lookback_days = 5

    def szse_ok(day):
        return [{'ts_code': 'sz.000001', 'rzye': 1.0}]

    def sse_blocked(day):
        raise RBE('[sse_szse] HTTP 400')

    m._szse_rows = szse_ok
    m._sse_rows = sse_blocked
    r = m.execute(trade_date='20261120', write_report=False)
    assert r['blocked'] is True
    assert r['stats']['szse_rows'] == 1
    assert upserted and upserted[0][0]['ts_code'] == 'sz.000001'  # 深市行已入库
    assert any('深市已入库' in e for e in r['errors'])
    assert any('沪市风控急停待补' in e for e in r['errors'])


def test_margin_markets_param_limits_scope():
    from src.sync.margin_manager import MarginManager
    called = {'szse': 0, 'sse': 0}

    class DB:
        def upsert_margin_trades(self, rows):
            return len(rows)

    m = MarginManager.__new__(MarginManager)
    m.db = DB()
    m.lookback_days = 5

    def szse(day):
        called['szse'] += 1
        return [{'ts_code': 'sz.000001'}]

    def sse(day):
        called['sse'] += 1
        return [{'ts_code': 'sh.600000'}]

    m._szse_rows = szse
    m._sse_rows = sse
    m.execute(trade_date='20261120', write_report=False, markets={'szse'})
    assert called['szse'] == 1 and called['sse'] == 0  # 只补缺失侧


# ============================================================ 新浪板块节点验收门控

_SECTOR_CONFIG = {
    'review_sync': {
        'sina_recheck_budget': 3,
        'industry': {'enabled': True},
    },
}


class _SectorCfg:
    def __init__(self, config):
        self._config = config

    def load_config(self):
        return self._config


class _SectorDB:
    def __init__(self, active):
        self.active = active
        self.maps = []
        self.concepts = []
        self.retries = []

    def ensure_review_tables(self):
        pass

    def fetch_concept_active_codes(self, source=None):
        return self.active

    def upsert_concepts(self, rows):
        self.concepts.extend(rows)

    def upsert_concept_maps(self, rows):
        self.maps.extend(rows)

    def register_retry(self, **kw):
        self.retries.append(kw)

    def complete_scope_retry(self, dataset, target_date, sub_key):
        return True


class TruncSource:
    """node_a 稳定返回截短成员集（16 < 在册 20，20% 缩减 ≤30% 但差 4 > 容差 3），
    node_b 正常"""

    def __init__(self):
        self.member_calls = []
        self.request_interval = 0

    @staticmethod
    def _members(n, prefix):
        return [{'ts_code': f'sh.{prefix}{i:03d}'} for i in range(n)]

    def fetch_industry_list(self):
        return {'node_a': {'name': 'A', 'count': None},
                'node_b': {'name': 'B', 'count': None}}

    def fetch_node_members(self, node, expected_count=None):
        self.member_calls.append(node)
        if node == 'node_a':
            return self._members(16, '6001')
        return self._members(4, '0001')


def test_sector_moderate_shrink_gates_exits(monkeypatch):
    """中度缩减复核仍缩减 → 该节点本轮跳过快照，不产生任何成员退出"""
    from src.sync import sector_sync_base as ssb
    from src.sync.industry_manager import IndustryManager
    src = TruncSource()
    active = {
        'node_a': [f'sh.6001{i:03d}' for i in range(20)],
        'node_b': [f'sh.0001{i:03d}' for i in range(4)],
    }
    db = _SectorDB(active)
    monkeypatch.setattr(ssb, 'DatabaseConnection', lambda cm: db)
    monkeypatch.setattr(ssb, 'SinaSectorSource', lambda cfg: src)
    mgr = IndustryManager(_SectorCfg(_SECTOR_CONFIG))
    mgr.source = src
    r = mgr.execute(dry_run=False)
    assert r['success'] is True
    assert r['stats']['nodes_unverified'] == 1
    assert 'node_a' in r['stats']['unverified_nodes']
    exits = [m for m in db.maps if m.get('out_date')]
    assert not any(m['concept_code'] == 'node_a' for m in exits)
    assert not any(m['concept_code'] == 'node_a' for m in db.maps)
    # node_b 正常提交
    assert any(m['concept_code'] == 'node_b' for m in db.maps)


def test_sector_recheck_budget_exhausted(monkeypatch):
    """复核预算耗尽后不再外发复核请求，缩减节点按未验收处理"""
    from src.sync import sector_sync_base as ssb
    from src.sync.industry_manager import IndustryManager
    src = TruncSource()
    active = {'node_a': [f'sh.6001{i:03d}' for i in range(20)],
              'node_b': [f'sh.0001{i:03d}' for i in range(4)]}
    db = _SectorDB(active)
    monkeypatch.setattr(ssb, 'DatabaseConnection', lambda cm: db)
    monkeypatch.setattr(ssb, 'SinaSectorSource', lambda cfg: src)
    mgr = IndustryManager(_SectorCfg(_SECTOR_CONFIG))
    mgr.source = src
    mgr.recheck_budget = 0
    r = mgr.execute(dry_run=False)
    assert r['stats']['rechecks_used'] == 0
    assert r['stats']['nodes_unverified'] == 1


def test_sector_sw_gap_registered(monkeypatch):
    from src.sync import sector_sync_base as ssb
    from src.sync.industry_manager import IndustryManager
    cfg = {
        'review_sync': {
            'sina_recheck_budget': 3,
            'industry': {'enabled': True, 'sw_enabled': True},
        },
    }
    src = TruncSource()
    active = {'node_a': [f'sh.6001{i:03d}' for i in range(20)],
              'node_b': [f'sh.0001{i:03d}' for i in range(4)]}
    db = _SectorDB(active)
    monkeypatch.setattr(ssb, 'DatabaseConnection', lambda cm: db)
    monkeypatch.setattr(ssb, 'SinaSectorSource', lambda cfg_: src)

    class SwFailSource:
        def fetch_all_classification(self):
            raise ReviewBlockedError('[sw] HTTP 403')

    mgr = IndustryManager(_SectorCfg(cfg))
    mgr.source = src
    mgr.sw_source = SwFailSource()
    r = mgr.execute(dry_run=False)
    assert r['success'] is True  # 新浪主口径成功
    assert any('增强口径风控急停' in e for e in r['errors'])
    assert db.retries and db.retries[0]['sub_key'] == 'sw_2021'
    assert db.retries[0]['action'] == 'SCOPE_RETRY'
    assert db.retries[0]['missing_scope'] == {'scope': 'sw_2021'}
