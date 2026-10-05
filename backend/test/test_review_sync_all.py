"""一键快捷接口 sync-review-all 编排器测试（include_kline 波次重排、日K门控、周频自动纳入）

照 test_sector_managers.py 的 Fake 模式：FakeConfigManager + monkeypatch
DatabaseConnection / _build_runners / write_review_report，不触网不落库。
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.cninfo_source import PERIODIC_CATEGORIES
from src.sync import review_sync_orchestrator
from src.sync.review_sync_orchestrator import (
    DEFAULT_SOURCES,
    KLINE_SOURCE,
    SOURCE_WAVES,
    ReviewSyncOrchestrator,
)

# parallel_sources=False：串行执行，波次顺序可精确断言
CONFIG = {
    'review_sync': {
        'parallel_sources': False,
        'weekly_refresh_weekday': 6,  # 0=周一……6=周日（与 config.yaml 注释口径一致）
    },
}

ALL_RUNNERS = [KLINE_SOURCE] + DEFAULT_SOURCES + ['concept', 'unlock', 'ths_l2']
# 2026-09-27 为周日；2026-09-29 为周二
SUNDAY, TUESDAY = '20260927', '20260929'


class FakeConfigManager:
    def __init__(self, config):
        self._config = config

    def load_config(self):
        return self._config

    def get(self, key, default=None):
        return default

    def get_database_config(self):
        return {}


class FakeOrchDB:
    """只服务 _check_kline_ready 的两个 COUNT 查询"""

    def __init__(self, kline_rows=6000, prev_rows=6000):
        self.kline_rows = kline_rows
        self.prev_rows = prev_rows

    def execute_query(self, sql, params=None):
        if 'MAX(trade_date)' in sql:
            return [{'n': self.prev_rows}]
        return [{'n': self.kline_rows}]


def _make_runners(order, fail=None, results=None):
    """生成记录执行顺序的 fake runner 集合；fail/results 按源名覆写返回值"""
    fail = fail or set()
    results = results or {}

    def build(name):
        def runner(dry):
            order.append((name, dry))
            r = {'success': True, 'stats': {'rows': 100}, 'errors': [], 'blocked': False}
            if name in fail:
                r['success'] = False
                r['errors'] = [f'{name} failed']
            r.update(results.get(name, {}))
            return r
        return runner

    return {name: build(name) for name in ALL_RUNNERS}


def _build_orchestrator(monkeypatch, db, runners):
    monkeypatch.setattr(review_sync_orchestrator, 'DatabaseConnection', lambda cm: db)
    monkeypatch.setattr(review_sync_orchestrator, 'write_review_report', lambda *a, **k: None)
    monkeypatch.setattr(ReviewSyncOrchestrator, '_build_runners', lambda self, as_of: runners)
    return ReviewSyncOrchestrator(FakeConfigManager(CONFIG))


# ---------- 一键模式：波次重排 ----------

def test_one_key_wave_layout(monkeypatch):
    """kline 编入波1与独立源并发位（串行模式下按序），依赖源顺延波2/波3"""
    order = []
    orch = _build_orchestrator(monkeypatch, FakeOrchDB(), _make_runners(order))
    result = orch.execute(as_of=TUESDAY, include_kline=True)

    assert [name for name, _ in order] == [
        'kline', 'index', 'overseas', 'overseas_kline', 'valuation_idx', 'announcement',
        'disclosure', 'lhb', 'margin', 'industry', 'bj_kline',
        'factor', 'money_flow', 'ths_finance',
        'valuation_calc', 'sentiment_metrics',
    ]
    assert [e['source'] for e in result['summary']] == [KLINE_SOURCE] + DEFAULT_SOURCES
    assert result['summary'][0]['source'] == KLINE_SOURCE
    assert '内置日K已同步' in result['kline_check']
    assert result['success'] is True


def test_one_key_kline_failure_gates_dependents(monkeypatch):
    """内置日K同步失败：factor/money_flow/valuation_calc 跳过不执行，其余源照常"""
    order = []
    orch = _build_orchestrator(
        monkeypatch, FakeOrchDB(), _make_runners(order, fail={KLINE_SOURCE}))
    result = orch.execute(as_of=TUESDAY, include_kline=True)

    executed = [name for name, _ in order]
    assert KLINE_SOURCE in executed
    for name in ('factor', 'money_flow', 'valuation_calc', 'sentiment_metrics'):
        assert name not in executed
        entry = next(e for e in result['summary'] if e['source'] == name)
        assert entry['status'] == '跳过'
        assert '日K同步未成功' in entry['detail']
    for name in ('index', 'announcement', 'lhb', 'ths_finance'):
        assert name in executed
    assert '日K同步未成功' in result['kline_check']


def test_one_key_kline_rowcheck_gate(monkeypatch):
    """日K同步"成功"但行数校验不过（如节假日/部分同步）：依赖源仍门控跳过"""
    order = []
    db = FakeOrchDB(kline_rows=100, prev_rows=6000)
    orch = _build_orchestrator(monkeypatch, db, _make_runners(order))
    result = orch.execute(as_of=TUESDAY, include_kline=True)

    assert 'kline' in [name for name, _ in order]
    for name in ('factor', 'money_flow', 'valuation_calc', 'sentiment_metrics'):
        assert name not in [name for name, _ in order]
        entry = next(e for e in result['summary'] if e['source'] == name)
        assert entry['status'] == '跳过'
        assert '日K行数校验未过' in entry['detail']


# ---------- 缺省模式回归：sync-review-data 行为不变 ----------

def test_default_mode_unchanged(monkeypatch):
    """include_kline 缺省 False：不编入 kline，波次与原 SOURCE_WAVES 一致"""
    order = []
    orch = _build_orchestrator(monkeypatch, FakeOrchDB(), _make_runners(order))
    result = orch.execute(as_of=TUESDAY)

    assert 'kline' not in [name for name, _ in order]
    assert [name for name, _ in order] == [
        name for wave in SOURCE_WAVES for name in wave if name in DEFAULT_SOURCES]
    assert result['summary'][0]['source'] == 'index'
    assert result['kline_check']  # 前置校验照旧执行并记录


def test_default_mode_kline_missing_aborts(monkeypatch):
    """缺省模式日K未同步：整体中止并提示先跑 sync-kline-day（原行为保留）"""
    order = []
    orch = _build_orchestrator(
        monkeypatch, FakeOrchDB(kline_rows=0, prev_rows=6000), _make_runners(order))
    result = orch.execute(as_of=TUESDAY)

    assert order == []
    assert result['summary'] == []
    assert any('sync-kline-day' in e for e in result['errors'])


# ---------- 周频自动纳入 ----------

def test_weekly_auto_include_on_sunday_only(monkeypatch):
    """缺省源集 + 周频日（周日）自动并入 concept/unlock；周二或显式 --sources 不并入"""
    order = []
    orch = _build_orchestrator(monkeypatch, FakeOrchDB(), _make_runners(order))
    result = orch.execute(as_of=SUNDAY, include_kline=True)

    executed = [name for name, _ in order]
    assert 'concept' in executed and 'unlock' in executed
    # N3（2026-10-04）：WEEKLY_SOURCES 追加 ths_l2，周频汇总尾部 concept -> unlock -> ths_l2
    assert [e['source'] for e in result['summary'][-3:]] == ['concept', 'unlock', 'ths_l2']

    order.clear()
    orch.execute(as_of=TUESDAY, include_kline=True)
    assert 'concept' not in [name for name, _ in order]

    order.clear()
    orch.execute(sources='index', as_of=SUNDAY, include_kline=True)
    assert [name for name, _ in order] == ['kline', 'index']


# ---------- 公告历史回补（开发项 K：显式区间不推游标） ----------

def _raw_ann(ann_id, title='2021年年度报告', sec_code='600000'):
    """巨潮原始公告样例（announcementTime epoch 毫秒 ≈ 2021-04-12）"""
    return {'announcementId': ann_id, 'announcementTitle': title,
            'announcementTime': 1618200000000, 'secCode': sec_code,
            'secName': '浦发银行', 'adjunctUrl': 'finalpage/2021-04-12/x.pdf'}


class FakeCninfoSource:
    def __init__(self, pages=None, fail_on=()):
        self.calls = []
        self.pages = pages or {}
        self.fail_on = set(fail_on)

    def query_market_window(self, start_date, end_date, searchkey='', category=''):
        self.calls.append((start_date, end_date, category))
        if (start_date, end_date) in self.fail_on:
            from src.data_sources.cninfo_source import CninfoBlockedError
            raise CninfoBlockedError('HTTP 403，疑似触发风控')
        return self.pages.get((start_date, end_date), [])


class FakeAnnDB:
    def __init__(self):
        self.upserted = []
        self.cursor_calls = []
        self.backfill_calls = 0

    def upsert_announcements(self, rows):
        self.upserted.extend(rows)

    def upsert_performance_forecasts(self, rows):
        pass

    def set_cursor(self, *args):
        self.cursor_calls.append(args)

    def backfill_financial_disclosure_dates(self):
        self.backfill_calls += 1
        return 42


def _make_announcement_manager(monkeypatch, source, db):
    """__new__ 绕过 __init__（不连库不触网），属性手工装配（项目测试惯例）"""
    from src.sync.announcement_manager import AnnouncementManager, DEFAULT_TYPE_KEYWORDS
    monkeypatch.setattr('src.sync.announcement_manager.write_review_report',
                        lambda *a, **k: None)
    m = AnnouncementManager.__new__(AnnouncementManager)
    m.source = source
    m.db = db
    m.type_keywords = [(str(k), list(v)) for k, v in dict(DEFAULT_TYPE_KEYWORDS).items()]
    return m


def test_announcement_backfill_weekly_windows_no_cursor(monkeypatch):
    """K 回补：按周切窗、category 透传、不推游标、结束触发 COALESCE 回填"""
    source = FakeCninfoSource(pages={
        ('2021-01-01', '2021-01-07'): [_raw_ann('a1')],
        ('2021-01-08', '2021-01-14'): [_raw_ann('a2'), _raw_ann('a3', sec_code='000002')],
        # 末窗口不足 7 天：2021-01-15 ~ 2021-01-20
        ('2021-01-15', '2021-01-20'): [],
    })
    db = FakeAnnDB()
    m = _make_announcement_manager(monkeypatch, source, db)

    result = m.backfill_range('20210101', '20210120', category='periodic')

    assert result['success'] is True and result['blocked'] is False
    assert source.calls == [
        ('2021-01-01', '2021-01-07', ';'.join(PERIODIC_CATEGORIES)),
        ('2021-01-08', '2021-01-14', source.calls[0][2]),
        ('2021-01-15', '2021-01-20', source.calls[0][2]),
    ]
    assert len(db.upserted) == 3
    assert db.upserted[0]['ts_code'] == 'sh.600000'
    assert db.upserted[0]['ann_type'] == '定期报告'
    assert db.cursor_calls == []            # 回补模式不推游标
    assert db.backfill_calls == 1           # 结束显式 COALESCE
    assert result['stats']['disclosure_backfill_rows'] == 42
    assert result['stats']['windows_done'] == 3
    assert result['stats']['last_completed_window'] == '2021-01-20'


def test_announcement_backfill_blocked_stops_with_resume_point(monkeypatch):
    """风控急停：中止后续窗口、已完成窗口保留、报告给出续跑点"""
    source = FakeCninfoSource(
        pages={('2021-01-01', '2021-01-07'): [_raw_ann('a1')]},
        fail_on={('2021-01-08', '2021-01-14')})
    db = FakeAnnDB()
    m = _make_announcement_manager(monkeypatch, source, db)

    result = m.backfill_range('20210101', '20210120', category='periodic')

    assert result['blocked'] is True
    assert result['success'] is False
    assert len(source.calls) == 2           # 急停后不再发后续窗口
    assert db.backfill_calls == 0           # 未完整跑完不触发 COALESCE
    assert result['stats']['windows_done'] == 1
    assert result['stats']['last_completed_window'] == '2021-01-07'
    assert any('2021-01-08' in e for e in result['errors'])


def test_announcement_backfill_dry_run_no_db_writes(monkeypatch):
    """dry-run：不写库、不触发 COALESCE，统计照常累计"""
    source = FakeCninfoSource(pages={('2021-01-01', '2021-01-07'): [_raw_ann('a1')]})
    db = FakeAnnDB()
    m = _make_announcement_manager(monkeypatch, source, db)

    result = m.backfill_range('20210101', '20210107', category='periodic', dry_run=True)

    assert db.upserted == []
    assert db.backfill_calls == 0
    assert result['stats']['announcements_new'] == 1
    assert result['dry_run'] is True


def test_announcement_backfill_invalid_range(monkeypatch):
    """区间非法（起 > 止 / 解析失败）直接拒绝，不发任何请求"""
    source = FakeCninfoSource()
    db = FakeAnnDB()
    m = _make_announcement_manager(monkeypatch, source, db)
    with pytest.raises(ValueError):
        m.backfill_range('20210120', '20210101')
    assert source.calls == []


# ---------- dry-run ----------

def test_dry_run_kline_skipped_and_no_gate(monkeypatch):
    """dry-run：kline 标跳过、门控不生效，全部源按计划以 dry 模式展开"""
    order = []
    db = FakeOrchDB(kline_rows=0, prev_rows=6000)  # 行数校验必不过，验证门控确实未生效
    results = {KLINE_SOURCE: {'success': False, 'skipped_dry_run': True, 'errors': []}}
    orch = _build_orchestrator(monkeypatch, db, _make_runners(order, results=results))
    result = orch.execute(as_of=TUESDAY, include_kline=True, dry_run=True)

    assert all(dry for _, dry in order)
    kline_entry = next(e for e in result['summary'] if e['source'] == KLINE_SOURCE)
    assert kline_entry['status'] == '跳过'
    assert 'dry-run' in kline_entry['detail']
    for name in ('factor', 'money_flow', 'valuation_calc', 'sentiment_metrics'):
        assert name in [name for name, _ in order]
        entry = next(e for e in result['summary'] if e['source'] == name)
        assert entry['status'] == '成功'


# ---------- 日K runner 适配 ----------

def test_kline_runner_adapter(monkeypatch):
    """_run_kline_sync：SyncManager 顶层返回重塑为编排器契约；dry-run 占位跳过"""
    captured = {}

    class FakeSyncManager:
        def __init__(self, cm):
            pass

        def sync_kline_day(self, **kwargs):
            captured.update(kwargs)
            return {'success': True, 'db_rows': 5200, 'records': 5200, 'errors': [],
                    'failed_stocks': 0, 'report_path': '/tmp/kline_report.md'}

    monkeypatch.setattr(review_sync_orchestrator, 'write_review_report', lambda *a, **k: None)
    monkeypatch.setattr(review_sync_orchestrator, 'DatabaseConnection', lambda cm: None)
    monkeypatch.setattr('src.sync.sync_manager.SyncManager', FakeSyncManager)
    orch = ReviewSyncOrchestrator(FakeConfigManager(CONFIG))

    result = orch._run_kline_sync(False, '20260929')
    assert result['success'] is True
    assert result['stats']['rows'] == 5200
    assert result['report_path'] == '/tmp/kline_report.md'
    assert captured['init_mode'] is False
    assert captured['save_to_db'] is True
    assert captured['start_date'] == '20260929'
    assert captured['end_date'] == '20260929'

    result = orch._run_kline_sync(False)
    assert captured['start_date'] is None  # 缺省走"当日"路径

    class FailingSyncManager(FakeSyncManager):
        def sync_kline_day(self, **kwargs):
            return {'success': True, 'db_rows': 5200, 'records': 5200, 'errors': [],
                    'failed_stocks': 3, 'report_path': None}

    monkeypatch.setattr('src.sync.sync_manager.SyncManager', FailingSyncManager)
    result = orch._run_kline_sync(False)
    assert result['success'] is True
    assert any('3 只个股同步失败' in e for e in result['errors'])

    result = orch._run_kline_sync(True)
    assert result['skipped_dry_run'] is True


# ---------- 公告重分类（开发项 H） ----------

class FakeReclassDB:
    """execute_query/execute_update 桩：updates 回写行数据以验证幂等"""

    def __init__(self, rows):
        self.rows = [dict(r) for r in rows]
        self.updates = []

    def execute_query(self, sql, params=None):
        return self.rows

    def execute_update(self, sql, params=None):
        self.updates.append(params)
        ann_id, new_type = params[1], params[0]
        for r in self.rows:
            if str(r['announcement_id']) == str(ann_id):
                r['ann_type'] = new_type
        return 1


def test_announcement_reclassify_changes_and_idempotent(monkeypatch):
    """H 重分类：旧类型按新词表更新；重跑零变更（幂等）"""
    from src.sync.announcement_manager import AnnouncementManager
    monkeypatch.setattr('src.sync.announcement_manager.write_review_report',
                        lambda *a, **k: None)
    db = FakeReclassDB([
        {'announcement_id': 'a1', 'ann_type': '其他', 'title': '关于回购部分股份的通知'},
        {'announcement_id': 'a2', 'ann_type': '重大事项', 'title': '2021年年度报告'},
        {'announcement_id': 'a3', 'ann_type': '其他', 'title': '董事会决议公告'},
    ])
    m = _make_announcement_manager(monkeypatch, FakeCninfoSource(), db)
    # 默认词表（6 类）无 回购/公司治理——显式用增补后的词表（模拟 config 新词表）
    m.type_keywords = [('定期报告', ['年度报告']), ('回购', ['回购']),
                       ('公司治理', ['董事会'])]

    r1 = m.reclassify(days_back=7)
    assert r1['success'] is True
    assert r1['stats']['changed'] == 3            # a1 回购、a2 定期报告、a3 公司治理
    assert r1['stats']['transitions'].get('其他→回购') == 1
    assert r1['stats']['transitions'].get('重大事项→定期报告') == 1
    assert r1['stats']['other_ratio'] == 0

    r2 = m.reclassify(days_back=7)                # 重跑：分类已一致
    assert r2['stats']['changed'] == 0


def test_announcement_reclassify_dry_run_no_write(monkeypatch):
    from src.sync.announcement_manager import AnnouncementManager
    monkeypatch.setattr('src.sync.announcement_manager.write_review_report',
                        lambda *a, **k: None)
    db = FakeReclassDB([{'announcement_id': 'a1', 'ann_type': '其他',
                         'title': '关于回购部分股份的通知'}])
    m = _make_announcement_manager(monkeypatch, FakeCninfoSource(), db)
    m.type_keywords = [('回购', ['回购'])]
    r = m.reclassify(days_back=7, dry_run=True)
    assert r['stats']['changed'] == 1
    assert db.updates == []                       # dry-run 不写库
