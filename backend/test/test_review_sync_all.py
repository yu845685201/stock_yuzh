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


# ---------- 一键模式：依赖驱动调度（2026-11-01，替代波次 barrier） ----------

def test_one_key_wave_layout(monkeypatch):
    """串行退化（1 worker）执行序 = 拓扑序：THS 只等公告/披露（不再串行在日K后），
    factor/money_flow/估值自算等 kline 完成后才跑"""
    order = []
    orch = _build_orchestrator(monkeypatch, FakeOrchDB(), _make_runners(order))
    result = orch.execute(as_of=TUESDAY, include_kline=True)

    assert [name for name, _ in order] == [
        'index', 'overseas', 'overseas_kline', 'valuation_idx', 'announcement',
        'disclosure', 'lhb', 'margin', 'ths_finance', 'industry', 'bj_kline',
        'news', 'hkex_connect', 'etf_scale',
        # F15（方案 V1.0 §F15）：macro/option 入盘后编排
        'macro', 'option',
        'kline', 'factor', 'money_flow', 'valuation_calc', 'sentiment_metrics',
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
    """include_kline 缺省 False：不编入 kline；执行序 = DEFAULT_SOURCES 序
    （无 kline 在请求集，factor/money_flow 的依赖视为外部已满足，前置校验兜底）"""
    order = []
    orch = _build_orchestrator(monkeypatch, FakeOrchDB(), _make_runners(order))
    result = orch.execute(as_of=TUESDAY)

    assert 'kline' not in [name for name, _ in order]
    assert [name for name, _ in order] == list(DEFAULT_SOURCES)
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


# ---------- 依赖驱动并发调度（2026-11-01：THS 不被日K阻塞） ----------

def test_ths_finance_not_blocked_by_kline(monkeypatch):
    """并发调度的核心收益：kline 慢（全市场逐股）不再阻塞 ths_finance——
    公告/披露完成后 THS 立即启动，两者并行；其余快源流水消化不受影响"""
    import threading
    par_config = {'review_sync': {'parallel_sources': True, 'parallel_workers': 4,
                                  'weekly_refresh_weekday': 6}}
    started, finished = [], []
    kline_block = threading.Event()

    def make(name, block=None):
        def runner(dry):
            started.append(name)
            if block is not None:
                block.wait(5)  # 防死锁上限；主线程 finally 必 set
            finished.append(name)
            return {'success': True, 'stats': {'rows': 1}, 'errors': [], 'blocked': False}
        return runner

    runners = {name: (make(KLINE_SOURCE, block=kline_block) if name == KLINE_SOURCE
                      else make(name))
               for name in ALL_RUNNERS}

    monkeypatch.setattr(review_sync_orchestrator, 'DatabaseConnection',
                        lambda cm: FakeOrchDB())
    monkeypatch.setattr(review_sync_orchestrator, 'write_review_report',
                        lambda *a, **k: None)
    monkeypatch.setattr(ReviewSyncOrchestrator, '_build_runners',
                        lambda self, as_of: runners)
    orch = ReviewSyncOrchestrator(FakeConfigManager(par_config))
    try:
        result = orch.execute(as_of=TUESDAY, include_kline=True)
    finally:
        kline_block.set()

    assert 'ths_finance' in started
    # THS 启动早于 kline 完成——THS 只等公告/披露，不等日K（并行性）
    assert started.index('ths_finance') < finished.index(KLINE_SOURCE)
    assert result['success'] is True


# ---------- 交易所系源串行化（2026-11-17：disclosure→lhb→margin 防同域并发风控） ----------

def test_exchange_dependency_chain_declared():
    """SOURCE_DEPENDENCIES 含 disclosure→lhb→margin 链，且全图无环"""
    deps = review_sync_orchestrator.SOURCE_DEPENDENCIES
    assert deps.get('lhb') == {'disclosure'}
    assert deps.get('margin') == {'lhb'}

    def reaches(src, dst, seen=None):
        """src 沿依赖边出发能否到达 dst（含 src==dst）——用于环检测"""
        seen = seen or set()
        if src == dst:
            return True
        if src in seen:
            return False
        seen.add(src)
        return any(reaches(d, dst, seen) for d in deps.get(src, set()))

    for name, ds in deps.items():
        for d in ds:
            assert not reaches(d, name), f'{name} -> {d} 形成依赖环'


def test_exchange_sources_serialized_by_dependency(monkeypatch):
    """并行槽位空闲时 lhb/margin 仍必须等前序交易所源完成（同域防并发）；
    依赖"完成即放行（无论成败）"：disclosure 失败也放行 lhb → margin"""
    import threading
    import time
    par_config = {'review_sync': {'parallel_sources': True, 'parallel_workers': 4,
                                  'weekly_refresh_weekday': 6}}
    started, finished = [], []
    disclosure_block = threading.Event()
    observed = {}

    def make(name, block=None, fail=False):
        def runner(dry):
            started.append(name)
            if block is not None:
                block.wait(5)  # 防死锁上限；观察线程必 set
            finished.append(name)
            r = {'success': True, 'stats': {'rows': 1}, 'errors': [], 'blocked': False}
            if fail:
                r['success'] = False
                r['errors'] = [f'{name} simulated failure']
            return r
        return runner

    runners = {
        'disclosure': make('disclosure', block=disclosure_block, fail=True),
        'lhb': make('lhb'),
        'margin': make('margin'),
    }

    monkeypatch.setattr(review_sync_orchestrator, 'DatabaseConnection',
                        lambda cm: FakeOrchDB())
    monkeypatch.setattr(review_sync_orchestrator, 'write_review_report',
                        lambda *a, **k: None)
    monkeypatch.setattr(ReviewSyncOrchestrator, '_build_runners',
                        lambda self, as_of: runners)
    orch = ReviewSyncOrchestrator(FakeConfigManager(par_config))

    def watcher():
        # 等 disclosure 进入运行中，再留一个抢跑窗口供 lhb/margin"若依赖失效则已启动"
        for _ in range(50):
            if 'disclosure' in started:
                break
            time.sleep(0.02)
        time.sleep(0.15)
        observed['lhb_held'] = 'lhb' not in started
        observed['margin_held'] = 'margin' not in started
        disclosure_block.set()  # 放行 disclosure（其 runner 返回失败）

    threading.Thread(target=watcher, daemon=True).start()
    result = orch.execute(sources='disclosure,lhb,margin', as_of=TUESDAY,
                          include_kline=False)

    assert observed.get('lhb_held') is True, 'lhb 在 disclosure 未完成时抢跑'
    assert observed.get('margin_held') is True, 'margin 在前序源未完成时抢跑'
    assert started == ['disclosure', 'lhb', 'margin']
    assert finished.index('disclosure') < started.index('lhb')
    assert finished.index('lhb') < started.index('margin')


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
    assert [name for name, _ in order] == ['index', 'kline']


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


# ---------- THS 财报：增量并发 + 连续失败快速中止（2026-11-01） ----------

def test_ths_finance_fast_fail_aborts_increment(monkeypatch):
    """增量模式连续 15 只失败：快速中止——未跑完全部目标（20 只只跑 15），
    整源标失败并注明重跑续采；成功会清零连续计数"""
    from datetime import datetime
    from src.sync.ths_finance_manager import ThsFinanceManager
    monkeypatch.setattr('src.sync.ths_finance_manager.write_review_report',
                        lambda *a, **k: None)
    m = ThsFinanceManager.__new__(ThsFinanceManager)
    m.enabled = True
    m.concurrent_workers = 2
    m._sync_one = lambda code, share: 0  # 空结果（THS 瞬时拦截语义 = 失败）
    calls = []

    def counting_sync_one(code, share):
        calls.append(code)
        return 0
    m._sync_one = counting_sync_one

    stats = {'mode': '增量', 'targets': 20, 'rows': 0, 'stocks_ok': 0,
             'stocks_empty': 0, 'failed': []}
    result = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
    m._run_concurrent(list('ABCDEFGHIJKLMNOPQRST'), {}, stats, result, datetime.now())

    assert len(calls) == 15                       # 第 16 只起被中止（20 只目标未跑完）
    assert len(stats['failed']) == 15
    assert stats['stocks_empty'] == 15
    assert any('快速中止' in e for e in result['errors'])


def test_ths_finance_fast_fail_resets_on_success(monkeypatch):
    """成功穿插清零连续计数：2 失败 + 1 成功 + 2 失败 + 1 成功……不触发中止"""
    from datetime import datetime
    from src.sync.ths_finance_manager import ThsFinanceManager
    monkeypatch.setattr('src.sync.ths_finance_manager.write_review_report',
                        lambda *a, **k: None)
    m = ThsFinanceManager.__new__(ThsFinanceManager)
    m.enabled = True
    m.concurrent_workers = 2
    calls = []

    def alternating_sync_one(code, share):
        calls.append(code)
        # 每组 [失败,失败,成功] 循环：连续失败最多 2，永不达 15
        return 0 if len(calls) % 3 != 0 else 5
    m._sync_one = alternating_sync_one

    stats = {'mode': '增量', 'targets': 30, 'rows': 0, 'stocks_ok': 0,
             'stocks_empty': 0, 'failed': []}
    result = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
    m._run_concurrent([f'{i:06d}' for i in range(30)], {}, stats, result, datetime.now())

    assert len(calls) == 30                       # 无中止，跑完全部
    assert not result['errors']
    assert stats['stocks_ok'] == 10 and stats['stocks_empty'] == 20


# ---------- CLI 退出码映射（技术方案 §6.1：0 完整 / 1 失败 / 2 参数错误 / 3 部分） ----------

class TestOrchestratorExitCode:
    from src.cli.review_sync_cli import orchestrator_exit_code as _fn  # noqa: 模块级引用走函数内 import

    def test_full_execution_is_zero(self):
        from src.cli.review_sync_cli import orchestrator_exit_code
        assert orchestrator_exit_code({'execution_status': '0'}) == 0

    def test_failed_is_one(self):
        from src.cli.review_sync_cli import orchestrator_exit_code
        assert orchestrator_exit_code({'execution_status': '1'}) == 1

    def test_partial_is_three(self):
        from src.cli.review_sync_cli import orchestrator_exit_code
        assert orchestrator_exit_code({'execution_status': '3'}) == 3

    def test_early_abort_falls_back_to_success_flag(self):
        # 早退路径（前置校验/中间件预检失败）无记账 → 按 success 布尔回退
        from src.cli.review_sync_cli import orchestrator_exit_code
        assert orchestrator_exit_code({'execution_status': None, 'success': False}) == 1
        assert orchestrator_exit_code({'execution_status': None, 'success': True}) == 0

    def test_dry_run_success_is_zero(self):
        from src.cli.review_sync_cli import orchestrator_exit_code
        # dry-run 不记账：execution_status=None、success=True
        assert orchestrator_exit_code({'execution_status': None, 'success': True}) == 0
