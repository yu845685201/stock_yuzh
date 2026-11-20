"""D01 闭环控制层测试（每日复盘采集与分析闭环技术方案 V1.0 §3/§4，不触真实数据库）

覆盖：
- 公共契约：分区键构造/解析对称、目标日解析、编排 entry → PartitionResult 映射；
- 源期望：数据集注册表查询、NOT_DUE/CALENDAR_UNKNOWN 判定、两融 T-1 展开；
- 编排器：循环依赖检测（T10）、execution_status 分层（§6.1）、detail 累积不覆盖；
- apply_partition 事务模板：锁 → 父版本 → 业务行 → 验收+修订+任务 → 提交顺序，
  异常回滚（T12 结构验证），内容未变不新增修订（T13 去重语义）。

重算任务租约（claim/complete）依赖真实 SQL 语义（FOR UPDATE SKIP LOCKED、条件
更新），fake conn 无法验证，留待 uat 库跑迁移 SQL 后人工验证（见实施总结）。
"""

import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.database.dao_collection_lineage import CollectionLineageDaoMixin
from src.sync.collection_contracts import (
    AvailabilityBasis,
    PartitionResult,
    PartitionStatus,
    RunContext,
    RunMode,
    build_partition_key,
    entry_to_partition_result,
    new_id,
    normalize_ymd,
    parse_partition_key,
    resolve_target_date,
)
from src.sync.source_expectations import DATASET_REGISTRY, expected_partition


# ---------- 公共契约 ----------

class TestPartitionKey:
    def test_roundtrip_without_code(self):
        key = build_partition_key('20261118', 'etf_scale', 'CN', exchange='szse',
                                  sub_dataset='1945')
        parsed = parse_partition_key(key)
        assert key == '20261118|CN|szse|etf_scale|1945'
        assert parsed['target_date'] == '20261118'
        assert parsed['dataset'] == 'etf_scale'
        assert parsed['code'] is None

    def test_roundtrip_with_code(self):
        key = build_partition_key('2026-11-18', 'kline', 'CN', exchange='sse', code='sh.600375')
        assert key == '20261118|CN|sse|kline||sh.600375'  # 固定 5 段 + 可选代码段
        parsed = parse_partition_key(key)
        assert parsed['sub_dataset'] == ''
        assert parsed['code'] == 'sh.600375'

    def test_date_normalized(self):
        assert build_partition_key('2026-11-18', 'kline', 'CN').startswith('20261118|')

    def test_invalid_date_raises(self):
        with pytest.raises(ValueError):
            build_partition_key('2026111', 'kline', 'CN')

    def test_separator_in_segment_raises(self):
        with pytest.raises(ValueError):
            build_partition_key('20261118', 'a|b', 'CN')
        with pytest.raises(ValueError):
            build_partition_key('20261118', 'kline', 'CN', code='sh|600375')


class TestResolveTargetDate:
    def test_daily_uses_latest_closed(self):
        assert resolve_target_date(RunMode.DAILY, None, '20261118') == '20261118'

    def test_daily_ignores_explicit(self):
        assert resolve_target_date(RunMode.DAILY, '20261110', '20261118') == '20261118'

    def test_backfill_requires_explicit(self):
        with pytest.raises(ValueError):
            resolve_target_date(RunMode.BACKFILL, None, '20261118')
        assert resolve_target_date(RunMode.BACKFILL, '20261112', '20261118') == '20261112'

    def test_normalize_ymd(self):
        assert normalize_ymd('2026-11-18') == '20261118'
        assert normalize_ymd(None) is None
        assert normalize_ymd('bad') is None


class TestEntryMapping:
    def _entry(self, status='成功', rows=10, detail=''):
        return {'source': 'etf_scale', 'label': 'ETF份额', 'status': status,
                'rows': rows, 'duration': 1.0, 'detail': detail}

    def test_success_with_rows_is_complete(self):
        pr = entry_to_partition_result(self._entry(), 'etf_scale', 'CN', '20261118')
        assert pr.status == PartitionStatus.COMPLETE
        assert pr.availability_basis == AvailabilityBasis.SYSTEM_OBSERVED
        assert pr.expected_codes is None and pr.observed_codes == 10

    def test_success_zero_rows_is_confirmed_empty(self):
        pr = entry_to_partition_result(self._entry(rows=0), 'etf_scale', 'CN', '20261118')
        assert pr.status == PartitionStatus.CONFIRMED_EMPTY

    def test_failed_maps_request_failed(self):
        pr = entry_to_partition_result(self._entry(status='失败'), 'lhb', 'CN', '20261118')
        assert pr.status == PartitionStatus.FAILED
        assert 'REQUEST_FAILED' in pr.reason_codes

    def test_pending_maps_partial(self):
        pr = entry_to_partition_result(self._entry(status='待补', detail='数据日滞后'),
                                       'lhb', 'CN', '20261118')
        assert pr.status == PartitionStatus.PARTIAL
        assert pr.reason_codes == ['SOURCE_STALE']

    def test_skip_kline_gate_maps_blocked(self):
        pr = entry_to_partition_result(self._entry(status='跳过', detail='日K行数校验未过'),
                                       'money_flow', 'CN', '20261118')
        assert pr.status == PartitionStatus.BLOCKED

    def test_skip_other_maps_not_applicable(self):
        pr = entry_to_partition_result(self._entry(status='跳过', detail='runner 未注册'),
                                       'news', 'CN', '20261118')
        assert pr.status == PartitionStatus.NOT_APPLICABLE

    def test_partition_key_in_row_stable(self):
        pr = entry_to_partition_result(self._entry(), 'etf_scale', 'CN', '20261118')
        row = pr.to_row('job-1')
        assert row['partition_key'] == '20261118|CN||etf_scale|'
        assert row['job_id'] == 'job-1'
        # partition_id 为写入时兜底生成（dataclass 不预置，to_row 兜底 new_id）
        assert row['partition_id'] and len(row['partition_id']) == 36


# ---------- 源期望 ----------

class _FakeDb:
    """execute_query 按调用序返回预置结果集"""

    def __init__(self, results):
        self._results = list(results)
        self.queries = []

    def execute_query(self, query, params=None):
        self.queries.append((query, params))
        return self._results.pop(0) if self._results else []


class TestExpectedPartition:
    CTX = RunContext(job_id='j1', target_date='20261118', run_mode=RunMode.DAILY)

    def test_unknown_dataset_pending(self):
        out = expected_partition(self.CTX, 'not_registered')
        assert out['status_hint'] == 'CALENDAR_UNKNOWN'
        assert out['expected_date'] is None

    def test_cn_daily_without_db_uses_target(self):
        out = expected_partition(self.CTX, 'kline')
        assert out['expected_date'] == '20261118'
        assert out['status_hint'] == 'OK'
        assert out['due_at'].tzinfo is not None  # UTC aware

    def test_margin_without_db_pending_offset(self):
        # 两融 T-1 业务日：无日历连接时无法确认 T-1 → 待核，不硬算自然日差
        out = expected_partition(self.CTX, 'margin')
        assert out['status_hint'] == 'CALENDAR_UNKNOWN'
        assert out['exchanges'] == ['sse', 'szse']

    def test_cn_with_calendar_expands_offset(self):
        # 日历：最新收盘=20261118；其前一交易日=20261117（两融应到 17 日）
        db = _FakeDb([[{'calendar_date': '2026-11-18'}],
                      [{'calendar_date': '2026-11-17'}]])
        out = expected_partition(self.CTX, 'margin', db=db)
        assert out['expected_date'] == '20261117'
        assert out['status_hint'] == 'OK'

    def test_non_trading_target_rolls_back(self):
        # 目标日为周末（日历返回上一交易日）→ expected 用最近已收盘交易日
        db = _FakeDb([[{'calendar_date': '2026-11-14'}]])
        out = expected_partition(self.CTX, 'kline', db=db)
        assert out['expected_date'] == '20261114'

    def test_us_market_calendar_unknown(self):
        out = expected_partition(self.CTX, 'overseas_kline')
        assert out['market'] == 'MULTI'
        assert out['status_hint'] == 'CALENDAR_UNKNOWN'

    def test_not_due_when_cutoff_before_publish(self):
        # 截止时刻设为当地（上海）上午 10 点：A 股收盘 15:00+延迟 → 尚未应发布
        ctx = RunContext(job_id='j1', target_date='20261118', run_mode=RunMode.DAILY,
                         cutoff_at=datetime(2026, 11, 18, 10, 0,
                                            tzinfo=ZoneInfo('Asia/Shanghai')))
        out = expected_partition(ctx, 'kline')
        assert out['status_hint'] == 'NOT_DUE'
        assert 'NOT_PUBLISHED_YET' in out['reason_codes']

    def test_due_after_close(self):
        ctx = RunContext(job_id='j1', target_date='20261118', run_mode=RunMode.DAILY,
                         cutoff_at=datetime.now(timezone.utc) + timedelta(hours=1))
        out = expected_partition(ctx, 'kline')
        assert out['status_hint'] == 'OK'

    def test_registry_has_orchestrator_sources(self):
        # 注册表覆盖编排器主要源（缺失时 expected_partition 走 UNKNOWN 待核，记账降级）
        for name in ('kline', 'index', 'lhb', 'margin', 'etf_scale', 'money_flow',
                     'hkex_connect', 'overseas'):
            assert name in DATASET_REGISTRY


# ---------- 编排器：循环依赖 / 分层状态 / detail 累积 ----------

class TestDependencyCycles:
    def _orch(self):
        from src.sync.review_sync_orchestrator import ReviewSyncOrchestrator
        return ReviewSyncOrchestrator.__new__(ReviewSyncOrchestrator)

    def test_current_declaration_acyclic(self):
        from src.sync.review_sync_orchestrator import SOURCE_DEPENDENCIES
        assert self._orch()._check_dependency_cycles(SOURCE_DEPENDENCIES) == []

    def test_direct_cycle_detected(self):
        cycles = self._orch()._check_dependency_cycles({'a': {'b'}, 'b': {'a'}})
        assert cycles and 'a' in cycles[0] and 'b' in cycles[0]

    def test_self_cycle_detected(self):
        cycles = self._orch()._check_dependency_cycles({'a': {'a'}})
        assert cycles

    def test_transitive_cycle_detected(self):
        cycles = self._orch()._check_dependency_cycles(
            {'a': {'b'}, 'b': {'c'}, 'c': {'a'}})
        assert cycles

    def test_external_dependency_ignored(self):
        # 依赖声明外的源视为外部已满足，不构成环
        assert self._orch()._check_dependency_cycles({'a': {'x'}, 'b': {'a'}}) == []


class TestExecutionStatus:
    def _orch(self):
        from src.sync.review_sync_orchestrator import ReviewSyncOrchestrator
        return ReviewSyncOrchestrator.__new__(ReviewSyncOrchestrator)

    def test_all_success_is_zero(self):
        summary = [{'source': 'kline', 'status': '成功', 'rows': 5000, 'detail': ''},
                   {'source': 'lhb', 'status': '成功', 'rows': 100, 'detail': ''}]
        status, readiness = self._orch()._derive_execution_status(summary)
        assert status == '0'
        assert readiness == {'kline': 'COMPLETE', 'lhb': 'COMPLETE'}

    def test_partial_failure_is_three(self):
        summary = [{'source': 'kline', 'status': '成功', 'rows': 5000, 'detail': ''},
                   {'source': 'lhb', 'status': '失败', 'rows': 0, 'detail': 'x'}]
        status, _ = self._orch()._derive_execution_status(summary)
        assert status == '3'

    def test_all_failed_is_one(self):
        summary = [{'source': 'lhb', 'status': '失败', 'rows': 0, 'detail': 'x'},
                   {'source': 'margin', 'status': '待补', 'rows': 0, 'detail': 'y'}]
        status, readiness = self._orch()._derive_execution_status(summary)
        assert status == '1'
        assert readiness['margin'] == 'PARTIAL'

    def test_skip_not_applicable_does_not_fail(self):
        summary = [{'source': 'kline', 'status': '成功', 'rows': 5000, 'detail': ''},
                   {'source': 'news', 'status': '跳过', 'rows': 0, 'detail': 'runner 未注册'}]
        status, readiness = self._orch()._derive_execution_status(summary)
        assert status == '0'
        assert readiness['news'] == 'NOT_APPLICABLE'


class TestFillEntryDetailAccumulates:
    """技术方案 §6.1：detail 为累积语义，末段失败摘要不得覆盖已有原因"""

    def _orch_with_cfg(self):
        from src.sync.review_sync_orchestrator import ReviewSyncOrchestrator
        orch = ReviewSyncOrchestrator.__new__(ReviewSyncOrchestrator)
        orch.config = {'review_sync': {'data_date_check_enabled': False}}
        return orch

    def test_failure_detail_appended_not_overwritten(self):
        orch = self._orch_with_cfg()
        entry = {'source': 'x', 'label': 'x', 'status': '成功', 'rows': 5,
                 'duration': 0.0, 'detail': '数据日滞后：入库最大 20261117'}
        src_result = {'success': True, 'stats': {'rows': 5, 'failed': ['a']}, 'errors': []}
        orch._fill_entry(entry, src_result)
        assert '数据日滞后' in entry['detail']
        assert '1 只失败' in entry['detail']

    def test_empty_error_keeps_existing_detail(self):
        orch = self._orch_with_cfg()
        entry = {'source': 'x', 'label': 'x', 'status': '成功', 'rows': 5,
                 'duration': 0.0, 'detail': '数据日滞后：入库最大 20261117'}
        src_result = {'success': True, 'stats': {'rows': 5}, 'errors': []}
        orch._fill_entry(entry, src_result)
        assert entry['detail'] == '数据日滞后：入库最大 20261117'


# ---------- apply_partition 事务模板（T12/T13 结构验证） ----------

class _FakeCursor:
    def __init__(self, log, fetch_queue=None):
        self._log = log
        self._fetch_queue = fetch_queue or []

    def execute(self, sql, params=None):
        self._log.append(('execute', ' '.join(sql.split())[:80], params))

    def fetchone(self):
        return self._fetch_queue.pop(0) if self._fetch_queue else None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeConn:
    def __init__(self):
        self.log = []
        self._fetch_queue = []

    def set_fetch(self, rows):
        self._fetch_queue = list(rows)

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self.log, self._fetch_queue)

    def commit(self):
        self.log.append(('commit',))

    def rollback(self):
        self.log.append(('rollback',))


class _FakeDao(CollectionLineageDaoMixin):
    """仅重定向连接获取；其余 *_tx 方法走真实 SQL 组装路径"""

    def __init__(self, conn):
        self._conn = conn

    @contextmanager
    def get_connection(self):
        yield self._conn


class TestApplyPartition:
    PARTITION_ROW = {
        'partition_id': 'p1', 'job_id': 'j1', 'dataset': 'etf_scale',
        'source': 'sse+szse', 'market': 'CN',
        'partition_key': '20261118|CN||etf_scale|',
        'expected_date': '20261118', 'status': 'COMPLETE',
        'reason_codes': [], 'observed_codes': 1696,
    }
    REVISION_ROW = {'content_hash': 'h2', 'quality_hash': 'q2', 'schema_version': 'v1'}

    def test_happy_path_order_lock_rows_revision_commit(self):
        conn = _FakeConn()
        dao = _FakeDao(conn)
        written = []

        def writer(c, rows):
            written.extend(rows)

        out = dao.apply_partition('etf_scale', '20261118|CN||etf_scale|',
                                  dict(self.PARTITION_ROW), [{'fund': '510300'}],
                                  writer, revision_row=dict(self.REVISION_ROW),
                                  tasks=[{'module': 'm', 'module_version': 'v1',
                                          'anchor_start': '20261118',
                                          'anchor_end': '20261118',
                                          'scope_hash': 's'}])
        kinds = [e[0] if e[0] != 'execute' else 'execute' for e in conn.log]
        assert kinds[0] == 'execute'          # advisory lock
        assert written == [{'fund': '510300'}]
        assert out['committed'] is True
        assert out['revision_id'] and out['unchanged'] is False
        assert conn.log[-1] == ('commit',)
        sqls = [e[1] for e in conn.log if e[0] == 'execute']
        assert any('pg_advisory_xact_lock' in s for s in sqls)
        assert any('INSERT INTO sync_partition' in s for s in sqls)
        assert any('INSERT INTO dataset_revision' in s for s in sqls)
        assert any('INSERT INTO recompute_task' in s for s in sqls)

    def test_unchanged_content_no_revision(self):
        # 本次内容与父版本 content/quality hash 均一致 → 不新增修订、不排任务（§4.3 第 3 条）
        conn = _FakeConn()
        conn.set_fetch([{'revision_id': 'parent-1', 'content_hash': 'h1',
                         'quality_hash': 'q1'}])
        dao = _FakeDao(conn)
        out = dao.apply_partition('etf_scale', 'k', dict(self.PARTITION_ROW),
                                  [{'fund': '510300'}],
                                  lambda c, r: None,
                                  revision_row={'content_hash': 'h1', 'quality_hash': 'q1'})
        sqls = [e[1] for e in conn.log if e[0] == 'execute']
        assert out['unchanged'] is True
        assert out['revision_id'] is None
        assert not any('INSERT INTO dataset_revision' in s for s in sqls)
        assert not any('INSERT INTO recompute_task' in s for s in sqls)
        assert conn.log[-1] == ('commit',)

    def test_writer_exception_rolls_back(self):
        # T12：业务行写入失败 → 整体回滚，无验收/修订残留
        conn = _FakeConn()
        dao = _FakeDao(conn)

        def bad_writer(c, rows):
            raise RuntimeError('业务写入失败')

        with pytest.raises(RuntimeError):
            dao.apply_partition('etf_scale', 'k', dict(self.PARTITION_ROW),
                                [{'fund': 'x'}], bad_writer,
                                revision_row=dict(self.REVISION_ROW))
        assert conn.log[-1] == ('rollback',)
        assert not any(e[0] == 'commit' for e in conn.log)

    def test_quality_hash_change_triggers_revision(self):
        # 内容同、质量从待核变完整 → quality_hash 变化仍需新增修订（§4.3）
        conn = _FakeConn()
        conn.set_fetch([{'revision_id': 'parent-1', 'content_hash': 'h1',
                         'quality_hash': 'q_pending'}])
        dao = _FakeDao(conn)
        out = dao.apply_partition('etf_scale', 'k', dict(self.PARTITION_ROW), [],
                                  None,
                                  revision_row={'content_hash': 'h1', 'quality_hash': 'q_full'})
        assert out['unchanged'] is False
        assert out['revision_id'] is not None


# ---------- 迁移 SQL 与 ensure DDL 一致性 ----------

class TestMigrationSqlParity:
    """up 迁移与 DAO ensure 覆盖同一组表（防两处漂移）"""

    def test_migration_files_exist(self):
        up = Path(__file__).resolve().parents[2] / 'doc/migrations/20261119_review_closed_loop_up.sql'
        down = Path(__file__).resolve().parents[2] / 'doc/migrations/20261119_review_closed_loop_down.sql'
        assert up.exists() and down.exists()

    def test_up_sql_contains_all_tables(self):
        up = Path(__file__).resolve().parents[2] / 'doc/migrations/20261119_review_closed_loop_up.sql'
        text = up.read_text(encoding='utf-8')
        for table in ('sync_job', 'sync_partition', 'dataset_revision', 'recompute_task',
                      'review_batch', 'review_verification_version', 'market_quote_snapshot'):
            assert f'CREATE TABLE IF NOT EXISTS {table} ' in text, table

    def test_ensure_ddl_tables_match(self):
        from src.database.dao_review_publication import ReviewPublicationDaoMixin
        assert set(CollectionLineageDaoMixin._COLLECTION_TABLE_DDL) == {
            'sync_job', 'sync_partition', 'dataset_revision', 'recompute_task'}
        assert set(ReviewPublicationDaoMixin._PUBLICATION_TABLE_DDL) == {
            'review_batch', 'review_verification_version'}
