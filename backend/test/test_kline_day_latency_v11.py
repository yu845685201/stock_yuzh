"""数据采集链路耗时优化 V1.1（20261120）单测

覆盖：
- 批次1 §3.1：TdxApiUnavailableError 分支不再触发 NameError（local_source_missing
  提前初始化），失败结果携带 ts_code/请求阶段/错误类别，分类计数（biz/conn/other）
- 批次1 §3.1：conn 类失败喂熔断（F5 语义保持）
- 批次4 §3.2：分组熔断观察模式（qfq/raw × 市场）计数
- 批次2 §4：独立恢复作业 recover_raw_fallback_job（领取→qfq→原子销账/退避/积压账目）
"""
import logging
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.config.config_manager import ConfigManager
from src.sync import sync_manager as sync_manager_module
from src.sync.kline_day_pipeline import (
    KlineDayDeps, KlineDaySharedState, process_one_stock)
from src.sync.sync_manager import SyncManager
from src.data_sources.tdx_api_source import TdxApiUnavailableError


@pytest.fixture(scope='module')
def manager():
    return SyncManager(ConfigManager())


# ---------- 批次1：单股失败分类与熔断（§3.1） ----------

class FakeDb:
    """最小 db_conn 桩：pipeline 内 execute_query 仅用于 merged_empty 异常采样"""

    def execute_query(self, sql, params=None):
        return []

    def fetch_prev_his_kline_day_close(self, code, date):
        return None


class FakeSource:
    def __init__(self, qfq=None, raw=None, qfq_err=None, raw_err=None):
        self.qfq = qfq if qfq is not None else []
        self.raw = raw if raw is not None else []
        self.qfq_err = qfq_err
        self.raw_err = raw_err

    def get_kline_qfq_tail(self, code, limit):
        if self.qfq_err:
            raise self.qfq_err
        return self.qfq

    def get_kline_raw_tail(self, code, limit):
        if self.raw_err:
            raise self.raw_err
        return self.raw


def _make_deps(source, ctx, threshold=3):
    def _noop_merge(qfq, raw, trade_dates=None, ts_code=None, budget=None):
        return [], []
    return KlineDayDeps(
        tdx_api_source=source, csv_writer=None, db_conn=FakeDb(),
        logger=logging.getLogger('test'), fundamentals_map={},
        trade_dates=['20261117'], init_mode=False, is_explicit_range=False,
        save_to_csv=False, save_to_db=False, fetch_full=False, tail_limit=5,
        total_stocks=1, log_progress=lambda *a: None,
        merge_sources=_noop_merge,
        detect_refetch=lambda *a, **k: (False, {}),
        filter_raw=lambda *a, **k: [],
        normalize_records=lambda *a, **k: ([], []),
        raw_fallback_budget=None, circuit_breaker_threshold=threshold)


_STOCK = {'ts_code': 'sh.600000', 'stock_code': '600000', 'stock_name': '浦发银行'}


class TestTdxUnavailableBranch:
    def test_biz_error_no_nameerror(self):
        """§3.1 核心修复：业务错误分支不再触发 NameError（原 bug：
        local_source_missing 在请求后才初始化，异常分支引用未定义变量）"""
        ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0,
                                          'records': 0, 'db_rows': 0})
        deps = _make_deps(FakeSource(qfq_err=TdxApiUnavailableError('业务错误: x', 'biz')), ctx)
        res = process_one_stock(_STOCK, 1, ctx=ctx, deps=deps)
        # 原行为：NameError 被上层兜成 "单股日K采集失败: name ... is not defined"
        assert "not defined" not in (res.get('error') or '')
        assert res['ts_code'] == 'sh.600000'
        assert res['error_kind'] == 'biz'
        assert res['error_stage'] == 'qfq_tail'
        assert res['source_missing'] == {'qfq_empty': 0, 'raw_empty': 0}
        assert ctx.failure_counts['biz'] == 1
        assert ctx.failure_counts['conn'] == 0
        assert len(ctx.stock_failures) == 1
        assert ctx.stock_failures[0]['stage'] == 'qfq_tail'
        # biz 类不喂熔断（F5 语义保持）
        assert ctx.circuit_tripped is False

    def test_raw_error_stage_labeled(self):
        """raw 路抛错时请求阶段标注为 raw_tail（不误标为 qfq）"""
        ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0,
                                          'records': 0, 'db_rows': 0})
        deps = _make_deps(FakeSource(raw_err=TdxApiUnavailableError('重试耗尽', 'conn')), ctx)
        res = process_one_stock(_STOCK, 1, ctx=ctx, deps=deps)
        assert res['error_stage'] == 'raw_tail'
        assert res['error_kind'] == 'conn'

    def test_conn_error_fires_circuit(self):
        """连续 conn 失败达阈值触发熔断（F5 语义保持），计数计入 conn"""
        ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0,
                                          'records': 0, 'db_rows': 0})
        deps = _make_deps(FakeSource(qfq_err=TdxApiUnavailableError('conn down', 'conn')),
                          ctx, threshold=3)
        for _ in range(3):
            res = process_one_stock(_STOCK, 1, ctx=ctx, deps=deps)
        assert ctx.failure_counts['conn'] == 3
        assert ctx.circuit_tripped is True
        assert res['circuit_tripped'] is True

    def test_pipeline_other_exception_counted(self):
        """非 TdxApiUnavailableError 的 pipeline 内部异常归入 other 类别（safe 兜底层）"""
        ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0,
                                          'records': 0, 'db_rows': 0})

        class BoomSource(FakeSource):
            def get_kline_qfq_tail(self, code, limit):
                raise KeyError('boom')

        from src.sync.kline_day_pipeline import safe_process_one_stock
        deps = _make_deps(BoomSource(), ctx)
        res = safe_process_one_stock(_STOCK, 1, ctx=ctx, deps=deps)
        assert res['error_kind'] == 'other'
        assert ctx.failure_counts['other'] == 1


class TestGroupStatsObserver:
    def test_failures_grouped_by_endpoint_and_market(self):
        """§3.2 观察模式：失败按 (接口×市场) 计数"""
        ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0,
                                          'records': 0, 'db_rows': 0})
        deps = _make_deps(FakeSource(qfq_err=TdxApiUnavailableError('biz', 'biz')), ctx)
        sz_stock = {'ts_code': 'sz.000001', 'stock_code': '000001', 'stock_name': '平安银行'}
        process_one_stock(_STOCK, 1, ctx=ctx, deps=deps)
        process_one_stock(sz_stock, 2, ctx=ctx, deps=deps)
        process_one_stock(sz_stock, 3, ctx=ctx, deps=deps)
        assert ctx.group_stats['qfq|sh']['biz'] == 1
        assert ctx.group_stats['qfq|sz']['biz'] == 2

    def test_success_counts_ok(self):
        """成功路径累计 ok（失败比例分母）"""
        ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0,
                                          'records': 0, 'db_rows': 0})
        deps = _make_deps(FakeSource(qfq=[{'Time': '2026-11-17T15:00:00+08:00'}],
                                     raw=[{'Time': '2026-11-17T15:00:00+08:00'}]), ctx)
        process_one_stock(_STOCK, 1, ctx=ctx, deps=deps)
        assert ctx.group_stats['qfq|sh']['ok'] == 1
        assert ctx.group_stats['raw|sh']['ok'] == 1


# ---------- 批次2：独立恢复作业（§4） ----------

class FakeLockConn:
    def __init__(self):
        self.autocommit = False

    def cursor(self):
        return _NullCtx(FakeLockCursor())

    def close(self):
        pass


class FakeLockCursor:
    def execute(self, *a, **k):
        pass

    def fetchone(self):
        return (True,)


class _NullCtx:
    def __init__(self, cur):
        self.cur = cur

    def __enter__(self):
        return self.cur

    def __exit__(self, *a):
        return False


class TestRecoverRawFallbackJob:
    def _patch_lock(self, manager, monkeypatch):
        monkeypatch.setattr(sync_manager_module.psycopg2, 'connect',
                            lambda **kw: FakeLockConn())

    def test_intraday_bar_not_recovered(self, manager, monkeypatch):
        """盘中实时 bar（THS 盘中 qfq 返回当日 bar）不得作为"已发布"依据——
        2026-11-20 实战案例：恢复作业把 11-20 盘中价覆盖到兜底行并销账（26 行）。
        修复：target（最近已收盘交易日）之后的 bar 被忽略，记录按未发布退避。"""
        self._patch_lock(manager, monkeypatch)
        monkeypatch.setattr(manager.db_conn, 'ensure_raw_fallback_table', lambda: None)
        monkeypatch.setattr(manager.db_conn, 'raw_fallback_backlog_stats',
                            lambda: {'open_n': 1, 'deferred_n': 0, 'oldest_fallback_at': None})
        claims = [{'ts_code': 'sh.600519', 'trade_date': '20261120', 'attempts': 1,
                   'last_error': None}]
        TestRecoverRawFallbackJob._claimed_intraday = False

        def _fake_claim(**kw):
            if TestRecoverRawFallbackJob._claimed_intraday:
                return []
            TestRecoverRawFallbackJob._claimed_intraday = True
            return claims
        monkeypatch.setattr(manager.db_conn, 'claim_raw_fallback_batch', _fake_claim)
        atomic_calls = []
        monkeypatch.setattr(manager.db_conn, 'recover_raw_fallback_row_atomic',
                            lambda *a, **k: (atomic_calls.append(a) or 'RECOVERED_PRICE'))
        deferred = []
        monkeypatch.setattr(manager.db_conn, 'defer_raw_fallback_rows',
                            lambda pairs, secs, last_error=None:
                            deferred.extend(pairs) or len(pairs))
        monkeypatch.setattr(manager.db_conn, 'fetch_trade_calendar',
                            lambda s, e: [{'calendar_date': s}, {'calendar_date': e}])
        monkeypatch.setattr(manager, '_latest_closed_trade_date', lambda: '2026-11-19')

        class FakeSource3:
            def get_kline_qfq_tail(self, code, limit):
                # 11-19 收盘 bar + 11-20 盘中 bar（THS 实时）
                return [{'Time': '2026-11-19T15:00:00+08:00', 'Open': 10000,
                         'High': 10100, 'Low': 9900, 'Close': 10100,
                         'Last': 10000, 'Volume': 1000},
                        {'Time': '2026-11-20T10:30:00+08:00', 'Open': 10150,
                         'High': 10200, 'Low': 10100, 'Close': 10150,
                         'Last': 10100, 'Volume': 500}]

        monkeypatch.setattr(manager, 'tdx_api_source', FakeSource3())

        job = manager.recover_raw_fallback_job(max_stocks=10, budget_seconds=60,
                                               backoff_seconds=900)
        assert job['success'] is True
        # 11-20 的记录不得用盘中 bar 恢复——走未发布退避
        assert atomic_calls == []
        assert ('sh.600519', '20261120') in deferred
        assert job['deferred'] == 1
        assert job['recovered_price'] == 0

    def test_recover_and_defer_flow(self, manager, monkeypatch):
        """同股两日：qfq 已发布一日 → 原子销账；未发布一日 → 退避；账目核对"""
        self._patch_lock(manager, monkeypatch)
        monkeypatch.setattr(manager.db_conn, 'ensure_raw_fallback_table', lambda: None)
        _stats = [
            {'open_n': 2, 'deferred_n': 0, 'oldest_fallback_at': None},
            {'open_n': 1, 'deferred_n': 1, 'oldest_fallback_at': '2026-11-17'},
        ]
        monkeypatch.setattr(manager.db_conn, 'raw_fallback_backlog_stats',
                            lambda: _stats.pop(0))
        claims = [
            {'ts_code': 'sh.600519', 'trade_date': '20261117', 'attempts': 1, 'last_error': None},
            {'ts_code': 'sh.600519', 'trade_date': '20261118', 'attempts': 1, 'last_error': None},
        ]
        TestRecoverRawFallbackJob._claimed = False

        def _fake_claim(**kw):
            if TestRecoverRawFallbackJob._claimed:
                return []
            TestRecoverRawFallbackJob._claimed = True
            return claims
        monkeypatch.setattr(manager.db_conn, 'claim_raw_fallback_batch', _fake_claim)

        atomic_calls = []
        monkeypatch.setattr(manager.db_conn, 'recover_raw_fallback_row_atomic',
                            lambda *a, **k: (atomic_calls.append(a) or 'RECOVERED_PRICE'))
        deferred = []
        monkeypatch.setattr(manager.db_conn, 'defer_raw_fallback_rows',
                            lambda pairs, secs, last_error=None:
                            deferred.extend(pairs) or len(pairs))
        monkeypatch.setattr(manager.db_conn, 'fetch_trade_calendar',
                            lambda s, e: [{'calendar_date': s}, {'calendar_date': e}])
        monkeypatch.setattr(manager, '_latest_closed_trade_date', lambda: '2026-11-18')

        class FakeSource2:
            def get_kline_qfq_tail(self, code, limit):
                return [{'Time': '2026-11-17T15:00:00+08:00', 'Open': 10000,
                         'High': 10100, 'Low': 9900, 'Close': 10100,
                         'Last': 10000, 'Volume': 1000}]

        monkeypatch.setattr(manager, 'tdx_api_source', FakeSource2())

        job = manager.recover_raw_fallback_job(max_stocks=10, budget_seconds=60,
                                               backoff_seconds=900)
        assert job['success'] is True
        assert job['exit_reason'] in ('no_due',)
        assert job['recovered_price'] == 1
        assert job['deferred'] == 1
        assert job['stocks_processed'] == 1
        # 原子销账只对已发布日调用；未发布日进入退避
        assert len(atomic_calls) == 1
        assert atomic_calls[0][0] == 'sh.600519' and atomic_calls[0][1] == '20261117'
        assert ('sh.600519', '20261118') in deferred
        # 账目：期初 2 − 结清 1 = 期末 1
        assert job['backlog_start']['open_n'] == 2
        assert job['backlog_end']['open_n'] == 1
        assert job['budget']['backoff_seconds'] == 900

    def test_lock_busy_exits(self, manager, monkeypatch):
        """互斥：advisory lock 被占时立即退出（locked）"""
        class _BusyCursor(FakeLockCursor):
            def fetchone(self):
                return (False,)

        class FakeLockConnBusy(FakeLockConn):
            def cursor(self):
                return _NullCtx(_BusyCursor())

        monkeypatch.setattr(sync_manager_module.psycopg2, 'connect',
                            lambda **kw: FakeLockConnBusy())
        monkeypatch.setattr(manager.db_conn, 'ensure_raw_fallback_table', lambda: None)
        job = manager.recover_raw_fallback_job(max_stocks=10, budget_seconds=60,
                                               backoff_seconds=900)
        assert job['success'] is False
        assert job['exit_reason'] == 'locked'
