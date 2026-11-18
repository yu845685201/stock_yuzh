"""raw 兜底与自动恢复单测（日K线raw兜底与自动恢复方案 V1.0）

覆盖：
- merge 层兜底触发（qfq 尾部缺口 + raw 有该日 + 除权校验通过）
- 除权校验拦截（Last != 前日 close / Last 缺失 / preclose 上调）
- 非尾部缺口不兜底（结构性历史差异维持原跳过行为）
- trade_dates 过滤与 budget 上限
- normalize 出口 source 标记（TDXAPI_RAWFB）
- 恢复阶段（qfq 已发布→定点覆盖+销账；仍缺→保留 open）
"""
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.config.config_manager import ConfigManager
from src.sync.kline_day_pipeline import RawFallbackBudget
from src.sync.sync_manager import SyncManager


@pytest.fixture(scope='module')
def manager():
    return SyncManager(ConfigManager())


def _qfq(time_str, close_li, last_li, volume=1000):
    return {'Time': time_str, 'Open': close_li, 'High': close_li, 'Low': close_li,
            'Close': close_li, 'Last': last_li, 'Volume': volume}


def _raw(time_str, close_li, amount_li, last_li=None, volume=1000):
    item = {'Time': time_str, 'Open': close_li, 'High': close_li, 'Low': close_li,
            'Close': close_li, 'Volume': volume, 'Amount': amount_li}
    if last_li is not None:
        item['Last'] = last_li
    return item


def _no_exrights_ok(manager, monkeypatch):
    """屏蔽除权校验的查库回落（不触网不触库）"""
    monkeypatch.setattr(
        manager.db_conn, 'fetch_prev_his_kline_day_close',
        lambda code, date: None)


class TestMergeRawFallback:
    def test_tail_gap_fallback_triggered(self, manager, monkeypatch):
        """qfq 缺尾部日期 + raw 有该日 + Last==前日 raw close → 兜底行生成并带标记"""
        _no_exrights_ok(manager, monkeypatch)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-16T15:00:00+08:00', 10000, 156000000),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=10000)]
        merged, skips = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261117'])
        assert len(merged) == 2
        fb = [r for r in merged if r.get('raw_fallback')]
        assert len(fb) == 1
        assert fb[0]['Close'] == 10100
        assert fb[0]['RawClose'] == 10100
        assert fb[0]['Amount'] == 160000000
        assert skips == []

    def test_interior_or_older_gap_not_fallback(self, manager, monkeypatch):
        """qfq 序列中段/早期缺失（结构性差异）维持原跳过行为"""
        _no_exrights_ok(manager, monkeypatch)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-14T15:00:00+08:00', 9800, 100000, last_li=9700)]
        merged, skips = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261114'])
        assert len(merged) == 1  # 仅 qfq 那行
        assert skips == []

    def test_exrights_suspect_skipped(self, manager, monkeypatch):
        """Last != 前日 raw close（除权嫌疑）→ 不兜底、记 skip"""
        _no_exrights_ok(manager, monkeypatch)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-16T15:00:00+08:00', 10000, 156000000),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=9500)]
        merged, skips = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261117'])
        assert len(merged) == 1
        assert not any(r.get('raw_fallback') for r in merged)
        assert len(skips) == 1
        assert skips[0]['reason'] == 'exrights_suspected'
        assert skips[0]['trade_date'] == '20261117'

    def test_preclose_upward_suspect_skipped(self, manager, monkeypatch):
        """Last > 前日 close（preclose 上调，非除权特征）→ 同样跳过"""
        _no_exrights_ok(manager, monkeypatch)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-16T15:00:00+08:00', 10000, 156000000),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=10200)]
        merged, skips = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261117'])
        assert len(merged) == 1
        assert len(skips) == 1

    def test_missing_last_skipped(self, manager, monkeypatch):
        """raw 行缺 Last 字段（校验数据不全）→ 保守跳过"""
        _no_exrights_ok(manager, monkeypatch)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-16T15:00:00+08:00', 10000, 156000000),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000)]
        merged, skips = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261117'])
        assert len(merged) == 1
        assert len(skips) == 1
        assert skips[0]['reason'] == 'missing_last'  # 校验数据不全，与除权嫌疑区分

    def test_trade_dates_filter(self, manager, monkeypatch):
        """请求区间外的日期不兜底（P-11 日期过滤语义）"""
        _no_exrights_ok(manager, monkeypatch)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-16T15:00:00+08:00', 10000, 156000000),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=10000)]
        merged, skips = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261118'])
        assert len(merged) == 1
        assert skips == []

    def test_budget_exhausted(self, manager, monkeypatch):
        """额度耗尽 → 不兜底"""
        _no_exrights_ok(manager, monkeypatch)
        budget = RawFallbackBudget(0)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-16T15:00:00+08:00', 10000, 156000000),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=10000)]
        merged, _ = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261117'],
                                                     raw_fallback_budget=budget)
        assert len(merged) == 1

    def test_budget_one_per_stock(self, manager, monkeypatch):
        """一股一额度：同股多条兜底行只消耗 1 个额度"""
        _no_exrights_ok(manager, monkeypatch)
        budget = RawFallbackBudget(2)
        qfq = [_qfq('2026-11-15T15:00:00+08:00', 9900, 9800)]
        raw = [_raw('2026-11-15T15:00:00+08:00', 9900, 100000),
               _raw('2026-11-16T15:00:00+08:00', 10000, 156000000, last_li=9900),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=10000)]
        merged, _ = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261116', '20261117'],
                                                     raw_fallback_budget=budget)
        assert len([r for r in merged if r.get('raw_fallback')]) == 2
        assert budget.exhausted is False  # 两行兜底仅消耗 1 个额度（一股一额度）

    def test_disabled_via_config(self, manager, monkeypatch):
        """config 开关关闭 → 不兜底"""
        _no_exrights_ok(manager, monkeypatch)
        monkeypatch.setattr(manager.config_manager, 'get',
                            lambda key, default=None: False if 'raw_fallback.enabled' in key else default)
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-16T15:00:00+08:00', 10000, 156000000),
               _raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=10000)]
        merged, _ = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261117'])
        assert len(merged) == 1

    def test_no_exrights_db_fallback_path(self, manager, monkeypatch):
        """raw_list 内无前日（窗口首日兜底）→ 回落查库比对（元口径）"""
        monkeypatch.setattr(
            manager.db_conn, 'fetch_prev_his_kline_day_close',
            lambda code, date: 10.00)  # 库内前日 close 10 元 = raw Last 10000 厘
        qfq = [_qfq('2026-11-16T15:00:00+08:00', 10000, 9900)]
        raw = [_raw('2026-11-17T15:00:00+08:00', 10100, 160000000, last_li=10000)]
        merged, skips = manager._merge_kline_day_sources(qfq, raw, trade_dates=['20261117'],
                                                         ts_code='sh.600000')
        assert len([r for r in merged if r.get('raw_fallback')]) == 1
        assert skips == []


class TestNormalizeFallbackSource:
    def test_source_flag(self, manager):
        """normalize 出口：兜底行 source=TDXAPI_RAWFB，普通行 TDXAPI"""
        rows = [
            {'Time': '2026-11-16T15:00:00+08:00', 'Open': 10000, 'High': 10000,
             'Low': 10000, 'Close': 10000, 'Last': 9900, 'Volume': 1000,
             'Amount': 156000000, 'RawClose': 10000},
            {'Time': '2026-11-17T15:00:00+08:00', 'Open': 10100, 'High': 10100,
             'Low': 10100, 'Close': 10100, 'Last': 10000, 'Volume': 1000,
             'Amount': 160000000, 'RawClose': 10100, 'raw_fallback': True},
        ]
        stock = {'ts_code': 'sh.600000', 'stock_code': '600000', 'stock_name': '浦发银行'}
        records, _ = manager._normalize_kline_day_records(rows, stock, {})
        assert records[0]['source'] == 'TDXAPI'
        assert records[1]['source'] == 'TDXAPI_RAWFB'


class TestRecoverRawFallback:
    def _prepare(self, manager, monkeypatch, open_rows, qfq_dates):
        """fake db_conn 与 tdx source"""
        monkeypatch.setattr(manager.db_conn, 'fetch_open_raw_fallback',
                            lambda: open_rows)
        updates = []
        monkeypatch.setattr(manager.db_conn, 'update_raw_fallback_row_prices',
                            lambda *a: updates.append(a) or 1)
        marked = []
        monkeypatch.setattr(manager.db_conn, 'mark_raw_fallback_recovered',
                            lambda pairs, recover_source='recovery_stage': marked.extend(pairs) or len(pairs))
        monkeypatch.setattr(manager.db_conn, 'fetch_trade_calendar',
                            lambda s, e: [{'calendar_date': s}, {'calendar_date': e}])

        class FakeSource:
            def get_kline_qfq_tail(self, code, limit):
                return [{'Time': f'{d[0:4]}-{d[4:6]}-{d[6:8]}T15:00:00+08:00',
                         'Open': 10000, 'High': 10000, 'Low': 10000,
                         'Close': 10100, 'Last': 10000, 'Volume': 1000}
                        for d in qfq_dates]
        monkeypatch.setattr(manager, 'tdx_api_source', FakeSource())
        return updates, marked

    def test_recover_when_qfq_published(self, manager, monkeypatch):
        """THS 已发布 → 定点覆盖 + 销账"""
        updates, marked = self._prepare(manager, monkeypatch,
                                        open_rows=[{'ts_code': 'sh.600519', 'trade_date': '20261117'}],
                                        qfq_dates=['20261116', '20261117'])
        result = {}
        n = manager._recover_raw_fallback(['20261117'], result)
        assert n == 1
        assert result['fallback_recovered'] == 1
        assert len(updates) == 1
        # 定点更新参数：(ts_code, trade_date, open, high, low, close, preclose, change_rate)
        assert updates[0][0] == 'sh.600519'
        assert updates[0][1] == '20261117'
        assert updates[0][5] == pytest.approx(10.10)   # close 10100 厘 → 10.10 元
        assert marked == [('sh.600519', '20261117')]

    def test_keep_open_when_qfq_still_missing(self, manager, monkeypatch):
        """THS 仍缺 → 保留 open，不销账"""
        updates, marked = self._prepare(manager, monkeypatch,
                                        open_rows=[{'ts_code': 'sh.600519', 'trade_date': '20261117'}],
                                        qfq_dates=['20261116'])  # qfq 仍缺 11-17
        result = {}
        n = manager._recover_raw_fallback(['20261117'], result)
        assert n == 0
        assert updates == []
        assert marked == []

    def test_no_open_rows_noop(self, manager, monkeypatch):
        updates, marked = self._prepare(manager, monkeypatch, open_rows=[], qfq_dates=[])
        result = {}
        assert manager._recover_raw_fallback(['20261117'], result) == 0
        assert updates == []
        assert marked == []


class TestRawFallbackBudget:
    def test_acquire_and_exhaust(self):
        b = RawFallbackBudget(2)
        assert b.try_acquire() is True
        assert b.try_acquire() is True
        assert b.try_acquire() is False
        assert b.exhausted is True

    def test_zero_budget(self):
        b = RawFallbackBudget(0)
        assert b.try_acquire() is False
        assert b.exhausted is True
