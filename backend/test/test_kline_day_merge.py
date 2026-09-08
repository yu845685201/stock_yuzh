"""日K双源合并与除权检测单测"""
import pytest

from src.config.config_manager import ConfigManager
from src.sync.sync_manager import SyncManager


@pytest.fixture(scope='module')
def manager():
    """SyncManager 实例（db_conn 在各用例中按需替换为 mock）"""
    return SyncManager(ConfigManager())


def _qfq(time_str, close_li, last_li, volume=1000):
    return {'Time': time_str, 'Open': close_li, 'High': close_li, 'Low': close_li,
            'Close': close_li, 'Last': last_li, 'Volume': volume}


def _raw(time_str, close_li, amount_li, volume=1000):
    return {'Time': time_str, 'Open': close_li, 'High': close_li, 'Low': close_li,
            'Close': close_li, 'Volume': volume, 'Amount': amount_li}


class TestMergeKlineDaySources:
    def test_qfq_master_with_raw_overlay(self, manager):
        qfq = [_qfq('2026-09-01T15:00:00+08:00', 10000, 0),
               _qfq('2026-09-02T15:00:00+08:00', 10100, 10000),
               _qfq('2026-09-03T15:00:00+08:00', 10200, 10100)]
        raw = [_raw('2026-09-01T15:00:00+08:00', 10000, 156000000),
               _raw('2026-09-02T15:00:00+08:00', 10100, 160000000)]
        merged = manager._merge_kline_day_sources(qfq, raw)

        assert len(merged) == 3
        assert merged[0]['Amount'] == 156000000
        assert merged[0]['RawClose'] == 10000
        assert merged[1]['Amount'] == 160000000
        # 原始域缺失的日期留空
        assert merged[2]['Amount'] is None
        assert merged[2]['RawClose'] is None
        # 按日期升序
        assert merged[0]['Time'] < merged[-1]['Time']

    def test_raw_only_date_skipped(self, manager):
        """前复权主表没有的日期（两源不一致）应跳过"""
        qfq = [_qfq('2026-09-01T15:00:00+08:00', 10000, 0)]
        raw = [_raw('2026-09-01T15:00:00+08:00', 10000, 100),
               _raw('1999-01-04T15:00:00+08:00', 5000, 50)]
        merged = manager._merge_kline_day_sources(qfq, raw)
        assert len(merged) == 1
        assert merged[0]['Amount'] == 100

    def test_empty_inputs(self, manager):
        assert manager._merge_kline_day_sources([], []) == []
        assert manager._merge_kline_day_sources(None, None) == []


class TestDetectKlineDayRefetch:
    def _prepare(self, manager, monkeypatch, prev_close=None, stored_raw=None):
        monkeypatch.setattr(
            manager.db_conn, 'fetch_prev_his_kline_day_close',
            lambda code, date: prev_close)
        monkeypatch.setattr(
            manager.db_conn, 'execute_query',
            lambda q, p: stored_raw or [])

    def test_rebase_hit(self, manager, monkeypatch):
        """Last 与库存 close 错位超阈值 → 命中除权重基"""
        self._prepare(manager, monkeypatch, prev_close=10.00)
        # 库存基准 10.00 元，除权后新基准 Last=9.60 元，错位 0.40 元
        merged = manager._merge_kline_day_sources(
            [_qfq('2026-06-12T15:00:00+08:00', 9640, 9600)],
            [_raw('2026-06-12T15:00:00+08:00', 9640, 100)])
        hit, detail = manager._detect_kline_day_refetch('sz.000001', merged)
        assert hit is True
        assert detail['type'] == 'qfq_rebase'
        assert detail['stored_close'] == 10.00
        assert detail['fresh_last'] == 9.60

    def test_rebase_no_hit_within_threshold(self, manager, monkeypatch):
        """正常波动（错位在阈值内）不命中"""
        self._prepare(manager, monkeypatch, prev_close=10.00)
        merged = manager._merge_kline_day_sources(
            [_qfq('2026-09-04T15:00:00+08:00', 9990, 10000)],  # Last=9.99元
            [_raw('2026-09-04T15:00:00+08:00', 9990, 100)])
        hit, detail = manager._detect_kline_day_refetch('sz.000001', merged)
        assert hit is False

    def test_no_stored_data_no_hit(self, manager, monkeypatch):
        """库存无前置数据（新股）不命中"""
        self._prepare(manager, monkeypatch, prev_close=None)
        merged = manager._merge_kline_day_sources(
            [_qfq('2026-09-04T15:00:00+08:00', 9990, 0)],
            [_raw('2026-09-04T15:00:00+08:00', 9990, 100)])
        hit, _ = manager._detect_kline_day_refetch('sz.000001', merged)
        assert hit is False

    def test_raw_revision_hit(self, manager, monkeypatch):
        """库存 raw_close 与新拉不一致 → 源数据修订命中"""
        self._prepare(manager, monkeypatch, prev_close=None, stored_raw=[
            {'trade_date': '20260904', 'raw_close': 10.00}])
        merged = manager._merge_kline_day_sources(
            [_qfq('2026-09-04T15:00:00+08:00', 10500, 10000)],
            [_raw('2026-09-04T15:00:00+08:00', 10500, 100)])
        hit, detail = manager._detect_kline_day_refetch('sz.000001', merged)
        assert hit is True
        assert detail['type'] == 'raw_revision'


class TestNormalizeRawClose:
    def test_raw_close_passthrough(self, manager):
        """合并字典的 RawClose（厘）经 normalize 转为元"""
        raw = {'Time': '2026-09-04T15:00:00+08:00', 'Open': 11800, 'High': 11900,
               'Low': 11700, 'Close': 11890, 'Last': 11730, 'Volume': 838126,
               'Amount': 969948416000, 'RawClose': 11730}
        record = manager._normalize_kline_day_record(
            raw,
            {'ts_code': 'sz.000001', 'stock_code': '000001', 'stock_name': '平安银行'})
        assert record['raw_close'] == pytest.approx(11.73)
        assert record['close'] == pytest.approx(11.89)
        assert record['amount'] == pytest.approx(969948416.0)  # 厘→元
        assert record['volume'] == pytest.approx(83812600)     # 手→股
        assert record['_raw_last'] == pytest.approx(11.73)
