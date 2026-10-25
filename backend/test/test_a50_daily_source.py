"""A50 日频源接入测试（P1-5 采集侧收口，附录 C.5，2026-10-25）。

三层：SinaSource 解析器（纯静态）／kline manager 源感知分支（stub sina+db）／
报告聚合器 A50 日频优先链（stub db）。不触网不触库。
"""
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

_SAMPLE = (
    "/*<script>location.href='//sina.com';</script>*/\n"
    'var t=([{"date":"2016-10-25","open":"9906.000","high":"9922.000",'
    '"low":"9825.000","close":"9868.000","volume":"0","position":"0"},'
    '{"date":"2016-10-26","open":"9860.000","high":"9867.000","low":"9787.000",'
    '"close":"9850.000","volume":"0"}]);'
)


class TestSinaGlobalFuturesParser:
    def test_parse_rows_and_filter(self):
        from src.data_sources.sina_source import SinaSource
        rows = SinaSource.parse_global_futures_daily(_SAMPLE)
        assert len(rows) == 2
        assert rows[0]['trade_date'] == '20161025'
        assert rows[0]['close'] == 9868.0 and rows[1]['low'] == 9787.0
        # 闭区间过滤
        only2 = SinaSource.parse_global_futures_daily(_SAMPLE, start='20161026')
        assert [r['trade_date'] for r in only2] == ['20161026']

    def test_parse_bad_response_raises(self):
        from src.data_sources.sina_source import SinaSource
        try:
            SinaSource.parse_global_futures_daily('var t=(null);')
            raised = False
        except ValueError:
            raised = True
        assert raised

    def test_parse_skips_bad_rows(self):
        from src.data_sources.sina_source import SinaSource
        rows = SinaSource.parse_global_futures_daily(
            'var t=([{"date":"","close":"1"},{"date":"20260101","close":null},'
            '{"date":"20260102","close":"100.5"}]);')
        assert [r['trade_date'] for r in rows] == ['20260102']


class TestKlineManagerSinaFuturesBranch:
    def _manager(self, sina_rows, db_latest=None):
        from src.sync.overseas_kline_manager import OverseasKlineManager
        m = OverseasKlineManager.__new__(OverseasKlineManager)

        class _S:
            def fetch_global_futures_daily(self, symbol, start=None, end=None):
                return list(sina_rows)
        m.sina = _S()

        class _DB:
            def fetch_overseas_kline_latest_date(self, code):
                return db_latest
        m.db = _DB()
        return m

    ROWS = [{'trade_date': '20261021', 'close': 13000.0},
            {'trade_date': '20261022', 'close': 13100.0},
            {'trade_date': '20261023', 'close': 13200.0}]

    def test_incremental_filters_after_db_latest(self):
        m = self._manager(self.ROWS, db_latest='20261022')
        last, rows = m._collect_sina_futures('CHA50CFD', None)
        assert [r['trade_date'] for r in rows] == ['20261023']
        assert rows[0]['adj_type'] == 'raw'
        assert last == {'trade_date': '20261023', 'close': 13200.0}

    def test_seed_takes_full_history(self):
        m = self._manager(self.ROWS, db_latest=None)
        last, rows = m._collect_sina_futures('CHA50CFD', None)
        assert len(rows) == 3 and rows[-1]['trade_date'] == '20261023'

    def test_windows_mode_filters_by_range(self):
        m = self._manager(self.ROWS)
        _, rows = m._collect_sina_futures(
            'CHA50CFD', [('2026-10-22', '2026-10-22')])
        assert [r['trade_date'] for r in rows] == ['20261022']


class TestAggregatorA50Preference:
    class _DB:
        def __init__(self, rows):
            self.rows = rows
            self.captured = []

        def _query(self, sql, params=None):
            if 'CHA50CFD' in sql and 'overseas_kline_day' in sql:
                self.captured.append((sql, params))
                return self.rows
            raise AssertionError(f'意外查询: {sql[:60]}')

    def _agg(self, rows):
        from src.review.aggregator import ReviewAggregator
        return ReviewAggregator(self._DB(rows), {'review': {}})

    def test_kline_preferred(self):
        agg = self._agg([('20261023', 13200.0), ('20261022', 13000.0)])
        out = agg._a50_from_kline()
        assert out['name'] == '富时A50期货'
        assert out['change_pct'] == 1.54 and out['close'] == 13200.0

    def test_asof_cutoff_no_lookahead(self):
        """历史重跑防前视（附录 D.2 勘误）：as_of 给定时只取 ≤ as_of 的两根"""
        agg = self._agg([('20260929', 13100.0), ('20260926', 13000.0)])
        out = agg._a50_from_kline('20260929')
        assert out['close'] == 13100.0
        sql, params = agg.db.captured[-1]
        assert 'trade_date <= %s' in sql and params == ('20260929',)
        # as_of 之前无两根 → None（回退快照）
        agg2 = self._agg([('20261023', 13200.0)])
        assert agg2._a50_from_kline('20260929') is None

    def test_missing_rows_fallback_none(self):
        agg = self._agg([('20261023', 13200.0)])
        assert agg._a50_from_kline() is None
        agg2 = self._agg([])
        assert agg2._a50_from_kline() is None

    def test_zero_prev_guarded(self):
        agg = self._agg([('20261023', 13200.0), ('20261022', 0.0)])
        assert agg._a50_from_kline() is None
