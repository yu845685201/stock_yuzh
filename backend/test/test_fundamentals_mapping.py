"""基本面采集映射单测：pubDate/statDate 提取、缺失留白、透传"""
import pytest

from src.data_sources.baostock_source import BaostockSource

FIELDS = ['code', 'pubDate', 'statDate', 'roeAvg', 'npMargin', 'gpMargin',
          'netProfit', 'epsTTM', 'MBRevenue', 'totalShare', 'liqaShare']


class FakeRS:
    def __init__(self, fields, rows, error_code='0'):
        self.fields = fields
        self._rows = rows
        self.error_code = error_code
        self.error_msg = ''
        self._i = 0

    def next(self):
        if self._i < len(self._rows):
            self._i += 1
            return True
        return False

    def get_row_data(self):
        return self._rows[self._i - 1]


@pytest.fixture
def source():
    src = BaostockSource({'data_path': '/tmp', 'financial_data_rate_limit': {'enabled': False}})
    src._connected = True
    return src


def _patch_query(monkeypatch, src, rs):
    monkeypatch.setattr(src, '_execute_query_with_retry', lambda fn, ctx: rs)


def test_pubdate_statdate_mapping(source, monkeypatch):
    """pubDate/statDate 正确提取并归一化为 yyyyMMdd"""
    rs = FakeRS(FIELDS, [[
        'sz.000001', '2025-10-25', '2025-09-30', '0.075', '0.38', '',
        '38339000000.000', '2.22', '', '19405918198.00', '19405600653.00']])
    _patch_query(monkeypatch, source, rs)

    data = source.get_financial_data('sz.000001', 2025, 3)
    assert data is not None
    assert data['disclosure_date'] == '20251025'
    assert data['stat_date'] == '20250930'
    assert data['total_share'] == pytest.approx(19405918198.0)
    assert data['float_share'] == pytest.approx(19405600653.0)


def test_pubdate_missing_keeps_statdate(source, monkeypatch):
    """pubDate 缺失时 disclosure_date 留空，stat_date 照存（不回填推算值）"""
    row = ['sz.000001', '', '2025-09-30', '0.075', '0.38', '',
           '38339000000.000', '2.22', '', '19405918198.00', '19405600653.00']
    _patch_query(monkeypatch, source, FakeRS(FIELDS, [row]))

    data = source.get_financial_data('sz.000001', 2025, 3)
    assert data is not None
    assert data['disclosure_date'] is None
    assert data['stat_date'] == '20250930'


def test_statdate_missing_skips_row(source, monkeypatch):
    """statDate（自然键）缺失时跳过该条"""
    row = ['sz.000001', '2025-10-25', '', '0.075', '0.38', '',
           '38339000000.000', '2.22', '', '19405918198.00', '19405600653.00']
    _patch_query(monkeypatch, source, FakeRS(FIELDS, [row]))

    assert source.get_financial_data('sz.000001', 2025, 3) is None


def test_empty_result_returns_none(source, monkeypatch):
    _patch_query(monkeypatch, source, FakeRS(FIELDS, []))
    assert source.get_financial_data('sz.000001', 2025, 3) is None


def test_get_stock_fundamentals_passthrough(source, monkeypatch):
    """get_stock_fundamentals 透传 stat_date/disclosure_date 并补 ts_code"""
    rs = FakeRS(FIELDS, [[
        'sz.000001', '2025-10-25', '2025-09-30', '0.075', '0.38', '',
        '38339000000.000', '2.22', '', '19405918198.00', '19405600653.00']])
    _patch_query(monkeypatch, source, rs)

    data = source.get_stock_fundamentals('sz.000001', year=2025, quarter=3)
    assert data is not None
    assert data['ts_code'] == 'sz.000001'
    assert data['stock_code'] == '000001'
    assert data['stat_date'] == '20250930'
    assert data['disclosure_date'] == '20251025'
    assert data['data_source'] == 'baostock'


def test_disconnect_returns_none(source):
    source._connected = False
    assert source.get_financial_data('sz.000001', 2025, 3) is None
