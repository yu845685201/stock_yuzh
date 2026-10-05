"""P0-1 is_st 死列修复单测（复盘报告层与 P0 修复技术方案 §1.4）

覆盖：
1. ``derive_is_st``：含/不含 ST 名称的行 dict 派生正确（覆盖既有误传值）；
2. ``check_is_st`` 边界：``*ST`` / ``ST`` / ``S*ST`` / ``SST`` / 正常名 / 空；
3. DAO 两条写路径 SQL：写入列含 is_st、upsert DO UPDATE SET 含 is_st、
   参数元组与列清单逐位对齐（列数 == 参数数）；
4. DAO 派生与存量回填 SQL 口径（``stock_name LIKE '%ST%'``）在现实名称全集上一致。
"""
import pytest

from src.database.dao_kline_day import (
    KLINE_DAY_COLUMNS,
    KlineDayDaoMixin,
    derive_is_st,
)
from src.utils.data_transformer import DataTransformer


class TestDeriveIsSt:
    def test_st_name_flags_true(self):
        rows = [{'stock_name': 'ST康乐', 'is_st': False},
                {'stock_name': '*ST海航'},
                {'stock_name': 'S*ST前锋'},
                {'stock_name': 'SST华新'}]
        derive_is_st(rows)
        assert [r['is_st'] for r in rows] == [True, True, True, True]

    def test_normal_name_flags_false(self):
        rows = [{'stock_name': '平安银行', 'is_st': True},   # 既有误传值被覆盖
                {'stock_name': '贵州茅台'},
                {'stock_name': ''},
                {}]  # 缺 stock_name 键
        derive_is_st(rows)
        assert [r['is_st'] for r in rows] == [False, False, False, False]

    def test_none_name_flags_false(self):
        rows = [{'stock_name': None}]
        derive_is_st(rows)
        assert rows[0]['is_st'] is False


class TestCheckIsStBoundaries:
    @pytest.mark.parametrize('name,expected', [
        ('ST康乐', True), ('*ST海航', True), ('S*ST前锋', True), ('SST华新', True),
        ('平安银行', False), ('ST', True), ('*', False), ('S', False),
        ('', False),
    ])
    def test_boundaries(self, name, expected):
        assert DataTransformer.check_is_st(name) is expected

    def test_none_safe(self):
        assert DataTransformer.check_is_st(None) is False


class _CapturingUpsertDao(KlineDayDaoMixin):
    """捕获 execute_values SQL/参数的假 DAO（不触库）"""

    def __init__(self):
        self.captured = None

    def execute_values(self, sql, params, page_size=1000):
        self.captured = (sql, params)
        return len(params)


class _FakeCursor:
    def __init__(self):
        self.sql = None

    def execute(self, sql, params=None):
        self.sql = sql

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self):
        self.cursor_obj = _FakeCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _CapturingReplaceDao(KlineDayDaoMixin):
    """捕获整股替换 SQL/参数的假 DAO（get_connection 返回假连接）"""

    def __init__(self):
        self.conn = _FakeConn()
        self.captured_sql = None
        self.captured_params = None

    def get_connection(self):
        return self.conn


def _row(name):
    return {'ts_code': 'sz.000001', 'stock_code': '000001', 'stock_name': name,
            'trade_date': '20260929', 'open': 10.0, 'high': 11.0, 'low': 9.9,
            'close': 11.0, 'preclose': 10.0, 'volume': 1000, 'amount': 11000.0,
            'raw_close': 11.0, 'adjust_flag': 2, 'change_rate': 10.0,
            'turnover_rate': 1.0, 'source': 'test'}


class TestWriteSqlContainsIsSt:
    def test_upsert_columns_and_do_update(self, monkeypatch):
        dao = _CapturingUpsertDao()
        rows = [_row('ST康乐'), _row('平安银行')]
        dao.upsert_his_kline_day(rows)
        sql, params = dao.captured

        assert 'is_st' in KLINE_DAY_COLUMNS
        assert 'is_st' in sql
        assert 'is_st = EXCLUDED.is_st' in sql
        # 列清单与参数元组逐位对齐
        for row, param in zip(rows, params):
            assert len(param) == len(KLINE_DAY_COLUMNS)
            idx = KLINE_DAY_COLUMNS.index('is_st')
            assert param[idx] == DataTransformer.check_is_st(row['stock_name'])

    def test_replace_columns(self, monkeypatch):
        dao = _CapturingReplaceDao()
        rows = [_row('ST康乐')]
        import src.database.dao_kline_day as mod
        captured = {}

        def _fake_pg_execute_values(cursor, sql, params, page_size=1000):
            captured['sql'] = sql
            captured['params'] = params

        monkeypatch.setattr(mod, 'pg_execute_values', _fake_pg_execute_values)
        dao.replace_his_kline_day('sz.000001', rows)

        assert 'is_st' in captured['sql']
        param = captured['params'][0]
        assert len(param) == len(KLINE_DAY_COLUMNS)
        assert param[KLINE_DAY_COLUMNS.index('is_st')] is True


class TestBackfillConsistency:
    """DAO 派生（check_is_st，名称前缀判定）与存量回填 SQL（stock_name LIKE '%ST%'，
    名称包含判定）口径一致性：现实 A 股名称全集上二者重合。"""

    @pytest.mark.parametrize('name', [
        'ST康乐', '*ST海航', 'S*ST前锋', 'SST华新',
        '平安银行', '贵州茅台', '宁德时代', '五 粮 液', 'TCL科技',
    ])
    def test_dao_matches_backfill_rule(self, name):
        assert DataTransformer.check_is_st(name) == ('ST' in name)
