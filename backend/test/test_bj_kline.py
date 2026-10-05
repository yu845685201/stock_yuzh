"""P0-2 北交所日K（方案 B：通达信官网盘后包）解析器与 manager 单测

fixture 全部为程序构造的二进制包（cod 150B/条 + md1 512B/块，小端布局见
tdx_official_source 模块文档），覆盖方案 §2.8 验收清单：
正常解析 / 指数行剔除 / 截断 / 条数块数不一致 / 代码重复 / seq 越界 /
非有限值 / 有价行数下限不足 / 缺 bj 文件（2022-05-06 前后） / 非法 zip /
404 / manager 组装（行口径、白名单拒绝、对拍差异、清单补录）。
"""
import io
import struct
import zipfile

import pytest

from src.data_sources.tdx_official_source import (
    BSE_PACKAGE_START,
    COD_RECORD_SIZE,
    MD1_BLOCK_SIZE,
    MARKET_MIN_PRICED_ROWS,
    TdxOfficialSource,
    parse_market,
    parse_package,
)
from src.sync.bj_kline_manager import AUDIT_DIFF_TOLERANCE, BjKlineManager


# ------------------------------------------------------------------ fixture 构造

def _cod_record(code: str, seq: int, name: str) -> bytes:
    assert len(code) == 6 and code.isdigit(), code
    rec = bytearray(COD_RECORD_SIZE)
    rec[0:6] = code.encode('ascii')
    struct.pack_into('<H', rec, 32, seq)
    name_bytes = name.encode('gbk')[:31]
    rec[40:40 + len(name_bytes)] = name_bytes
    return bytes(rec)


def _md1_block(prev_close, open_, high, low, close, volume, amount) -> bytes:
    blk = bytearray(MD1_BLOCK_SIZE)
    struct.pack_into('<d', blk, 4, prev_close)
    struct.pack_into('<4d', blk, 12, open_, high, low, close)
    struct.pack_into('<Q', blk, 56, volume)
    struct.pack_into('<d', blk, 72, amount)
    return bytes(blk)


def _market_files(rows):
    """rows: [(code, name, prev, o, h, l, c, v, a)] → (cod, md1)"""
    cod = b''.join(_cod_record(r[0], i, r[1]) for i, r in enumerate(rows))
    md1 = b''.join(_md1_block(*r[2:]) for r in rows)
    return cod, md1


def _zip_bytes(files: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _minimal_market(market: str, n: int) -> list:
    base = 600000 if market == 'sh' else 100000
    return [(f'{base + i}', f'{market}股{i}', 10.0, 10.1, 10.2,
             9.9, 10.0, 1000, 10000.0) for i in range(n)]


def _full_package(bj_rows=None, ymd='20260929') -> bytes:
    """最小合法包：sh/sz/bj 均满足有价行数下限"""
    sh = _minimal_market('sh', MARKET_MIN_PRICED_ROWS['sh'])
    sz = _minimal_market('sz', MARKET_MIN_PRICED_ROWS['sz'])
    bj = bj_rows if bj_rows is not None else [
        ('920982', '锦波生物', 127.63, 135.01, 137.99, 126.88, 128.66, 2209575, 290472051.35),
        ('920575', '*ST康乐', 4.97, 5.03, 5.08, 4.85, 4.94, 3101032, 15422456.33),
    ]
    bj = list(bj) + [(f'9200{i:02d}', f'填充{i}', 5.0, 5.1, 5.2, 4.9, 5.0, 100, 500.0)
                     for i in range(max(0, MARKET_MIN_PRICED_ROWS['bj'] - len(bj)))]
    files = {}
    for mkt, rows in (('sh', sh), ('sz', sz), ('bj', bj)):
        files[f'{mkt}{ymd[2:]}.cod'], files[f'{mkt}{ymd[2:]}.md1'] = _market_files(rows)
    return _zip_bytes(files)


def _sh_sz_only_files(ymd='20260929') -> dict:
    """只含 sh/sz 的包文件（无 bj）"""
    files = {}
    for mkt, n in (('sh', MARKET_MIN_PRICED_ROWS['sh']), ('sz', MARKET_MIN_PRICED_ROWS['sz'])):
        files[f'{mkt}{ymd[2:]}.cod'], files[f'{mkt}{ymd[2:]}.md1'] = \
            _market_files(_minimal_market(mkt, n))
    return files


# ------------------------------------------------------------------ 解析器

class TestParsePackage:
    def test_normal_parse(self):
        rows = parse_package(_full_package(), '20260929')
        bj = [r for r in rows if r['market'] == 'bj']
        assert len(bj) == MARKET_MIN_PRICED_ROWS['bj']
        jb = next(r for r in bj if r['code'] == '920982')
        assert jb['name'] == '锦波生物'
        assert jb['prev_close'] == pytest.approx(127.63)
        assert jb['close'] == pytest.approx(128.66)
        assert jb['volume'] == 2209575                      # 股
        assert jb['amount'] == pytest.approx(290472051.35)  # 元
        assert sum(1 for r in rows if r['market'] == 'sh') == MARKET_MIN_PRICED_ROWS['sh']
        assert sum(1 for r in rows if r['market'] == 'sz') == MARKET_MIN_PRICED_ROWS['sz']
        assert len(bj) == MARKET_MIN_PRICED_ROWS['bj']

    def test_index_rows_skipped(self):
        # close<=0 为通达信自编板块指数，跳过不计入有价行
        bj = [('920982', '锦波生物', 127.63, 135.01, 137.99, 126.88, 128.66, 1, 1.0),
              ('880001', '板块指数', 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0)]
        rows = parse_market(*_market_files(bj), 'bj', '20260929', min_priced_rows=1)
        assert [r['code'] for r in rows] == ['920982']

    def test_priced_rows_below_floor_raises(self, monkeypatch):
        monkeypatch.setattr('src.data_sources.tdx_official_source.MARKET_MIN_PRICED_ROWS',
                            {'sh': 1, 'sz': 1, 'bj': 100})  # 包内 bj 仅 50 行
        with pytest.raises(RuntimeError, match='低于下限'):
            parse_package(_full_package(), '20260929')

    def test_truncated_cod_raises(self):
        cod, md1 = _market_files([('920982', '锦波生物', 1, 1, 1, 1, 1, 1, 1.0)])
        with pytest.raises(RuntimeError, match='不能整除'):
            parse_market(cod[:-1], md1, 'bj', '20260929')

    def test_count_mismatch_raises(self):
        cod, md1 = _market_files([('920982', '锦波生物', 1, 1, 1, 1, 1, 1, 1.0)])
        with pytest.raises(RuntimeError, match='不一致'):
            parse_market(cod, md1 + b'\x00' * MD1_BLOCK_SIZE, 'bj', '20260929')

    def test_duplicate_code_raises(self):
        rec = _cod_record('920982', 0, 'x')
        with pytest.raises(RuntimeError, match='重复'):
            parse_market(rec + rec, _md1_block(1, 1, 1, 1, 1, 1, 1.0) * 2,
                         'bj', '20260929')

    def test_seq_out_of_range_raises(self):
        rec = _cod_record('920982', 7, 'x')  # md1 只有 1 块
        md1 = _md1_block(1, 1, 1, 1, 1, 1, 1.0)
        with pytest.raises(RuntimeError, match='越界'):
            parse_market(rec, md1, 'bj', '20260929')

    def test_nan_price_raises(self):
        rows = [('920982', '锦波生物', float('nan'), 1, 1, 1, 1, 1, 1.0)]
        with pytest.raises(RuntimeError, match='非有限数'):
            parse_market(*_market_files(rows), 'bj', '20260929')

    def test_missing_bj_file(self):
        # 2022-05-06 起：缺 bj 报错；此前：不报错、仅沪深行
        assert BSE_PACKAGE_START == '20220506'
        with pytest.raises(RuntimeError, match='北交所'):
            parse_package(_zip_bytes(_sh_sz_only_files()), '20260929')
        rows = parse_package(_zip_bytes(_sh_sz_only_files('20220505')), '20220505')
        assert rows and all(r['market'] in ('sh', 'sz') for r in rows)

    def test_invalid_zip_raises(self):
        with pytest.raises(RuntimeError, match='不是合法 zip'):
            parse_package(b'not-a-zip', '20260929')


class TestFetchDailyPackage:
    _SMALL_FLOORS = {'sh': 1, 'sz': 1, 'bj': 1}

    def _make_source(self, monkeypatch):
        monkeypatch.setattr('src.data_sources.tdx_official_source.MARKET_MIN_PRICED_ROWS',
                            dict(self._SMALL_FLOORS))
        return TdxOfficialSource({'review_sync': {'tdx_official_interval': 0}})

    def test_404_raises_value_error(self, monkeypatch):
        src = self._make_source(monkeypatch)

        class FakeResp:
            status_code = 404
            content = b'not found'

        monkeypatch.setattr(src, '_request', lambda method, url, **kw: FakeResp())
        with pytest.raises(ValueError, match='未发布'):
            src.fetch_daily_package('20260925')

    def test_ok_package_parsed(self, monkeypatch):
        src = self._make_source(monkeypatch)

        class FakeResp:
            status_code = 200
            content = _full_package()

        monkeypatch.setattr(src, '_request', lambda method, url, **kw: FakeResp())
        rows = src.fetch_daily_package('20260929')
        assert any(r['code'] == '920982' for r in rows)


# ------------------------------------------------------------------ manager

class FakeDB:
    def __init__(self):
        self.kline_closes = {}
        self.upserted = []
        self.stock_upserts = []

    def fetch_all(self, sql, params=None):
        if 'base_trade_calendar' in sql:
            d = params[0] if params else '2026-09-30'
            return [{'calendar_date': d}]
        if 'his_kline_day' in sql:
            # 对拍查询：ts_code 前缀提供市场维度
            return [{'ts_code': f'sh.{c}', 'stock_code': c, 'raw_close': v}
                    if str(c).startswith(('6',))
                    else {'ts_code': f'sz.{c}', 'stock_code': c, 'raw_close': v}
                    for c, v in self.kline_closes.items()]
        return []

    def fetch_one(self, sql, params=None):
        return None

    def upsert_his_kline_day(self, rows):
        self.upserted.extend(rows)
        return len(rows)

    def execute_values(self, sql, params, page_size=1000):
        self.stock_upserts.extend(params)
        return len(params)


class FakeSource:
    def __init__(self):
        self.package = []
        self.calls = []

    def fetch_daily_package(self, ymd):
        self.calls.append(ymd)
        return self.package


@pytest.fixture
def manager():
    mgr = BjKlineManager.__new__(BjKlineManager)  # 跳过真实 DB/源构造
    mgr.config = {'review_sync': {'tdx_official_interval': 0,
                                  'bj_kline': {'enabled': True}}}
    mgr.enabled = True
    mgr.db = FakeDB()
    mgr.source = FakeSource()
    mgr._load_float_share_map = lambda: {}  # 换手率留空分支
    return mgr


class TestBjKlineManager:
    def _package(self):
        return [
            {'market': 'bj', 'code': '920982', 'name': '锦波生物',
             'prev_close': 127.63, 'open': 135.01, 'high': 137.99,
             'low': 126.88, 'close': 128.66, 'volume': 2209575,
             'amount': 290472051.35},
            {'market': 'sh', 'code': '600519', 'name': '贵州茅台',
             'prev_close': 1230.0, 'open': 1231.0, 'high': 1244.0,
             'low': 1230.0, 'close': 1243.88, 'volume': 2636630,
             'amount': 3260057865.0},
        ]

    def test_build_kline_rows_assembly(self, manager):
        rows = manager._build_kline_rows(self._package(), '20260929')
        assert len(rows) == 1
        r = rows[0]
        assert r['ts_code'] == 'bj.920982'
        assert r['stock_code'] == '920982'
        assert r['stock_name'] == '锦波生物'
        assert r['raw_close'] == r['close'] == 128.66   # 不复权口径
        assert r['preclose'] == 127.63
        assert r['adjust_flag'] == 3
        assert r['turnover_rate'] is None               # 缺股本留空
        assert r['source'] == 'TDX_OFFICIAL'
        expected_cr = round((128.66 - 127.63) / 127.63 * 100, 6)
        assert r['change_rate'] == pytest.approx(expected_cr)

    def test_whitelist_rejection(self, manager):
        for bad_code in ('921001',   # 921 未启用
                         '880001'):  # 未知段（不在已知非股票段白名单内）
            bad = {'market': 'bj', 'code': bad_code, 'name': '越界段',
                   'prev_close': 1, 'open': 1, 'high': 1, 'low': 1,
                   'close': 1, 'volume': 1, 'amount': 1.0}
            with pytest.raises(ValueError, match='白名单'):
                manager._build_kline_rows([bad], '20260929')

    def test_known_non_stock_segments_skipped(self, manager):
        pkg = [dict(self._package()[0])] + [
            {'market': 'bj', 'code': code, 'name': name,
             'prev_close': 1, 'open': 1, 'high': 1, 'low': 1,
             'close': 1, 'volume': 1, 'amount': 1.0}
            for code, name in (('899050', '北证50'), ('821001', '转债X'),
                               ('910001', '北证指数'), ('810011', '其他'))]
        rows = manager._build_kline_rows(pkg, '20260929')
        assert [r['stock_code'] for r in rows] == ['920982']  # 非股票段全部跳过
        assert manager.non_stock_skipped == 4

    def test_audit_cross_check_rules(self, manager):
        manager.db.kline_closes = {'600519': 1243.88, '600000': 9.18}
        pkg = self._package() + [
            {'market': 'sh', 'code': '600000', 'name': '浦发银行',
             'prev_close': 9.2, 'open': 9.2, 'high': 9.2, 'low': 9.1,
             'close': 9.30, 'volume': 1, 'amount': 1.0},   # 差异 >0.01
            {'market': 'sz', 'code': '000002', 'name': '库内缺失',
             'prev_close': 1, 'open': 1, 'high': 1, 'low': 1,
             'close': 1, 'volume': 1, 'amount': 1.0},
            {'market': 'sh', 'code': '999999', 'name': '上证指数',
             'prev_close': 1, 'open': 1, 'high': 1, 'low': 1,
             'close': 3830.45, 'volume': 0, 'amount': 1.0},  # 指数条目不对拍
            {'market': 'sh', 'code': '000001', 'name': '上证指数',
             'prev_close': 1, 'open': 1, 'high': 1, 'low': 1,
             'close': 3830.45, 'volume': 0, 'amount': 1.0},  # sh 指数不撞 sz 同码
        ]
        # 库内 raw_close 为 NULL（原始域缺失）不崩溃，按"库内缺失"形态计入
        manager.db.kline_closes = {'600519': None, '600000': 9.18}
        diffs = manager._audit_cross_check(pkg, '20260929')
        codes = {d['code'] for d in diffs}
        assert codes == {'600000', '000002', '600519'}
        d600000 = next(d for d in diffs if d['code'] == '600000')
        assert d600000['diff'] == pytest.approx(9.30 - 9.18, abs=AUDIT_DIFF_TOLERANCE)

    def test_upsert_stock_list_skips_non_stock(self, manager):
        pkg = self._package() + [
            {'market': 'bj', 'code': '899050', 'name': '北证50',
             'prev_close': 1, 'open': 1, 'high': 1, 'low': 1,
             'close': 1, 'volume': 1, 'amount': 1.0},   # 已知指数段 → 跳过
        ]
        manager._upsert_bj_stock_list(pkg, '20260929')
        assert len(manager.db.stock_upserts) == 1
        assert manager.db.stock_upserts[0][0] == 'bj.920982'

    def test_turnover_calc(self):
        assert BjKlineManager._calc_turnover(1_000_000, 50_000_000) == pytest.approx(2.0)
        assert BjKlineManager._calc_turnover(1_000_000, None) is None
        assert BjKlineManager._calc_turnover(1_000_000, 0) is None
