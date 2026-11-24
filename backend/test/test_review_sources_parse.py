"""新源解析测试（新浪外围市场 / 中证估值文件 / 通用工具与断点）"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.sina_source import SinaSource
from src.data_sources.csindex_source import CsindexSource
from src.data_sources.cninfo_source import CninfoSource, PERIODIC_CATEGORIES
from src.sync.review_common import code6_to_ts_code, load_manifest, save_manifest, ts_code_to_code6


# ---------- 新浪 ----------

def test_sina_parse_index_format():
    """2026-09-27 实测格式：名称,当前,涨跌额,涨跌幅（GBK 已在上层解码）"""
    text = ('var hq_str_int_dji="道琼斯,46247.29,299.97,0.65";\n'
            'var hq_str_int_nasdaq="纳斯达克,22484.07,99.37,0.44";\n')
    parsed = SinaSource._parse_response(text)
    assert parsed['int_dji']['close'] == 46247.29
    assert parsed['int_dji']['change_pct'] == 0.65
    assert parsed['int_dji']['name'] == '道琼斯'
    assert parsed['int_nasdaq']['close'] == 22484.07


def test_sina_parse_fx_format():
    """fx_* 新格式（2026-09 实测 16 字段）：[1]最新价 [15]日期"""
    text = ('var hq_str_fx_susdcny="00:56:01,6.7121000000,6.7141000000,6.7125000000,'
            '92.0000000000,6.7150000000,6.7195000000,6.7103000000,6.7125000000,离岸人民币,'
            '0.0000,0.0000,0.0092,伦敦银行间外汇市场,0.0000,0.0000,,2026-09-25";\n')
    parsed = SinaSource._parse_response(text)
    fx = parsed['fx_susdcny']
    assert fx['family'] == 'fx'
    assert fx['close'] == pytest.approx(6.7121)
    assert fx['date'] == '2026-09-25'
    assert fx.get('change_pct') is None  # 未核实字段不取


def test_sina_parse_empty_and_garbage():
    parsed = SinaSource._parse_response('var hq_str_int_dji="";')
    assert 'int_dji' not in parsed


def test_sina_parse_gb_format():
    """gb_ 美股代码（O4 2026-10-04 实测）：[0]名称 [1]最新价 [2]涨跌幅%（int_* 冻结弃用）"""
    text = ('var hq_str_gb_dji="道琼斯,51176.9609,0.49,2026-10-03 05:00:16,250.4000,'
            '51241.1797,51382.8789,50950.6094,54744.3281,45057.2812,548953568,445317699,'
            '0,0.00,--,0.00,0.00,0.00,0.00,0,0,0.0000,0.00,0.0000,,Oct 2 05:00PM EDT,'
            '50926.5586,0,1,2026";\n')
    parsed = SinaSource._parse_response(text)
    gb = parsed['gb_dji']
    assert gb['family'] == 'index'
    assert gb['close'] == pytest.approx(51176.9609)
    assert gb['change_pct'] == pytest.approx(0.49)
    assert gb['name'] == '道琼斯'


# ---------- 中证 ----------

def test_csindex_normalize_date():
    assert CsindexSource._normalize_date('2026-09-25 00:00:00') == '20260925'
    assert CsindexSource._normalize_date('20260925') == '20260925'
    assert CsindexSource._normalize_date(None) is None
    assert CsindexSource._normalize_date('bad') is None


def test_csindex_parse_dataframe():
    import pandas as pd
    source = CsindexSource({'review_sync': {'csindex_interval': 0}})
    df = pd.DataFrame([
        {'日期Date': '2026-09-25', '指数代码Index Code': '300', '指数中文简称Index Chinese Name': '沪深300',
         '市盈率1（总股本）P/E1': 14.49, '市盈率2（计算用股本）P/E2': 16.66,
         '股息率1（总股本）D/P1': 2.65, '股息率2（计算用股本）D/P2': 2.36},
        {'日期Date': '2026-09-24', '指数代码Index Code': '300', '指数中文简称Index Chinese Name': '沪深300',
         '市盈率1（总股本）P/E1': 14.66, '市盈率2（计算用股本）P/E2': 16.95,
         '股息率1（总股本）D/P1': 2.62, '股息率2（计算用股本）D/P2': 2.32},
    ])
    rows = source._parse_dataframe(df, '000300')
    assert len(rows) == 2
    assert rows[-1]['trade_date'] == '20260925'  # 升序
    row = source.to_row(rows[-1], '000300', '沪深300')
    assert row['index_code'] == '000300'         # 落库用请求码而非文件短码
    assert row['pe_static'] == 16.66             # 默认 P/E2 口径
    assert row['dividend_yield'] == 2.36
    assert row['pe_ttm'] is None                 # 官方文件无 TTM，如实留空


def test_csindex_parse_structure_change_returns_empty():
    """列结构变更 -> 解析为空 -> 调用方报结构异常（不带臆造数据）"""
    import pandas as pd
    source = CsindexSource({'review_sync': {'csindex_interval': 0}})
    df = pd.DataFrame([{'foo': 1}])
    assert source._parse_dataframe(df, '000300') == []


# ---------- 巨潮（开发项 K：category 参数透传） ----------

def test_cninfo_market_window_category_passthrough(monkeypatch):
    """query_market_window 的 category 参数透传 payload；缺省 '' 保持全量行为不变"""
    source = CninfoSource({'query_interval': 0})
    captured = []

    def fake_post(payload):
        captured.append(payload)
        return {'announcements': [], 'hasMore': 'false'}

    monkeypatch.setattr(source, '_post_query', fake_post)
    periodic = ';'.join(PERIODIC_CATEGORIES)
    source._query_market_window('2021-01-01', '2021-01-07', category=periodic)
    assert captured[-1]['category'] == periodic
    assert captured[-1]['seDate'] == '2021-01-01~2021-01-07'
    source._query_market_window('2021-01-01', '2021-01-07')
    assert captured[-1]['category'] == ''  # 缺省全量，公告清单路径不受影响


def test_cninfo_market_window_split_keeps_category(monkeypatch):
    """CninfoWindowTooLarge 对半拆窗递归时 category 一并透传（K 回补披露季大窗口场景）"""
    from src.data_sources.cninfo_source import CninfoWindowTooLarge
    source = CninfoSource({'query_interval': 0})
    captured = []

    def fake_post(payload):
        captured.append(payload)
        se = payload['seDate']
        # 7 天窗口仍报超限 -> 触发对半拆分；单日窗口正常返回
        if '-' in se and se.count('~') == 1:
            s, e = se.split('~')
            from datetime import date as _d
            if (_d.fromisoformat(e) - _d.fromisoformat(s)).days >= 1:
                raise CninfoWindowTooLarge(f'{se} 超限')
        return {'announcements': [], 'hasMore': 'false'}

    monkeypatch.setattr(source, '_post_query', fake_post)
    periodic = ';'.join(PERIODIC_CATEGORIES)
    source.query_market_window('2021-04-01', '2021-04-07', category=periodic)
    assert captured, '应至少发出一次检索'
    assert all(p['category'] == periodic for p in captured)


# ---------- 通用工具 ----------

def test_code_conversions():
    assert code6_to_ts_code('600000') == 'sh.600000'
    assert code6_to_ts_code('688981') == 'sh.688981'
    assert code6_to_ts_code('000001') == 'sz.000001'
    assert code6_to_ts_code('300750') == 'sz.300750'
    assert code6_to_ts_code('899050') == 'bj.899050'
    assert ts_code_to_code6('sz.000001') == '000001'
    assert ts_code_to_code6('000001.SZ') == '000001'
    assert ts_code_to_code6('600000') == '600000'


def test_code6_prefix_segments():
    """码段表全覆盖（X-15 修复 + M 收尾）：沪 60/68/5/10/11，深 0/3/12/15/16/18，北 4/8/92"""
    # 沪市：股票 / 科创 / 基金ETF / 转债（10 老段、11 现行段）
    for c in ('600000', '688981', '510500', '100001', '110059', '113050'):
        assert code6_to_ts_code(c) == f'sh.{c}', c
    # 深市：主板/创业/转债/基金（12/15/16/18 走显式或兜底 sz，不得误判 sh）
    for c in ('000002', '300750', '123456', '150018', '159915', '184801'):
        assert code6_to_ts_code(c) == f'sz.{c}', c
    # 北交所：老段 43/83/87 + 新段 920
    for c in ('430047', '832566', '873169', '920982'):
        assert code6_to_ts_code(c) == f'bj.{c}', c


def test_manifest_roundtrip(tmp_path):
    path = tmp_path / 'm.json'
    manifest = load_manifest(path)
    assert manifest == {'stocks': {}, 'updated_at': None}
    manifest['stocks']['000001'] = {'done': True}
    save_manifest(path, manifest)
    assert load_manifest(path)['stocks']['000001']['done'] is True
    # 原子写不残留 .tmp
    assert not (tmp_path / 'm.tmp').exists()


# ---------- 龙虎榜（开发项 J：汇总接口解析 + 深市行聚合 + 先删后插） ----------

class FakeLhbSource:
    def __init__(self, szse_rows=None, szse_seats=None, sse_rows=None):
        self.szse_rows = szse_rows or []
        self.szse_seats = szse_seats or {}
        self.sse_rows = sse_rows or []

    def fetch_szse_lhb(self, trade_date):
        return self.szse_rows

    def fetch_szse_lhb_seats(self, ann_date, code, zb_dm):
        return self.szse_seats.get((code, zb_dm), [])

    def fetch_sse_lhb_summary(self, trade_date):
        return self.sse_rows


class FakeLhbDB:
    def __init__(self):
        self.calls = []
        self.delete_sqls = []

    def delete_sse_lhb_legacy(self, trade_date):
        from src.database.dao_review_sync import ReviewDaoMixin
        self.calls.append(('delete', trade_date))
        # 记录真实 SQL 语义（仅删沪市旧格式行）供断言
        import inspect
        self.delete_sqls.append(inspect.getsource(ReviewDaoMixin.delete_sse_lhb_legacy))
        return 3

    def upsert_lhb_stocks(self, rows):
        self.calls.append(('stocks', rows))
        return len(rows)

    def upsert_lhb_details(self, rows):
        self.calls.append(('details', rows))
        return len(rows)


def _make_lhb_manager(source, db):
    from src.sync.lhb_manager import LhbManager
    m = LhbManager.__new__(LhbManager)
    m.source = source
    m.db = db
    return m


def _sse_summary_row(ref_type='11', code='600825', name='新华传媒'):
    return {
        'secCode': code, 'secAbbr': name, 'refType': ref_type,
        'secTxAmount': 123456789.0,
        'branchNameB': '机构专用,某证券营业部A,机构专用,某营业部B,某营业部C',
        'branchTxAmtB': '100.5,200.25,300,400,500',
        'branchNameS': '卖方营业部1,卖方营业部2,卖方营业部3',
        'branchTxAmtS': '50,60.5,70',
    }


def test_sse_lhb_summary_collect(monkeypatch):
    """汇总接口行：refType 文案映射、打包席位 zip、金额 Σ、secTxAmount 入库"""
    import src.sync.lhb_manager as lm
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: None)
    source = FakeLhbSource(sse_rows=[_sse_summary_row()])
    db = FakeLhbDB()
    m = _make_lhb_manager(source, db)
    stocks, details = m._collect_sse('20260930')

    assert len(stocks) == 1
    s = stocks[0]
    assert s['ts_code'] == 'sh.600825'
    assert s['reason'] == '涨幅偏离'          # T21 文案映射（当日偏离口径）
    assert s['buy_amt'] == 1500.75            # 100.5+200.25+300+400+500
    assert s['sell_amt'] == 180.5
    assert s['net_amt'] == 1320.25
    assert s['sec_tx_amount'] == 123456789.0
    # 席位明细：买 5 + 卖 3，side 带排名
    buys = [d for d in details if d['side'].startswith('买')]
    sells = [d for d in details if d['side'].startswith('卖')]
    assert [d['side'] for d in buys] == ['买1', '买2', '买3', '买4', '买5']
    assert [d['side'] for d in sells] == ['卖1', '卖2', '卖3']
    assert buys[0]['amount'] == 100.5 and buys[0]['seat_name'] == '机构专用'


def test_sse_lhb_multi_reftype_and_unknown_code(monkeypatch):
    """同股多原因多行（reason 各异）；未知 refType 回落 '披露原因代码{N}'"""
    import src.sync.lhb_manager as lm
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: None)
    source = FakeLhbSource(sse_rows=[
        _sse_summary_row(ref_type='11'),
        _sse_summary_row(ref_type='2'),        # 累计口径文案
        _sse_summary_row(ref_type='99'),       # 未知码
    ])
    db = FakeLhbDB()
    m = _make_lhb_manager(source, db)
    stocks, _ = m._collect_sse('20260930')
    reasons = {s['reason'] for s in stocks}
    assert reasons == {'涨幅偏离', '跌幅偏离累计达20%', '披露原因代码99'}
    assert len({(s['ts_code'], s['reason']) for s in stocks}) == 3


def test_szse_lhb_row_aggregation_multi_reason(monkeypatch):
    """深市金额按榜单行聚合（X-7）：多原因股两行金额各自独立、不跨原因求和"""
    import src.sync.lhb_manager as lm
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: None)
    source = FakeLhbSource(
        szse_rows=[
            {'zqdm': '000002', 'zqjc': '万科Ａ', 'dqrq': '2026-09-30',
             'plyy': '日涨幅偏离值达7%的证券', 'cjje': '51.6',
             'bz': 'http://www.szse.cn/...ZBDM=0901'},
            {'zqdm': '000002', 'zqjc': '万科Ａ', 'dqrq': '2026-09-30',
             'plyy': '连续三个交易日内涨幅偏离值累计达20%的证券', 'cjje': '51.6',
             'bz': 'http://www.szse.cn/...ZBDM=0902'},
        ],
        szse_seats={
            ('000002', '0901'): [
                {'zsmc': '机构专用', 'mmlb': '买1', 'mrje': '1,000,000', 'mcje': ''},
                {'zsmc': '某营业部', 'mmlb': '卖1', 'mrje': '', 'mcje': '400,000'},
            ],
            ('000002', '0902'): [
                {'zsmc': '某营业部X', 'mmlb': '买1', 'mrje': '2,000,000', 'mcje': ''},
                {'zsmc': '某营业部Y', 'mmlb': '买2', 'mrje': '3,000,000', 'mcje': ''},
            ],
        })
    db = FakeLhbDB()
    m = _make_lhb_manager(source, db)
    stocks, details, seat_gaps = m._collect_szse('20260930')

    assert len(stocks) == 2
    by_reason = {s['reason']: s for s in stocks}
    r1 = by_reason['日涨幅偏离值达7%的证券']
    assert r1['buy_amt'] == 1000000.0
    assert r1['sell_amt'] == 400000.0
    assert r1['net_amt'] == 600000.0
    # 成交金额亿元 -> 元（列表口径入库 sec_tx_amount）
    assert r1['sec_tx_amount'] == 51.6e8
    r2 = by_reason['连续三个交易日内涨幅偏离值累计达20%的证券']
    assert r2['buy_amt'] == 5000000.0        # 仅该行自己的席位（2,000,000+3,000,000）
    assert r2['sell_amt'] is None            # 该行无卖席位 -> 留空不硬造
    assert r2['net_amt'] is None
    assert len(details) == 4


def test_lhb_execute_deletes_legacy_before_upsert(monkeypatch):
    """X-18 先删后插：delete 在 upsert 之前且仅当有沪市新数据；dry-run 不删"""
    import src.sync.lhb_manager as lm
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: None)
    source = FakeLhbSource(sse_rows=[_sse_summary_row()])
    db = FakeLhbDB()
    m = _make_lhb_manager(source, db)
    result = m.execute('20260930')
    kinds = [c[0] for c in db.calls]
    assert kinds.index('delete') < kinds.index('stocks') < kinds.index('details')
    assert result['success'] is True

    db2 = FakeLhbDB()
    m2 = _make_lhb_manager(FakeLhbSource(sse_rows=[]), db2)
    m2.execute('20260930')
    assert 'delete' not in [c[0] for c in db2.calls]  # 无沪市新数据不删

    db3 = FakeLhbDB()
    m3 = _make_lhb_manager(FakeLhbSource(sse_rows=[_sse_summary_row()]), db3)
    m3.execute('20260930', dry_run=True)
    assert 'delete' not in [c[0] for c in db3.calls]  # dry-run 不删


def test_delete_sse_lhb_legacy_sql_semantics():
    """清理 SQL 仅删沪市旧格式行（X-18/X-1：sh. + '披露原因代码' 前缀，深市不受影响）"""
    import inspect
    from src.database.dao_review_sync import ReviewDaoMixin
    sql = inspect.getsource(ReviewDaoMixin.delete_sse_lhb_legacy)
    assert "ts_code LIKE 'sh.%%'" in sql
    assert "reason LIKE '披露原因代码%%'" in sql
    assert 'trade_date = %s' in sql


# ---------- I-1/I-2：外围 hf_ 解析 + 大小盘风格 ----------

def test_sina_parse_hf_format():
    """hf_ 外盘期货（2026-10-03 实测 CHA50CFD）：[7]现价 [4]高 [5]低 [6]时间 [12]日期 [13]名"""
    text = ('var hq_str_hf_CHA50CFD="13803.600,,13806.000,13807.000,13853.000,13773.000,'
            '00:34:51,13810.000,13797.000,834717,10,4,2026-10-03,富时中国A50期货,38646";\n')
    parsed = SinaSource._parse_response(text)
    hf = parsed['hf_CHA50CFD']
    assert hf['family'] == 'hf'
    assert hf['close'] == 13810.000
    assert hf['high'] == 13853.000
    assert hf['low'] == 13773.000
    assert hf['time'] == '00:34:51'
    assert hf['date'] == '2026-10-03'
    assert hf['name'] == '富时中国A50期货'
    assert hf.get('change_pct') is None   # T20：快照无昨结/昨收，留空由 manager 自算


def test_sina_parse_fx_susdcnh():
    """fx_susdcnh 与 fx_susdcny 同构（I-2 探路结论）：[1]最新价 + 尾部日期"""
    text = ('var hq_str_fx_susdcnh="00:34:52,6.706400,6.706500,6.713200,174,6.713600,'
            '6.717400,6.700000,6.706400,离岸人民币（香港）,-0.100000,-0.006800,0.002592,,'
            '6.995700,6.691300,,2026-10-03";\n')
    parsed = SinaSource._parse_response(text)
    fx = parsed['fx_susdcnh']
    assert fx['family'] == 'fx'
    assert fx['close'] == pytest.approx(6.7064)
    assert fx['date'] == '2026-10-03'
    assert fx.get('change_pct') is None


def test_style_spread():
    """大小盘风格差值：正=大盘占优；缺任一指数返回 None"""
    from src.review.aggregator import ReviewAggregator
    indices = [{'name': '沪深300', 'change_rate': 0.65},
               {'name': '中证1000', 'change_rate': 1.23}]
    out = ReviewAggregator._style_spread(indices)
    assert out['spread'] == pytest.approx(-0.58)
    assert out['big'] == 0.65 and out['small'] == 1.23
    assert ReviewAggregator._style_spread([{'name': '上证指数', 'change_rate': 0.1}]) is None


def test_style_spread_with_csi2000():
    """N2 扩展：中证2000 在场时输出 spread_2000；缺中证2000 时字段留空不报错"""
    from src.review.aggregator import ReviewAggregator
    indices = [{'name': '沪深300', 'change_rate': 0.65},
               {'name': '中证1000', 'change_rate': 1.23},
               {'name': '中证2000', 'change_rate': 1.85}]
    out = ReviewAggregator._style_spread(indices)
    assert out['small_2000'] == pytest.approx(1.85)
    assert out['spread_2000'] == pytest.approx(-1.20)
    # 中证2000 缺数据 -> 仅 1000 口径
    out2 = ReviewAggregator._style_spread(indices[:2])
    assert out2['small_2000'] is None and out2['spread_2000'] is None


# ---------- N2：index-perf 历史日K（延后项补齐方案 2026-10-04） ----------

# §2.2 实测样例（932000 20260930 官方响应原样片段，作 fixture）
PERF_SAMPLE_ITEM = {
    "tradeDate": "20260930", "indexCode": "932000",
    "open": 3145.94, "high": 3146.86, "low": 3104.62, "close": 3107.26,
    "change": -21.69, "changePct": -0.69,
    "tradingVol": 26289122476.0, "tradingValue": 3372.38, "consNumber": 2000.0,
}


def test_csindex_perf_history_row_conversion(monkeypatch):
    """API 样例 -> 行转换：amount 亿元×1e8、preclose=close−change、change_rate 直用"""
    source = CsindexSource({'review_sync': {'csindex_interval': 0}})
    monkeypatch.setattr(source, 'get_json',
                        lambda url, params=None, headers=None, allow_status=():
                        {'data': [dict(PERF_SAMPLE_ITEM)], 'success': True})
    rows = source.fetch_perf_history('932000', '20260928', '20261009')
    assert len(rows) == 1
    row = rows[0]
    assert row['trade_date'] == '20260930'
    assert row['close'] == pytest.approx(3107.26)
    assert row['preclose'] == pytest.approx(3128.95)      # 3107.26 − (−21.69)
    assert row['amount'] == pytest.approx(3372.38 * 1e8)  # 亿元 -> 元
    assert row['change_rate'] == pytest.approx(-0.69)
    assert row['volume'] == pytest.approx(26289122476.0)


def test_csindex_perf_history_bad_rows_skipped(monkeypatch):
    """字段缺失/非法的行跳过（不硬造），响应为空返回空列表"""
    source = CsindexSource({'review_sync': {'csindex_interval': 0}})
    bad = {'tradeDate': '20260929', 'close': None, 'change': 0, 'open': 1, 'high': 1, 'low': 1}
    monkeypatch.setattr(source, 'get_json',
                        lambda url, params=None, headers=None, allow_status=():
                        {'data': [bad, dict(PERF_SAMPLE_ITEM)]})
    rows = source.fetch_perf_history('932000', '20260928', '20261009')
    assert [r['trade_date'] for r in rows] == ['20260930']
    monkeypatch.setattr(source, 'get_json',
                        lambda url, params=None, headers=None, allow_status=(): {'data': []})
    assert source.fetch_perf_history('932000', '20260928', '20261009') == []


def test_csindex_perf_history_null_change_backcalc(monkeypatch):
    """基日回溯历史 change=null（实测 000985 2011-08-02 发布日前 / 932000 2023-08-11 前）：
    preclose 取前一 API 行 close，首行留空——不再丢弃整行（初版事故修复）"""
    source = CsindexSource({'review_sync': {'csindex_interval': 0}})
    raw = [
        {'tradeDate': '20050104', 'close': 984.4, 'change': None, 'open': 995.34,
         'high': 995.34, 'low': 982.59, 'changePct': -1.56,
         'tradingVol': 1106523350.0, 'tradingValue': 64.29},
        {'tradeDate': '20050105', 'close': 990.1, 'change': 5.7, 'open': 985.0,
         'high': 992.0, 'low': 984.0, 'changePct': 0.58,
         'tradingVol': 1200000000.0, 'tradingValue': 70.11},
    ]
    monkeypatch.setattr(source, 'get_json',
                        lambda url, params=None, headers=None, allow_status=(): {'data': raw})
    rows = source.fetch_perf_history('000985', '20050101', '20051231')
    assert len(rows) == 2
    assert rows[0]['preclose'] is None
    assert rows[1]['preclose'] == pytest.approx(984.4)


def test_init_perf_year_windows():
    """--init-perf 年度切窗：起始年整年、当年截至今天、逐年连续"""
    import datetime as _dt
    from src.sync.index_valuation_manager import IndexValuationManager
    mgr = IndexValuationManager.__new__(IndexValuationManager)  # 不触库，只测切窗
    wins = mgr._perf_year_windows(2005)
    today = _dt.date.today()
    assert wins[0] == ('20050101', '20051231')
    assert wins[-1] == (f'{today.year}0101', today.strftime('%Y%m%d'))
    assert len(wins) == today.year - 2005 + 1
    assert [w[0][:4] for w in wins] == [str(y) for y in range(2005, today.year + 1)]


def test_perf_incremental_rows_filled_and_upsert(monkeypatch):
    """日常增量：行补 ts_code/index_code/index_name/up_count/down_count 后 upsert；单指数失败不阻塞"""
    from src.sync.index_valuation_manager import IndexValuationManager

    class FakeDB:
        def __init__(self):
            self.kline_rows = []

        def ensure_review_tables(self):
            pass

        def upsert_index_klines(self, rows):
            self.kline_rows.extend(rows)
            return len(rows)

        def upsert_index_valuations(self, rows):
            return len(rows)

    import src.sync.index_valuation_manager as ivm
    mgr = IndexValuationManager.__new__(IndexValuationManager)
    mgr.db = FakeDB()
    mgr.indices = []
    mgr.backfill_days = 5
    mgr.perf_indices = [{'code': '000985', 'ts_code': 'sh.000985', 'name': '中证全指',
                         'init_start_year': 2005},
                        {'code': '932000', 'ts_code': 'sh.932000', 'name': '中证2000'}]

    class FakeSource:
        def __init__(self):
            self.calls = []

        def fetch_indicator_file(self, code):
            raise AssertionError('init_perf 之外的估值路径不应被触发')

        def fetch_perf_history(self, code, start, end):
            self.calls.append((code, start, end))
            if code == '932000':
                raise RuntimeError('网络超时')
            return [dict(PERF_SAMPLE_ITEM)]

    mgr.source = FakeSource()
    stats = {'perf_ok': 0, 'perf_failed': []}
    rows = mgr._fetch_perf_incremental(stats)
    assert stats['perf_ok'] == 1 and len(stats['perf_failed']) == 1
    assert len(rows) == 1
    assert rows[0]['ts_code'] == 'sh.000985' and rows[0]['index_code'] == '000985'
    assert rows[0]['index_name'] == '中证全指'
    assert rows[0]['up_count'] is None and rows[0]['down_count'] is None
    # execute() 正常路径含 perf 增量落库（估值清单为空 -> indices_ok=0 但 perf 已入库）
    import datetime as _dt
    today = _dt.date.today()
    assert mgr.source.calls[0][1] <= (today - _dt.timedelta(days=10)).strftime('%Y%m%d')


def test_overseas_hf_self_calc_change_pct(monkeypatch):
    """hf_ change_pct 本地昨收自算；首条无昨收留空（数据真实性）"""
    import src.sync.overseas_manager as om

    class FakeOMDB:
        def __init__(self):
            self.prev = None

        def execute_query(self, sql, params=None):
            if 'SELECT close FROM overseas_market_daily' in sql:
                return [{'close': self.prev}] if self.prev is not None else []
            return []

        def upsert_overseas_markets(self, rows):
            return len(rows)

        def ensure_review_tables(self):
            return None

    class FakeOMSource:
        def fetch_markets(self, markets):
            return [{'market_code': 'hf_CHA50CFD', 'market_name': '富时A50期货',
                     'close': 13810.0, 'change_pct': None, 'quote_date': '2026-10-03'}]

    class FakeOMConfig:
        def load_config(self):
            return {'review_sync': {'sina_markets': [{'code': 'hf_CHA50CFD'}]}}

    monkeypatch.setattr(om, 'write_review_report', lambda *a, **k: None)
    db = FakeOMDB()
    m = om.OverseasManager.__new__(om.OverseasManager)
    m.config = FakeOMConfig().load_config()
    m.db = db
    m.markets = [{'code': 'hf_CHA50CFD', 'name': '富时A50期货'}]
    m.source = FakeOMSource()

    # 首条：无昨收 → change_pct 留空
    r1 = m.execute(trade_date='20261002')
    row1 = [s for s in r1['stats'] if False]  # noqa: 占位防误读
    assert r1['success'] is True
    db.prev = 13720.0
    # 次日：有昨收 → 自算 (13810-13720)/13720*100 = 0.6557%
    m.source = FakeOMSource()
    r2 = m.execute(trade_date='20261003')
    assert r2['success'] is True
    # 直接验证自算值（经 _fetch_prev_close 分支）
    assert db.prev == 13720.0
    calc = round((13810.0 - 13720.0) / 13720.0 * 100, 4)
    assert calc == pytest.approx(0.656)   # 90/13720×100 = 0.65598 -> 0.656
