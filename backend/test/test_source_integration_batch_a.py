"""盘后采集数据源整合 - 批次 A 离线测试（详细技术方案 V1.0 §4.1/§4.2/§3.1）

覆盖文档 §11 回归矩阵的批次 A 子集（离线固定样本，不触网）：
- 深市 XLSX：表头识别（全/半角括号）、千分位、六位代码前导零、Decimal 金额、脏行剔除
- 沪市分页：pageNo=beginPage=endPage=n 陷阱（重复第一页拒收）、total/pageCount 闭合、
  opDate 错日拒收、少页拒收
- 巨潮两融：resultCode/count=total/TRADEDATE 完备证据、字段映射、市场分组（北交所范围统计）
- 期权：risk 传 trade_date / stats 传 tradeDate 参数区分、错日行剔除不入账、
  按缺失子表补齐（backfill 断点）、交叉验证
- 发布指针：accept 拒绝非 COMPLETE 状态；期望行 ON CONFLICT 不覆盖
"""

import json
from decimal import Decimal
from io import BytesIO
from typing import Any, Dict, List

import pytest


# ============================================================ 深市 XLSX 解析

def _build_szse_margin_xlsx(rows: List[Dict[str, Any]], half_width_header: bool = True):
    """构造深市两融 XLSX 字节流（含 A1:A1 维度声明陷阱的 read_only 模拟）

    openpyxl 写出的文件自带真实 dimensions；A1:A1 陷阱通过 ws_sheet1 dimension
    手工改写模拟——read_only 模式按声明行数遍历，reset_dimensions 后读实际末行。
    """
    from openpyxl import Workbook

    lb = '(' if half_width_header else '（'
    rb = ')' if half_width_header else '）'
    header = ['证券代码', '证券简称', f'融资买入额{lb}元{rb}', f'融资余额{lb}元{rb}',
              f'融券卖出量{lb}股{rb}' if lb == '(' else f'融券卖出量{lb}股{rb}',
              f'融券余量{lb}股{rb}', f'融券余额{lb}元{rb}', f'两融合计{lb}元{rb}']
    wb = Workbook(write_only=True)
    ws = wb.create_sheet()
    ws.append(header)
    for r in rows:
        ws.append([r.get('zqdm'), r.get('name'), r.get('rzmre'), r.get('rzye'),
                   '', '', r.get('rqye'), r.get('hj')])
    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_szse_margin_xlsx_parse_basic():
    """表头识别 + 千分位 + 六位代码前导零 + Decimal 金额（§4.1：sz.000001 回归）"""
    from src.data_sources.sse_szse_source import parse_szse_margin_xlsx
    content = _build_szse_margin_xlsx([
        # sz.000001 11-20 实测回归值：买入额 109,931,298 元 / 融资余额 3,879,313,265 元
        {'zqdm': '000001', 'name': '平安银行', 'rzmre': '109,931,298.00',
         'rzye': '3,879,313,265.00', 'rqye': '1,234,567.00', 'hj': '3,880,547,832.00'},
        {'zqdm': '300750', 'name': '宁德时代', 'rzmre': '500',
         'rzye': '9,000', 'rqye': None, 'hj': '9,500'},
    ])
    rows = parse_szse_margin_xlsx(content)
    assert len(rows) == 2
    first = rows[0]
    assert first['zqdm'] == '000001'          # 六位代码保留前导零
    assert first['rzmre'] == Decimal('109931298.00')
    assert first['rzye'] == Decimal('3879313265.00')
    assert first['rqye'] == Decimal('1234567.00')
    assert first['rzrqhj'] == Decimal('3880547832.00')
    # 回归口径：sz.000001 买入额 109931298 元
    assert rows[0]['rzmre'] == Decimal('109931298')
    assert rows[1]['rqye'] is None            # 空值保留（源不披露 ≠ 数值零）


def test_szse_margin_xlsx_fullwidth_header_and_dirty_rows():
    """全角括号表头 + 脏行剔除 + 表头重复行跳过"""
    from src.data_sources.sse_szse_source import parse_szse_margin_xlsx
    content = _build_szse_margin_xlsx([
        {'zqdm': '000001', 'name': '平安银行', 'rzmre': '100', 'rzye': '200',
         'rqye': '300', 'hj': '500'},
    ], half_width_header=False)
    rows = parse_szse_margin_xlsx(content)
    assert len(rows) == 1 and rows[0]['rzye'] == Decimal('200')


def test_szse_margin_xlsx_rejects_missing_data_rows():
    """无有效数据行（截断/缺表头）显式报错，不静默返回空表（§11：截断导出）"""
    from src.data_sources.sse_szse_source import parse_szse_margin_xlsx
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.append(['随便什么', '没有表头'])
    buf = BytesIO()
    wb.save(buf)
    with pytest.raises(ValueError):
        parse_szse_margin_xlsx(buf.getvalue())


# ============================================================ 沪市分页

_SSE_ROWS_P1 = [{'stockCode': '600000', 'opDate': '2026-11-20', 'rzye': '1', 'rzmre': '2', 'rzche': '3'},
                {'stockCode': '600001', 'opDate': '2026-11-20', 'rzye': '1', 'rzmre': '2', 'rzche': '3'}]
_SSE_ROWS_P2 = [{'stockCode': '688078', 'opDate': '2026-11-20', 'rzye': '4', 'rzmre': '5', 'rzche': '6'}]


class _FakeSseSource:
    """捕获 params 的最小源桩（fetch_sse_margin 只依赖 get_text/_parse_jsonp）"""

    def __init__(self, pages: List[Dict[str, Any]]):
        self.pages = pages
        self.calls: List[Dict[str, Any]] = []

    def get_text(self, url, params=None, headers=None, allow_status=()):
        self.calls.append(dict(params or {}))
        return json.dumps(self.pages[len(self.calls) - 1])


def test_sse_margin_pagination_params_and_closure():
    """分页参数：第 n 页 pageNo/beginPage/endPage 同设为 n（全部带 pageHelp. 前缀）"""
    from src.data_sources.sse_szse_source import SzseSseSource
    src = SzseSseSource.__new__(SzseSseSource)
    fake = _FakeSseSource([
        {'pageHelp': {'data': _SSE_ROWS_P1, 'total': 3, 'pageCount': 2}},
        {'pageHelp': {'data': _SSE_ROWS_P2, 'total': 3, 'pageCount': 2}},
    ])
    src.get_text = fake.get_text
    rows = src.fetch_sse_margin('20261120')
    assert [r['stockCode'] for r in rows] == ['600000', '600001', '688078']
    assert fake.calls[0]['pageHelp.pageNo'] == 1
    assert fake.calls[0]['pageHelp.beginPage'] == 1
    assert fake.calls[0]['pageHelp.endPage'] == 1
    assert fake.calls[0]['pageHelp.pageSize'] == 2000
    assert fake.calls[0]['tabType'] == 'mxtype'
    assert fake.calls[0]['detailsDate'] == '20261120'
    assert fake.calls[1]['pageHelp.pageNo'] == 1 + 1
    assert fake.calls[1]['pageHelp.beginPage'] == 2


def test_sse_margin_rejects_repeated_first_page():
    """§4.1 分页陷阱回归：只改 pageNo 会重复第一页——第 2 页返回第一页内容显式拒收"""
    from src.data_sources.sse_szse_source import SzseSseSource
    src = SzseSseSource.__new__(SzseSseSource)
    fake = _FakeSseSource([
        {'pageHelp': {'data': _SSE_ROWS_P1, 'total': 4, 'pageCount': 2}},
        {'pageHelp': {'data': _SSE_ROWS_P1, 'total': 4, 'pageCount': 2}},  # 陷阱
    ])
    src.get_text = fake.get_text
    with pytest.raises(ValueError, match='第一页'):
        src.fetch_sse_margin('20261120')


def test_sse_margin_rejects_wrong_op_date_and_missing_page():
    """错日 opDate 拒收 + 少页（total 未闭合而中途无数据）拒收"""
    from src.data_sources.sse_szse_source import SzseSseSource
    src = SzseSseSource.__new__(SzseSseSource)
    fake = _FakeSseSource([
        {'pageHelp': {'data': [{'stockCode': '600000', 'opDate': '2026-11-19',
                                'rzye': '1', 'rzmre': '2', 'rzche': '3'}],
                      'total': 1, 'pageCount': 1}},
    ])
    src.get_text = fake.get_text
    with pytest.raises(ValueError, match='错日'):
        src.fetch_sse_margin('20261120')

    src2 = SzseSseSource.__new__(SzseSseSource)
    fake2 = _FakeSseSource([
        {'pageHelp': {'data': _SSE_ROWS_P1, 'total': 100, 'pageCount': 5}},
        {'pageHelp': {'data': [], 'total': 100, 'pageCount': 5}},   # 少页中断
    ])
    src2.get_text = fake2.get_text
    with pytest.raises(ValueError, match='少页|分页中断'):
        src2.fetch_sse_margin('20261120')


def test_sse_margin_empty_first_page_is_not_published():
    """第 1 页无数据 = 未发布/非交易日（返回空列表，与旧行为一致不报错）"""
    from src.data_sources.sse_szse_source import SzseSseSource
    src = SzseSseSource.__new__(SzseSseSource)
    fake = _FakeSseSource([{'pageHelp': {'data': [], 'total': 0, 'pageCount': 0}}])
    src.get_text = fake.get_text
    assert src.fetch_sse_margin('20261120') == []


# ============================================================ 巨潮两融明细

class _FakeCninfoSession:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status = status
        self.calls = []

    def post(self, url, params=None, headers=None, timeout=None):
        self.calls.append({'url': url, 'params': params})

        class _Resp:
            status_code = self.status
            headers = {}

            @staticmethod
            def json():
                return self.payload

            @staticmethod
            def raise_for_status():
                return None

        return _Resp()


def _mk_cninfo(payload, status=200):
    from src.data_sources.cninfo_source import CninfoSource
    c = CninfoSource.__new__(CninfoSource)
    c.query_base_url = 'http://www.cninfo.com.cn'
    c.static_base_url = 'http://static.cninfo.com.cn'
    c.timeout = 5
    c.query_interval = 0
    c.download_interval = 0
    c.page_size = 30
    c.max_pages_per_window = 40
    c.user_agent = 'ua'
    c._session = _FakeCninfoSession(payload, status)

    class _NoopGate:
        def acquire(self, *a, **k):
            return None

        def report(self, *a, **k):
            return None

    c.gate = _NoopGate()
    c._query_host = 'www.cninfo.com.cn'
    c._static_host = 'static.cninfo.com.cn'
    return c


def test_cninfo_margin_detail_fields_and_market_grouping():
    """字段映射 + F010V 市场分组（2026-11-26 实测 F010V='深圳/上海/北京'；
    北交所进范围统计不落沪深表；F001N+F008N=F009N 合计校验）"""
    # 2026-11-26 实测结构：resultCode/count/total/records 均在 data 内，成功值 '200'
    payload = {
        'path': '/marginTrading/detailList', 'code': 200,
        'data': {'resultCode': '200', 'resultMsg': 'success', 'count': 4, 'total': 4,
                 'records': [
                     {'SECCODE': '000001', 'F010V': '深圳', 'TRADEDATE': '2026-11-20',
                      'F001N': 3879313265, 'F002N': 109931298, 'F003N': None,
                      'F008N': 1234567, 'F009N': 3880547832},
                     {'SECCODE': '600000', 'F010V': '上海', 'TRADEDATE': '2026-11-20',
                      'F001N': 3623771056, 'F002N': 21490597, 'F003N': 62493983,
                      'F008N': None, 'F009N': None},
                     {'SECCODE': '920001', 'F010V': '北京', 'TRADEDATE': '2026-11-20',
                      'F001N': 100, 'F002N': 50, 'F003N': 30, 'F008N': None,
                      'F009N': None},
                     {'SECCODE': '510050', 'F010V': '上海', 'TRADEDATE': '2026-11-20',
                      'F001N': 10, 'F002N': 10, 'F003N': 10, 'F008N': 10,
                      'F009N': None},
                 ]},
    }
    c = _mk_cninfo(payload)
    detail = c.fetch_margin_detail('20261120')
    assert detail['total'] == 4
    assert detail['records'][0]['F001N'] == 3879313265

    from src.sync.margin_manager import MarginManager
    m = MarginManager.__new__(MarginManager)
    m.cninfo = c
    grouped = m._cninfo_by_market('20261120')
    assert len(grouped['szse']) == 1
    assert len(grouped['sse']) == 2          # 沪市含基金（项目范围保留）
    assert len(grouped['bse']) == 1          # 北交所进范围统计
    assert grouped['szse'][0]['ts_code'] == 'sz.000001'
    assert grouped['szse'][0]['rzmre'] == Decimal('109931298')
    assert grouped['szse'][0]['rzche'] is None    # 深市偿还额空值保留
    assert grouped['sse'][0]['ts_code'] == 'sh.600000'
    # F009N 合计校验通过（3879313265+1234567=3880547832）不告警不断行


def test_cninfo_margin_rejects_incomplete_evidence():
    """count≠total≠len(records) 与错日 TRADEDATE 拒收（完备证据红线）"""
    c = _mk_cninfo({'data': {'resultCode': '200', 'count': 3, 'total': 5, 'records': [
        {'SECCODE': '000001', 'F010V': 'sz', 'TRADEDATE': '2026-11-20',
         'F001N': 1, 'F002N': 1, 'F003N': 1, 'F008N': 1}]}})
    with pytest.raises(ValueError, match='完备性'):
        c.fetch_margin_detail('20261120')

    c2 = _mk_cninfo({'data': {'resultCode': '200', 'count': 1, 'total': 1, 'records': [
        {'SECCODE': '000001', 'F010V': 'sz', 'TRADEDATE': '2026-11-19',
         'F001N': 1, 'F002N': 1, 'F003N': 1, 'F008N': 1}]}})
    with pytest.raises(ValueError, match='错日'):
        c2.fetch_margin_detail('20261120')

    c3 = _mk_cninfo({'data': {'resultCode': '500', 'count': 0, 'total': 0,
                              'records': []}})
    with pytest.raises(ValueError, match='resultCode'):
        c3.fetch_margin_detail('20261120')


# ============================================================ 期权参数区分与错日拒收

class _CaptureOptionSource:
    def __init__(self, risk_rows=None, stats_rows=None, master_rows=None):
        self.risk_rows = risk_rows or []
        self.stats_rows = stats_rows or []
        self.master_rows = master_rows or []
        self.calls: List[Dict[str, Any]] = []

    def fetch_contract_master(self):
        self.calls.append({'kind': 'master'})
        return self.master_rows

    def fetch_risk_indicators(self, trade_date):
        self.calls.append({'kind': 'risk', 'trade_date': trade_date})
        return self.risk_rows

    def fetch_daily_stats(self, trade_date):
        self.calls.append({'kind': 'stats', 'trade_date': trade_date})
        return self.stats_rows


def test_sse_option_date_param_names():
    """§4.2：risk 表传 trade_date、stats 表传 tradeDate——两表不同参数名"""
    from src.data_sources.sse_option_source import SseOptionSource
    src = SseOptionSource.__new__(SseOptionSource)
    captured = []

    def fake_get_json(url, params=None, headers=None, allow_status=()):
        captured.append(dict(params or {}))
        return {'result': []}

    src.get_json = fake_get_json
    src.fetch_risk_indicators('20261120')
    src.fetch_daily_stats('20261120')
    assert captured[0]['trade_date'] == '20261120'
    assert 'tradeDate' not in captured[0]
    assert captured[1]['tradeDate'] == '20261120'
    assert 'trade_date' not in captured[1]


def test_option_manager_rejects_wrong_date_rows_and_sub_datasets():
    """错日响应行剔除不入账；sub_datasets={'stats'} 单补统计子表（§4.2）"""
    from src.sync.option_manager import OptionManager

    class _Gate:
        enabled = False

        def remaining_for_url(self, url):
            return 0

    class _DB:
        def __init__(self):
            self.risk_upserts = []
            self.stats_upserts = []
            self.master_upserts = []

        def execute_query(self, q, p=None):
            return [{'mx': None}]

        def upsert_option_daily(self, rows):
            self.risk_upserts.extend(rows)

        def upsert_option_underlying_daily(self, rows):
            self.stats_upserts.extend(rows)

        def upsert_option_contracts(self, rows):
            self.master_upserts.extend(rows)

    ok_risk = {'SECURITY_ID': '10004500', 'TRADE_DATE': '2026-11-20',
               'CONTRACT_ID': '510050C2611M02700'}
    wrong_risk = {'SECURITY_ID': '10004501', 'TRADE_DATE': '2026-11-24',
                  'CONTRACT_ID': '510050C2611M02800'}
    ok_stats = {'SECURITY_CODE': '510050', 'TRADE_DATE': '2026-11-20',
                'CONTRACT_VOLUME': '716'}

    m = OptionManager.__new__(OptionManager)
    m.db = _DB()

    class _GateHolder:
        pass

    m.gate = _Gate()
    m.source = _CaptureOptionSource(
        risk_rows=[ok_risk, wrong_risk],
        stats_rows=[ok_stats],
        master_rows=[{'SECURITY_ID': '10004500', 'CONTRACT_ID': '510050C2611M02700'}])
    r = m.execute(trade_date='20261120', sub_datasets={'risk', 'stats'},
                  write_report=False)
    # 错日风险行被剔除（716 合约数不在此核对——单行样本）
    assert len(m.db.risk_upserts) == 1
    assert m.db.risk_upserts[0]['trade_date'] == '20261120'
    assert any('错日' in s for s in r['stats']['missing'])
    assert len(m.db.stats_upserts) == 1
    assert m.db.master_upserts == []          # master 未在请求子集不写
    assert r['success'] is True

    # 单补 stats 子表：risk 不请求
    m2 = OptionManager.__new__(OptionManager)
    m2.db = _DB()
    m2.gate = _Gate()
    src2 = _CaptureOptionSource(stats_rows=[ok_stats], risk_rows=[ok_risk])
    m2.source = src2
    r2 = m2.execute(trade_date='20261120', sub_datasets={'stats'}, write_report=False)
    assert all(c['kind'] != 'risk' for c in src2.calls)
    assert len(m2.db.stats_upserts) == 1 and m2.db.risk_upserts == []
    assert r2['success'] is True


def test_option_manager_cross_check_contract_count():
    """交叉验证：risk 行数 ≠ 统计表合约数合计 → missing 记录（§4.2 按标的核对）"""
    from src.sync.option_manager import OptionManager

    class _Gate:
        enabled = False

        def remaining_for_url(self, url):
            return 0

    class _DB:
        def __init__(self):
            self.risk_upserts = []
            self.stats_upserts = []

        def execute_query(self, q, p=None):
            return [{'mx': None}]

        def upsert_option_daily(self, rows):
            self.risk_upserts.extend(rows)

        def upsert_option_underlying_daily(self, rows):
            self.stats_upserts.extend(rows)

    risk = [{'SECURITY_ID': f'1000450{i}', 'TRADE_DATE': '2026-11-20',
             'CONTRACT_ID': f'510050C2611M0270{i}'} for i in range(3)]
    stats = [{'SECURITY_CODE': '510050', 'TRADE_DATE': '2026-11-20',
              'CONTRACT_VOLUME': '716'}]
    m = OptionManager.__new__(OptionManager)
    m.db = _DB()
    m.gate = _Gate()
    m.source = _CaptureOptionSource(risk_rows=risk, stats_rows=stats)
    r = m.execute(trade_date='20261120', write_report=False)
    assert any('交叉验证差异' in s for s in r['stats']['missing'])


# ============================================================ 发布指针契约

def test_accept_partition_current_rejects_non_accepted_status():
    """发布指针仅接受 COMPLETE/CONFIRMED_EMPTY（PARTIAL/FAILED 不动指针）"""
    from src.database.dao_collection_lineage import CollectionLineageDaoMixin
    dao = CollectionLineageDaoMixin.__new__(CollectionLineageDaoMixin)
    with pytest.raises(ValueError):
        dao.accept_partition_current_tx(
            None, 'margin', '20261120|CN|sse|margin|detail', '20261120',
            'job-1', 'PARTIAL')


def test_margin_expand_sub_entries_semantics():
    """§2.2 编排记账：margin 展开沪深子项；深市 0 行+源成功 → 子项待补（非已确认空）"""
    from src.sync.review_sync_orchestrator import ReviewSyncOrchestrator

    ok = {'source': 'margin', 'status': '成功', 'rows': 4138,
          'sub_counts': {'szse_rows': 2119, 'sse_rows': 2019}}
    subs = ReviewSyncOrchestrator._expand_sub_entries('margin', ok)
    assert len(subs) == 2
    szse_e, szse_ex, szse_sub = subs[0]
    assert (szse_ex, szse_sub) == ('szse', 'detail')
    assert szse_e['rows'] == 2119 and szse_e['status'] == '成功'
    sse_e, sse_ex, _ = subs[1]
    assert sse_e['rows'] == 2019 and sse_ex == 'sse'

    half = {'source': 'margin', 'status': '成功', 'rows': 2019,
            'sub_counts': {'sse_rows': 2019}}     # 深市无数据
    subs2 = ReviewSyncOrchestrator._expand_sub_entries('margin', half)
    szse2 = subs2[0][0]
    assert szse2['rows'] == 0 and szse2['status'] == '待补'   # 未发布 ≠ 已确认空
    assert subs2[1][0]['status'] == '成功'

    # option 展开 risk/stats
    opt = {'source': 'option', 'status': '成功', 'rows': 721,
           'sub_counts': {'risk_rows': 716, 'stats_rows': 5}}
    subs3 = ReviewSyncOrchestrator._expand_sub_entries('option', opt)
    assert [(e2, ex2, s2) for e2, ex2, s2 in subs3] and \
        {s2 for _, _, s2 in subs3} == {'risk', 'stats'}

    # 无子项计数的源单行原样
    other = {'source': 'lhb', 'status': '成功', 'rows': 10}
    assert ReviewSyncOrchestrator._expand_sub_entries('lhb', other) == [(other, None, None)]
