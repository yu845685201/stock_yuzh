"""外围日K采集测试（技术方案 V1.0 20261004 §6.3）

- day 行解析：指数 6 列 / 港股个股带第 7 元素（字典、空字典、缺省）防御式
- open/close 位次防颠倒回归（真实响应样本固化——sina rt_hkHSI 位次坑前车之鉴）
- qt 自洽校验（同日期超差告警 / 跨日期跳过）
- 结构变更 → IfzqSourceError
- 增量 upsert 幂等（同日重跑零重复）
- config a_code 映射校验（合法 ts_code 且存在于 base_stock_info）
- init 分窗（10 年 → 4 窗）与编排器挂载
"""

import sys
from datetime import date
from pathlib import Path

import pytest
import yaml

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.ifzq_source import IfzqSource, IfzqSourceError

REPO_ROOT = BACKEND.parent


# ---------- 真实响应样本（2026-10-04 curl 实测固化） ----------

# hkHSI 指数行（6 列，无第 7 元素）：09-25 open 24523.500 / close 24510.090
HKHSI_DAY = [
    ['2026-09-25', '24523.500', '24510.090', '24537.140', '24275.560', '102202959184.000'],
    ['2026-09-28', '24554.580', '24642.510', '24767.220', '24554.580', '177482238485.000'],
    ['2026-10-02', '24099.730', '23972.290', '24099.730', '23865.330', '145804272792.000'],
]
# hk00700 个股行（7 列，第 7 元素为公司行动字典）
HK00700_ROW_DICT = ['2026-09-25', '433.800', '436.600', '437.200', '431.200',
                    '9113746.000',
                    {'cqr': '2026-09-25', 'FHcontent': '',
                     'HGcontent': '回购23.10万股，均价434.236港元',
                     'paixiri': '', 'hgcgContent': '', 'ggContent': ''}]
# usDJI 美股指数行 + qt 块（10-02 收 51176.96，O4 三方对拍一致）
USDJI_DAY = [['2026-10-02', '51241.18', '51176.96', '51382.88', '50950.61', '548953568']]
USDJI_QT = (['200', '道琼斯', '.DJI', '51176.96', '50926.56', '51241.18',
             '548953568', '0', '0', '51106.95', '0', '0']
            + ['0'] * 18 + ['2026-10-02 17:00:17'] + ['0'] * 41)


def _payload(code, day_rows, qt=None):
    node = {'day': day_rows}
    if qt is not None:
        node['qt'] = {code: qt}
    return {'code': 0, 'data': {code: node}}


# ---------- day 行解析（§6.3-1） ----------

def test_parse_index_row_six_columns():
    parsed = IfzqSource._parse_payload('hkHSI', _payload('hkHSI', HKHSI_DAY))
    assert parsed['skipped_rows'] == 0
    assert [r['trade_date'] for r in parsed['rows']] == ['20260925', '20260928', '20261002']
    r0 = parsed['rows'][0]
    assert r0['open'] == pytest.approx(24523.500)
    assert r0['close'] == pytest.approx(24510.090)
    assert r0['volume'] == pytest.approx(102202959184.0)
    assert r0['adj_type'] == 'raw' and r0['source'] == 'ifzq'


def test_parse_stock_row_with_corporate_action_dict():
    """第 7 元素为公司行动字典：正常解析前 6 列，字典忽略不报错"""
    parsed = IfzqSource._parse_payload('hk00700', _payload('hk00700', [HK00700_ROW_DICT]))
    r = parsed['rows'][0]
    assert r['open'] == pytest.approx(433.800)
    assert r['close'] == pytest.approx(436.600)
    assert parsed['skipped_rows'] == 0


def test_parse_stock_row_seventh_element_variants():
    """第 7 元素为空字典/字符串/缺省：防御式不炸"""
    variants = [
        HK00700_ROW_DICT[:6] + [{}],
        HK00700_ROW_DICT[:6] + [''],
        HK00700_ROW_DICT[:6],
    ]
    for row in variants:
        parsed = IfzqSource._parse_payload('hk00700', _payload('hk00700', [row]))
        assert parsed['rows'][0]['close'] == pytest.approx(436.600)


def test_parse_bad_rows_skipped():
    """日期非法/价格不可解析/元素不足的行跳过并计数（如实不入库）"""
    rows = [
        ['bad-date', '1', '1', '1', '1', '0'],
        ['2026-10-02', 'x', '436.6', '437.2', '431.2', '0'],
        ['2026-10-02', '433.8', '436.6'],
        HK00700_ROW_DICT,
    ]
    parsed = IfzqSource._parse_payload('hk00700', _payload('hk00700', rows))
    assert len(parsed['rows']) == 1
    assert parsed['skipped_rows'] == 3


def test_parse_empty_window():
    """无数据窗口（HSTECH 发布前实测 day: []）→ 0 行非错误"""
    parsed = IfzqSource._parse_payload('hkHSTECH', _payload('hkHSTECH', []))
    assert parsed['rows'] == []


# ---------- open/close 位次防颠倒回归（§6.3-2） ----------

def test_open_close_position_regression_usdji():
    """真实样本固化：usDJI 2026-10-02 open=51241.18（[1]）、close=51176.96（[2]）。
    若解析颠倒，close 会错成 51241.18（与 O4 三方对拍值 51176.96 不符）"""
    parsed = IfzqSource._parse_payload('usDJI', _payload('usDJI', USDJI_DAY))
    r = parsed['rows'][0]
    assert r['close'] == pytest.approx(51176.96)
    assert r['open'] == pytest.approx(51241.18)
    assert r['high'] == pytest.approx(51382.88) and r['low'] == pytest.approx(50950.61)


def test_open_close_position_regression_hkhsi():
    """hkHSI 09-25 低开高走日：close 24510.09 ≠ open 24523.50；
    09-28 open==low（24554.58），若取错列会误成 high/low"""
    parsed = IfzqSource._parse_payload('hkHSI', _payload('hkHSI', HKHSI_DAY))
    by_date = {r['trade_date']: r for r in parsed['rows']}
    assert by_date['20260925']['close'] == pytest.approx(24510.09)
    assert by_date['20260925']['open'] == pytest.approx(24523.50)
    assert by_date['20260928']['low'] == pytest.approx(24554.58)
    assert by_date['20260928']['close'] == pytest.approx(24642.51)


# ---------- qt 自洽校验（§6.1） ----------

def test_self_check_same_date_consistent():
    parsed = IfzqSource._parse_payload('usDJI', _payload('usDJI', USDJI_DAY, USDJI_QT))
    assert parsed['qt_close'] == pytest.approx(51176.96)
    assert parsed['qt_date'] == '20261002'
    assert IfzqSource.self_check(parsed) is None


def test_self_check_same_date_mismatch_alarms():
    """同日期 close 与 qt 现值超差 → 告警文案（调用方告警不入库）"""
    bad_qt = list(USDJI_QT)
    bad_qt[3] = '50000.00'
    parsed = IfzqSource._parse_payload('usDJI', _payload('usDJI', USDJI_DAY, bad_qt))
    alarm = IfzqSource.self_check(parsed)
    assert alarm is not None and '自洽校验失败' in alarm


def test_self_check_cross_date_skipped():
    """qt 日期与 day 末行不一致（跨日取数）→ 校验跳过不误报"""
    stale_qt = list(USDJI_QT)
    stale_qt[30] = '2026-10-05 05:00:00'
    parsed = IfzqSource._parse_payload('usDJI', _payload('usDJI', USDJI_DAY, stale_qt))
    assert IfzqSource.self_check(parsed) is None


def test_self_check_no_qt_skipped():
    parsed = IfzqSource._parse_payload('hkHSI', _payload('hkHSI', HKHSI_DAY))
    assert IfzqSource.self_check(parsed) is None


# ---------- 结构变更 ----------

def test_structure_change_raises():
    with pytest.raises(IfzqSourceError):
        IfzqSource._parse_payload('hkHSI', {'code': -1, 'msg': 'err'})
    with pytest.raises(IfzqSourceError):
        IfzqSource._parse_payload('hkHSI', {'code': 0, 'data': {}})
    with pytest.raises(IfzqSourceError):
        IfzqSource._parse_payload('hkHSI', {'code': 0, 'data': {'hkHSI': {'qt': None}}})


def test_us_code_routes_to_usfqkline_endpoint():
    """美股代码走 usfqkline/get（fqkline 对美股损坏：尾部恒回 1 根、区间回 0 根——实测）"""
    source = IfzqSource({'review_sync': {'overseas_kline_interval': 0}})
    captured = {}

    def fake_get_json(url, params=None, headers=None, allow_status=()):
        captured['url'] = url
        code = params['param'].split(',')[0]
        return _payload(code, USDJI_DAY)

    source.get_json = fake_get_json
    source.fetch_daily('usDJI', count=20)
    assert 'usfqkline' in captured['url']
    source.fetch_daily('hkHSI', count=20)
    assert 'usfqkline' not in captured['url']
    assert IfzqSource.is_us_code('usDJI') and not IfzqSource.is_us_code('hkHSI')


def test_window_filter_trims_rows_before_start(monkeypatch):
    """美股端点 start 不生效、自 end 向后取：窗口起点之前的行按边界过滤（不跨窗重复）"""
    db = FakeKlineDB()
    m = _mk_manager(monkeypatch, [('usDJI', False)], db)
    outside = {'market_code': 'usDJI', 'trade_date': '20260701', 'open': 1, 'high': 1,
               'low': 1, 'close': 1, 'volume': 1, 'adj_type': 'raw', 'source': 'ifzq'}
    inside = {'market_code': 'usDJI', 'trade_date': '20260901', 'open': 1, 'high': 1,
              'low': 1, 'close': 1, 'volume': 1, 'adj_type': 'raw', 'source': 'ifzq'}
    monkeypatch.setattr(m, '_fetch_and_selfcheck', lambda *a, **k: {
        'code': 'usDJI', 'rows': [outside, inside], 'skipped_rows': 0,
        'qt_close': None, 'qt_date': None})
    stats = {'skipped_rows': 0, 'windows': 0}
    rows = m._fetch_windows('usDJI', [('2026-08-01', '2026-09-30')], stats)
    assert [r['trade_date'] for r in rows] == ['20260901']


# ---------- 幂等（§6.3-3） ----------

class FakeKlineDB:
    def __init__(self):
        self.rows = {}   # (code, date) -> row

    def upsert_overseas_klines(self, rows):
        for r in rows:
            self.rows[(r['market_code'], r['trade_date'])] = r
        return len(rows)

    def fetch_overseas_klines_since(self, code, start):
        return {d: r['close'] for (c, d), r in self.rows.items()
                if c == code and d >= start and r['close'] is not None}

    def fetch_overseas_kline_recent_closes(self, code, limit):
        closes = [r['close'] for (c, d), r in sorted(self.rows.items())
                  if c == code and r['close'] is not None]
        return closes[-limit:]

    def fetch_overseas_kline_latest_date(self, code):
        dates = [d for (c, d) in self.rows if c == code]
        return max(dates) if dates else None


def _mk_manager(monkeypatch, registry_codes, db):
    """构造不触库/不触网的 OverseasKlineManager（__new__ 绕过 __init__）"""
    import src.sync.overseas_kline_manager as okm
    monkeypatch.setattr(okm, 'write_review_report', lambda *a, **k: None)
    m = okm.OverseasKlineManager.__new__(okm.OverseasKlineManager)
    m.db = db
    m.init_years = 10
    m.registry = {}
    for code, is_stock in registry_codes:
        m.registry[code] = {'name': code, 'a_code': None, 'is_stock': is_stock,
                            'adj_type': 'qfq' if is_stock else 'raw'}
    return m


def test_incremental_idempotent_same_day(monkeypatch):
    """增量同日重跑：尾部 20 根窗口与库内完全重叠 → 零新增、总行数不变（验收标准 3）"""
    rows = [{'market_code': 'hkHSI', 'trade_date': f'202609{d:02d}', 'open': 1, 'high': 1,
             'low': 1, 'close': 100 + d, 'volume': 1, 'adj_type': 'raw', 'source': 'ifzq'}
            for d in range(1, 26)]
    db = FakeKlineDB()
    db.upsert_overseas_klines(rows)

    m = _mk_manager(monkeypatch, [('hkHSI', False)], db)
    monkeypatch.setattr(m, '_fetch_and_selfcheck', lambda *a, **k: {
        'code': 'hkHSI', 'rows': rows[-20:], 'skipped_rows': 0,
        'qt_close': None, 'qt_date': None})
    stats = {'skipped_rows': 0, 'windows': 0}
    last, out = m._collect_code('hkHSI', m.registry['hkHSI'], None, stats)
    db.upsert_overseas_klines(out)
    assert len(db.rows) == 25 and last['trade_date'] == '20260925'
    assert 'qfq_refresh' not in stats  # 完全重叠且同值 → 不触发重拉


def test_restatement_triggers_full_refetch(monkeypatch):
    """qfq 重述检测：尾部重叠日期 close 变化 → 全历史重拉（防新旧复权基准拼接跳变）"""
    old = [{'market_code': 'hk00700', 'trade_date': f'2026{m:02d}{d:02d}', 'open': 1,
            'high': 1, 'low': 1, 'close': 400 + m, 'volume': 1,
            'adj_type': 'qfq', 'source': 'ifzq'}
           for m, d in [(9, 1), (9, 15), (10, 2)]]
    db = FakeKlineDB()
    db.upsert_overseas_klines(old)

    m = _mk_manager(monkeypatch, [('hk00700', True)], db)
    # 尾部窗口：0915 行 qfq 基准滚动后 close 变化（400+15=415 -> 410），1002 为新行
    tail = [{'market_code': 'hk00700', 'trade_date': '20260915', 'open': 1, 'high': 1,
             'low': 1, 'close': 410.0, 'volume': 1, 'adj_type': 'qfq', 'source': 'ifzq'},
            {'market_code': 'hk00700', 'trade_date': '20261002', 'open': 1, 'high': 1,
             'low': 1, 'close': 421.2, 'volume': 1, 'adj_type': 'qfq', 'source': 'ifzq'}]
    monkeypatch.setattr(m, '_fetch_and_selfcheck', lambda *a, **k: {
        'code': 'hk00700', 'rows': tail, 'skipped_rows': 0,
        'qt_close': None, 'qt_date': None})
    # 全历史重拉返回重述后的完整序列（升序：0901 重述在前，1002 新行在末）
    full = [dict(old[0], close=390.0)] + tail
    monkeypatch.setattr(m, '_fetch_windows', lambda *a, **k: full)
    stats = {'skipped_rows': 0, 'windows': 0, 'qfq_refresh': []}
    last, out = m._collect_code('hk00700', m.registry['hk00700'], None, stats)
    assert stats['qfq_refresh'] == ['hk00700']
    assert len(out) == 3 and last['trade_date'] == '20261002'


def test_frozen_tripwire(monkeypatch):
    """冻结 tripwire：连续 3 个市场日 close 完全相同 → 标疑似冻结"""
    db = FakeKlineDB()
    rows = [{'market_code': 'usDJI', 'trade_date': f'202609{d:02d}', 'open': 1, 'high': 1,
             'low': 1, 'close': 51176.96, 'volume': 1, 'adj_type': 'raw', 'source': 'ifzq'}
            for d in (28, 29, 30)]
    db.upsert_overseas_klines(rows)
    m = _mk_manager(monkeypatch, [('usDJI', False)], db)
    stats = {'frozen_suspect': []}
    m._frozen_tripwire('usDJI', stats)
    assert stats['frozen_suspect'] == ['usDJI']
    # 正常波动不误报
    db.rows[('usDJI', '20260929')]['close'] = 51200.0
    stats2 = {'frozen_suspect': []}
    m._frozen_tripwire('usDJI', stats2)
    assert stats2['frozen_suspect'] == []


# ---------- a_code 校验（§6.3-4） ----------

def test_a_code_validation_warns_missing(monkeypatch):
    """a_code 不在 base_stock_info → 告警不阻塞；全部在库 → 无告警"""
    class FakeACodeDB:
        def __init__(self, found):
            self.found = found

        def execute_query(self, sql, params=None):
            assert 'base_stock_info' in sql
            return [{'ts_code': c} for c in self.found]

    m = _mk_manager(monkeypatch, [('hk01211', True), ('hk00700', False)], None)
    m.registry['hk01211']['a_code'] = 'sz.002594'
    m.registry['hk03968'] = {'name': '招商银行', 'a_code': 'sh.999999',
                             'is_stock': True, 'adj_type': 'qfq'}
    m.db = FakeACodeDB({'sz.002594'})
    warnings = m._validate_a_codes()
    assert len(warnings) == 1 and 'sh.999999' in warnings[0]
    m.db = FakeACodeDB({'sz.002594', 'sh.999999'})
    assert m._validate_a_codes() == []


def test_config_stocks_a_code_format():
    """config 清单：6 指数 + 33 个股；a_code 均为合法 ts_code 形态且无重复代码"""
    cfg = yaml.safe_load((REPO_ROOT / 'config' / 'config.yaml').read_text())
    review = cfg['review_sync']
    assert len(review['overseas_kline_indices']) == 6
    stocks = review['overseas_kline_stocks']
    assert len(stocks) == 33
    codes = [s['code'] for s in stocks] + [i['code'] for i in review['overseas_kline_indices']]
    assert len(codes) == len(set(codes))
    a_stocks = [s for s in stocks if s.get('a_code')]
    assert len(a_stocks) == 15
    for s in a_stocks:
        assert s['a_code'] in ('sh.601766', 'sh.688981') or s['a_code'][:5] in ('sh.60', 'sz.00', 'sz.30')
    # 仅港股组不带 a_code
    for s in stocks:
        if s['code'] in ('hk00700', 'hk03690', 'hk00175'):
            assert not s.get('a_code')


# ---------- 分窗 ----------

def test_windows_ten_years_four_requests():
    """10 年分窗 → 4 窗（4 请求/代码，方案 §6.1），窗口连续无缝覆盖"""
    from src.sync.overseas_kline_manager import OverseasKlineManager
    end = date(2026, 10, 4)
    start = date(end.year - 10, end.month, end.day)
    wins = OverseasKlineManager._windows(start, end)
    assert len(wins) == 4
    assert wins[0][0] == start.isoformat() and wins[-1][1] == end.isoformat()
    for (_, e1), (s2, _) in zip(wins, wins[1:]):
        assert (date.fromisoformat(s2) - date.fromisoformat(e1)).days == 1


def test_fmt_d_variants():
    from src.sync.overseas_kline_manager import _fmt_d
    assert _fmt_d('20261004') == '2026-10-04'
    assert _fmt_d('2026-10-04') == '2026-10-04'
    with pytest.raises(ValueError):
        _fmt_d('2026')


# ---------- 编排器挂载（§6.2） ----------

def test_orchestrator_wiring():
    from src.sync.review_sync_orchestrator import (DEFAULT_SOURCES, SOURCE_LABELS,
                                                   SOURCE_WAVES)
    assert 'overseas_kline' in DEFAULT_SOURCES
    assert SOURCE_WAVES[0].count('overseas_kline') == 1  # 波 1，与外围快照并发
    assert SOURCE_LABELS['overseas_kline'] == '外围日K'
    assert 'overseas_kline' not in __import__(
        'src.sync.review_sync_orchestrator', fromlist=['KLINE_DEPENDENT_SOURCES']
    ).KLINE_DEPENDENT_SOURCES


def test_orchestrator_runner_registered(monkeypatch):
    """runner 已注册：_build_runners 含 overseas_kline（延迟 import 不触网）"""
    from src.sync.review_sync_orchestrator import ReviewSyncOrchestrator

    class FakeCM:
        def load_config(self):
            return {'review_sync': {}}

    orch = ReviewSyncOrchestrator.__new__(ReviewSyncOrchestrator)
    orch.config_manager = FakeCM()
    orch.config = FakeCM().load_config()
    runners = orch._build_runners('20261009')
    assert 'overseas_kline' in runners
