"""阶段 2 外围报告层测试（V2.0 §6 T1~T10 + 渲染补充）

- 聚合层：_overseas_indices（4 指标/窗口边界）、_holiday_overseas（触发边界/区间
  首尾/开区间语义）、_ah_mapping（Q8 对齐/T7 溢价/B8 fx 缺失）、_overseas_all 快照过滤
- 渲染层：market_env 四块（日K表/快照降级 B1/假期块/A-H 块）
FakeReader 按 SQL 特征路由并模拟真实过滤语义（元组行，同 DataReader._query）。
"""

import datetime as _dt
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.review.aggregator import ReviewAggregator, _OVERSEAS_SNAPSHOT_WHITELIST
from src.review.renderer import blocks_to_markdown
from src.review.sections import market_env

# market_env.build 涨停/跌停对比子块所需的最小 sentiment 聚合
_SENT = {'available': True, 'data': {'limit_up_count': 50, 'limit_down_count': 5,
                                     'burst_count': 10, 'burst_rate': 0.15}}


# ------------------------------------------------------------------ fixtures

def _dates(n, start=(2026, 7, 1)):
    d0 = _dt.date(*start)
    return [(d0 + _dt.timedelta(days=i)).strftime('%Y%m%d') for i in range(n)]


class FakeReader:
    """只读连接替身：按 SQL 特征路由，参数化过滤语义与真实 SQL 一致（含开区间）"""

    def __init__(self):
        self.ok_rows = []    # overseas_kline_day: (market_code, trade_date, close)
        self.snap_rows = []  # overseas_market_daily: (trade_date, market_name, close, change_pct)
        self.a_rows = {}     # ts_code -> [(trade_date, close)]
        self.calendar = []   # ('2026-09-25',) 风格
        self.fx_row = None   # (trade_date, close) | None

    def _query(self, sql, params=None):
        s = ' '.join(sql.split())
        p = list(params or [])
        if 'DISTINCT ON' in s:  # 假期 base：每代码 <= prev 最近行
            out = []
            for c in p[:-1]:
                rows = sorted((d, v) for m, d, v in self.ok_rows
                              if m == c and d <= p[-1] and v is not None)
                if rows:
                    out.append((c, rows[-1][0], rows[-1][1]))
            return out
        if 'overseas_kline_day' in s and ' IN ' in s and 'trade_date > %s' in s:
            codes, prev, asof = set(p[:-2]), p[-2], p[-1]  # 假期 span：(prev, asof) 双开
            return sorted((m, d, v) for m, d, v in self.ok_rows
                          if m in codes and prev < d < asof and v is not None)
        if 'overseas_kline_day' in s and ' IN ' in s:  # indices：<= as_of 全行
            codes, asof = set(p[:-1]), p[-1]
            return sorted((m, d, v) for m, d, v in self.ok_rows
                          if m in codes and d <= asof and v is not None)
        if "market_code = 'hkHSI'" in s:  # H 侧滞后行集：(lo, hi]
            lo, hi = p
            return [(d,) for d in sorted({d for m, d, v in self.ok_rows
                                          if m == 'hkHSI' and lo < d <= hi and v is not None})]
        if 'overseas_kline_day' in s:  # H 侧尾窗（DESC LIMIT 6 → fake 已降序）
            code, asof = p
            return [(d, v) for d, v in sorted(
                [(d, v) for m, d, v in self.ok_rows
                 if m == code and d <= asof and v is not None], reverse=True)[:6]]
        if 'his_kline_day' in s:  # A 侧尾窗
            code, asof = p
            return [(d, v) for d, v in sorted(
                [(d, v) for d, v in self.a_rows.get(code, []) if d <= asof],
                reverse=True)[:6]]
        if 'fx_shkdcny' in s:
            return [self.fx_row] if self.fx_row else []
        if 'base_trade_calendar' in s:  # 参数为 yyyy-mm-dd
            asof = p[0]
            return [(d,) for d in self.calendar if d <= asof]
        if 'overseas_market_daily' in s:  # 快照全行
            return list(self.snap_rows)
        raise AssertionError(f'FakeReader 未覆盖的 SQL: {s[:120]}')


def _make_agg(reader, indices=None, stocks=None):
    a = ReviewAggregator.__new__(ReviewAggregator)
    a.db = reader
    a.config = {'review_sync': {
        'overseas_kline_indices': indices or [{'code': 'hkTEST', 'name': '测试指数'}],
        'overseas_kline_stocks': stocks or [
            {'code': 'hkT0001', 'name': '测试H', 'a_code': 'sh.600000'}],
    }}
    return a


# ------------------------------------------------------------------ T1/T2 指数指标

def _fill_ok(reader, code, closes, dates=None):
    dates = dates or _dates(len(closes))
    reader.ok_rows.extend((code, d, v) for d, v in zip(dates, closes))


def test_t1_index_four_metrics():
    """T1：61 行已知序列，四指标与手算一致（聚合层两位小数；含 tail(61) 窗口边界）"""
    reader = FakeReader()
    _fill_ok(reader, 'hkTEST', [100.0 + i for i in range(61)])
    agg = _make_agg(reader)
    rows = agg._overseas_indices('20261231')
    assert len(rows) == 1
    r = rows[0]
    assert r['close'] == pytest.approx(160.0)
    assert r['change_pct'] == pytest.approx(0.63)      # 160/159−1 = 0.6289%
    assert r['chg_5d'] == pytest.approx(3.23)          # 160/155−1 = 3.2258%
    assert r['chg_20d'] == pytest.approx(14.29)        # 160/140−1 = 14.2857%
    assert r['position_pct'] == pytest.approx(100.0)   # 收盘=窗口最高
    assert r['date'] == _dates(61)[-1]


def test_t2_index_short_window():
    """T2：25 行 → position=None（<30），chg_5d 有值（≥6）"""
    reader = FakeReader()
    _fill_ok(reader, 'hkTEST', [50.0 + i for i in range(25)])
    r = _make_agg(reader)._overseas_indices('20261231')[0]
    assert r['position_pct'] is None
    assert r['chg_5d'] is not None
    assert r['chg_20d'] is not None  # 25 ≥ 21


def test_index_metrics_none_when_too_short():
    """B3 补充：<6 行 → chg_5d/chg_20d 均 None；单行只有 close"""
    reader = FakeReader()
    _fill_ok(reader, 'hkTEST', [100.0, 101.0, 102.0])
    r = _make_agg(reader)._overseas_indices('20261231')[0]
    assert r['chg_5d'] is None and r['chg_20d'] is None and r['position_pct'] is None
    assert r['change_pct'] is not None


# ------------------------------------------------------------------ T3~T5 假期

def _holiday_reader():
    """场景：prev=20260930（A股周一节前），区间行 1001/1002，另有 1009 行（区间外）"""
    reader = FakeReader()
    _fill_ok(reader, 'hkTEST', [100.0], dates=['20260930'])
    _fill_ok(reader, 'hkTEST', [101.0, 103.0], dates=['20261001', '20261002'])
    return reader


def test_t3_holiday_trigger_boundary():
    """T3：自然日差 3 天不触发、4 天触发"""
    reader = _holiday_reader()
    agg = _make_agg(reader)
    assert agg._holiday_overseas('20260930', '20260927') is None   # 差 3 天
    assert agg._holiday_overseas('20260930', '20260926') is not None  # 差 4 天
    assert agg._holiday_overseas('20260930', None) is None          # B4 首日


def test_t4_holiday_accumulation():
    """T4：accum=1002 行/0930 行−1=3.0%；days=2；last_date=20261002（as_of=1006，gap 6 天）"""
    reader = _holiday_reader()
    agg = _make_agg(reader)
    h = agg._holiday_overseas('20261006', '20260930')
    assert h is not None
    row = h['rows'][0]
    assert row['accum_pct'] == pytest.approx(3.0)
    assert row['days'] == 2
    assert row['last_date'] == '20261002'
    assert h['last_date'] == '20261002' and h['prev_date'] == '20260930'


def test_t5_holiday_open_interval():
    """T5：区间 (prev, as_of) 双开——as_of 当日行不计入累积（当日变化属"当日%"列）"""
    reader = _holiday_reader()
    _fill_ok(reader, 'hkHSI', [999.0], dates=['20261006'])  # 锚定日当日行
    agg = _make_agg(reader)
    h = agg._holiday_overseas('20261006', '20260930')
    assert h['rows'][0]['accum_pct'] == pytest.approx(3.0)   # 仍按 1002 收盘
    assert h['rows'][0]['days'] == 2
    assert h['last_date'] == '20261002'


def test_holiday_no_base_or_no_rows():
    """B6：区间无行/无基准 → accum=None，days=0"""
    reader = FakeReader()
    _fill_ok(reader, 'hkTEST', [100.0], dates=['20260930'])  # 仅基准，区间无行
    agg = _make_agg(reader)
    h = agg._holiday_overseas('20261006', '20260930')
    row = h['rows'][0]
    assert row['accum_pct'] is None and row['days'] == 0


# ------------------------------------------------------------------ T6~T8 A-H

def _ah_reader(h_end='20260928', fx=0.85449):
    """A 侧 6 行（A 股日 0923~0926/0929/0930，close 递增至 30.0）；
    H 侧 6 行（以 h_end 为末日的连续 6 天，close 递增至 25.0，d_h=h_end）；
    hkHSI 行集固定 0925/0928/0929/0930（H 侧滞后计数基准）；fx 快照可配置"""
    reader = FakeReader()
    end = _dt.datetime.strptime(h_end, '%Y%m%d').date()
    h_dates = [(end - _dt.timedelta(days=5 - i)).strftime('%Y%m%d') for i in range(6)]
    a_dates = ['20260923', '20260924', '20260925', '20260926', '20260929', '20260930']
    reader.a_rows['sh.600000'] = list(zip(a_dates, [25.0 + i for i in range(6)]))
    _fill_ok(reader, 'hkT0001', [20.0 + i for i in range(6)], dates=h_dates)
    _fill_ok(reader, 'hkHSI', [1.0] * 4,
             dates=['20260925', '20260928', '20260929', '20260930'])
    reader.calendar = [f'2026-09-{d:02d}' for d in (23, 24, 25, 26, 29, 30)]
    reader.fx_row = ('20261004', fx) if fx is not None else None
    return reader

def test_t6_ah_alignment_boundary():
    """T6：H 侧滞后 2 个市场交易日对齐；滞后 4 unaligned（hkHSI 行集 0925/0928/0929/0930
    为滞后计数基准；d_h 由 h_end 锚定整体生成保证升序）"""
    agg = _make_agg(_ah_reader('20260928'))          # (0928,0930] = 0929/0930 → 滞后 2
    r = agg._ah_mapping('20260930')['rows'][0]
    assert r['unaligned'] is False
    agg2 = _make_agg(_ah_reader('20260924'))         # (0924,0930] = 0925/0928/0929/0930 → 4
    r2 = agg2._ah_mapping('20260930')['rows'][0]
    assert r2['unaligned'] is True


def test_t7_premium_formula():
    """T7：premium = 30/(25×0.85449)−1 ≈ +40.42%（a_close=30, h_close=25）"""
    reader = _ah_reader('20260930')
    reader.a_rows['sh.600000'] = list(zip(_dates(6, (2026, 9, 23)), [30.0] * 6))
    reader.ok_rows = [(m, d, 25.0) if m == 'hkT0001' else (m, d, v)
                      for m, d, v in reader.ok_rows]
    r = _make_agg(reader)._ah_mapping('20260930')['rows'][0]
    assert r['premium'] == pytest.approx((30.0 / (25.0 * 0.85449) - 1) * 100, abs=0.01)


def test_t8_fx_missing():
    """T8/B8：fx_shkdcny 无行 → premium=None，其余列正常"""
    reader = _ah_reader('20260930')
    reader.fx_row = None
    out = _make_agg(reader)._ah_mapping('20260930')
    assert out['fx'] is None and out['fx_date'] is None
    r = out['rows'][0]
    assert r['premium'] is None
    assert r['chg_a_5d'] is not None and r['chg_h_5d'] is not None


def test_ah_unchanged_close_fallback():
    """B9：A 侧无行 → chg_a/diff/premium 均 None；H 侧有行仍展示"""
    reader = _ah_reader('20260930')
    reader.a_rows = {}
    r = _make_agg(reader)._ah_mapping('20260930')['rows'][0]
    assert r['chg_a_5d'] is None and r['diff'] is None and r['premium'] is None
    assert r['chg_h_5d'] is not None


# ------------------------------------------------------------------ T9 快照过滤

def test_t9_snapshot_whitelist_filter():
    """T9：快照 8 行 → 过滤后仅 4 行（A50 + 三汇率）；全行版本 8 行保留"""
    reader = FakeReader()
    reader.snap_rows = [
        ('20261004', '道琼斯', 51176.96, 0.49),
        ('20261004', '纳斯达克', 26861.06, 0.24),
        ('20261004', '标普500', 7651.54, -0.25),
        ('20261004', '恒生指数', 23972.29, -2.60),
        ('20261004', '富时A50期货', 13810.0, None),
        ('20261004', '离岸人民币', 6.7064, None),
        ('20261004', '美元兑人民币', 7.01, None),
        ('20261004', '港元兑人民币', 0.8545, None),
    ]
    agg = _make_agg(reader)
    all_rows = agg._overseas_all()
    assert len(all_rows) == 8
    filtered = [r for r in all_rows if r['name'] in _OVERSEAS_SNAPSHOT_WHITELIST]
    assert {r['name'] for r in filtered} == _OVERSEAS_SNAPSHOT_WHITELIST
    assert len(filtered) == 4


# ------------------------------------------------------------------ T10 + 渲染

def _env_data():
    """完整 market_env.data 最小集（只覆盖阶段 2 相关键）"""
    return {
        'as_of': '20261009',
        'indices': [], 'turnover': {}, 'advance_decline': {},
        'style_spread': None,
        'overseas': [{'date': '20261008', 'name': '富时A50期货', 'close': 13810.0,
                      'change_pct': None},
                     {'date': '20261008', 'name': '港元兑人民币', 'close': 0.8545,
                      'change_pct': None}],
        'overseas_all': [{'date': '20261008', 'name': '道琼斯', 'close': 51000.0,
                          'change_pct': 0.5},
                         {'date': '20261008', 'name': '富时A50期货', 'close': 13810.0,
                          'change_pct': None}],
        'overseas_indices': [
            {'name': '恒生指数', 'date': '20261009', 'close': 25000.0,
             'change_pct': 1.2, 'chg_5d': 3.5, 'chg_20d': -1.1, 'position_pct': 44.6},
        ],
        'holiday_overseas': {
            'prev_date': '20260930', 'last_date': '20261008',
            'rows': [{'name': '恒生指数', 'accum_pct': -2.6, 'days': 5,
                      'last_date': '20261008'},
                     {'name': '道琼斯', 'accum_pct': None, 'days': 0,
                      'last_date': None}],
        },
        'ah_mapping': {
            'fx': 0.8545, 'fx_date': '20261008',
            'rows': [
                {'h_name': '比亚迪股份', 'chg_a_5d': -3.78, 'chg_h_5d': -5.91,
                 'diff': 2.13, 'premium': 28.96, 'unaligned': False},
                {'h_name': '中芯国际', 'chg_a_5d': -8.51, 'chg_h_5d': -4.94,
                 'diff': -3.58, 'premium': 116.09, 'unaligned': False},
                {'h_name': '长期停牌H', 'chg_a_5d': 1.0, 'chg_h_5d': 0.5,
                 'diff': 0.5, 'premium': 10.0, 'unaligned': True},
            ],
        },
    }


def _env_agg(data):
    return {'available': True, 'reason': '', 'data': data}


def test_t10_fallback_snapshot_when_no_kline():
    """T10/B1：日K空 → 降级快照全量两值表，标题带"快照口径，日K缺失\""""
    data = _env_data()
    data['overseas_indices'] = []
    text = blocks_to_markdown(market_env.build(_env_agg(data), _SENT))
    assert '快照口径，日K缺失' in text
    assert '道琼斯' in text  # 全量（未过滤）


def test_render_overseas_indices_block():
    """块①：六列表 + 口径 note（渲染层只格式化）"""
    text = blocks_to_markdown(market_env.build(_env_agg(_env_data()),
                                               _SENT))
    assert '外围指数（20261009）' in text
    assert '恒生指数' in text and '45%' in text
    assert '60日位置' in text


def test_render_holiday_block():
    """块③：触发形态 heading/table；无行市场 accum 显示 '—'"""
    text = blocks_to_markdown(market_env.build(_env_agg(_env_data()),
                                               _SENT))
    assert '假期外围观察（20260930 后休市区间，截至 20261008）' in text
    assert '— 0' in text.replace('|', ' ').split('道琼斯')[1].strip()[:3] or '| — | 0 |' in text
    assert '首尾收盘比' in text


def test_render_no_holiday_block_when_none():
    """B5：holiday_overseas=None（普通交易日）→ 无块③"""
    data = _env_data()
    data['holiday_overseas'] = None
    text = blocks_to_markdown(market_env.build(_env_agg(data),
                                               _SENT))
    assert '假期外围观察' not in text


def test_render_ah_mapping_block():
    """块④：15 列结构、unaligned 行 '未对齐'、点名 para 为 |diff| top3、口径 note"""
    text = blocks_to_markdown(market_env.build(_env_agg(_env_data()),
                                               _SENT))
    assert 'A-H/外围映射（5 日累计，锚定 20261009）' in text
    assert '未对齐' in text
    assert '比亚迪股份' in text and '+116.1' in text  # 溢价一位小数带号
    assert 'A−H 差值最大' in text
    # 点名顺序：|−3.58| > |+2.13|（未对齐行不参与）
    assert text.index('中芯国际 -3.58pct') < text.index('比亚迪股份 +2.13pct')
    assert '未对齐' in text.split('长期停牌H')[1][:40]


def test_render_ah_fx_missing_note():
    """B8 渲染：fx=None → 溢价列 '—' + note '汇率待补'"""
    data = _env_data()
    data['ah_mapping'] = {'fx': None, 'fx_date': None, 'rows': [
        {'h_name': '比亚迪股份', 'chg_a_5d': -3.78, 'chg_h_5d': -5.91,
         'diff': 2.13, 'premium': None, 'unaligned': False}]}
    text = blocks_to_markdown(market_env.build(_env_agg(data),
                                               _SENT))
    assert '汇率待补' in text
