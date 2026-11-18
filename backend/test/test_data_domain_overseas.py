"""数据域策略与外围模块测试（港美股指标与港股个股分析链整合方案 V1.0 序 2/序 3）。

- 域注册与链配置 domain 校验（未知值加载报错）
- 外围域 universe（config 池 → OverseasStockInfo，a_code 保留）
- 外围域 load：change_rate 自算 / is_st 恒 0 / 行数窗口 / a_close as-of 回挂边界 /
  fx 环境值装配
- 外围域 precheck：尾部滞后（3 天不标 / 4 天标「数据滞后」）、缺日 gap 分流、
  连续缺行→疑似停牌（不计存疑）、无前复权断裂规则
- 5 新模块：chg_5d / chg_20d / position_60d / ah_5d_diff（无A股映射/未对齐）/
  ah_premium（汇率待补）
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.analysis.chain_config import load_chain
from src.analysis.data_domain import (
    FX_HKDCNY_KEY,
    LAG_REASON_PREFIX,
    AShareDomain,
    OverseasIndicesDomain,
    OverseasStocksDomain,
    OverseasStockInfo,
    get_domain,
)
from src.analysis.module_base import ChainContext, StockInfo
from src.analysis.module_registry import build, load_registry, readme_registered_names


# ------------------------------------------------------------------ fixtures

def _kline(codes_closes, start=(2026, 8, 1)):
    """codes_closes: {code: [close,...]} → 按 ts_code/trade_date 升序的 DataFrame"""
    import datetime as _dt
    rows = []
    d0 = _dt.date(*start)
    for code, closes in codes_closes.items():
        for i, v in enumerate(closes):
            rows.append({
                'ts_code': code, 'stock_code': code,
                'trade_date': (d0 + _dt.timedelta(days=i)).strftime('%Y%m%d'),
                'open': v, 'high': v, 'low': v, 'close': v, 'volume': 1000.0,
            })
    return pd.DataFrame(rows)


class FakeReader:
    """域策略的数据访问替身（_query 元组行 / _copy DataFrame）"""

    def __init__(self, ok_rows=None, a_rows=None, fx=0.8545, calendars=None):
        self.ok_rows = ok_rows or []          # [(market_code, trade_date, close)]
        self.a_rows = a_rows or {}            # {ts_code: [(date, close), ...]}
        self.fx = fx
        self.calendars = calendars or {}      # {'hkHSI': [...], 'usDJI': [...]}

    def _query(self, sql, params=None):
        s = ' '.join(sql.split())
        if 'overseas_kline_day' in s and 'MAX(trade_date)' in s:
            if 'ANY(%s)' in s:
                codes = set(params[0])
                dates = [d for m, d, _ in self.ok_rows if m in codes]
            else:
                dates = [d for _, d, _ in self.ok_rows]
            return [(max(dates),)] if dates else [(None,)]
        if "SELECT 1 FROM overseas_kline_day" in s:
            return [(1,)] if any(d == params[0] for _, d, _ in self.ok_rows) else []
        if "market_code = 'fx_shkdcny'" in s:
            return [(self.fx,)] if self.fx is not None else []
        if 'FROM overseas_kline_day' in s and 'market_code = %s' in s:
            mc = params[0]
            return [(d,) for m, d, _ in sorted(self.ok_rows)
                    if m == mc and d <= params[1]]
        raise AssertionError(f'FakeReader 未覆盖 SQL: {s[:100]}')

    def _copy(self, sql, params=()):
        s = ' '.join(sql.split())
        if 'FROM overseas_kline_day' in s:
            codes = set(params[0])
            as_of = params[1]
            rows = sorted((m, d, v) for m, d, v in self.ok_rows
                          if m in codes and d <= as_of)
            return pd.DataFrame([{'ts_code': m, 'stock_code': m, 'trade_date': d,
                                  'open': v, 'high': v, 'low': v, 'close': v,
                                  'volume': 1000.0} for m, d, v in rows])
        if 'FROM his_kline_day' in s:
            codes = set(params[0])
            as_of = params[1]
            rows = []
            for code in sorted(codes):
                for d, v in self.a_rows.get(code, []):
                    if d <= as_of:
                        rows.append({'ts_code': code, 'trade_date': d, 'close': v})
            return pd.DataFrame(rows)
        raise AssertionError(f'FakeReader 未覆盖 COPY: {s[:100]}')


class FakeConfigManager:
    def __init__(self, review_sync):
        self._review_sync = review_sync

    def load_config(self):
        return {'review_sync': self._review_sync}


INDICES_CFG = [{'code': 'hkHSI', 'name': '恒生指数'},
               {'code': 'usDJI', 'name': '道琼斯'}]
STOCKS_CFG = [{'code': 'hk01211', 'name': '比亚迪股份', 'a_code': 'sz.002594'},
              {'code': 'hk00700', 'name': '腾讯控股'}]


class _ChainStub:
    lookback = '60d'


class _Opts:
    lookback = None
    check_missing_day = True
    missing_day_mode = 'gap'
    break_tolerance_pct = 0.6


# ------------------------------------------------------------------ 域注册/配置

def test_domain_registry_and_chain_config():
    assert get_domain('a_share').name == 'a_share'
    assert isinstance(get_domain('overseas_indices'), OverseasIndicesDomain)
    assert isinstance(get_domain('overseas_stocks'), OverseasStocksDomain)
    with pytest.raises(ValueError):
        get_domain('bogus')
    # A 股域基列与空池文案保持原样（回归红线）
    d = AShareDomain()
    # P0-1 增 raw_close 动态阈值数据源（有意扩展，非回归）
    assert d.base_columns == ['change_rate', 'is_st', 'raw_close']
    assert "base_stock_info" in d.universe_empty_error


def test_chain_config_domain_field(tmp_path):
    base = ("chain: t\ndomain: {domain}\nmodules:\n  - name: chg_5d\n")
    ok = tmp_path / 't.yaml'
    ok.write_text(base.format(domain='overseas_stocks'), encoding='utf-8')
    assert load_chain('t', tmp_path).domain == 'overseas_stocks'
    bad = tmp_path / 'b.yaml'
    bad.write_text(base.format(domain='us_market'), encoding='utf-8')
    with pytest.raises(ValueError, match='domain 非法'):
        load_chain('b', tmp_path)
    default = tmp_path / 'd.yaml'
    default.write_text("chain: d\nmodules:\n  - name: chg_5d\n", encoding='utf-8')
    assert load_chain('d', tmp_path).domain == 'a_share'  # 缺省 A 股域


# ------------------------------------------------------------------ universe

def test_overseas_universe_from_config():
    cm = FakeConfigManager({'overseas_kline_indices': INDICES_CFG,
                            'overseas_kline_stocks': STOCKS_CFG})
    idx = OverseasIndicesDomain().load_universe(None, cm)
    assert [s.ts_code for s in idx] == ['hkHSI', 'usDJI']
    assert idx[0].board == '外围指数' and idx[0].stock_name == '恒生指数'
    stocks = OverseasStocksDomain().load_universe(None, cm)
    assert [s.ts_code for s in stocks] == ['hk01211', 'hk00700']
    assert stocks[0].a_code == 'sz.002594' and stocks[1].a_code is None
    assert isinstance(stocks[0], OverseasStockInfo)
    assert stocks[0].board == '港股'


def test_overseas_resolve_as_of():
    reader = FakeReader(ok_rows=[('hkHSI', '20261002', 1.0), ('hkHSI', '20260930', 1.0)])
    dom = OverseasIndicesDomain()
    assert dom.resolve_as_of(reader, None) == '20261002'      # 自身日历最新日
    assert dom.resolve_as_of(reader, '20260930') == '20260930'
    with pytest.raises(ValueError, match='overseas_kline_day 中无任何数据'):
        dom.resolve_as_of(reader, '20260101')


def test_overseas_resolve_as_of_pool_scoped():
    """锚定按域内池解析：A50 期货夜盘行（sina_futures）trade_date 计入新交所
    下一交易日（晚于港美现货收盘），不得顶高港美域锚定日（20261027 实跑缺陷）。"""
    reader = FakeReader(ok_rows=[
        ('hkHSI', '20261027', 1.0), ('hkHSI', '20261024', 1.0),
        ('hk00700', '20261027', 1.0),
        ('CHA50CFD', '20261028', 1.0),          # 夜盘行：晚于港美现货最新收盘
    ])
    dom = OverseasIndicesDomain()
    cm = FakeConfigManager({'overseas_kline_indices': INDICES_CFG})
    assert dom.resolve_as_of(reader, None, cm) == '20261027'   # 池内 MAX（排除夜盘行）
    assert dom.resolve_as_of(reader, None) == '20261028'       # 无 config → 全表回退（历史行为）
    # 显式 as-of 校验仍按全表任意行（含夜盘行）判定数据存在性
    assert dom.resolve_as_of(reader, '20261028', cm) == '20261028'
    stocks = OverseasStocksDomain()
    cm2 = FakeConfigManager({'overseas_kline_stocks': STOCKS_CFG})
    assert stocks.resolve_as_of(reader, None, cm2) == '20261027'
    # 池配置为空 → 回退全表
    assert dom.resolve_as_of(reader, None, FakeConfigManager({})) == '20261028'
    assert dom.resolve_as_of(reader, None, FakeConfigManager(
        {'overseas_kline_indices': []})) == '20261028'


# ------------------------------------------------------------------ load

def test_overseas_load_change_rate_window_and_fx():
    reader = FakeReader(
        ok_rows=[('hkHSI', '20260928', 100.0), ('hkHSI', '20260929', 110.0),
                 ('hkHSI', '20260930', 121.0)],
        fx=0.8545)
    cm = FakeConfigManager({'overseas_kline_indices': INDICES_CFG})
    dom = OverseasIndicesDomain()
    universe = dom.load_universe(None, cm)
    load = dom.load(reader, ['close', 'change_rate'], '20260930', _ChainStub(),
                    _Opts(), module_days=2, universe=universe)
    kdf = load.kdf
    cr = kdf['change_rate'].tolist()
    assert pd.isna(cr[0])                        # 组内首行 pct_change=NaN
    assert cr[1] == pytest.approx(10.0) and cr[2] == pytest.approx(10.0)
    assert (kdf['is_st'] == 0).all()
    assert load.window_start == '20260928'
    assert '市场自身日历' in load.lookback_desc
    dom2 = OverseasStocksDomain()
    universe2 = dom2.load_universe(None, FakeConfigManager(
        {'overseas_kline_stocks': STOCKS_CFG}))
    load2 = dom2.load(FakeReader(fx=0.9), ['close'], '20260930', _ChainStub(),
                      _Opts(), 0, universe2)
    assert load2.env_extras == {FX_HKDCNY_KEY: 0.9}


def test_overseas_load_rejects_a_close_on_indices_domain():
    dom = OverseasIndicesDomain()
    with pytest.raises(ValueError, match='a_close'):
        dom.load(None, ['close', 'a_close'], '20260930', _ChainStub(), _Opts(),
                 0, [])


def test_a_close_asof_boundary():
    """A 侧回挂：H 行日无 A 行 → 取 ≤该日最近 A 收盘；H 早于 A 首日 → NaN"""
    ok = [('hk01211', '20260904', 100.0), ('hk01211', '20260905', 101.0),
          ('hk01211', '20260908', 102.0)]
    a_rows = {'sz.002594': [('20260905', 300.0), ('20260908', 310.0)]}  # 09-04 无 A 行
    reader = FakeReader(ok_rows=ok, a_rows=a_rows)
    cm = FakeConfigManager({'overseas_kline_stocks': [
        {'code': 'hk01211', 'name': '比亚迪', 'a_code': 'sz.002594'},
        {'code': 'hk00700', 'name': '腾讯'}]})
    dom = OverseasStocksDomain()
    universe = dom.load_universe(None, cm)
    kdf = dom.load(reader, ['close', 'a_close'], '20260908', _ChainStub(),
                   _Opts(), 0, universe).kdf
    by_date = kdf.set_index('trade_date')
    assert by_date.loc['20260904', 'a_close'] is np.nan or \
        pd.isna(by_date.loc['20260904', 'a_close'])       # 早于 A 首行 → NaN
    assert by_date.loc['20260905', 'a_close'] == 300.0    # 当日有 A 行
    assert by_date.loc['20260908', 'a_close'] == 310.0
    tencent = kdf[kdf.ts_code == 'hk00700']
    assert pd.isna(tencent['a_close']).all()              # 无 a_code → 整列 NaN


# ------------------------------------------------------------------ precheck

def _precheck_reader():
    # hkHSI 日历 = 0901~0910 连续 10 个"市场日"
    cal = [f'202609{d:02d}' for d in range(1, 11)]
    rows = []
    for d in cal:
        rows.append(('hkHSI', d, 1.0))
    return FakeReader(ok_rows=rows, calendars={'hkHSI': cal, 'usDJI': cal}), cal


def _run_precheck(stock_rows, missing_mode='gap', universe_codes=('hk00700',)):
    reader, cal = _precheck_reader()
    for code, dates in stock_rows.items():
        for d in dates:
            reader.ok_rows.append((code, d, 100.0))
    dom = OverseasStocksDomain()
    universe = [OverseasStockInfo(ts_code=c, stock_code=c, stock_name=c, board='港股')
                for c in universe_codes]
    opts = _Opts()
    opts.missing_day_mode = missing_mode
    kdf = dom.load(reader, ['close'], '20260910', _ChainStub(), opts, 0, universe).kdf
    return dom.precheck(reader, kdf, '20260910', None, universe, opts)


def test_precheck_tail_lag_boundary():
    """尾部滞后：最近行距锚定日 3 个市场交易日不标；4 个标「数据滞后」"""
    # 锚定 0910：最近行 0907 → 滞后 3（0908/0909/0910）→ 不标
    r = _run_precheck({'hk00700': [f'202609{d:02d}' for d in range(1, 8)]})
    assert 'hk00700' not in r.suspects
    # 最近行 0906 → 滞后 4 → 标
    r2 = _run_precheck({'hk00700': [f'202609{d:02d}' for d in range(1, 7)]})
    reasons = r2.suspects['hk00700']
    assert any(x.startswith(LAG_REASON_PREFIX) for x in reasons)
    # 窗口内无行 → 也标数据滞后
    r3 = _run_precheck({'hk00700': []})
    assert any(x.startswith(LAG_REASON_PREFIX) for x in r3.suspects['hk00700'])


def test_precheck_missing_day_gap_and_suspension():
    """缺日：孤立单日→存疑；连续 ≥2 缺行→疑似停牌不计存疑；off 模式关闭"""
    normal = [f'202609{d:02d}' for d in range(1, 11)]
    # 单日缺 0905
    r = _run_precheck({'hk00700': [d for d in normal if d != '20260905']})
    assert any('缺日' in x for x in r.suspects['hk00700'])
    assert r.missing_count == 1
    # 连续缺 0904~0906 → 疑似停牌（不计存疑），但尾部不滞后（0907~0910 有行）
    r2 = _run_precheck({'hk00700': [d for d in normal
                                    if d not in ('20260904', '20260905', '20260906')]})
    assert 'hk00700' not in r2.suspects
    assert 'hk00700' in r2.suspended
    opts_off = 'off'
    r3 = _run_precheck({'hk00700': [d for d in normal if d != '20260905']},
                       missing_mode=opts_off)
    assert 'hk00700' not in r3.suspects
    assert r3.missing_count == 0


def test_precheck_no_break_rule_for_overseas():
    """外围无前复权断裂规则：单日 ±30% 合法（美股），不产生存疑（回归 §3.3）"""
    reader, cal = _precheck_reader()
    rows = []
    prev = 100.0
    for d in cal:
        rows.append(('hk00700', d, prev))
        prev = prev * 1.3  # 连续 +30%（A 股域会全部判断裂）
    reader.ok_rows.extend(rows)
    dom = OverseasStocksDomain()
    universe = [OverseasStockInfo(ts_code='hk00700', stock_code='hk00700',
                                  stock_name='x', board='港股')]
    kdf = dom.load(reader, ['close'], '20260910', _ChainStub(), _Opts(), 0, universe).kdf
    pre = dom.precheck(reader, kdf, '20260910', None, universe, _Opts())
    assert 'hk00700' not in pre.suspects and pre.break_count == 0


# ------------------------------------------------------------------ 5 新模块

def _ctx(code='hk00700', reasons=None, fx=0.8545, as_of='20260906'):
    stock = OverseasStockInfo(ts_code=code, stock_code=code, stock_name=code,
                              board='港股')
    upstream = {FX_HKDCNY_KEY: fx} if fx is not None else {}
    return ChainContext(as_of=as_of, stock=stock,
                        suspect_reasons=list(reasons or []), upstream=upstream)


def _df(closes, a_closes=None, dates=None):
    import datetime as _dt
    # 缺省日期序列收尾于锚定日 20260906（与 _ctx 缺省 as_of 对齐）
    dates = dates or [( _dt.date(2026, 9, 6) - _dt.timedelta(days=len(closes) - 1 - i))
                      .strftime('%Y%m%d') for i in range(len(closes))]
    data = {'ts_code': 'hkT', 'stock_code': 'hkT', 'trade_date': dates,
            'close': closes, 'volume': [1000.0] * len(closes)}
    if a_closes is not None:
        data['a_close'] = a_closes
    return pd.DataFrame(data)


def test_chg_5d_and_20d_math():
    assert build('chg_5d').analyze(_df([100, 102, 104, 106, 108, 110]),
                                   _ctx()).value == 10.0
    closes = [100.0] * 20 + [110.0]
    assert build('chg_20d').analyze(_df(closes), _ctx()).value == 10.0
    assert build('chg_5d').analyze(_df([100, 101, 102]), _ctx()).value == '数据不足'
    # 锚定日无行（最新行早于锚定日）→ 数据不足
    ctx_stale = _ctx(as_of='20260910')
    assert build('chg_5d').analyze(_df([100, 102, 104, 106, 108, 110]),
                                   ctx_stale).value == '数据不足'


def test_position_60d_math():
    closes = [50.0] * 40 + [100.0, 150.0] + [120.0] * 19   # 窗口 lo=50 hi=150
    r = build('position_60d').analyze(_df(closes), _ctx())
    assert r.value == pytest.approx((120.0 - 50.0) / 100.0 * 100, abs=0.05)
    assert build('position_60d').analyze(_df([1.0] * 10), _ctx()).value == '数据不足'
    flat = build('position_60d').analyze(_df([5.0] * 61), _ctx())
    assert flat.value == '数据不足'  # 窗口无波动


def test_ah_5d_diff_paths():
    closes = [100, 101, 102, 103, 104, 105]
    a_closes = [200, 202, 204, 206, 208, 210]
    # 正常：A 5日=5%，H 5日=5% → 0
    r = build('ah_5d_diff').analyze(_df(closes, a_closes), _ctx())
    assert r.value == 0.0
    # A 涨 10% / H 涨 5% → +5
    r2 = build('ah_5d_diff').analyze(_df(closes, [180, 189, 198, 207, 216, 198]),
                                     _ctx())
    assert r2.value == pytest.approx((198 / 180 - 1) * 100 - 5.0, abs=0.02)
    # 无A股映射（整列 NaN）
    r3 = build('ah_5d_diff').analyze(_df(closes, [np.nan] * 6), _ctx())
    assert r3.value == '无A股映射'
    r3b = build('ah_5d_diff').analyze(_df(closes), _ctx())  # 列不存在
    assert r3b.value == '无A股映射'
    # 未对齐（预检尾部滞后）
    ctx_lag = _ctx(reasons=[f'{LAG_REASON_PREFIX}(最近行20260904距锚定日5个市场交易日)'])
    r4 = build('ah_5d_diff').analyze(_df(closes, a_closes), ctx_lag)
    assert r4.value == '未对齐'
    # A 侧末段有 NaN → 数据不足
    r5 = build('ah_5d_diff').analyze(_df(closes, [np.nan] + a_closes[1:]), _ctx())
    assert r5.value == '数据不足'


def test_ah_premium_paths():
    closes = [100, 101, 102, 103, 104, 105]
    # 正常：a=210, h=105, fx=0.8545 → 210/(105×0.8545)−1 = 134.03%
    r = build('ah_premium').analyze(_df(closes, [200, 202, 204, 206, 208, 210]),
                                    _ctx(fx=0.8545))
    assert r.value == pytest.approx((210 / (105 * 0.8545) - 1) * 100, abs=0.01)
    # fx 缺失 → 汇率待补
    r2 = build('ah_premium').analyze(_df(closes, [200] * 6), _ctx(fx=None))
    assert r2.value == '汇率待补'
    # 无A股映射
    r3 = build('ah_premium').analyze(_df(closes, [np.nan] * 6), _ctx())
    assert r3.value == '无A股映射'
    # 未对齐优先于汇率待补
    ctx_lag = _ctx(fx=None,
                   reasons=[f'{LAG_REASON_PREFIX}(x)'])
    r4 = build('ah_premium').analyze(_df(closes, [200] * 6), ctx_lag)
    assert r4.value == '未对齐'


# ------------------------------------------------------------------ 登记

def test_new_modules_registered_and_documented():
    registry = load_registry()
    for name in ('chg_5d', 'chg_20d', 'position_60d', 'ah_5d_diff', 'ah_premium'):
        assert name in registry
    registered, exists = readme_registered_names()
    assert exists
    missing = set(registry) - registered
    assert not missing, f'未登记 README.MD: {missing}'
