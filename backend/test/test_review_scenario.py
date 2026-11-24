# -*- coding: utf-8 -*-
"""盘前情景判档测试（每日复盘报告优化技术方案 §3.1，测试矩阵 A 组）。

A1：同业务日港美收盘被剔除（防前视，2026-11-23 案例）——库里存在 as_of 当日
    行时仍只取业务日 < as_of 的最新；仅凭两次滚动五日涨幅之差不构成新观测。
A2：真实持平记零变动、同一旧报价重复出现记无新观测、阈值恰为 ±1.5%、两锚
    反向 → 分歧、另一锚缺失 → 部分证据/待核、节假日（跨多日增量）与零缺口
    方向判定。
"""
import pandas as pd

from src.review.scenario import (DIVERGENT, NO_NEW, P1, P2, P3, PARTIAL,
                                 PENDING, build_anchor_obs,
                                 classify_scenario, read_anchor_quote)
from src.review.verifier import verify_predictions


# ------------------------------------------------------------------ 夹具

class _Reader:
    """最小 reader：overseas_kline_day 行 (close, trade_date, first_seen)
    按 market_code 提供；查询按 trade_date < as_of 过滤取最新（SQL 同语义）。"""

    def __init__(self, by_code=None, index_rows=None):
        self.by_code = by_code or {}
        self.index_rows = index_rows or []

    def _query(self, sql, params=None):
        s = str(sql)
        if 'overseas_kline_day' in s:
            code, as_of = params[0], params[1]
            rows = [r for r in self.by_code.get(code, [])
                    if str(r[1]) < str(as_of)]
            rows = sorted(rows, key=lambda r: str(r[1]), reverse=True)
            return [rows[0]] if rows else []
        if 'trade_index_info' in s:
            return self.index_rows
        return []


def _cs(date='20261123'):
    df = pd.DataFrame({'stock_code': ['600010'], 'ts_code': ['sh.600010'],
                       'trade_date': [date], 'change_rate': [0.0],
                       '_limit_up': [False], '_limit_down': [False],
                       '_burst': [False], '_traded': [True],
                       'amount': [1e8], 'volume': [1000]})
    return df


def _payload(gap=1.5, hsi=None, nasdaq=None):
    hsi = {'close': 24613.0, 'date': '20261120', **(hsi or {})}
    nasdaq = {'close': 26861.0, 'date': '20261120', **(nasdaq or {})}
    return {'scenarios': {'gap_pct': gap,
                          'anchors': {'恒生指数': hsi, '纳斯达克': nasdaq}}}


def _obs(name, base_close, base_date, new_close, new_date, **kw):
    quote = ({'close': new_close, 'date': new_date,
              'first_seen': kw.get('first_seen')}
             if new_close is not None else None)
    return build_anchor_obs(name, {'close': base_close, 'date': base_date}, quote)


# ------------------------------------------------------------------ A1 防前视

def test_read_anchor_quote_excludes_same_day_close():
    """A1：恒指 1123 当日收盘（A 股 1123 开盘后才产生）不得成为新观测。"""
    reader = _Reader({'hkHSI': [(24300.0, '20261123', '2026-11-23 17:00'),
                                (24400.0, '20261120', '2026-11-21 08:00')]})
    q = read_anchor_quote(reader, 'hkHSI', '20261123')
    assert q['date'] == '20261120' and q['close'] == 24400.0


def test_a1_same_day_closes_cannot_trigger():
    """A1 集成：库里有 as_of 当日港美收盘（大涨），核验仍无新观测——
    2026-11-23 案例中同日收盘不得进入核验、不得产生 P3。"""
    reader = _Reader({'hkHSI': [(25200.0, '20261123', None),   # +2.4% 若被采用
                                (24613.0, '20261120', None)],  # = 基准 → 无新观测
                      'usIXIC': [(27500.0, '20261123', None),  # +2.4% 若被采用
                                 (26861.0, '20261120', None)]},
                     index_rows=[(3810.0, 3828.0)])
    out = verify_predictions(_payload(), '20261120', _cs(), {}, reader,
                             {'恒生指数': 'hkHSI', '纳斯达克': 'usIXIC'})
    sc = out['scenario']
    assert sc['triggered'] == NO_NEW
    assert sc['direction_eligible'] is False
    assert sc['fulfilled'] is None
    # 旧实现初值 P2 + trade_date<=as_of 会取 1123 行越线成 P3（前视）；
    # 新实现 P3 不可能出现，且也不得退回默认 P2（无新观测 ≠ P2）
    assert sc['triggered'] not in (P2, P3)


def test_a1_holiday_gap_has_new_observation():
    """A1 正向：T 与 N 间隔出海外增量交易日（A 股假期场景）才有新观测。
    基准 1120（周五），as_of=1126（节后首日），恒指 1125 收盘为合格新观测。"""
    reader = _Reader({'hkHSI': [(25000.0, '20261125', None),
                                (24613.0, '20261120', None)],
                      'usIXIC': [(26900.0, '20261125', None),
                                 (26861.0, '20261120', None)]},
                     index_rows=[(3828.0, 3828.0)])
    out = verify_predictions(_payload(), '20261120', _cs('20261126'), {},
                             reader,
                             {'恒生指数': 'hkHSI', '纳斯达克': 'usIXIC'})
    sc = out['scenario']
    # 恒指 +1.57% 越线、纳指 +0.15% 未越线 → 两锚有效 + 单侧越线 → P3
    assert sc['triggered'] == P3
    assert sc['direction_eligible'] is True
    assert sc['expectation'] == '高开'


# ------------------------------------------------------------------ A2 判档规则表

def test_classify_p1_p3_p2_by_threshold():
    """A2：两锚有效更新按 ±1.5% 判 P1/P3；未取整值判定（展示层才舍入）。"""
    # 两锚均跌越线 → P1
    r = classify_scenario(_obs('恒生', 24613, '20261120', 24200, '20261125'),
                          _obs('纳指', 26861, '20261120', 26000, '20261125'), 1.5)
    assert r['triggered'] == P1 and r['direction_eligible'] is True
    # 单侧涨越线（另一锚平稳）→ P3
    r = classify_scenario(_obs('恒生', 24613, '20261120', 24600, '20261125'),
                          _obs('纳指', 26861, '20261120', 27500, '20261125'), 1.5)
    assert r['triggered'] == P3
    # 均未越线 → P2
    r = classify_scenario(_obs('恒生', 24613, '20261120', 24600, '20261125'),
                          _obs('纳指', 26861, '20261120', 26900, '20261125'), 1.5)
    assert r['triggered'] == P2 and r['direction_eligible'] is False


def test_classify_boundary_exactly_threshold():
    """A2：变动恰为 ±1.5%（阈值含边界）→ 越线。"""
    up = 24613.0 * 1.015      # 恰 +1.5%
    r = classify_scenario(_obs('恒生', 24613, '20261120', up, '20261125'),
                          _obs('纳指', 26861, '20261120', 26900, '20261125'), 1.5)
    assert r['triggered'] == P3
    down = 24613.0 * 0.985    # 恰 -1.5%
    r = classify_scenario(_obs('恒生', 24613, '20261120', down, '20261125'),
                          _obs('纳指', 26861, '20261120', 26900, '20261125'), 1.5)
    assert r['triggered'] == P1


def test_classify_same_price_new_session_is_zero_move():
    """A2：真实新收盘与上次同价 → 零变动，属有效新观测（P2 证据），
    与「同一旧报价重复出现」的无新观测区分（依据业务日而非价格）。"""
    r = classify_scenario(_obs('恒生', 24613, '20261120', 24613.0, '20261125'),
                          _obs('纳指', 26861, '20261120', 26861.0, '20261125'), 1.5)
    assert r['triggered'] == P2
    assert r['moves'][0]['accum'] == 0.0
    assert all(m['has_new'] for m in r['moves'])


def test_classify_no_new_observation():
    """A2：两锚最新合格报价与基准同业务日（旧价重复出现）→ 无新观测，
    排除于方向命中率分母（不是 P2）。"""
    r = classify_scenario(
        _obs('恒生', 24613, '20261120', 24613.0, '20261120'),
        _obs('纳指', 26861, '20261120', 26861.0, '20261120'), 1.5)
    assert r['triggered'] == NO_NEW
    assert r['direction_eligible'] is False
    assert '无新观测' in r['reason']


def test_classify_divergent():
    """A2：两锚反向越线 → 分歧（单列，不计方向战绩）。"""
    r = classify_scenario(_obs('恒生', 24613, '20261120', 25200, '20261125'),
                          _obs('纳指', 26861, '20261120', 26000, '20261125'), 1.5)
    assert r['triggered'] == DIVERGENT
    assert r['direction_eligible'] is False


def test_classify_partial_evidence():
    """A2：唯一合格新锚越线、另一锚缺失 → 部分证据（保留方向与缺失原因）。"""
    r = classify_scenario(_obs('恒生', 24613, '20261120', 25200, '20261125'),
                          _obs('纳指', 26861, '20261120', None, None), 1.5)
    assert r['triggered'] == PARTIAL
    assert '恒生' in r['reason']
    assert r['direction_eligible'] is False


def test_classify_pending_cases():
    """A2：可用新锚少于两个且无有效越线 / 基准不可验证 → 待核并列原因。"""
    # 唯一新锚未越线 + 另一锚缺失 → 待核
    r = classify_scenario(_obs('恒生', 24613, '20261120', 24650, '20261125'),
                          _obs('纳指', 26861, '20261120', None, None), 1.5)
    assert r['triggered'] == PENDING
    # 基准缺失 → 待核（基准不可验证）
    r = classify_scenario(_obs('恒生', None, '20261120', 24650, '20261125'),
                          _obs('纳指', 26861, '20261120', 26900, '20261125'), 1.5)
    assert r['triggered'] == PENDING
    assert '基准' in r['reason']


def test_zero_open_gap_unfulfilled_both_directions():
    """A2 补测零缺口（保留既有行为：缺口两位小数按正负判）：
    开盘缺口恰为 0.00 → P1 低开 / P3 高开 均为「不成立」。"""
    for trig, chg in ((P1, 24000.0), (P3, 25200.0)):
        reader = _Reader({'hkHSI': [(chg, '20261125', None),
                                    (24613.0, '20261120', None)],
                          'usIXIC': [(26900.0, '20261125', None),
                                     (26861.0, '20261120', None)]},
                         index_rows=[(3828.0, 3828.0)])   # open == preclose
        out = verify_predictions(_payload(), '20261120',
                                 _cs('20261126'), {}, reader,
                                 {'恒生指数': 'hkHSI', '纳斯达克': 'usIXIC'})
        sc = out['scenario']
        assert sc['triggered'] == trig, f'{trig}: {sc}'
        assert sc['open_gap'] == 0.0
        assert sc['fulfilled'] == '不成立'
        assert sc['direction_eligible'] is True


def test_quoted_fivday_diff_not_construed_as_quote():
    """A1 语义（摘要防线）：恒指两次滚动五日涨幅之差不是报价——新观测只能
    来自 read_anchor_quote 的业务日收盘行；无行即无新观测，不得用任何
    间接差值替代（2026-11-24 摘要 1.24pct 案例）。"""
    # 库中只有基准日行（N 盘前无新收盘行）：任何差值拼不出新观测
    reader = _Reader({'hkHSI': [(24613.0, '20261120', None)],
                      'usIXIC': [(26861.0, '20261120', None)]},
                     index_rows=[(3828.0, 3828.0)])
    out = verify_predictions(_payload(), '20261120', _cs('20261123'), {},
                             reader,
                             {'恒生指数': 'hkHSI', '纳斯达克': 'usIXIC'})
    sc = out['scenario']
    assert sc['triggered'] == NO_NEW
    assert sc['moves'][0]['reason'] is not None
