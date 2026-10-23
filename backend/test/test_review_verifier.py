"""复盘核验器单测（四期方案 §1；verifier.py 纯函数 + stub reader）。

覆盖：预期抽取 build_predictions / 候选池战绩与超额基准 / V2 三态 /
情景触发与兑现 / 滚动战绩聚合。三态规范：数据不足 ≠ 判错。
"""
import pandas as pd

from src.review.verifier import (build_predictions, rolling_stats,
                                 verify_predictions)


# ------------------------------------------------------------------ 夹具

def _cs(rows, date='20260930'):
    """rows: [(stock_code, change, up, down, burst)] → 最小截面"""
    df = pd.DataFrame(rows, columns=['stock_code', 'change_rate', '_limit_up',
                                     '_limit_down', '_burst'])
    df.insert(0, 'ts_code', 'sh.' + df['stock_code'].astype(str))
    df.insert(1, 'trade_date', date)
    for col in ('_limit_up', '_limit_down', '_burst'):
        df[col] = df[col].astype(bool)
    df['amount'] = 1e8
    df['volume'] = 1000
    df['_traded'] = True
    return df


class _StubReader:
    """DataReader 替身：overseas_rows 按 market_code（首个查询参数）区分返回"""

    def __init__(self, overseas_rows=None, index_rows=None, verify_rows=None,
                 overseas_by_code=None):
        self.overseas_rows = overseas_rows or []
        self.index_rows = index_rows or []
        self.verify_rows = verify_rows or []
        self.overseas_by_code = overseas_by_code or {}

    def _query(self, sql, params=None):
        s = str(sql)
        if 'overseas_kline_day' in s:
            code = params[0] if params else None
            if code in self.overseas_by_code:
                return [(self.overseas_by_code[code],)]
            return self.overseas_rows
        if 'trade_index_info' in s:
            return self.index_rows
        if 'review_verification_daily' in s:
            return self.verify_rows
        return []


def _pred_payload(**overrides):
    payload = {
        'sentiment': {'position': '回暖/震荡', 'max_streak': 7,
                      'limit_up_count': 56, 'burst_rate': 0.291,
                      'premium_mean': 1.3},
        'verify_points': {
            'V1': {'base_yi': 14327.0, 'band_pct': 20},
            'V1.5': {'quads': [{'name': '上证指数', 'quad': '缩量涨'}]},
            'V2': {'code': '600825', 'name': '新华传媒', 'streak': 7},
            'V3': {'premium_mean': 1.30, 'limit_up_count': 56},
        },
        'scenarios': {'gap_pct': 1.5,
                      'anchors': {'恒生指数': {'close': 24613.0, 'date': '20260930'},
                                  '纳斯达克': {'close': 26861.0, 'date': '20260930'}}},
        'candidates': {
            'R1': [{'ts_code': 'sh.600001', 'code': '600001', 'name': '甲',
                    'label': '7板/开板0次', 'evidence_count': 2}],
            'R2': [{'ts_code': 'sh.600002', 'code': '600002', 'name': '乙',
                    'label': '首板/创新药', 'evidence_count': 3}],
            'R3': [], 'R4': [],
        },
        'version': 1,
    }
    payload.update(overrides)
    return payload


# ------------------------------------------------------------------ 预期抽取

def test_build_predictions_from_agg():
    agg = {
        'sentiment': {'data': {'max_streak': 7, 'limit_up_count': 56,
                               'burst_rate': 0.291, 'premium_mean': 1.3,
                               'cycle': {'position': '回暖/震荡'}}},
        'market_env': {'data': {'turnover': {'today_total': 1.4327e12},
                                'overseas_indices': [
                                    {'name': '恒生指数', 'close': 24613.27, 'date': '20260930'},
                                    {'name': '纳斯达克', 'close': 26861.06, 'date': '20260930'}]}},
        'volume_price': {'data': {'anchors': [{'name': '上证指数', 'quad': '缩量涨'}]}},
        'movers': {'data': {'limit_up': [{'code': '600825', 'name': '新华传媒',
                                          'streak': 7}]}},
        'candidates': {'R1': [{'ts_code': 'sh.600825', 'code': '600825',
                               'name': '新华传媒', 'label': '7板', 'evidence_count': 2}],
                       'R2': [], 'R3': [], 'R4': []},
    }
    pred = build_predictions(agg, {'tomorrow_plan': {'overseas_gap_pct': 1.5,
                                                     'volume_band_pct': 20}})
    assert pred['verify_points']['V1'] == {'base_yi': 14327.0, 'band_pct': 20}
    assert pred['verify_points']['V2']['code'] == '600825'
    assert pred['scenarios']['anchors']['恒生指数']['close'] == 24613.27
    assert pred['candidates']['R1'][0]['code'] == '600825'
    assert pred['sentiment']['position'] == '回暖/震荡'


# ------------------------------------------------------------------ 核验

def test_verify_candidates_excess_and_states():
    """候选池战绩：超额 = 池中位 − 全市场中位；涨/炸/跌分类计数。"""
    today = _cs([
        ('600001', 10.0, True, False, False),    # R1 甲 涨停
        ('600002', -2.0, False, False, False),   # R2 乙 小跌
        ('600010', 1.0, False, False, False),    # 全市场样本
        ('600011', -1.0, False, False, False),
        ('600012', 0.0, False, False, False),
    ])
    metrics = {'premium_mean': 2.0, 'limit_up_count': 1}
    reader = _StubReader(
        overseas_rows=[(24131.0,)],           # 恒指最新收盘（-1.96% → P1 触发）
        index_rows=[(3810.0, 3828.0)],        # 上证 open/preclose → 低开
    )
    out = verify_predictions(_pred_payload(), '20260929', today, metrics, reader,
                             {'恒生指数': 'hkHSI', '纳斯达克': 'usIXIC'})
    cand = out['candidates']
    assert cand['market_median'] == 0.0
    r1 = cand['pools']['R1']
    assert r1['traded'] == 1 and r1['limit_up_n'] == 1
    assert r1['median'] == 10.0 and r1['excess_median'] == 10.0
    r2 = cand['pools']['R2']
    assert r2['median'] == -2.0 and r2['excess_median'] == -2.0


def test_verify_points_v2_three_states():
    """V2 三态：晋级=成立 / 断板=不成立 / 无行=数据不足。"""
    metrics = {'premium_mean': 2.0}
    today_ok = _cs([('600825', 10.0, True, False, False),
                    ('600010', 0.0, False, False, False)])
    today_break = _cs([('600825', -1.0, False, False, False),
                       ('600010', 0.0, False, False, False)])
    today_suspended = _cs([('600010', 0.0, False, False, False)])
    reader = _StubReader()
    for today, expect in ((today_ok, '成立'), (today_break, '不成立'),
                          (today_suspended, '数据不足')):
        out = verify_predictions(_pred_payload(), '20260929', today, metrics,
                                 reader, {})
        v2 = next(p for p in out['verify_points'] if p['key'] == 'V2 梯队')
        assert v2['verdict'] == expect, f'{expect}: {v2}'


def test_verify_scenario_trigger_and_fulfillment():
    """情景：恒指累计 ≤ -1.5% → P1 触发；上证低开 → 低开预期成立。"""
    metrics = {'premium_mean': 2.0}
    today = _cs([('600010', 0.0, False, False, False)])
    reader = _StubReader(overseas_rows=[(24131.0,)],   # 24131/24613-1 ≈ -1.96%
                         index_rows=[(3810.0, 3828.0)])
    out = verify_predictions(_pred_payload(), '20260929', today, metrics, reader,
                             {'恒生指数': 'hkHSI', '纳斯达克': 'usIXIC'})
    sc = out['scenario']
    assert sc['triggered'] == 'P1'
    assert sc['expectation'] == '低开'
    assert sc['fulfilled'] == '成立'


def test_verify_scenario_p2_no_fulfillment():
    """P2 平稳情景：无方向预期 → fulfilled=None（不计入战绩）。"""
    metrics = {'premium_mean': 2.0}
    today = _cs([('600010', 0.0, False, False, False)])
    reader = _StubReader(
        overseas_by_code={'hkHSI': 24600.0, 'usIXIC': 26900.0},  # ≈0% → P2
        index_rows=[(3828.0, 3828.0)])
    out = verify_predictions(_pred_payload(), '20260929', today, metrics, reader,
                             {'恒生指数': 'hkHSI', '纳斯达克': 'usIXIC'})
    assert out['scenario']['triggered'] == 'P2'
    assert out['scenario']['fulfilled'] is None


def test_v3_direction_with_eps():
    """V3 溢价方向：eps=0.5pct 内算持平。"""
    today = _cs([('600010', 0.0, False, False, False)])
    reader = _StubReader()
    out = verify_predictions(_pred_payload(), '20260929', today,
                             {'premium_mean': 1.6}, reader, {})   # +0.3pct
    v3 = next(p for p in out['verify_points'] if p['key'] == 'V3 溢价')
    assert '持平' in v3['actual']
    out2 = verify_predictions(_pred_payload(), '20260929', today,
                              {'premium_mean': 2.5}, reader, {})   # +1.2pct
    v3b = next(p for p in out2['verify_points'] if p['key'] == 'V3 溢价')
    assert '上升' in v3b['actual']


# ------------------------------------------------------------------ 滚动战绩

def test_rolling_stats_aggregation():
    """滚动战绩：超额均值/胜率/情景兑现/V2 晋级 跨日聚合。"""
    def _verify_row(trigger, fulfilled, v2_ok, excess):
        return {'scenario': {'triggered': trigger, 'fulfilled': fulfilled,
                             'expectation': {'P1': '低开', 'P3': '高开'}.get(trigger)},
                'verify_points': [{'key': 'V2 梯队', 'verdict': v2_ok}],
                'candidates': {'market_median': 0.0, 'market_limit_rate': 0.01,
                               'pools': {'R1': {'count': 5, 'traded': 5,
                                                'median': excess,
                                                'excess_median': excess,
                                                'limit_up_n': 1}}}}

    history = [
        ('20260926', _verify_row('P1', '成立', '成立', 2.0)),
        ('20260927', _verify_row('P1', '不成立', '不成立', -1.0)),
        ('20260928', _verify_row('P3', '成立', '成立', 0.5)),
    ]
    verify_rows = [(p,) for _, p in history]   # SELECT payload → 单列元组
    reader = _StubReader(verify_rows=verify_rows)
    current = _verify_row('P1', '成立', '不成立', 3.0)
    stats = rolling_stats('20260930', current, reader)
    assert stats['days'] == 4
    r1 = stats['pools']['R1']
    assert r1['days'] == 4
    assert r1['win_rate'] == 0.75          # 2,0.5,3 正 / -1 负
    assert r1['avg_excess'] == round((2.0 - 1.0 + 0.5 + 3.0) / 4, 2)
    assert stats['scenario']['P1'] == {'n': 3, 'ok': 2}
    assert stats['scenario']['P3'] == {'n': 1, 'ok': 1}
    assert stats['v2'] == {'n': 4, 'ok': 2}   # 历史成立2 + 当日不成立
