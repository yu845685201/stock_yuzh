"""赚钱效应/亏钱效应聚合单测（四期方案 §2；money_effect.py 纯函数）。

截面为合成最小列集（判定列由 annotate_panel 语义直接给定），验证：
反馈矩阵五分类互斥与板位分组 / 停牌单列 / 炸板修复 / 连板溢价 / 大面 / 分档分布。
"""
import pandas as pd

from src.review.money_effect import money_effect


def _cs(rows, date='20260930'):
    """rows: [(stock_code, change, up, down, burst, streak)] → 截面 DataFrame"""
    df = pd.DataFrame(rows, columns=['stock_code', 'change_rate', '_limit_up',
                                     '_limit_down', '_burst', '_streak'])
    df.insert(0, 'ts_code', 'sh.' + df['stock_code'].astype(str))
    df.insert(1, 'trade_date', date)
    df['_limit_up'] = df['_limit_up'].astype(bool)
    df['_limit_down'] = df['_limit_down'].astype(bool)
    df['_burst'] = df['_burst'].astype(bool)
    df['_streak'] = df['_streak'].astype('int64')
    df['amount'] = 1e7
    df['volume'] = 1000
    df['_traded'] = True
    return df


def test_feedback_matrix_five_categories():
    """昨日首板 5 只：今日覆盖跌停/跌超5%/小跌/收红/晋级涨停 各 1（互斥优先级）。"""
    prev = _cs([
        ('600001', 10.0, True, False, False, 1),   # 昨日首板
        ('600002', 10.0, True, False, False, 1),
        ('600003', 10.0, True, False, False, 1),
        ('600004', 10.0, True, False, False, 1),
        ('600005', 10.0, True, False, False, 1),
    ], date='20260929')
    today = _cs([
        ('600001', -10.0, False, True, False, 0),   # 跌停
        ('600002', -6.0, False, False, False, 0),   # 跌超5%（未跌停）
        ('600003', -1.0, False, False, False, 0),   # 小跌
        ('600004', 3.0, False, False, False, 0),    # 收红
        ('600005', 10.0, True, False, False, 2),    # 晋级涨停
    ])
    out = money_effect(today, prev)
    fb = out['feedback']
    assert fb is not None and fb['suspended'] == 0
    row = next(r for r in fb['rows'] if r['bucket'] == '首板')
    assert row['count'] == 5
    assert row['cats']['跌停'] == 1 and row['cats']['跌超5%'] == 1
    assert row['cats']['小跌'] == 1 and row['cats']['收红'] == 1
    assert row['cats']['晋级涨停'] == 1
    assert row['limit_rate'] == 0.2
    total = next(r for r in fb['rows'] if r['bucket'] == '合计')
    assert total['count'] == 5


def test_feedback_matrix_bucket_and_suspended():
    """4 板+ 分桶与停牌单列：4板股归 4板+；一只昨日涨停今日无行计停牌。"""
    prev = _cs([
        ('600001', 10.0, True, False, False, 4),
        ('600002', 10.0, True, False, False, 5),
        ('600003', 10.0, True, False, False, 1),
    ], date='20260929')
    today = _cs([
        ('600001', 2.0, False, False, False, 0),
        ('600002', -2.0, False, False, False, 0),
        # 600003 今日缺行（停牌）
    ])
    out = money_effect(today, prev)
    fb = out['feedback']
    assert fb['suspended'] == 1
    big = next(r for r in fb['rows'] if r['bucket'] == '4板+')
    assert big['count'] == 2 and big['cats']['收红'] == 1 and big['cats']['小跌'] == 1


def test_burst_recovery_and_consec_premium():
    """昨日炸板 2 只：今日一红一绿；昨日 2 板+ 1 只今日 +5%。"""
    prev = _cs([
        ('600001', 5.0, False, False, True, 0),   # 昨日炸板
        ('600002', 5.0, False, False, True, 0),   # 昨日炸板
        ('600003', 10.0, True, False, False, 2),  # 昨日 2 板
    ], date='20260929')
    today = _cs([
        ('600001', 4.0, False, False, False, 0),
        ('600002', -3.0, False, False, False, 0),
        ('600003', 5.0, False, False, False, 0),
    ])
    out = money_effect(today, prev)
    br = out['burst_recovery']
    assert br['count'] == 2 and br['traded'] == 2
    assert br['mean'] == 0.5 and br['red_rate'] == 0.5 and br['relimit'] == 0
    cp = out['consec_premium']
    assert cp['count'] == 1 and cp['mean'] == 5.0 and cp['red_rate'] == 1.0


def test_loss_effect_and_change_dist():
    """大面：跌超5%/7% 与最惨跌幅；分档累计口径单调不增。"""
    today = _cs([
        ('600001', -10.0, False, True, False, 0),
        ('600002', -7.5, False, False, False, 0),
        ('600003', -5.2, False, False, False, 0),
        ('600004', 3.0, False, False, False, 0),
        ('600005', 12.0, True, False, False, 1),
        ('600006', 1.0, False, False, False, 0),
    ])
    out = money_effect(today, _cs([], date='20260929'))
    loss = out['loss']
    assert loss['traded'] == 6
    assert loss['down5'] == 3 and loss['down7'] == 2 and loss['limit_down'] == 1
    assert loss['worst'] == -10.0
    dist = {d['bucket']: d['count'] for d in out['change_dist']}
    assert dist['涨≥10%'] == 1 and dist['涨≥2%'] == 2
    assert dist['跌≤-2%'] == 3 and dist['跌≤-10%'] == 1
    # 单调性：累计口径档位越高家数越少
    ups = [dist[f'涨≥{t}%'] for t in (2, 4, 6, 8, 10)]
    assert ups == sorted(ups, reverse=True)


def test_empty_prev_graceful():
    """昨日截面为空：反馈/修复/溢价为 None，全市场口径仍可用。"""
    today = _cs([('600001', 2.0, False, False, False, 0)])
    out = money_effect(today, pd.DataFrame())
    assert out['feedback'] is None
    assert out['burst_recovery'] is None
    assert out['consec_premium'] is None
    assert out['loss']['traded'] == 1
    assert out['change_dist'][0]['bucket'] == '涨≥2%'
