# -*- coding: utf-8 -*-
"""行业逐日收益与五日复利测试（每日复盘报告优化技术方案 §3.2，测试矩阵 B 组）。

B1：固定数据复利与独立复算一致；一日部分缺股按可得样本计算；一日整日缺失
    时五日值为空、其余可算日仍显示；成员中途变化按当日归属。
B2：榜序按未舍入收益降序、同值按行业名稳定；渲染层同周期今昨比较与
    跨周期排名差互不混写。
"""
import pandas as pd

from src.analysis.sector_metrics import industry_returns
from src.review.sections import sector_mainline


def _panel(rows):
    """rows: (trade_date, ts_code, change_rate, volume) → 最小面板"""
    df = pd.DataFrame(rows, columns=['trade_date', 'ts_code',
                                      'change_rate', 'volume'])
    df['amount'] = df['volume'] * 100.0
    return df


def _member(dates, members):
    """members: {ts_code: (industry, [生效日])} → {date: {ts_code: industry}}"""
    return {d: {ts: ind for ts, (ind, days) in members.items() if d in days}
            for d in dates}


DATES = ['20261117', '20261118', '20261119', '20261120', '20261123']


def _media_rows():
    rows = []
    daily_chgs = {'A': [1.0, 2.0, -1.0, 4.0, 1.0], 'B': [3.0, 2.0, 1.0, 2.0, 1.0]}
    for ts, chgs in daily_chgs.items():
        for d, c in zip(DATES, chgs):
            rows.append((d, f'sh.{ts}', c, 1000))
    return rows


def _media_member():
    return _member(DATES, {'sh.A': ('传媒', DATES), 'sh.B': ('传媒', DATES)})


# ------------------------------------------------------------------ B1 复利

def test_b1_compound_matches_independent_recalc():
    """B1：传媒五日复利与独立复算逐项一致（+8.23%），非旧口径股日均值。"""
    out = industry_returns(_panel(_media_rows()), _media_member(), DATES)
    assert len(out) == 1
    r = out[0]
    # 独立复算：r[d] = (A+B)/2 → +2.0 / +2.0 / 0.0 / +3.0 / +1.0
    # 复利 = 1.02*1.02*1.00*1.03*1.01 − 1 = +8.2328…%
    expect = 100.0 * (1.02 * 1.02 * 1.00 * 1.03 * 1.01 - 1.0)
    assert abs(r['return_5d_pct'] - expect) < 1e-9
    # 旧口径（窗口全部股日均值）= 1.6%——两者显著不同
    assert abs(r['return_5d_pct'] - 1.6) > 1.0
    assert [d['ret_pct'] for d in r['daily']] == [2.0, 2.0, 0.0, 3.0, 1.0]
    assert r['daily_counts'] == [2, 2, 2, 2, 2]
    assert r['unique_stocks'] == 2 and r['stock_days'] == 10
    assert r['days_valid'] == 5


def test_b1_partial_day_missing_stocks():
    """B1：一日部分缺股（B 停牌）→ 当日按可得样本等权、家数对账。"""
    rows = _media_rows()
    rows = [r for r in rows if not (r[0] == '20261119' and r[1] == 'sh.B')]
    out = industry_returns(_panel(rows), _media_member(), DATES)
    r = out[0]
    # D3 仅 A（-1.0）
    assert r['daily'][2] == {'date': '20261119', 'ret_pct': -1.0, 'n': 1}
    assert r['daily_counts'] == [2, 2, 1, 2, 2]
    assert r['stock_days'] == 9
    expect = 100.0 * (1.02 * 1.02 * 0.99 * 1.03 * 1.01 - 1.0)
    assert abs(r['return_5d_pct'] - expect) < 1e-9


def test_b1_whole_day_missing_is_null():
    """B1：整日缺失（行业当日无有效成员）→ 五日值为空、其余可算日仍显示。"""
    rows = [r for r in _media_rows() if r[0] != '20261119']
    out = industry_returns(_panel(rows), _media_member(), DATES)
    r = out[0]
    assert r['return_5d_pct'] is None
    assert r['daily'][2] is None
    assert r['days_valid'] == 4
    # 其余可算日仍可对账
    assert [d['ret_pct'] for d in r['daily'] if d] == [2.0, 2.0, 3.0, 1.0]


def test_b1_membership_change_mid_window():
    """B1：成员中途变化（B 于 D3 移出传媒入军工）→ 逐日按当日归属计算。"""
    rows = _media_rows() + [
        ('20261119', 'sh.C', 2.0, 1000), ('20261120', 'sh.C', 2.0, 1000),
        ('20261123', 'sh.C', 2.0, 1000)]
    member = _member(DATES, {
        'sh.A': ('传媒', DATES),
        'sh.B': ('传媒', DATES[:2] + DATES[3:]),   # B D3 起移出传媒
        'sh.C': ('军工', DATES[2:]),               # C D3 起入军工
    })
    out = {r['industry']: r for r in industry_returns(_panel(rows), member, DATES)}
    media = out['传媒']
    assert media['daily'][2]['n'] == 1            # D3 传媒只剩 A
    assert media['unique_stocks'] == 2
    mil = out['军工']
    assert mil['daily'][0] is None and mil['daily'][1] is None
    assert [d['ret_pct'] for d in mil['daily'] if d] == [2.0, 2.0, 2.0]
    assert mil['return_5d_pct'] is None           # 军工前两日缺 → 五日不可算


def test_b1_suspended_and_invalid_rows_excluded():
    """B1：无成交（volume/amount=0）与收益无效行剔除。"""
    rows = _media_rows() + [
        ('20261123', 'sh.D', 5.0, 0),             # 无成交
        ('20261123', 'sh.E', None, 1000),         # 收益无效
    ]
    member = _media_member()
    member['20261123'].update({'sh.D': '传媒', 'sh.E': '传媒'})
    out = industry_returns(_panel(rows), member, DATES)
    r = out[0]
    assert r['daily'][4]['n'] == 2                # D/E 未计入
    assert r['unique_stocks'] == 2


# ------------------------------------------------------------------ B2 榜序

def test_b2_rank_order_and_tie():
    """B2：榜序按未舍入收益降序；同值按行业名稳定排序；空值不入榜。"""
    rows = []
    member = {'x': '银行', 'y': '采掘', 'z': '房地产'}
    chgs = {'x': [0.4, 0.4, 0.4, 0.4, 0.4],    # +2.02%
            'y': [0.5, 0.5, 0.5, 0.5, 0.5],    # +2.53%
            'z': [0.4, 0.4, 0.4, 0.4, 0.4]}    # 同银行 +2.02%
    for ts, arr in chgs.items():
        for d, c in zip(DATES, arr):
            rows.append((d, f'sz.{ts}', c, 1000))
    memb = {d: {f'sz.{t}': ind for t, ind in member.items()} for d in DATES}
    out = industry_returns(_panel(rows), memb, DATES)
    # 采掘 > 银行 = 房地产（同值按行业名 Unicode 码点稳定排序：
    # 房 U+623F < 银 U+94F6）
    assert [r['industry'] for r in out[:3]] == ['采掘', '房地产', '银行']
    # 未舍入排序：银行/房地产 +2.0202% 精确相等（计算路径同构）


def test_b2_render_same_period_vs_cross_period():
    """B2：渲染层区分同周期今昨比较（新进前十）与跨周期排名差。
    跨周期差异不得混入「新进前十」（方案 §2 第 3 点）。"""
    data = {
        'industry_today': [
            {'industry': '军工', 'mean_change': 3.0, 'stocks': 50,
             'is_concept': False},                        # 仅日榜前 10
            {'industry': '银行', 'mean_change': 1.0, 'stocks': 40,
             'is_concept': False}],
        'industry_5d': [
            {'industry': '银行', 'return_5d_pct': 2.0,
             'daily': [{'ret_pct': 1.0, 'n': 40, 'date': DATES[0]}] * 5,
             'daily_counts': [40] * 5, 'unique_stocks': 40, 'stock_days': 200,
             'days_valid': 5, 'is_concept': False},
            {'industry': '房地产', 'return_5d_pct': 1.5,
             'daily': [{'ret_pct': 0.3, 'n': 30, 'date': DATES[0]}] * 5,
             'daily_counts': [30] * 5, 'unique_stocks': 30, 'stock_days': 150,
             'days_valid': 5, 'is_concept': False}],
        'industry_5d_prev': [
            {'industry': '银行', 'return_5d_pct': 1.9,
             'daily': [], 'daily_counts': [], 'unique_stocks': 40,
             'stock_days': 200, 'days_valid': 5, 'is_concept': False}],
        # 今日榜进 5 日前 10、昨日同周期榜未列 → 新进前十
        'industry_5d_sessions': DATES,
    }
    blocks = sector_mainline.build({'available': True, 'data': data})
    text = '\n'.join(str(b.get('text', '')) + ';'.join(str(i) for i in b.get('items') or [])
                     for b in blocks)
    # 房地产：今日 5 日榜前 10（有效榜第 2）且昨日榜未列 → 同周期新进
    assert '新进入 5 日榜前 10' in text and '房地产' in text
    # 军工仅日榜前 10 → 跨周期差单列（不写入「新进前十」行）
    assert '今日与五日排名差' in text
    switch = next(b for b in blocks if b.get('text') == '高低切换观察（同周期今昨比较）')
    idx_switch = blocks.index(switch)
    idx_cross = next(i for i, b in enumerate(blocks)
                     if b.get('text') == '今日与五日排名差（跨周期）')
    assert idx_cross > idx_switch
    # 军工只出现在跨周期段落，不出现在同周期新进行
    same_period_items = []
    for b in blocks[idx_switch:idx_cross]:
        same_period_items.extend(b.get('items') or [])
    assert all('军工' not in it for it in same_period_items)


def test_b2_render_whole_day_missing_disclosed():
    """B1 渲染：整日缺有效收益的行业单独披露、不入排名。"""
    data = {
        'industry_5d': [
            {'industry': '银行', 'return_5d_pct': 2.0,
             'daily': [{'ret_pct': 0.4, 'n': 40, 'date': DATES[0]}] * 5,
             'daily_counts': [40] * 5, 'unique_stocks': 40, 'stock_days': 200,
             'days_valid': 5, 'is_concept': False},
            {'industry': '传媒', 'return_5d_pct': None,
             'daily': [{'ret_pct': 1.0, 'n': 30, 'date': DATES[0]},
                       None, None, None, None],
             'daily_counts': [30, 0, 0, 0, 0], 'unique_stocks': 30,
             'stock_days': 30, 'days_valid': 1, 'is_concept': False}],
        'industry_5d_sessions': DATES,
    }
    blocks = sector_mainline.build({'available': True, 'data': data})
    text = '\n'.join(str(b.get('text', '')) + str(b.get('items') or '')
                     for b in blocks)
    assert '整日缺有效收益' in text and '传媒' in text and '1/5' in text
    # 排名表中不出现传媒行
    table = next(b for b in blocks if b.get('type') == 'table')
    assert all('传媒' not in str(row[0]) for row in table.get('rows', []))
