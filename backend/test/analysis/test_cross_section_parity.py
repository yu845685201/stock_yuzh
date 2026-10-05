"""报告层 cross_section 对拍用例（复盘报告层方案 §5.7.1——独立重算正确性的保证）。

同一份合成面板，链模块逐股判定（真实 ChainContext 调用）vs cross_section 向量化
截面，结果必须一致。覆盖主板/主板ST/创业板/北交所、连板、炸板、跌停、停牌缺行、
停牌标记行（volume=0）等形态。
"""
import numpy as np
import pandas as pd
import pytest

from src.analysis.limit_up_rules import board_of
from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
from src.analysis.modules.big_mover import BigMover
from src.analysis.modules.burst_limit import BurstLimit
from src.analysis.modules.consecutive_limit_up import ConsecutiveLimitUp
from src.analysis.modules.limit_down_today import LimitDownToday
from src.analysis.modules.limit_up_premium import LimitUpPremium
from src.analysis.modules.limit_up_today import LimitUpToday
from src.analysis.modules.new_high_low import NewHighLow
from src.analysis.modules.volume_surge import VolumeSurge
from src.review.cross_section import (
    VOLUME_SURGE_WINDOW,
    annotate_panel,
    cross_section_at,
)

DATES = [d.strftime('%Y%m%d') for d in pd.bdate_range('2026-08-03', periods=30)]
ANCHOR = DATES[-1]

# ---- 合成面板：4 只股票 × 30 交易日 ----
# 600001 主板：11~13 连板 3 日、14 炸板、15~16 再连板、18 跌停、21 停牌标记行
# 600002 主板 ST（is_st=1）：偶发涨停（5% 阈值）
# 300001 创业板：20% 阈值，一次 19.95 涨停
# 920001 北交所：30% 阈值，价格平稳（第 22 日整行缺测=停牌缺行）


def _norm(i):
    return {'change_rate': 1.0, 'open': 10.0, 'high': 10.2, 'low': 9.9,
            'close': 10.1, 'preclose': 10.0, 'volume': 1000 + i,
            'amount': 10000.0}


def _limit_up_row(i, preclose, pct_change=9.95):
    c = round(preclose * (1 + pct_change / 100), 2)
    return {'change_rate': round((c - preclose) / preclose * 100, 4),
            'open': preclose, 'high': c, 'low': preclose,
            'close': c, 'preclose': preclose, 'volume': 2000 + i,
            'amount': c * 2000}


def _burst_row(i, preclose):
    high = round(preclose * 1.0995, 2)
    close = round(preclose * 1.02, 2)
    return {'change_rate': round((close - preclose) / preclose * 100, 4),
            'open': preclose, 'high': high, 'low': preclose,
            'close': close, 'preclose': preclose, 'volume': 1500,
            'amount': close * 1500}


def _limit_down_row(i, preclose):
    c = round(preclose * 0.9005, 2)
    return {'change_rate': round((c - preclose) / preclose * 100, 4),
            'open': preclose, 'high': preclose, 'low': c,
            'close': c, 'preclose': preclose, 'volume': 1800,
            'amount': c * 1800}


def _halted_row(i, preclose):
    """停牌标记行：volume=0、价格定格"""
    return {'change_rate': 0.0, 'open': preclose, 'high': preclose,
            'low': preclose, 'close': preclose, 'preclose': preclose,
            'volume': 0, 'amount': 0.0}


def _build_stock(code, is_st):
    rows = []
    preclose = 10.0
    for i, d in enumerate(DATES):
        if code == '600001':
            if i == 21:
                r = _halted_row(i, preclose)
            elif i in (11, 12, 13):
                r = _limit_up_row(i, preclose)
            elif i == 14:
                r = _burst_row(i, preclose)
            elif i in (15, 16):
                r = _limit_up_row(i, preclose)
            elif i == 18:
                r = _limit_down_row(i, preclose)
            else:
                r = _norm(i)
        elif code == '600002':
            r = _limit_up_row(i, preclose, 4.95) if i % 5 == 3 else _norm(i)
        elif code == '300001':
            r = _limit_up_row(i, preclose, 19.95) if i == 20 else _norm(i)
        else:  # 920001
            r = _norm(i)
        r = {'ts_code': f'sh.{code}' if code.startswith('6') else
                        (f'sz.{code}' if code.startswith(('0', '3')) else f'bj.{code}'),
             'stock_code': code, 'stock_name': f'股{code}',
             'trade_date': d, 'is_st': 1 if is_st else 0, **r}
        preclose = r['close']
        rows.append(r)
    # 920001 第 22 日整行缺测（停牌缺行，前一可得交易日语义）
    if code == '920001':
        rows = [r for r in rows if r['trade_date'] != DATES[22]]
    return rows


def _panel():
    rows = []
    for code, is_st in (('600001', False), ('600002', True),
                        ('300001', False), ('920001', False)):
        rows.extend(_build_stock(code, is_st))
    df = pd.DataFrame(rows)
    for col in ('change_rate', 'open', 'high', 'low', 'close', 'preclose',
                'volume', 'amount', 'is_st'):
        df[col] = pd.to_numeric(df[col])
    df['is_st'] = df['is_st'].astype('int8')
    return df


MODULES = {
    'limit_up': LimitUpToday,
    'limit_down': LimitDownToday,
    'burst': BurstLimit,
    'streak': ConsecutiveLimitUp,
    'premium': LimitUpPremium,
    'big_move': BigMover,
    'volume_surge': VolumeSurge,
    'new_high_low': NewHighLow,
}


def _chain_values(code, anchor, panel):
    """链模块逐股判定（df 为该股 anchor 前全量窗口，与引擎行为一致）"""
    stock_df = panel[(panel['stock_code'] == code) & (panel['trade_date'] <= anchor)]
    stock_df = stock_df.sort_values('trade_date')
    ctx = ChainContext(
        as_of=anchor,
        stock=StockInfo(ts_code=f'sh.{code}', stock_code=str(code),
                        stock_name=f'股{code}', board=board_of(str(code))))
    values = {}
    for key, cls in MODULES.items():
        kwargs = {}
        if key == 'new_high_low':
            kwargs['params'] = {'window': 60}
        result = cls(**kwargs).analyze(stock_df, ctx)
        values[key] = result.value
    return values


class TestCrossSectionParity:
    @pytest.fixture(scope='class')
    def panel(self):
        return _panel()

    @pytest.mark.parametrize('code', ['600001', '600002', '300001', '920001'])
    def test_parity_with_chain_modules(self, panel, code):
        cs_panel = annotate_panel(panel)
        checked = 0
        for anchor in DATES[-8:]:  # 覆盖连板/炸板/跌停/停牌窗口
            cs = cross_section_at(cs_panel, anchor)
            row = cs[cs['stock_code'] == code]
            chain = _chain_values(code, anchor, panel)
            if row.empty:
                # 停牌缺行：链各模块应判 数据不足 或 前一可得交易日口径（premium 已有定义）
                continue
            row = row.iloc[0]
            # 涨停
            if chain['limit_up'] != '数据不足':
                assert bool(row['_limit_up']) == (chain['limit_up'] == '是'), \
                    f"{code}@{anchor} limit_up"
            # 跌停
            if chain['limit_down'] != '数据不足':
                assert bool(row['_limit_down']) == (chain['limit_down'] == '是'), \
                    f"{code}@{anchor} limit_down"
            # 炸板
            if chain['burst'] != '数据不足':
                assert bool(row['_burst']) == (chain['burst'] == '是'), \
                    f"{code}@{anchor} burst"
            # 连板数
            if chain['streak'] != '数据不足':
                assert int(row['_streak']) == int(chain['streak']), \
                    f"{code}@{anchor} streak: {row['_streak']} vs {chain['streak']}"
            # 大涨大跌
            if chain['big_move'] == '数据不足':
                assert row['_big_move'] == '数据不足', f"{code}@{anchor} big_move"
            else:
                assert row['_big_move'] == chain['big_move'], f"{code}@{anchor} big_move"
            # 昨日涨停溢价
            if chain['premium'] == '否':
                assert not bool(row['_prev_limit_up']), f"{code}@{anchor} premium-否"
            elif isinstance(chain['premium'], float):
                assert bool(row['_prev_limit_up']), f"{code}@{anchor} premium-值"
                assert round(float(row['_premium']), 2) == pytest.approx(chain['premium'])
            # 放量倍数
            if isinstance(chain['volume_surge'], float):
                assert float(row['_volume_surge']) == pytest.approx(chain['volume_surge']), \
                    f"{code}@{anchor} volume_surge"
            elif chain['volume_surge'] == '数据不足' and not bool(row['volume'] > 0):
                assert np.isnan(row['_volume_surge'])
            # 新高新低
            if chain['new_high_low'] in ('新高', '新低', '否'):
                expect = {'新高': True, '新低': False, '否': None}[chain['new_high_low']]
                if expect is None:
                    assert not bool(row['_new_high']) and not bool(row['_new_low'])
                else:
                    assert bool(row['_new_high']) == expect or bool(row['_new_low']) == expect
            checked += 1
        assert checked > 0

    def test_sentiment_counts(self, panel):
        cs_panel = annotate_panel(panel)
        cs = cross_section_at(cs_panel, ANCHOR)
        assert int(cs['_limit_up'].sum()) == 0  # 锚定日无涨停（末段为常态行）
        # 第 13 日截面：600001 三连板末日 + 600002（ST，i=13 为 5% 涨停日）
        cs13 = cross_section_at(cs_panel, DATES[13])
        assert int(cs13['_limit_up'].sum()) == 2
        assert int(cs13.loc[cs13['stock_code'] == '600001', '_streak'].iloc[0]) == 3
        assert int(cs13.loc[cs13['stock_code'] == '600002', '_streak'].iloc[0]) == 1
        cs14 = cross_section_at(cs_panel, DATES[14])
        assert bool(cs14.loc[cs14['stock_code'] == '600001', '_burst'].iloc[0])
        # ST 主板 5% 阈值生效（4.95 涨幅判涨停）
        cs_st = cross_section_at(cs_panel, DATES[3])
        assert bool(cs_st.loc[cs_st['stock_code'] == '600002', '_limit_up'].iloc[0])
