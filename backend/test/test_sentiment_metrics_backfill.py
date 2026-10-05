"""情绪指标日频回填测试（开发项 B，方案 4.2）

- 回填行与逐日 cross_section_at 手工计算一致（口径红线：同一实现产出）；
- 表行 JSONB 往返后与截面重算键值一致（报告层查表路径 = 回退重算路径）；
- 聚合器先查表/miss 回退两路径均可用；分位注记样本线（<30 不出）。
"""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.review.aggregator import ReviewAggregator
from src.review.cross_section import annotate_panel, cross_section_at, sentiment_metrics
from src.sync.sentiment_metrics_manager import SentimentMetricsManager

DATES = [d.strftime('%Y%m%d') for d in pd.bdate_range('2026-09-01', periods=12)]
ANCHOR, PREV = DATES[-1], DATES[-2]


def _norm(i, chg):
    return {'change_rate': chg, 'open': 10.0, 'high': 10.2, 'low': 9.9,
            'close': round(10.0 * (1 + chg / 100), 2), 'preclose': 10.0,
            'volume': 1000 + i, 'amount': 10000.0}


def _limit_up_row(i, preclose):
    c = round(preclose * 1.0995, 2)
    return {'change_rate': round((c - preclose) / preclose * 100, 4),
            'open': preclose, 'high': c, 'low': preclose,
            'close': c, 'preclose': preclose, 'volume': 2000 + i, 'amount': c * 2000}


def _burst_row(i, preclose):
    high = round(preclose * 1.0995, 2)
    close = round(preclose * 1.02, 2)
    return {'change_rate': round((close - preclose) / preclose * 100, 4),
            'open': preclose, 'high': high, 'low': preclose,
            'close': close, 'preclose': preclose, 'volume': 1500, 'amount': close * 1500}


def _stock(code, ts_prefix, kinds):
    """kinds: 逐日 ('norm', chg) / 'limit_up' / 'burst'"""
    rows = []
    preclose = 10.0
    for i, d in enumerate(DATES):
        kind = kinds[i]
        if isinstance(kind, tuple):     # ('norm', chg)
            r = _norm(i, kind[1])
        elif kind == 'limit_up':
            r = _limit_up_row(i, preclose)
        elif kind == 'burst':
            r = _burst_row(i, preclose)
        else:
            r = _norm(i, kind)
        rows.append({'ts_code': f'{ts_prefix}.{code}', 'stock_code': code,
                     'stock_name': f'股{code}', 'trade_date': d, 'is_st': 0, **r})
        preclose = r['close']
    return rows


def _panel():
    # 600001 主板：末日两连板（今日 2 板、昨日 1 板 -> 晋级率可算）
    # 600002 主板：末日炸板（触板未封；用主板 10cm 阈值——创业板 20cm 下 9.95% 不触板）
    kinds_a = [('norm', 0.5 if i % 2 == 0 else -0.5) for i in range(10)] \
        + ['limit_up', 'limit_up']
    kinds_b = [('norm', -0.5 if i % 2 == 0 else 0.5) for i in range(9)] \
        + [('norm', 1.0), ('norm', -1.0), 'burst']
    df = pd.DataFrame(_stock('600001', 'sh', kinds_a) + _stock('600002', 'sh', kinds_b))
    for col in ('change_rate', 'open', 'high', 'low', 'close', 'preclose',
                'volume', 'amount', 'is_st'):
        df[col] = pd.to_numeric(df[col])
    df['is_st'] = df['is_st'].astype('int8')
    return df


def _prepared_panel():
    panel = annotate_panel(_panel())
    return panel  # suppress_first_day 无上市日（未提供 list_dates）跳过——小面板无新股


# ---------- 回填行 = 逐日手工计算 ----------

def test_metrics_row_matches_cross_section():
    panel = _prepared_panel()
    cs_today = cross_section_at(panel, ANCHOR)
    cs_prev = cross_section_at(panel, PREV)
    row = SentimentMetricsManager._metrics_row(ANCHOR, cs_today, cs_prev)

    assert row['limit_up_count'] == int(cs_today['_limit_up'].sum()) == 1
    assert row['burst_count'] == int(cs_today['_burst'].sum()) == 1
    streaks = cs_today.loc[cs_today['_limit_up'], '_streak']
    assert row['max_streak'] == int(streaks.max())
    # JSON 对象键为字符串——按还原口径（int 键）断言
    assert {int(k): v for k, v in json.loads(row['ladder']).items()} == {2: 1}
    # 晋级率：今日 2 板 1 只 ÷ 昨日 1 板 1 只
    promotion = {int(k): v for k, v in json.loads(row['promotion']).items()}
    assert promotion[2]['rate'] == pytest.approx(1.0)
    # 涨跌平家数（有成交行）：末日 1 涨（涨停）+1 跌（炸板收 1.02>0? 炸板收涨）……
    change = pd.to_numeric(cs_today['change_rate'], errors='coerce')
    traded = cs_today['_traded'].fillna(False).astype(bool)
    assert row['up_count'] == int((traded & change.gt(0)).sum())
    assert row['down_count'] == int((traded & change.lt(0)).sum())
    assert row['up_count'] + row['down_count'] + row['flat_count'] == int(traded.sum())
    assert row['stock_count'] == int(cs_today['ts_code'].nunique())


# ---------- 表行往返 = 截面重算 ----------

def test_table_row_roundtrip_matches_recompute():
    panel = _prepared_panel()
    cs_today = cross_section_at(panel, ANCHOR)
    cs_prev = cross_section_at(panel, PREV)
    row = SentimentMetricsManager._metrics_row(ANCHOR, cs_today, cs_prev)
    direct = sentiment_metrics(cs_today, cs_prev)

    # 模拟 JSONB 往返：入库 json.dumps -> 读出 dict
    stored = dict(row)
    stored['ladder'] = json.loads(stored['ladder'])
    stored['promotion'] = json.loads(stored['promotion'])
    metrics = ReviewAggregator._sentiment_row_to_metrics(stored)

    for key in ('limit_up_count', 'limit_down_count', 'burst_count', 'burst_rate',
                'max_streak', 'premium_mean', 'premium_median', 'premium_count',
                'premium_continued'):
        assert metrics[key] == direct[key], key
    assert metrics['ladder'] == direct['ladder']
    assert metrics['promotion'] == direct['promotion']


# ---------- 聚合器先查表 / miss 回退 ----------

class FakeReader:
    """_query 桩：按 SQL 片段分发（单日行 / 分位历史）"""

    def __init__(self, today_row=None, history=None, fail=False):
        self.today_row = today_row
        self.history = history or []
        self.fail = fail

    def _query(self, sql, params=None):
        if self.fail:
            raise RuntimeError('db down')
        if 'LIMIT 251' in sql:
            return self.history
        return [self.today_row] if self.today_row else []


def _make_aggregator(reader):
    agg = ReviewAggregator.__new__(ReviewAggregator)
    agg.db = reader
    agg.config = {}
    agg.sentiment_rules = {}
    agg.sentiment_calibrated = False
    return agg


def test_aggregate_sentiment_prefers_table_row():
    panel = _prepared_panel()
    cs_today = cross_section_at(panel, ANCHOR)
    cs_prev = cross_section_at(panel, PREV)
    row = SentimentMetricsManager._metrics_row(ANCHOR, cs_today, cs_prev)
    stored = dict(row)
    stored['ladder'] = json.loads(stored['ladder'])
    stored['promotion'] = json.loads(stored['promotion'])
    reader = FakeReader(today_row=tuple(stored[c] for c in ReviewAggregator._SENTIMENT_TABLE_COLS))
    agg = _make_aggregator(reader)

    result = agg._aggregate_sentiment(cs_today, cs_prev, as_of=ANCHOR)
    m = result.data
    assert m['from_table'] is True
    assert m['limit_up_count'] == 1
    assert m['cycle']['position'] in ('回暖/震荡', '退潮', '冰点', '高潮')  # 周期照常计算
    assert result.available is True


def test_aggregate_sentiment_falls_back_on_missing_table():
    panel = _prepared_panel()
    cs_today = cross_section_at(panel, ANCHOR)
    cs_prev = cross_section_at(panel, PREV)
    reader = FakeReader(fail=True)  # 表缺失/读失败
    agg = _make_aggregator(reader)

    result = agg._aggregate_sentiment(cs_today, cs_prev, as_of=ANCHOR)
    m = result.data
    assert m['from_table'] is False
    assert m['limit_up_count'] == int(cs_today['_limit_up'].sum())
    assert 'cycle' in m


# ---------- 分位注记样本线 ----------

def test_percentiles_need_30_day_history():
    panel = _prepared_panel()
    cs_today = cross_section_at(panel, ANCHOR)
    metrics = sentiment_metrics(cs_today, cross_section_at(panel, PREV))

    agg = _make_aggregator(FakeReader(history=[('20260901', 10, 0.3, 3, 1.0)] * 10))
    assert agg._sentiment_percentiles(ANCHOR, metrics) is None  # 样本 <30 不出

    history = [(f'2026{i:04d}', 10, 0.3, 3, 1.0) for i in range(101, 141)]  # 40 天
    agg = _make_aggregator(FakeReader(history=history))
    out = agg._sentiment_percentiles(ANCHOR, metrics)
    assert out is not None
    assert out['limit_up_count']['n'] == 40
    assert 0 <= out['limit_up_count']['pct'] <= 100
