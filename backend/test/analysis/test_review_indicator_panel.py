"""报告层指标面板测试（P0-1 §3.1.7，增量 C2）。

覆盖：
- ``summarize_indicator_states`` 纯函数聚合（分布/分位/交叉榜，无需数据库）；
- ``_values_match`` 逐值比较口径；
- 黄金样本门控的**链 CSV vs 报告层面板对拍**（同一实现两入口的运行时校验，
  需数据库 + 已跑链，ANALYSIS_GOLDEN=1 开启）。
"""

import os

import pandas as pd
import pytest

from src.review.aggregator import _values_match
from src.review.indicator_panel import (
    INDICATOR_WINDOW_DAYS, compute_indicator_states, summarize_indicator_states,
)


def _states_df(rows):
    return pd.DataFrame(rows)


def _row(**kw):
    base = {
        'ts_code': 'sh.600000', '股票名称': '测试股', '成交额': 1e8,
        'MACD状态': '零轴上多头', 'KDJ状态': '否', 'RSI状态': '否',
        'BOLL位置%': 50.0, 'ATR波动%': 2.0, 'OBV趋势': '否',
        '均线排列': '纠缠', '放量倍数': 1.0, '60日新高新低': '否',
    }
    base.update(kw)
    return base


class TestSummarize:
    def test_empty_returns_empty(self):
        assert summarize_indicator_states(pd.DataFrame()) == {}

    def test_distributions(self):
        df = _states_df([
            _row(ts_code='sh.600000', MACD状态='金叉', KDJ状态='K=1 D=1 J=1；超买(J>100)'),
            _row(ts_code='sh.600001', MACD状态='死叉', KDJ状态='K=1 D=1 J=1；超卖(J<0)'),
            _row(ts_code='sh.600002', MACD状态='零轴上多头', OBV趋势='价升量增'),
            _row(ts_code='sh.600003', MACD状态='零轴下空头', OBV趋势='价跌量增',
                 均线排列='多头排列(MA5>10>20>60)'),
        ])
        d = summarize_indicator_states(df, volume_surge_min=2.0, top_n=5)
        assert d['macd_dist'] == {'金叉': 1, '死叉': 1, '零轴上多头': 1, '零轴下空头': 1}
        assert d['kdj_dist']['超买'] == 1 and d['kdj_dist']['超卖'] == 1
        assert d['rsi_dist'] == {'超买': 0, '超卖': 0}
        assert d['obv_dist']['价升量增'] == 1 and d['obv_dist']['价跌量增'] == 1
        assert d['ma_dist']['多头排列(MA5>10>20>60)'] == 1
        assert d['sample'] == 4

    def test_quantiles_and_sample(self):
        df = _states_df([
            _row(ts_code=f'sh.60000{i}',
                 **{'BOLL位置%': float(v), 'ATR波动%': float(v) / 10})
            for i, v in enumerate([10, 20, 30, 40])
        ])
        d = summarize_indicator_states(df)
        assert d['boll_pos']['sample'] == 4
        assert d['boll_pos']['p50'] == 25.0
        assert d['atr_pct']['p50'] == 2.5

    def test_top_macd_surge_cross(self):
        """MACD 金叉 × 放量≥阈值，按成交额降序。"""
        df = _states_df([
            _row(ts_code='sh.600000', MACD状态='金叉', 放量倍数=2.5, 成交额=5e7),
            _row(ts_code='sh.600001', MACD状态='金叉', 放量倍数=3.5, 成交额=1e8),
            _row(ts_code='sh.600002', MACD状态='金叉', 放量倍数=1.5, 成交额=9e7),  # 不达标
            _row(ts_code='sh.600003', MACD状态='零轴上多头', 放量倍数=4.0, 成交额=9e7),  # 非金叉
        ])
        d = summarize_indicator_states(df, volume_surge_min=2.0, top_n=5)
        top = d['top_macd_surge']
        assert [r['code'] for r in top] == ['600001', '600000']
        assert '放量3.5x' in top[0]['reason']

    def test_top_rsi_new_low_cross(self):
        df = _states_df([
            _row(ts_code='sh.600000', RSI状态='RSI6=15/12=18/24=20；超卖(RSI6<20)',
                 成交额=3e7, **{'60日新高新低': '新低'}),
            _row(ts_code='sh.600001', RSI状态='RSI6=15/12=18/24=20；超卖(RSI6<20)',
                 成交额=9e7, **{'60日新高新低': '否'}),
        ])
        d = summarize_indicator_states(df)
        top = d['top_rsi_new_low']
        assert [r['code'] for r in top] == ['600000']
        assert top[0]['reason'] == 'RSI超卖 / 60日新低'

    def test_insufficient_not_counted(self):
        """数据不足行不计入分布分子（样本数仍含）。"""
        df = _states_df([_row(MACD状态='数据不足'), _row(MACD状态='金叉')])
        d = summarize_indicator_states(df)
        assert d['macd_dist']['金叉'] == 1
        assert d['sample'] == 2


class TestValuesMatch:
    def test_numeric_tolerance(self):
        assert _values_match(45.2, '45.2') is True
        assert _values_match(45.2, '45.24') is True     # 展示舍入容差 0.05
        assert _values_match(45.2, '45.3') is False

    def test_string_exact(self):
        assert _values_match('金叉', '金叉') is True
        assert _values_match('金叉', '死叉') is False
        assert _values_match('数据不足', '数据不足') is True
        assert _values_match('否', '否') is True


class TestWindowConstant:
    def test_window_matches_chain(self):
        """面板窗口与链 daily_review 窗口对齐（520 缠论 + buffer 2；2026-10-25
        修复 133d 错位——对拍 731 处失配根因，附录 C.6）。"""
        assert INDICATOR_WINDOW_DAYS == 522


# --------------------------------------------------------------- 对拍（需 DB）
@pytest.mark.skipif(os.getenv('ANALYSIS_GOLDEN') != '1',
                    reason='设 ANALYSIS_GOLDEN=1 开启链 CSV vs 报告层面板对拍')
class TestParityWithChainCsv:
    def test_panel_matches_chain_csv(self):
        from src.analysis.data_reader import DataReader
        from src.analysis.lookback import Lookback
        from src.config import ConfigManager

        as_of = '20260930'
        csv_path = ('/Users/yuzh/develop/os/os_yuzh/04_工具/stock_yuzh_base/'
                    'stock_yuzh/data/analysis/daily_review_%s.csv' % as_of)
        if not os.path.exists(csv_path):
            pytest.skip('链 CSV 不存在（先跑 daily_review 链）')

        reader = DataReader(ConfigManager())
        try:
            start = reader.nth_trading_day_back(as_of, INDICATOR_WINDOW_DAYS - 1)
            panel = reader.load_kline(
                ['stock_name', 'open', 'high', 'low', 'close', 'volume', 'amount'],
                as_of, start_date=start)
        except Exception as exc:  # pragma: no cover
            pytest.skip(f'数据库不可用: {exc!r}')
        finally:
            reader.close()

        states = compute_indicator_states(panel, as_of)
        assert not states.empty
        csv_df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
        cols = ['MACD状态', 'KDJ状态', 'RSI状态', 'BOLL位置%', 'ATR波动%',
                'OBV趋势', '均线排列']
        missing = [c for c in cols if c not in csv_df.columns]
        assert not missing, f'链 CSV 缺指标列: {missing}'
        csv_idx = csv_df.set_index(csv_df['股票代码'].astype(str).str.zfill(6))
        compared = mismatches = 0
        for _, row in states.iterrows():
            code = str(row['ts_code']).split('.')[-1].zfill(6)
            if code not in csv_idx.index:
                continue
            crow = csv_idx.loc[code]
            if isinstance(crow, pd.DataFrame):
                crow = crow.iloc[0]
            for col in cols:
                compared += 1
                if not _values_match(row[col], crow[col]):
                    mismatches += 1
        assert compared > 1000, f'对拍样本不足: {compared}'
        assert mismatches == 0, f'链 CSV 与面板复算不一致 {mismatches}/{compared}'
