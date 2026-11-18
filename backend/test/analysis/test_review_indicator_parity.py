"""指标面板 × 链指标列对拍保障（indicator_panel docstring 引用，2026-10-25 补建）。

两个闸：
1. **窗口常量闸**：INDICATOR_WINDOW_DAYS 必须等于链 daily_review 的窗口公式
   （max 模块 effective_lookback + 引擎 buffer 2）。2026-10-25 前面板停在 133d
   而链被缠论扩到 520d，ewm 播种差异导致对拍 731 处失配（附录 C.6）——
   此闸防再次漂移：任何模块 max_lookback 变更而面板未跟，测试即红。
2. **对拍逻辑单测**：_indicator_parity 的逐值比对（数值容差 0.05/字符串精确）
   用临时 CSV stub 验证（不触网不触库）。
"""
import sys
from pathlib import Path

import pandas as pd

BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))


class TestIndicatorWindowGate:
    def test_panel_window_matches_chain_formula(self):
        """面板窗口 = 链 max 模块回看（含缠论 520）+ 引擎 buffer 2，逐字节对齐"""
        from src.analysis.chain_engine import ChainEngine  # noqa: F401 仅为引用一致性
        from src.analysis.module_base import AnalysisModule
        from src.analysis.modules.chan_divergence_buy import ChanDivergenceBuy
        from src.review.indicator_panel import (INDICATOR_WINDOW_DAYS,
                                                _PANEL_MODULES)

        # 面板模块自身 warmup 上限（防倒退：面板窗口至少覆盖自身 warmup+buffer）
        panel_max = max(m.effective_lookback() for m in _PANEL_MODULES)
        assert INDICATOR_WINDOW_DAYS >= panel_max + 2

        # 与链 daily_review 的 max 模块（缠论 520）对齐
        assert INDICATOR_WINDOW_DAYS == ChanDivergenceBuy().effective_lookback() + 2, (
            '指标面板窗口与链 daily_review 窗口漂移——ewm 播种差异会使对拍失配'
            '（2026-10-25 附录 C.6 教训），改缠论 max_lookback 时须同步'
            'INDICATOR_WINDOW_DAYS')

    def test_panel_window_start_no_off_by_one(self):
        """窗口起点必须为 nth_back(INDICATOR_WINDOW_DAYS)——调用处 -1 会让面板
        少 1 行（ewm 播种 + 平价股浮点边界漂移，20261027 对拍 2 处失配共因）。"""
        from src.review.indicator_panel import (INDICATOR_WINDOW_DAYS,
                                                panel_window_start)

        class _SpyReader:
            def __init__(self):
                self.n_seen = None

            def nth_trading_day_back(self, as_of, n):
                self.n_seen = n
                return f'{as_of}-start'

        spy = _SpyReader()
        assert panel_window_start(spy, '20261027') == '20261027-start'
        assert spy.n_seen == INDICATOR_WINDOW_DAYS, (
            'panel_window_start 必须传 nth_back(INDICATOR_WINDOW_DAYS)（不带 -1）')


class TestIndicatorParityLogic:
    """_indicator_parity 逐值比对逻辑（stub CSV，不触网不触库）。"""

    def test_parity_match_and_mismatch(self, tmp_path, monkeypatch):
        from src.review.aggregator import ReviewAggregator

        csv = tmp_path / 'daily_review_20261023.csv'
        csv.write_text(
            '\ufeff股票代码,MACD状态,RSI状态\n'
            '600000,零轴下空头,"RSI6=85.2/12=74.4/24=61.3；超买(RSI6>80)"\n'
            '000001,零轴上多头,"RSI6=50.0/12=45.0/24=40.0"\n', encoding='utf-8')

        states = pd.DataFrame([
            {'ts_code': 'sh.600000', 'MACD状态': '零轴下空头',
             'RSI状态': 'RSI6=85.2/12=74.4/24=61.3；超买(RSI6>80)'},   # 全等
            {'ts_code': 'sz.000001', 'MACD状态': '零轴下空头',          # MACD 失配
             'RSI状态': 'RSI6=50.0/12=45.0/24=40.0'},
        ])

        agg = ReviewAggregator.__new__(ReviewAggregator)
        monkeypatch.setattr(agg, '_csv_path', lambda as_of: csv)
        out = agg._indicator_parity(states, '20261023')
        assert out['compared'] == 4 and out['mismatch_count'] == 1
        assert out['mismatches'][0]['code'] == '000001'
        assert out['mismatches'][0]['column'] == 'MACD状态'
        assert out['ok'] is False

    def test_parity_numeric_tolerance(self, tmp_path, monkeypatch):
        """数值列容忍 ≤0.05 展示舍入（_values_match 契约）"""
        from src.review.aggregator import ReviewAggregator

        csv = tmp_path / 'daily_review_20261023.csv'
        csv.write_text('\ufeff股票代码,ATR波动%\n600000,3.14\n', encoding='utf-8')
        states = pd.DataFrame([{'ts_code': 'sh.600000', 'ATR波动%': '3.15'}])

        agg = ReviewAggregator.__new__(ReviewAggregator)
        monkeypatch.setattr(agg, '_csv_path', lambda as_of: csv)
        out = agg._indicator_parity(states, '20261023')
        assert out['ok'] is True and out['mismatch_count'] == 0

    def test_parity_csv_missing_returns_none(self, monkeypatch):
        from src.review.aggregator import ReviewAggregator
        agg = ReviewAggregator.__new__(ReviewAggregator)
        monkeypatch.setattr(agg, '_csv_path', lambda as_of: None)
        assert agg._indicator_parity(pd.DataFrame(), '20261023') is None
