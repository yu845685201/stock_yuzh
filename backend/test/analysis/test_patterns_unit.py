"""形态库单测（P0-2，方案 §3.1.5 同类）：注册表自检、构造用例、一字板安全、
优先级、模块边界。对拍（vs candlestick-patterns 克隆）见 test_patterns_parity.py。
"""

import pandas as pd
import pytest

from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.candle_pattern import CandlePattern
from src.analysis.patterns import PATTERN_REGISTRY, detect_all, load_all

AS_OF = "20260930"


def _ctx():
    return ChainContext(as_of=AS_OF,
                        stock=StockInfo(ts_code="sh.600001", stock_code="600001",
                                        stock_name="测试股", board="主板"))


def _bars(rows):
    """rows: [(open, high, low, close), ...] 升序；最后一行补齐为锚定日。"""
    dates = [f"202609{20 + i:02d}" for i in range(len(rows))]
    dates[-1] = AS_OF
    return pd.DataFrame({
        "trade_date": dates,
        "open": [r[0] for r in rows], "high": [r[1] for r in rows],
        "low": [r[2] for r in rows], "close": [r[3] for r in rows],
    })


class TestRegistry:
    def test_load_all_21_unique(self):
        specs = load_all()
        assert len(specs) == 21
        assert len({s.name for s in specs}) == 21
        assert len({s.key for s in specs}) == 21

    def test_priority_order(self):
        """注册表顺序 = 优先级：三根反转 > 双根反转 > 单根 > 持续。"""
        order = {'triple': 0, 'double': 1, 'single': 2, 'continuation': 3}
        ranks = [order[s.family] for s in PATTERN_REGISTRY]
        assert ranks == sorted(ranks)

    def test_self_built_marked(self):
        by_key = {s.key: s for s in PATTERN_REGISTRY}
        assert by_key['three_white_soldiers'].source == 'self-built'
        assert by_key['three_black_crows'].source == 'self-built'
        assert by_key['hammer'].source == 'candlestick-patterns'
        parity_count = sum(1 for s in PATTERN_REGISTRY
                           if s.source == 'candlestick-patterns')
        assert parity_count == 19


class TestConstructedPatterns:
    """构造用例：每个对拍形态至少 1 个命中场景（含边界）。"""

    def test_hammer(self):
        df = _bars([(10.0, 10.3, 9.0, 10.2)])
        assert bool(detect_all(df)['锤子线'][-1])

    def test_inverted_hammer(self):
        df = _bars([(10.0, 11.0, 9.9, 10.1)])
        assert bool(detect_all(df)['倒锤子线'][-1])

    def test_doji(self):
        df = _bars([(10.0, 10.5, 9.5, 10.05)])
        assert bool(detect_all(df)['十字星'][-1])

    def test_dragonfly_doji(self):
        df = _bars([(10.0, 10.03, 9.5, 10.02)])
        assert bool(detect_all(df)['蜻蜓十字'][-1])

    def test_gravestone_doji(self):
        df = _bars([(10.0, 10.5, 9.97, 9.98)])
        assert bool(detect_all(df)['墓碑十字'][-1])

    def test_bullish_engulfing(self):
        df = _bars([(10.2, 10.3, 10.0, 10.1),   # 前阴
                    (10.0, 10.6, 9.95, 10.55)])  # 今阳包住前实体
        assert bool(detect_all(df)['看涨吞没'][-1])

    def test_bearish_engulfing(self):
        df = _bars([(10.0, 10.3, 9.9, 10.2),    # 前阳
                    (10.4, 10.45, 9.85, 9.9)])  # 今阴包住
        assert bool(detect_all(df)['看跌吞没'][-1])

    def test_bullish_harami(self):
        df = _bars([(10.5, 10.6, 10.0, 10.1),   # 前阴（大实体）
                    (10.15, 10.25, 10.05, 10.2)])  # 今小阳落入前实体
        assert bool(detect_all(df)['看涨孕线'][-1])

    def test_dark_cloud_cover(self):
        df = _bars([(10.0, 10.4, 9.95, 10.35),  # 前阳
                    (10.5, 10.55, 10.0, 10.1)])  # 高开低走穿入过半
        assert bool(detect_all(df)['乌云盖顶'][-1])

    def test_piercing_pattern(self):
        df = _bars([(10.4, 10.45, 10.0, 10.05),  # 前阴
                    (9.95, 10.35, 9.9, 10.3)])   # 低开高走收复过半
        assert bool(detect_all(df)['穿刺'][-1])

    def test_morning_star(self):
        df = _bars([(10.5, 10.55, 10.2, 10.25),  # 大阴
                    (10.1, 10.15, 10.0, 10.08),  # 星形（低于前收）
                    (10.15, 10.6, 10.1, 10.55)])  # 阳线收于首根开盘上方
        assert bool(detect_all(df)['启明星'][-1])

    def test_evening_star(self):
        df = _bars([(10.2, 10.3, 10.15, 10.28),  # 大阳
                    (10.4, 10.45, 10.35, 10.38),  # 星形（高于前收）
                    (10.3, 10.35, 9.95, 10.0)])  # 阴线收于首根开盘下方
        assert bool(detect_all(df)['黄昏星'][-1])

    def test_hanging_man(self):
        df = _bars([(10.0, 10.2, 9.9, 10.1),    # 前两日高点低
                    (10.05, 10.15, 10.0, 10.1),
                    (10.25, 10.25, 9.6, 10.2)])  # 长下影 + 前两日高点 < 今开
        assert bool(detect_all(df)['吊人线'][-1])

    def test_three_white_soldiers(self):
        df = _bars([(10.0, 10.5, 9.95, 10.45),
                    (10.5, 11.0, 10.45, 10.95),
                    (11.0, 11.5, 10.95, 11.45)])
        assert bool(detect_all(df)['红三兵'][-1])

    def test_three_black_crows(self):
        df = _bars([(11.45, 11.5, 10.95, 11.0),
                    (11.0, 11.05, 10.5, 10.55),
                    (10.55, 10.6, 10.05, 10.1)])
        assert bool(detect_all(df)['三只乌鸦'][-1])


class TestOneWordBarSafety:
    """一字板（H=L=C）全部形态不命中（A 股涨跌停场景）。"""

    def test_all_patterns_false_on_one_word_bar(self):
        df = _bars([(10.0, 10.0, 10.0, 10.0)] * 3)
        hits = detect_all(df)
        assert not any(bool(arr[-1]) for arr in hits.values())

    def test_module_returns_none_with_flag(self):
        df = _bars([(10.0, 10.2, 9.8, 10.1)] * 2 + [(10.0, 10.0, 10.0, 10.0)])
        res = CandlePattern().analyze(df, _ctx())
        assert res.value == '无'
        assert res.detail['one_word_bar'] is True


class TestModuleBoundaries:
    def test_empty(self):
        assert CandlePattern().analyze(pd.DataFrame(), _ctx()).value == DATA_INSUFFICIENT

    def test_anchor_mismatch(self):
        df = _bars([(10.0, 10.2, 9.9, 10.1)] * 3)
        df.loc[df.index[-1], 'trade_date'] = '20260929'
        assert CandlePattern().analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_short_series(self):
        assert CandlePattern().analyze(_bars([(10.0, 10.2, 9.9, 10.1)] * 2),
                                       _ctx()).value == DATA_INSUFFICIENT

    def test_missing_column(self):
        df = _bars([(10.0, 10.2, 9.9, 10.1)] * 3).drop(columns=['low'])
        assert CandlePattern().analyze(df, _ctx()).value == DATA_INSUFFICIENT

    def test_multi_hit_detail_lists_all(self):
        """主形态取优先级首，detail 含全部命中（锤子线构造）。"""
        df = _bars([(10.0, 10.3, 9.0, 10.2)] * 3)
        res = CandlePattern().analyze(df, _ctx())
        assert res.value == res.detail['hits'][0]
        assert res.value in [s.name for s in PATTERN_REGISTRY]
