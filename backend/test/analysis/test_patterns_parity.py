"""形态库对拍测试（P0-2，方案 §3.2）：本库 vs SpiralDevelopment/candlestick-patterns 克隆。

对拍范围：19 个克隆形态（逐字对应）。克隆 3 个缺陷跳过（缺陷登记在案）：
``bullish_hanging_man`` 缺文件必炸、``evening_star`` 系两个形态未在 API 暴露
（本库自行 import 其类文件，仍参与对拍）。红三兵/三只乌鸦为自建，无基线。

数据：合成序列（固定 seed，含一字板/涨跌停/随机影线，150 根）+ 真实窗口
（10 只黄金样本股，需数据库，ANALYSIS_GOLDEN=1）。
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.analysis.patterns import detect_all

# --------------------------------------------------------------- 克隆导入
_CLONE_CANDIDATES = [
    Path(__file__).resolve().parents[4] / "refrences" / "SpiralDevelopment__candlestick-patterns",
    Path(__file__).resolve().parents[3] / "refrences" / "SpiralDevelopment__candlestick-patterns",
]
_CLONE_DIR = next((p for p in _CLONE_CANDIDATES if (p / "candlestick").exists()), None)

if _CLONE_DIR is not None and str(_CLONE_DIR) not in sys.path:
    sys.path.insert(0, str(_CLONE_DIR))

if _CLONE_DIR is None:  # pragma: no cover
    pytestmark = pytest.mark.skip(reason="candlestick-patterns 克隆缺失",
                                  allow_module_level=True)
else:
    from candlestick.patterns.bearish_engulfing import BearishEngulfing
    from candlestick.patterns.bearish_harami import BearishHarami
    from candlestick.patterns.bullish_engulfing import BullishEngulfing
    from candlestick.patterns.bullish_harami import BullishHarami
    from candlestick.patterns.dark_cloud_cover import DarkCloudCover
    from candlestick.patterns.doji import Doji
    from candlestick.patterns.dragonfly_doji import DragonflyDoji
    from candlestick.patterns.evening_star import EveningStar
    from candlestick.patterns.evening_star_doji import EveningStarDoji
    from candlestick.patterns.gravestone_doji import GravestoneDoji
    from candlestick.patterns.hammer import Hammer
    from candlestick.patterns.hanging_man import HangingMan
    from candlestick.patterns.inverted_hammer import InvertedHammer
    from candlestick.patterns.morning_star import MorningStar
    from candlestick.patterns.morning_star_doji import MorningStarDoji
    from candlestick.patterns.piercing_pattern import PiercingPattern
    from candlestick.patterns.rain_drop import RainDrop
    from candlestick.patterns.shooting_star import ShootingStar
    from candlestick.patterns.star import Star

#: 克隆类 → 本库中文名（19 项；红三兵/三只乌鸦自建无基线）
_PARITY_MAP = [
    (Hammer, '锤子线'), (InvertedHammer, '倒锤子线'), (Doji, '十字星'),
    (DragonflyDoji, '蜻蜓十字'), (GravestoneDoji, '墓碑十字'),
    (BullishEngulfing, '看涨吞没'), (BearishEngulfing, '看跌吞没'),
    (BullishHarami, '看涨孕线'), (BearishHarami, '看跌孕线'),
    (Star, '双根十字星'), (RainDrop, '雨滴'), (ShootingStar, '射击之星'),
    (DarkCloudCover, '乌云盖顶'), (PiercingPattern, '穿刺'),
    (MorningStar, '启明星'), (EveningStar, '黄昏星'),
    (MorningStarDoji, '启明星十字'), (EveningStarDoji, '黄昏星十字'),
    (HangingMan, '吊人线'),
]


def _synthetic(n=150, seed=42):
    """确定性合成日 K：随机漫步 + 一字板/涨跌停/大影线注入。"""
    rng = np.random.default_rng(seed)
    close = 10 + np.cumsum(rng.normal(0, 0.3, n))
    close = np.maximum(close, 1.0).round(2)
    high = (close * (1 + np.abs(rng.normal(0, 0.006, n)))).round(2)
    low = (close * (1 - np.abs(rng.normal(0, 0.006, n)))).round(2)
    opn = (close * (1 + rng.normal(0, 0.004, n))).round(2)
    for i in range(13, n, 17):      # 一字板
        high[i] = low[i] = opn[i] = close[i]
    for i in range(22, n, 23):      # 涨跌停大跳空
        close[i] = round(close[i] * 1.1, 2)
        high[i] = close[i]
        low[i] = round(close[i] * 0.995, 2)
    df = pd.DataFrame({'open': opn, 'high': high, 'low': low, 'close': close})
    # 修正 high/low 包住 open/close（合成数据合法性）
    df['high'] = df[['open', 'high', 'close']].max(axis=1)
    df['low'] = df[['open', 'low', 'close']].min(axis=1)
    return df


def _clone_column(cls, df):
    """克隆形态在 df 上的布尔列（前 required_count-1 行为 None → False）。"""
    inst = cls(target='parity')
    out = inst.has_pattern(df.copy(), ohlc=['open', 'high', 'low', 'close'],
                           is_reversed=False)
    col = out['parity']
    return col.fillna(False).astype(bool).to_numpy()


class TestParitySynthetic:
    @pytest.mark.parametrize("cls,our_name", _PARITY_MAP, ids=lambda x: getattr(x, '__name__', x))
    def test_pattern_matches_clone(self, cls, our_name):
        df = _synthetic()
        ours = np.asarray(detect_all(df)[our_name], dtype=bool)
        theirs = _clone_column(cls, df)
        assert ours.shape == theirs.shape
        assert np.array_equal(ours, theirs), (
            f'{our_name} 不一致：ours 命中 {int(ours.sum())} vs clone {int(theirs.sum())}')


@pytest.mark.skipif(os.getenv('ANALYSIS_GOLDEN') != '1',
                    reason='设 ANALYSIS_GOLDEN=1 开启真实数据对拍（需数据库）')
class TestParityRealData:
    def test_real_windows(self):
        from src.analysis.data_reader import DataReader
        from src.analysis.lookback import Lookback
        from src.config import ConfigManager

        as_of = '20260923'
        stocks = ['600418', '600825', '601811', '603636', '000560',
                  '000850', '300750', '688512', '600053', '002935']
        reader = DataReader(ConfigManager())
        try:
            universe = {s.stock_code: s for s in reader.load_stock_universe()}
            codes = [universe[c] for c in stocks if c in universe]
            start = reader.resolve_window_start(as_of, Lookback.parse('250d'))
            kdf = reader.load_kline(['open', 'high', 'low', 'close'], as_of,
                                    start_date=start,
                                    ts_codes=[s.ts_code for s in codes])
        except Exception as exc:  # pragma: no cover
            pytest.skip(f'数据库不可用: {exc!r}')
        finally:
            reader.close()
        assert not kdf.empty
        for _, g in kdf.groupby('ts_code', sort=False):
            g = g.sort_values('trade_date').tail(60)
            ours_all = detect_all(g)
            for cls, our_name in _PARITY_MAP:
                ours = bool(np.asarray(ours_all[our_name])[-1])
                theirs = bool(_clone_column(cls, g)[-1])
                assert ours == theirs, f'{our_name} 锚定日不一致'
