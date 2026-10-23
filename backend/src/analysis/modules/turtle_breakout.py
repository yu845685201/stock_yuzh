"""S2 ``turtle_breakout`` 海龟突破策略（P1-7 §4.6 C，Sequoia-X 策略移植）。

判定：收盘价突破近 ``N`` 日（默认 21）最高价（严格突破：前 N 日不含当日的
最高价）且 反高开低走护栏（开盘涨幅 > ``max_open_gain``% 且 收盘 < 开盘 →
剔除，与 R3 同口径）且 非 ST。流通市值 = close × float_share（当日
``his_kline_day.float_share`` 直接取，**免源项目的 baostock 反推**——实测
当日 5108/5554 行有值）；缺失时该行不输出市值但仍可命中（市值标注缺失）。

返回值：``突破/{市值}亿`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List, Optional

import pandas as pd

from ..candidate_rules import BUY_RULES
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class TurtleBreakout(AnalysisModule):
    """策略截面：N 日突破 + 反高开低走 + 流通市值。"""

    name: ClassVar[str] = '海龟突破'
    name_en: ClassVar[str] = 'turtle_breakout'
    description: ClassVar[str] = (
        "策略截面（P1-7 C，独立按需跑，不进 daily_review）：收盘严格突破近 N(21) 日"
        "最高价（前 N 日不含当日）且 反高开低走（开盘涨幅 > max_open_gain(5)% 且 "
        "收盘<开盘 → 剔除）且 非 ST；附流通市值（close×float_share 直取，免反推）。"
        "返回理由串（如「突破/128.5亿」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['high', 'close', 'open',
                                             'preclose', 'is_st', 'float_share']
    default_params: ClassVar[Dict[str, object]] = {
        'breakout_days': 21, 'max_open_gain': 5.0,
    }
    max_lookback: ClassVar[int] = 25
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or len(df) < self.max_lookback:
            return ModuleResult.insufficient('K线行数不足')
        ordered = df.sort_values('trade_date')
        anchor = ordered.iloc[-1]
        if ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(f'锚定日{ctx.as_of}无K线')
        if bool(anchor.get('is_st')):
            return ModuleResult('否')

        n = int(self.params.get('breakout_days') or 21)
        close = pd.to_numeric(ordered['close'], errors='coerce')
        high = pd.to_numeric(ordered['high'], errors='coerce')
        close_now = close.iloc[-1]
        prev_high = high.iloc[-(n + 1):-1].max()      # 前 N 日不含当日
        if pd.isna(close_now) or pd.isna(prev_high):
            return ModuleResult.insufficient('价格数据不足')
        if not (close_now > prev_high):
            return ModuleResult('否')

        # 反高开低走护栏（与 R3 同口径；输入缺失不过滤）
        try:
            o_v = float(anchor.get('open'))
            pc_v = float(anchor.get('preclose'))
            c_v = float(anchor.get('close'))
            if pc_v > 0:
                open_gain = (o_v - pc_v) / pc_v * 100.0
                if c_v < o_v and open_gain > float(
                        self.params.get('max_open_gain') or 5.0):
                    return ModuleResult('否')
        except (TypeError, ValueError):
            pass

        # 流通市值（close×float_share；缺失留空不影响命中）
        mv_yi: Optional[float] = None
        try:
            fs = float(anchor.get('float_share'))
            if fs > 0:
                mv_yi = round(c_v * fs / 1e8, 1)
        except (TypeError, ValueError):
            mv_yi = None
        label = f'突破/{mv_yi}亿' if mv_yi is not None else '突破/市值缺失'
        return ModuleResult(label, {'prev_high': round(float(prev_high), 2),
                                    'close': round(float(close_now), 2),
                                    'float_mv_yi': mv_yi})
