"""M24 ``rsi_state`` RSI 状态（P0-1，方案 §3.1.3；通达信口径，indicators 库）。

取值：``RSI6=xx/12=xx/24=xx`` 基础串 + 事件标记：
- ``超买(RSI6>80)``：RSI6 > 80（通达信常用短周期判超买）；
- ``超卖(RSI6<20)``：RSI6 < 20；
- 无标记 → ``否``；warmup 不足或含 NaN → ``数据不足``。

口径：RSI=SMA(MAX(DIF,0),N)/SMA(ABS(DIF),N)*100（附录 B；ta 库 ewm 双轨不同源），
默认 N=24，另出 6/12 两线（A 股软件惯例）。warmup 130。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators import rsi_multi
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class RsiState(AnalysisModule):
    """RSI 状态（三线数值 + 超买/超卖标记）。"""

    name: ClassVar[str] = 'RSI状态'
    name_en: ClassVar[str] = 'rsi_state'
    description: ClassVar[str] = (
        "RSI 状态（通达信口径，SMA 比值式；默认 N=24 另出 6/12 线）：输出 "
        "RSI6/12/24 数值，附超买(RSI6>80)/超卖(RSI6<20) 标记；无标记记否，"
        "warmup 不足或含 NaN 返回数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close']
    max_lookback: ClassVar[int] = 130
    default_params: ClassVar[Dict[str, object]] = {
        'overbought': 80.0, 'oversold': 20.0,
    }

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient('无K线数据')
        # 引擎加载已按 (ts_code, trade_date) 排序（load_kline ORDER BY）；
        # 单调则免排序（每模块每次调用省一次 sort，全市场 5820 只 × 7 模块）
        ordered = df if df['trade_date'].is_monotonic_increasing \
            else df.sort_values('trade_date')
        if ctx.as_of and str(ordered.iloc[-1]['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(
                f"锚定日{ctx.as_of}无K线(最新为{ordered.iloc[-1]['trade_date']})")
        close = pd.to_numeric(ordered['close'], errors='coerce')
        if close.isna().any():
            return ModuleResult.insufficient('close 含空值')
        if len(close) < self.max_lookback:
            return ModuleResult.insufficient(f'warmup 不足({self.max_lookback} 日)')

        r = rsi_multi(close)
        r6, r12, r24 = r.rsi6.iloc[-1], r.rsi12.iloc[-1], r.rsi24.iloc[-1]
        if pd.isna(r6) or pd.isna(r12) or pd.isna(r24):
            return ModuleResult.insufficient('warmup 不足(130 日)或全天平盘')

        base = f'RSI6={float(r6):.1f}/12={float(r12):.1f}/24={float(r24):.1f}'
        flags: List[str] = []
        ob, os_ = float(self.param('overbought')), float(self.param('oversold'))
        if float(r6) > ob:
            flags.append(f'超买(RSI6>{ob:g})')
        if float(r6) < os_:
            flags.append(f'超卖(RSI6<{os_:g})')
        if not flags:
            return ModuleResult('否', {'rsi': base})
        return ModuleResult(base + '；' + '、'.join(flags),
                            {'rsi': base, 'flags': flags})
