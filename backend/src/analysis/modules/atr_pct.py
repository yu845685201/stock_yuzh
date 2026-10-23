"""M26 ``atr_pct`` ATR 波动%（P0-1，方案 §3.1.3；通达信口径，indicators 库）。

取值：ATR(20)/close*100（2 位小数）——锚定日的真实波动幅度（占价比），
衡量个股自身波动率。P1-12 自适应异动阈值的输入（高波动股 ±5% 可能只是常态）。

口径：ATR=MA(TR,20) **N 日简单平均（通达信口径）**，非 Wilder 递推（附录 B）。
warmup 60。命中统计：连续比率，显示可算样本数（§2.5）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators import atr as tdx_atr
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class AtrPct(AnalysisModule):
    """ATR 波动%（ATR20 占收盘价比）。"""

    name: ClassVar[str] = 'ATR波动%'
    name_en: ClassVar[str] = 'atr_pct'
    description: ClassVar[str] = (
        "ATR(20)/close*100（2 位小数）：锚定日真实波动幅度占价比，衡量个股自身"
        "波动率（P1-12 自适应阈值输入）。ATR=MA(TR,20) 简单平均（通达信口径，"
        "非 Wilder）。warmup 60；warmup 不足或 close≤0 返回数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close', 'high', 'low']
    max_lookback: ClassVar[int] = 60
    default_params: ClassVar[Dict[str, object]] = {'n': 20}
    # 命中统计口径（方案 §2.5）：连续比率，显示可算样本数
    hit_rule: ClassVar[str] = 'sample'

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
        high = pd.to_numeric(ordered['high'], errors='coerce')
        low = pd.to_numeric(ordered['low'], errors='coerce')
        if close.isna().any() or high.isna().any() or low.isna().any():
            return ModuleResult.insufficient('OHLC 含空值')
        if len(close) < self.max_lookback:
            return ModuleResult.insufficient(f'warmup 不足({self.max_lookback} 日)')

        atr = tdx_atr(close, high, low, int(self.param('n')))
        a_now = float(atr.iloc[-1]) if not pd.isna(atr.iloc[-1]) else None
        c_now = float(close.iloc[-1])
        if a_now is None:
            return ModuleResult.insufficient('warmup 不足(60 日)')
        if c_now <= 0:
            return ModuleResult.insufficient('close≤0')
        pct = a_now / c_now * 100
        return ModuleResult(round(pct, 2), {'atr': round(a_now, 4),
                                            'close': c_now})
