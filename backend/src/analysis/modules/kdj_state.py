"""M23 ``kdj_state`` KDJ 状态（P0-1，方案 §3.1.3；通达信口径，indicators 库）。

取值：``K=xx.x D=xx.x J=xx.x`` 基础串 + 事件标记（多标记用「、」连接）：
- ``超买(J>100)``：J > 100（通达信惯例）；
- ``超卖(J<0)``：J < 0；
- ``金叉`` / ``死叉``：K 与 D 交叉（CROSS(K,D) / CROSS(D,K)）；
- 无任何标记 → ``否``；warmup 不足或含 NaN → ``数据不足``。

口径：K=EMA(RSV,5)、D=EMA(K,5)、J=3K-2D（附录 B；ta 库 rolling-mean 平滑不同源）。
warmup 60。命中统计：组合理由串不计命中（§2.5 原则，精确分桶不适用组合串）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators import kdj as tdx_kdj
from ..indicators.tdx_core import CROSS
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class KdjState(AnalysisModule):
    """KDJ 状态（数值三件套 + 超买/超卖/金叉/死叉标记）。"""

    name: ClassVar[str] = 'KDJ状态'
    name_en: ClassVar[str] = 'kdj_state'
    description: ClassVar[str] = (
        "KDJ 状态（通达信口径，K=EMA(RSV,5)/D=EMA(K,5)/J=3K-2D）：输出 K/D/J 数值，"
        "附超买(J>100)/超卖(J<0)/金叉/死叉标记（多标记「、」连接）；无标记记否，"
        "warmup 不足或含 NaN 返回数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close', 'high', 'low']
    max_lookback: ClassVar[int] = 60

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

        r = tdx_kdj(close, high, low)
        k, d, j = r.k, r.d, r.j
        if pd.isna(k.iloc[-1]) or pd.isna(d.iloc[-1]) or pd.isna(j.iloc[-1]):
            return ModuleResult.insufficient('warmup 不足(60 日)或一字板区间')

        k_now, d_now, j_now = float(k.iloc[-1]), float(d.iloc[-1]), float(j.iloc[-1])
        base = f'K={k_now:.1f} D={d_now:.1f} J={j_now:.1f}'
        flags: List[str] = []
        if j_now > 100:
            flags.append('超买(J>100)')
        if j_now < 0:
            flags.append('超卖(J<0)')
        if bool(CROSS(k, d).iloc[-1]):
            flags.append('金叉')
        if bool(CROSS(d, k).iloc[-1]):
            flags.append('死叉')
        label = base + ('；' + '、'.join(flags) if flags else '')
        if not flags:
            return ModuleResult('否', {'kdj': base})
        return ModuleResult(label, {'kdj': base, 'flags': flags})
