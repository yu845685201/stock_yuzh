"""M28 ``ma_arrangement`` 均线排列（P0-1，方案 §3.1.3；通达信口径，indicators 库）。

取值（按优先级）：
- ``收盘上穿MA60``：前一日 close ≤ MA60 且当日 close > MA60（事件优先）；
- ``多头排列(MA5>10>20>60)`` / ``空排列(MA5<10<20<60)``：严格全序；
- 其余 → ``纠缠``；warmup 不足或含 NaN → ``数据不足``。

口径：MA 为 rolling 简单平均（附录 B）。warmup 130（MA60 需 60 日 + EMA 系无涉，
取 130 与同批模块一致，留足稳定区）。多空严格序按 4 位小数比较（平价股浮点防护，
见 analyze 内注）。命中统计：状态串不计命中（§2.5 原则）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators.tdx_core import MA
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class MaArrangement(AnalysisModule):
    """均线排列（MA5/10/20/60 多空序列 + 收盘上穿 MA60 事件）。"""

    name: ClassVar[str] = '均线排列'
    name_en: ClassVar[str] = 'ma_arrangement'
    description: ClassVar[str] = (
        "均线排列：收盘上穿 MA60（事件优先）/ 多头排列(MA5>10>20>60) / "
        "空头排列(MA5<10<20<60) / 纠缠。MA 为简单平均（通达信口径）。"
        "warmup 130；warmup 不足返回数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close']
    max_lookback: ClassVar[int] = 130
    default_params: ClassVar[Dict[str, object]] = {'ns': [5, 10, 20, 60]}

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
        if close.isna().any() or len(close) < 2:
            return ModuleResult.insufficient('close 含空值或行数不足')
        if len(close) < self.max_lookback:
            return ModuleResult.insufficient(f'warmup 不足({self.max_lookback} 日)')

        ns = [int(x) for x in self.param('ns')]
        mas = {n: MA(close, n) for n in ns}
        latest = {n: float(mas[n].iloc[-1]) for n in ns}
        if any(pd.isna(v) for v in latest.values()):
            return ModuleResult.insufficient('warmup 不足(130 日)')

        ma60_last, ma60_prev = latest[60], float(mas[60].iloc[-2])
        c_now, c_prev = float(close.iloc[-1]), float(close.iloc[-2])
        if c_prev <= ma60_prev and c_now > ma60_last:
            return ModuleResult('收盘上穿MA60',
                                {'close': c_now, 'ma60': round(ma60_last, 4)})

        m5, m10, m20, m60 = (latest[5], latest[10], latest[20], latest[60])
        # 严格序比较按 4 位小数（与 extras 精度一致）：平价序列下 MA5 与 MA10 数学
        # 相等（如 20261027 的 002182/002488，前 5 日和恰等于后 5 日和），rolling
        # 浮点误差随窗口起点不同随机把相等翻成严格小于/大于（链/面板互斥翻车）。
        m5, m10, m20, m60 = (round(v, 4) for v in (m5, m10, m20, m60))
        if m5 > m10 > m20 > m60:
            label = '多头排列(MA5>10>20>60)'
        elif m5 < m10 < m20 < m60:
            label = '空头排列(MA5<10<20<60)'
        else:
            label = '纠缠'
        return ModuleResult(label, {
            'ma5': round(m5, 4), 'ma10': round(m10, 4),
            'ma20': round(m20, 4), 'ma60': round(m60, 4),
        })
