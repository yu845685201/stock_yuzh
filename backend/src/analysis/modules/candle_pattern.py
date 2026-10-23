"""M29 ``candle_pattern`` K 线形态（P0-2，方案 §3.2；patterns 库，向量化）。

取值：锚定日**主形态**（同日多命中按注册表优先级取首：三根反转 > 双根反转 >
单根 > 持续形态）；无命中 → ``无``；一字板 → ``无`` 且 detail 标记
``one_word_bar``（A 股涨跌停场景全部形态不命中，属结构性事实非缺陷）。

口径：19 个形态与 candlestick-patterns 克隆逐字对拍；红三兵/三只乌鸦为自建
（无对拍基线，README 标注）。强度分级（TA-Lib ±100 口径）**未实现**——本基线
无强度度量，不硬造（原则 P3）。

命中统计：形态是离散事件（§2.5 原则），声明 21 个档位分桶——摘要显示主形态
分布（如「K线形态: 看涨吞没 5 / 锤子线 3」）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult
from ..patterns import PATTERN_REGISTRY, detect_all


class CandlePattern(AnalysisModule):
    """K 线形态（锚定日主形态 + 全部命中 detail）。"""

    name: ClassVar[str] = 'K线形态'
    name_en: ClassVar[str] = 'candle_pattern'
    description: ClassVar[str] = (
        "锚定日 K 线主形态（21 式：5 单根 + 9 双根 + 5 三根反转 + 2 持续）；"
        "同日多命中按优先级取首（三根反转>双根反转>单根>持续），detail 记录全部"
        "命中；无命中记无，一字板记无并标记；19 式与 candlestick-patterns 对拍，"
        "红三兵/三只乌鸦自建"
    )
    required_columns: ClassVar[List[str]] = ['open', 'high', 'low', 'close']
    max_lookback: ClassVar[int] = 3
    # 命中统计分桶（方案 §2.5）：形态是离散事件，按主形态分桶
    hit_buckets: ClassVar[List[str]] = [s.name for s in PATTERN_REGISTRY]

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient('无K线数据')
        ordered = df.sort_values('trade_date')
        if ctx.as_of and str(ordered.iloc[-1]['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(
                f"锚定日{ctx.as_of}无K线(最新为{ordered.iloc[-1]['trade_date']})")
        for col in ('open', 'high', 'low', 'close'):
            if col not in ordered.columns:
                return ModuleResult.insufficient(f'缺列 {col}')
        if len(ordered) < 3:
            return ModuleResult.insufficient('warmup 不足(3 日)')

        # 只取尾部 3 根（三根形态的最大需求）；numpy 小数组运算，全市场可承受
        tail = ordered.tail(3)
        anchor = tail.iloc[-1]
        one_word = float(anchor['high']) == float(anchor['low'])
        hits = detect_all(tail)
        hit_names = [name for name, arr in hits.items() if bool(arr[-1])]

        detail: Dict[str, object] = {
            'one_word_bar': one_word,
            'hits': hit_names,
            'anchor_date': str(anchor['trade_date']),
        }
        if not hit_names:
            return ModuleResult('无', detail)
        # 注册表顺序即优先级，首个命中为主形态
        return ModuleResult(hit_names[0], detail)
