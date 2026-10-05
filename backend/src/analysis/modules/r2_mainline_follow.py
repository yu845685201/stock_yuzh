"""R2 ``r2_mainline_follow`` 主线跟随候选（转天买入候选链）。

判定（candidate_rules.is_r2 同口径）：首板涨停 且 非 ST 且 概念成分命中
涨停聚集前列概念（全部成分 ∩ 聚集前 3——非报告层题材候选截断串，避免
前 3 截断噪声）。概念与聚集前列均来自环境装配（sina_gn 时点匹配 +
概念内涨停家数降序）。R1 命中的股票不重复入选（去重优先级 R1 最高）。

依赖：链内须先配 consecutive_limit_up 与 r1_ladder_relay；env: a_share_candidates。

返回值：``首板/{概念名}`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..candidate_rules import is_hit, is_r2
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class R2MainlineFollow(AnalysisModule):
    """转天候选 R2：主线跟随（首板 × 概念聚集前列）。"""

    name: ClassVar[str] = 'R2主线跟随'
    name_en: ClassVar[str] = 'r2_mainline_follow'
    description: ClassVar[str] = (
        "转天买入候选 R2 主线跟随：首板涨停（连续涨停数 = 1）且 非 ST 且 概念成分"
        "（sina_gn 全部成分）命中涨停聚集前列概念（概念内当日涨停家数降序前 3，"
        "黑名单剔除，与报告层概念涨停聚集同口径）；R1 梯队接力命中股不重复入选。"
        "判定谓词与报告层第六章预案共用（candidate_rules）。"
        "返回理由串（如「首板/创新药」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['is_st']
    default_params: ClassVar[Dict[str, object]] = {}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        r1 = ctx.upstream.get('R1梯队接力')
        if r1 is None:
            return ModuleResult.insufficient('缺前置模块 r1_ladder_relay')
        if is_hit(r1.value):
            return ModuleResult('否')

        streak_res = ctx.upstream.get('连续涨停数')
        if streak_res is None:
            return ModuleResult.insufficient('缺前置模块 consecutive_limit_up')
        if streak_res.value == '数据不足':
            return ModuleResult.insufficient('连板数数据不足')
        try:
            streak = int(streak_res.value)
        except (TypeError, ValueError):
            return ModuleResult.insufficient('连板数值非法')

        if df is None or df.empty:
            return ModuleResult.insufficient('无K线数据')
        anchor = df.sort_values('trade_date').iloc[-1]
        if ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(f'锚定日{ctx.as_of}无K线')
        is_st = bool(anchor.get('is_st'))

        theme_map = ctx.upstream.get('cand_theme_map') or {}
        top_concepts = ctx.upstream.get('cand_top_concepts') or []
        concepts = theme_map.get(ctx.stock.ts_code) or []

        if not is_r2(streak, is_st, concepts, top_concepts):
            return ModuleResult('否')
        # 命中概念取聚集序最前（top_concepts 顺序即家数降序）
        hit_concept = next(c for c in top_concepts if c in set(concepts))
        return ModuleResult(f'首板/{hit_concept}',
                            {'concepts': concepts, 'top_concepts': top_concepts})
