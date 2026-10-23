"""R4 ``r4_institution_seat`` 机构席位候选（daily_review 链 R 列；2026-10-18 自独立候选链合入）。

判定（candidate_rules.is_r4 同口径 + 链侧叠加 is_st 列）：龙虎榜机构净买为正
且 非 ST。机构净买读环境装配 ``cand_institution_net_map``（lhb_detail 席位
「机构专用」归并，净买降序前 5——与报告层机构榜同 SQL 同 LIMIT，链命中
即报告前 5）。R1/R2/R3 命中股不重复入选（去重优先级最低）。

依赖：链内须先配 r1/r2/r3 三个模块；env: a_share_candidates。

返回值：``机构净买{+X.XX}亿`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..candidate_rules import is_hit, is_r4
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class R4InstitutionSeat(AnalysisModule):
    """转天候选 R4：机构席位（龙虎榜机构净买前列）。"""

    name: ClassVar[str] = 'R4机构席位'
    name_en: ClassVar[str] = 'r4_institution_seat'
    description: ClassVar[str] = (
        "转天买入候选 R4 机构席位：龙虎榜机构净买为正（lhb_detail 席位名含"
        "「机构专用」归并，净买降序前 5，与报告层机构榜同口径）且 非 ST"
        "（名称近似 + is_st 列双保险）；R1/R2/R3 命中股不重复入选。"
        "判定谓词与报告层第六章预案共用（candidate_rules）。"
        "返回理由串（如「机构净买+2.20亿」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['is_st']
    default_params: ClassVar[Dict[str, object]] = {}
    # 命中统计口径（方案 §2.5）：理由串类，非「否 / 数据不足」即命中
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        for pre in ('R1梯队接力', 'R2主线跟随', 'R3趋势新高'):
            res = ctx.upstream.get(pre)
            if res is None:
                return ModuleResult.insufficient(f'缺前置模块 {pre}')
            if is_hit(res.value):
                return ModuleResult('否')

        net = (ctx.upstream.get('cand_institution_net_map') or {}).get(
            ctx.stock.ts_code)
        if net is None:
            return ModuleResult('否')

        name_has_st = 'ST' in str(ctx.stock.stock_name or '')
        if df is not None and not df.empty:
            anchor = df.sort_values('trade_date').iloc[-1]
            if not (ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of)) \
                    and bool(anchor.get('is_st')):
                return ModuleResult('否')

        if not is_r4(net, name_has_st):
            return ModuleResult('否')
        return ModuleResult(f'机构净买{net / 1e8:+.2f}亿', {'net': net})
