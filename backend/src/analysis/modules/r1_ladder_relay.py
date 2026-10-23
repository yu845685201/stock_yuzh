"""R1 ``r1_ladder_relay`` 梯队接力候选（daily_review 链 R 列；2026-10-18 自独立候选链合入）。

判定（candidate_rules.is_r1 同口径，报告层第六章预案共用）：
连板 ≥ r1_min_streak 且 非 ST 且 开板 ≤ r1_max_reopen 次（封板质量；
开板数据缺失视为通过并标注）。**P1-7 分层（2026-10-21）**：高位板
（≥ r1_high_streak）开板上限收紧为 r1_high_max_reopen，理由串加分层标签
（低位板/中位板/高位板）。连板数读前置模块「连续涨停数」开板次数读环境装配
``cand_seal_open_map``（limit_seal_stats_daily）。

依赖：链内须先配 consecutive_limit_up；链 YAML 声明 env: a_share_candidates。

返回值：``{分层}/{N}板/开板{X}次`` / ``{分层}/{N}板/封板数据缺失`` / ``否`` /
``数据不足``（如 ``高位板/7板/开板0次``）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..candidate_rules import BUY_RULES, is_r1, r1_tier
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class R1LadderRelay(AnalysisModule):
    """转天候选 R1：梯队接力（连板高度 + 封板质量）。"""

    name: ClassVar[str] = 'R1梯队接力'
    name_en: ClassVar[str] = 'r1_ladder_relay'
    description: ClassVar[str] = (
        "转天买入候选 R1 梯队接力：连板 ≥ r1_min_streak(2) 且 非 ST 且 开板 ≤ "
        "r1_max_reopen(2) 次（封板质量，分笔数据缺失视为通过并标注「封板数据缺失」；"
        "高位板 ≥r1_high_streak(6) 开板上限收紧为 r1_high_max_reopen(1)，P1-7 分层）。"
        "连板数来自前置模块「连续涨停数」，开板次数来自 limit_seal_stats_daily 截面装配；"
        "判定谓词与报告层第六章预案共用（candidate_rules），同数据同结果。"
        "返回理由串（如「高位板/7板/开板0次」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['is_st']
    default_params: ClassVar[Dict[str, object]] = {
        'r1_min_streak': BUY_RULES['r1_min_streak'],
        'r1_max_reopen': BUY_RULES['r1_max_reopen'],
        'r1_high_max_reopen': BUY_RULES['r1_high_max_reopen'],
        'r1_high_streak': BUY_RULES['r1_high_streak'],
    }
    # 命中统计口径（方案 §2.5）：理由串类，非「否 / 数据不足」即命中
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
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

        seal_map = ctx.upstream.get('cand_seal_open_map') or {}
        open_count = seal_map.get(ctx.stock.ts_code)

        if not is_r1(streak, is_st, open_count, self.params):
            return ModuleResult('否')
        tier = r1_tier(streak, self.params) or ''
        if open_count is None:
            label = f'{tier}/{streak}板/封板数据缺失'
        else:
            label = f'{tier}/{streak}板/开板{int(open_count)}次'
        return ModuleResult(label, {'streak': streak, 'open_count': open_count,
                                    'tier': tier})
