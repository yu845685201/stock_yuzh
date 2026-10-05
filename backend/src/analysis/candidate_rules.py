"""转天买入候选共享规则（报告层第六章预案 × tomorrow_candidates 链同口径）。

R1-R4 判定谓词与阈值的**唯一定义处**：报告层（``review/sections/tomorrow_plan.py``）
与链模块（``analysis/modules/r*_*.py``）均从本模块取用，保证同数据同结果。

判据初版（2026-10-05 新增，待用户校准定稿，流程同情绪判据复盘框架.MD §3）：

- R1 梯队接力：连板 ≥ ``r1_min_streak`` 且 非 ST 且 开板 ≤ ``r1_max_reopen`` 次
  （封板质量；开板数据缺失视为通过，由展示层标注「封板数据缺失」）；
- R2 主线跟随：首板涨停 且 非 ST 且 概念成分命中涨停聚集前列概念
  （聚集前列 = 概念内当日涨停家数降序前 3，黑名单剔除——与报告层
  ``_limit_up_clusters`` 同口径）；
- R3 趋势新高：60 日新高 且 放量 ≥ ``r3_volume_min`` 倍 且 当日涨幅 ≥
  ``r3_change_min``% 且 非 ST；
- R4 机构席位：龙虎榜机构净买为正 且 非 ST（名称近似排除，链侧叠加 is_st）；
- 去重优先级 R1 > R2 > R3 > R4（一股只入最高优先级类）。

情绪开放度（高潮仅 R1 / 冰点·退潮不开放）由报告层施加；链输出全量底表，
解读层按当日情绪定位过滤视图（链不做情绪联动，见 tomorrow_candidates.yaml）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

#: 买入候选阈值（判据初版；config review.tomorrow_plan.buy_rules 可覆盖，
#: 报告层经 tomorrow_plan 传入；链模块经链 YAML params 覆盖，缺省即本值）
BUY_RULES: Dict[str, Any] = {
    'r1_min_streak': 2,    # R1 梯队接力：连板高度下限
    'r1_max_reopen': 2,    # R1：开板次数上限（封板质量；数据缺失视为通过）
    'r3_volume_min': 2.0,  # R3 趋势新高：放量倍数下限
    'r3_change_min': 3.0,  # R3：当日涨幅下限（%）
    'top_n': 5,            # 每类候选上限（报告层展示截断用；链层全量不截断）
}


#: 模块返回值中的非命中值（链模块去重判断用）
NO_HIT_VALUES = ('否', '数据不足')


def is_hit(value: Any) -> bool:
    """链模块值是否命中（题材/候选模块的返回值为理由串，非命中为 否/数据不足）。"""
    return value not in NO_HIT_VALUES


def is_r1(streak: Optional[int], is_st: bool, open_count: Optional[int],
          rules: Optional[Dict[str, Any]] = None) -> bool:
    """R1 梯队接力：连板高度 + 封板质量（开板数据缺失视为通过）。"""
    r = rules or BUY_RULES
    if streak is None or int(streak) < int(r['r1_min_streak']):
        return False
    if is_st:
        return False
    if open_count is not None and int(open_count) > int(r['r1_max_reopen']):
        return False
    return True


def is_r2(streak: Optional[int], is_st: bool, concepts: Optional[Sequence[str]],
          top_concepts: Sequence[str], rules: Optional[Dict[str, Any]] = None) -> bool:
    """R2 主线跟随：首板 + 概念成分命中涨停聚集前列概念。"""
    if streak is None or int(streak) != 1 or is_st:
        return False
    if not top_concepts or not concepts:
        return False
    return bool(set(concepts) & set(top_concepts))


def is_r3(is_new_high: bool, is_st: bool, volume_surge: Optional[float],
          change_rate: Optional[float],
          rules: Optional[Dict[str, Any]] = None) -> bool:
    """R3 趋势新高：60 日新高 + 放量 + 当日强势。"""
    r = rules or BUY_RULES
    if not is_new_high or is_st:
        return False
    if volume_surge is None or float(volume_surge) < float(r['r3_volume_min']):
        return False
    if change_rate is None or float(change_rate) < float(r['r3_change_min']):
        return False
    return True


def is_r4(institution_net: Optional[float], name_contains_st: bool) -> bool:
    """R4 机构席位：机构净买为正 且 非 ST（名称近似排除；链侧另叠加 is_st 列）。"""
    if institution_net is None or float(institution_net) <= 0:
        return False
    return not name_contains_st
