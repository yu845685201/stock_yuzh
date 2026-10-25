"""转天买入候选共享规则（报告层第六章预案 × daily_review 链 R 列同口径）。

R1-R4 判定谓词与阈值的**唯一定义处**：报告层（``review/sections/tomorrow_plan.py``）
与链模块（``analysis/modules/r*_*.py``）均从本模块取用，保证同数据同结果。

判据演进：

- **初版（2026-10-05）**：R1 连板≥2 + 封板质量；R2 首板 + 聚集概念；R3 新高 +
  放量 + 涨幅；R4 机构净买为正；去重优先级 R1 > R2 > R3 > R4；
- **V1.2 增强（2026-10-21，P1-7 §4.6）**：
  - A 判据分层：R1 高度分层（低位板 2-3 / 中位板 4-5 / 高位板 ≥6，高位板叠加
    开板 ≤ ``r1_high_max_reopen`` 才准入）；R3 叠加 Sequoia-X「反高开低走」
    （开盘涨幅 > ``r3_max_open_gain``% 且 收盘 < 开盘 → 剔除）；R2 叠加聚集
    强度阈值（聚集家数 ≥ ``r2_cluster_min``）；
  - B 一票否决 VETO：命中即出池（``VETO_SCORE = -100``，独立于加权评分）——
    ST/*ST、业绩暴雷（``net_profit_yoy`` 阈值）、大股东减持（窗口内有
    ``增减持`` 公告）、龙虎榜卖一占比超限、高位板放量长阴；
  - D 五维评分：权重集中 config ``review.tomorrow_plan.score_weights``
    （单一出处，源项目踩过 docstring 与代码不一致的坑）；
  - E ADX 闸门：指数 ADX 挡位控制候选开放度（full/half/closed）。

情绪开放度（高潮仅 R1 / 冰点·退潮不开放）由报告层施加；链输出全量底表，
解读层按当日情绪定位过滤视图（链不做情绪联动，见 daily_review.yaml）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

#: 买入候选阈值（判据初版；config review.tomorrow_plan.buy_rules 可覆盖，
#: 报告层经 tomorrow_plan 传入；链模块经链 YAML params 覆盖，缺省即本值）
BUY_RULES: Dict[str, Any] = {
    'r1_min_streak': 2,    # R1 梯队接力：连板高度下限
    'r1_max_reopen': 2,    # R1：开板次数上限（封板质量；数据缺失视为通过）
    'r1_high_max_reopen': 1,   # R1 高位板（≥r1_high_streak）开板次数上限（更严）
    'r1_high_streak': 6,   # R1 高位板起始连板数
    'r2_cluster_min': 2,   # R2：聚集概念内涨停家数下限（聚集强度）
    'r3_volume_min': 2.0,  # R3 趋势新高：放量倍数下限
    'r3_change_min': 3.0,  # R3：当日涨幅下限（%）
    'r3_max_open_gain': 5.0,   # R3 反高开低走：开盘涨幅上限（%），超过且收<开 → 剔除
    'top_n': 5,            # 每类候选上限（报告层展示截断用；链层全量不截断）
    'money_confirm': False,  # 资金确认过滤（R2/R3 额外要求当日大单净流入>0）——默认关，观察期后回测再开
    # VETO 阈值（P1-7 §4.6 B）
    'veto_profit_yoy': -50.0,   # 业绩暴雷：最新报告期归母净利同比下限（%）
    'veto_lhb_sell1_pct': 30.0,  # 龙虎榜卖一占比上限（%）
    'veto_high_board_lookback': 3,  # 反抽板 veto：回看交易日数（近 N 日高位板放量长阴，初版未校准）
}

#: VETO 一票否决分（独立于加权评分；命中即出池不看其他维度）
VETO_SCORE = -100

#: 五维评分默认权重（KHunter SCORE_WEIGHTS 映射；config 单一出处可覆盖）
DEFAULT_SCORE_WEIGHTS: Dict[str, float] = {
    'tech': 0.35,       # 技术：P0-1 指标 + P0-2 形态
    'money': 0.35,      # 资金：money_flow_daily + R4 机构
    'fundamental': 0.10,  # 基本面：P0-4 估值 calc
    'sector': 0.10,     # 板块：R2 概念聚集
    'event': 0.10,      # 事件：P1-11 公告归因
}


#: 模块返回值中的非命中值（链模块去重判断用）
NO_HIT_VALUES = ('否', '数据不足')


def is_hit(value: Any) -> bool:
    """链模块值是否命中（题材/候选模块的返回值为理由串，非命中为 否/数据不足）。"""
    return value not in NO_HIT_VALUES


def _rules(rules: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {**BUY_RULES, **rules} if rules else BUY_RULES


# ------------------------------------------------------------------ A 判据分层

def r1_tier(streak: Optional[int], rules: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """R1 高度分层标签：低位板（2-3）/ 中位板（4-5）/ 高位板（≥r1_high_streak）。

    未达 R1 下限返回 None（非 R1 候选无分层）。
    """
    r = _rules(rules)
    if streak is None or int(streak) < int(r['r1_min_streak']):
        return None
    s = int(streak)
    if s >= int(r['r1_high_streak']):
        return '高位板'
    if s >= 4:
        return '中位板'
    return '低位板'


def is_r1(streak: Optional[int], is_st: bool, open_count: Optional[int],
          rules: Optional[Dict[str, Any]] = None,
          market_weak: bool = False) -> bool:
    """R1 梯队接力：连板高度 + 封板质量（开板数据缺失视为通过）。

    高位板（≥ ``r1_high_streak``）叠加更严开板上限 ``r1_high_max_reopen``
    （P1-7 §4.6 A：高位板封板质量要求更高）。
    ``market_weak=True`` 时 R1 直接否决（P1-10 §4.4 弱档否决：弱势市场不打
    板——判定由报告层用指数 ADX 闸门 closed 档或简化 MA60 规则传入，谓词不
    自算市场状态）。
    """
    r = _rules(rules)
    if market_weak:
        return False
    if streak is None or int(streak) < int(r['r1_min_streak']):
        return False
    if is_st:
        return False
    if open_count is not None:
        cap = (int(r['r1_high_max_reopen'])
               if int(streak) >= int(r['r1_high_streak'])
               else int(r['r1_max_reopen']))
        if int(open_count) > cap:
            return False
    return True


def is_r2(streak: Optional[int], is_st: bool, concepts: Optional[Sequence[str]],
          top_concepts: Sequence[str], rules: Optional[Dict[str, Any]] = None,
          big_net: Optional[float] = None,
          cluster_count: Optional[int] = None) -> bool:
    """R2 主线跟随：首板 + 概念成分命中涨停聚集前列概念 + 聚集强度。

    money_confirm=True（观察期后回测开启）时额外要求当日大单净流入 > 0；
    big_net 为 None（无资金行）时按"资金未确认"不通过该附加条件（不硬造）。
    cluster_count = 命中聚集概念内当日涨停家数；< ``r2_cluster_min`` 不通过
    （P1-7 §4.6 A：聚集强度阈值，防「独苗概念」伪主线）；为 None 时不过该
    附加条件（数据缺失不硬造）。
    """
    if streak is None or int(streak) != 1 or is_st:
        return False
    if not top_concepts or not concepts:
        return False
    if not (set(concepts) & set(top_concepts)):
        return False
    r = _rules(rules)
    if cluster_count is not None and int(cluster_count) < int(r['r2_cluster_min']):
        return False
    if r.get('money_confirm') and (big_net is None or big_net <= 0):
        return False
    return True


def is_r3(is_new_high: bool, is_st: bool, volume_surge: Optional[float],
          change_rate: Optional[float], rules: Optional[Dict[str, Any]] = None,
          big_net: Optional[float] = None, open_gain: Optional[float] = None,
          close_lt_open: bool = False) -> bool:
    """R3 趋势新高：60 日新高 + 放量 + 当日强势 + 反高开低走。

    money_confirm=True（默认关）时额外要求当日大单净流入 > 0；big_net None
    （无资金行）不通过该附加条件（不硬造）。
    **反高开低走**（P1-7 §4.6 A，移植 Sequoia-X 海龟双层过滤）：开盘涨幅
    > ``r3_max_open_gain``% 且 收盘 < 开盘 → 剔除（防「高开诱多长阴」，
    源项目「郑州煤电式」踩坑护栏；open_gain/close_lt_open 缺失时不应用该
    过滤，不硬造）。
    """
    r = _rules(rules)
    if not is_new_high or is_st:
        return False
    if volume_surge is None or float(volume_surge) < float(r['r3_volume_min']):
        return False
    if change_rate is None or float(change_rate) < float(r['r3_change_min']):
        return False
    if open_gain is not None and close_lt_open \
            and float(open_gain) > float(r['r3_max_open_gain']):
        return False
    if r.get('money_confirm') and (big_net is None or big_net <= 0):
        return False
    return True


def is_r4(institution_net: Optional[float], name_contains_st: bool) -> bool:
    """R4 机构席位：机构净买为正 且 非 ST（名称近似排除；链侧另叠加 is_st 列）。"""
    if institution_net is None or float(institution_net) <= 0:
        return False
    return not name_contains_st


# ------------------------------------------------------------------ B 一票否决

def veto_reasons(*, is_st: bool = False, profit_yoy: Optional[float] = None,
                 has_reduction_ann: bool = False,
                 lhb_sell1_pct: Optional[float] = None,
                 streak: Optional[int] = None, volume_surge: Optional[float] = None,
                 change_rate: Optional[float] = None,
                 rules: Optional[Dict[str, Any]] = None) -> List[str]:
    """一票否决原因列表（空 = 无否决；非空即出池，``VETO_SCORE`` 独立于加权）。

    否决项（P1-7 §4.6 B）：
    - 技术面：高位板 + 放量长阴（连板 ≥ 高位板阈值 且 放量 ≥ R3 下限 且 当日
      跌幅 ≥ 3%）——M 头 + 多死叉共振版依赖 P0-2，后置；
    - 事件面：ST/*ST、业绩暴雷（``profit_yoy`` < 阈值）、大股东减持（窗口内有
      ``增减持`` 公告，依赖 P1-11 公告窗口）；
    - 资金面：龙虎榜卖一占比超限（``lhb_sell1_pct`` > 阈值）。

    缺失入参不过对应否决（不硬造）；暴雷阈值含等号（≤阈值即暴雷）。

    P4-1 语义修订（2026-10-25 已实施）：原"当日高位板放量长阴"三项在涨停池内
    结构性互斥（收盘涨停 change_rate ≥ +9.8% 与 ≤ −3% 不可能同日成立），已由
    可达的反抽板口径替代——见 :func:`high_board_longyin_recent`（近 N 日高位板
    放量长阴后的涨停入池即否决）；本函数不再承担该否决项。
    """
    r = _rules(rules)
    reasons: List[str] = []
    if is_st:
        reasons.append('ST/*ST')
    if profit_yoy is not None and float(profit_yoy) <= float(r['veto_profit_yoy']):
        reasons.append(f"业绩暴雷(净利同比{float(profit_yoy):.0f}%)")
    if has_reduction_ann:
        reasons.append('大股东减持')
    if lhb_sell1_pct is not None \
            and float(lhb_sell1_pct) > float(r['veto_lhb_sell1_pct']):
        reasons.append(f"龙虎榜卖一占比{float(lhb_sell1_pct):.0f}%超限")
    if (streak is not None and int(streak) >= int(r['r1_high_streak'])
            and volume_surge is not None
            and float(volume_surge) >= float(r['r3_volume_min'])
            and change_rate is not None and float(change_rate) <= -3.0):
        reasons.append('高位板放量长阴')
    return reasons


def is_vetoed(**kwargs: Any) -> bool:
    """是否触发一票否决（``veto_reasons`` 非空）。"""
    return bool(veto_reasons(**kwargs))


def high_board_longyin_recent(streaks, change_rates, volume_surges,
                              rules: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """近 N 日反抽板检测（P4-1 语义修订定稿，2026-10-25）。

    原"当日高位板放量长阴"三条件在涨停池内结构性互斥（veto_reasons docstring
    注记），语义修订为：**最近 ``veto_high_board_lookback``（默认 3）个交易日
    内存在某日 D——change_rate(D) ≤ −3% 且 volume_surge(D) ≥ ``r3_volume_min``
    且前一日连板 streak(D−1) ≥ ``r1_high_streak``**（高位板次日放量长阴 =
    出货结构），当日再涨停入池即为反抽板陷阱 → 返回理由串
    「近N日高位板放量长阴（反抽板风险）」；未命中返回 None。

    Args:
        streaks/change_rates/volume_surges：oldest→newest 的 lookback+1 日
        序列（含今日；今日为池内涨停行，change_rate ≥ +9.8% 天然不满足长阴
        条件，无需特判）。序列与面板 ``_streak``/``change_rate``/
        ``_volume_surge`` 同口径（annotate_panel 单一出处；链侧经
        candidate_veto.recent_longyin_window 的单股等价计算）。
        任一序列缺失/长度不足 → None（不硬造）。
    """
    r = _rules(rules)
    lb = int(r.get('veto_high_board_lookback', 3))
    n = lb + 1
    if (not streaks or not change_rates or not volume_surges
            or len(streaks) < n or len(change_rates) < n or len(volume_surges) < n):
        return None
    high = int(r['r1_high_streak'])
    vol_min = float(r['r3_volume_min'])
    for i in range(len(streaks) - lb, len(streaks)):
        cr, vs = change_rates[i], volume_surges[i]
        if cr is None or vs is None:
            continue
        if float(cr) <= -3.0 and float(vs) >= vol_min \
                and streaks[i - 1] is not None and int(streaks[i - 1]) >= high:
            return f'近{lb}日高位板放量长阴（反抽板风险）'
    return None


# ------------------------------------------------------------------ D 五维评分

def score_candidate(dims: Dict[str, Optional[float]],
                    weights: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """五维加权评分（KHunter SCORE_WEIGHTS 映射；config 单一出处）。

    Args:
        dims: 维度 → 0~100 分（None = 该维缺数，按剩余维权重归一化，不硬造）；
        weights: 覆盖默认权重（``DEFAULT_SCORE_WEIGHTS``）。

    Returns:
        ``{score, applied_weights, missing_dims}``；全缺 → ``score=None``。
    """
    w = {**DEFAULT_SCORE_WEIGHTS, **(weights or {})}
    applied: Dict[str, float] = {}
    total = 0.0
    acc = 0.0
    missing: List[str] = []
    for dim, weight in w.items():
        v = dims.get(dim)
        if v is None:
            missing.append(dim)
            continue
        applied[dim] = float(weight)
        acc += float(weight) * float(v)
        total += float(weight)
    if total <= 0:
        return {'score': None, 'applied_weights': {}, 'missing_dims': missing}
    return {'score': round(acc / total, 2), 'applied_weights': applied,
            'missing_dims': missing}


# ------------------------------------------------------------------ E ADX 闸门

def adx_gate(adx: Optional[float], adx_rising: bool, close: Optional[float],
             ma20: Optional[float]) -> str:
    """指数 ADX 挡位 → 候选开放度（P1-7 §4.6 E，升级 P1-10 简化否决）。

    - ``full``：ADX > 25 且上升 → 全开放；
    - ``half``：ADX < 18 且上升且 收盘 > MA20 → 半开放；
    - ``closed``：其他（震荡/下行不开放）。
    输入缺失 → ``closed``（禁不硬造默认开放）。
    """
    if adx is None or close is None or ma20 is None:
        return 'closed'
    a = float(adx)
    if a > 25 and adx_rising:
        return 'full'
    if a < 18 and adx_rising and float(close) > float(ma20):
        return 'half'
    return 'closed'
