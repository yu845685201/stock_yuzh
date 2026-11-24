"""维度6 子节：转天交易预案（情景应对）。

结构：基准定位（情绪周期 → 基准策略倾向）→ 盘前情景（隔夜外围三档，判据=较
锚定日收盘 ±overseas_gap_pct）→ 盘中验证（量能 / 梯队 / 溢价三验点）→
方向观察（涨停聚集 / 行业榜 / 新高新低 / 机构动向，数据驱动填充）→ 口径注记。

设计边界（继承复盘框架.MD §3 情绪判据定位）：情景应对为「条件-动作」参考
框架，非次日涨跌预测（校准实测冰点/退潮次日区分度 <0.3pct）；仓位语言为
倾向描述（防守/中性/跟随），不给具体仓位数字。阈值为判据初版，config
review.tomorrow_plan 可调，待用户校准定稿（流程同情绪判据 §3）。
"""

from __future__ import annotations

import json
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

#: 判据初版默认值（config review.tomorrow_plan 可覆盖；待用户校准定稿）
DEFAULT_RULES: Dict[str, Any] = {
    'overseas_gap_pct': 1.5,   # 盘前外围情景分档：恒指/纳指较锚定日收盘 ±1.5%
    'volume_band_pct': 20,     # 盘中量能验证分档：转天成交额较今日基准 ±20%
}

#: 买入候选规则默认值（判据初版；config review.tomorrow_plan.buy_rules 可覆盖）
DEFAULT_BUY_RULES: Dict[str, Any] = {
    'r1_min_streak': 2,    # R1 梯队接力：连板高度下限
    'r1_max_reopen': 2,    # R1：开板次数上限（封板质量；分笔缺失视为通过并标注）
    'r3_volume_min': 2.0,  # R3 趋势新高：放量倍数下限
    'r3_change_min': 3.0,  # R3：当日涨幅下限（%）
    'top_n': 5,            # 每类候选上限
}

#: 情绪周期定位 → 基准策略倾向（判据初版话术模板，待用户按方法论校准）
_BASE_POSITIONING = {
    '高潮': '谨慎乐观、防分歧兑现——高位兑现为主不追高，炸板率与梯队晋级验证优先于进攻',
    '回暖': '中性偏多、跟随确认——主线与梯队验证通过后再参与，不左侧抢跑',
    '震荡': '中性观望、等方向——主线清晰前降低操作频率，重点跟踪量能与梯队验证',
    # 组合态单独处理（技术方案 V1.0 §8.2：不通过子串包含"回暖"推导中性偏多，
    # 组合态话术不得偏向单边定性）
    '回暖/震荡': '中性偏观望、方向待确认——回暖与震荡证据并存时不单边定性：'
                 '主线与梯队验证通过后再参与，主线不清晰时降低操作频率',
    '冰点': '观察等企稳——跌停家数收敛与溢价转正前不左侧；企稳信号=跌停<涨停 且 溢价>0',
    '退潮': '防守降仓——避开高位股与断板梯队，等待高度重建或冰点后的企稳信号',
}

#: 未知状态兜底倾向（按震荡口径处理；组合态已有专属映射，不再兜底到"回暖"）
_FALLBACK_POSITIONING = _BASE_POSITIONING['震荡']

#: 盘前情景锚定的外围指数（A 股联动最强；数据来自报告口径 overseas_indices）
_OVERSEAS_ANCHORS = ('恒生指数', '纳斯达克')

#: 每类候选的转天买入决策树（判据初版话术模板；确认/否决/风控三分支）
_DECISION_TREES: Dict[str, Dict[str, str]] = {
    'R1': {
        'confirm': '确认买入：竞价 +2%~+6% 且大盘非 P1 强化档；开盘 30 分钟内封板或站稳'
                   '分时均价 → 接力',
        'veto': '否决放弃：竞价低开 / 高开 >+7%（透支，防开板）/ 开盘 30 分钟跌破分时均价',
        'risk': '风控：入场后跌破竞价低点离场；断板即走，不恋战',
    },
    'R2': {
        'confirm': '确认买入：所属概念转天再涨停 ≥2 家（板块效应延续）且自身竞价 +2% '
                   '以内 → 低吸跟风，不追高',
        'veto': '否决放弃：概念龙头竞价大幅低开或盘中炸板（效应断）/ 自身高开 >+5%'
                '（跟风透支更快）',
        'risk': '风控：跌破昨日收盘价离场；板块效应消失当日不补仓',
    },
    'R3': {
        'confirm': '确认买入：早盘量能不低于昨日同期且股价站稳昨收上方 → 顺势介入',
        'veto': '否决放弃：高开低走跌破昨收 / 明显缩量新高（动能衰减）→ 等回踩不追',
        'risk': '风控：跌破 5 日线减仓离场；新高股回撤容忍度低于低吸类',
    },
    'R4': {
        'confirm': '确认买入：竞价平开~+3% 且承接良好 → 跟随（一日榜为短线博弈资金，'
                   '快进快出预期）',
        'veto': '否决放弃：竞价低开 / 开盘放量下杀（机构一日榜反手卖出）',
        'risk': '风控：当日走势不及预期（未创日内新高）即离场，不隔夜扛',
    },
}

#: 候选类型展示名与入选规则说明（表格用）
_CANDIDATE_META = {
    'R1': ('梯队接力候选', '连板 ≥{r1_min_streak} 且 非 ST 且 开板 ≤{r1_max_reopen} 次'
                           '（按连板高度、成交额降序）'),
    'R2': ('主线跟随候选', '首板涨停 且 非 ST 且 题材命中涨停聚集前列概念'
                           '（按成交额降序）'),
    'R3': ('趋势新高候选', '60 日新高 且 放量 ≥{r3_volume_min}x 且 当日涨幅 ≥'
                           '{r3_change_min}% 且 非 ST（按成交额降序）'),
    'R4': ('机构席位候选', '龙虎榜机构净买前列 且 非 ST 且 不与前类重复（按净买额降序）'),
}


def build(agg: Dict[str, Any], review_cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """转天交易预案 blocks；情绪数据缺失时整节降级（缺数据标注规范）。"""
    sentiment = agg.get('sentiment') or {}
    m = sentiment.get('data') or {}
    if not sentiment.get('available', True) or not m:
        return [{'type': 'note',
                 'text': '⏳ 待补：情绪数据缺失，转天交易预案无法生成'}]

    rules = dict(DEFAULT_RULES)
    rules.update((review_cfg or {}).get('tomorrow_plan') or {})
    gap = float(rules['overseas_gap_pct'])
    band = float(rules['volume_band_pct'])

    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 3, 'text': '转天交易预案（情景应对，判据初版）'},
    ]

    # --- 基准定位 ---
    cycle = m.get('cycle') or {}
    position = str(cycle.get('position') or '')
    stance = _match_positioning(position)
    basis_bits = [f'情绪定位 {position or "待校准"}']
    if m.get('max_streak') is not None:
        basis_bits.append(f'空间板 {m["max_streak"]} 板')
    if m.get('limit_up_count') is not None:
        basis_bits.append(f'涨停 {m["limit_up_count"]} 家')
    if m.get('burst_rate') is not None:
        basis_bits.append(f'炸板率 {m["burst_rate"]:.0%}')
    if m.get('premium_mean') is not None:
        basis_bits.append(f'昨日涨停溢价均值 {m["premium_mean"]:+.2f}%')
    blocks.append({'type': 'para',
                   'text': f'基准倾向（{ "；".join(basis_bits) }）：{stance}'})

    # --- 盘前情景（隔夜外围三档）---
    anchor_rows = _overseas_anchor_rows(agg)
    if anchor_rows:
        blocks.append({'type': 'heading', 'level': 3, 'text': '盘前情景：隔夜外围分档应对'})
        blocks.append({'type': 'table',
                       'headers': ['情景', f'触发判据（较锚定日收盘 ±{gap}%）', '应对预案'],
                       'rows': [
                           [f'P1 外围续弱',
                            f'恒指或纳指隔夜累计 ≤ -{gap}%',
                            '预期低开：不抢反弹，开盘后先看承接——低开高走且量能放大'
                            ' → 分批回补；低开低走 → 维持防守；A-H 联动链（汽车/科技）'
                            '开盘压力最大，两地共振强势方向（见下）相对抗压'],
                           ['P2 外围平稳',
                            f'两锚变动均在 ±{gap}% 以内',
                            '按 A 股自身节奏：以盘中验证点（量能/梯队/溢价）为准，'
                            '验证通过再进攻，不因外围平静而放松验证'],
                           [f'P3 外围反弹',
                            f'恒指或纳指 ≥ +{gap}%',
                            '预期高开：高开幅度 >2% 警惕高开低走（兑现压力）；'
                            '首小时放量高开高走 → 跟随主线方向；高开缩量 → 观望为主'],
                       ]})
        blocks.append({'type': 'table',
                       'headers': ['盘前对照基准（锚定日收盘，报告口径）', '收盘', '5日%', '60日位置'],
                       'rows': anchor_rows})
        blocks.append({'type': 'note',
                       'text': '对照口径：转天盘前以隔夜（假期场景为累计）收盘较上表基准的分档'
                               f'变动触发情景；基准为报告锚定日口径（同锚定日可复现），'
                               '假期最新外围状态以链 CSV 与 digest 解读为准'})
        # W7 Phase 1：A50 观察锚（不入触发）+ 周末触发缝提示（零红线冲突：
        # 基准为锚定日口径的 A50 行，实时读数观察归解读/盘前操作层）
        env = (agg.get('market_env') or {}).get('data') or {}
        a50_row = next((o for o in (env.get('overseas') or [])
                        if o.get('name') == '富时A50期货'), None)
        if (a50_row and env.get('a50_source') == 'kline'
                and a50_row.get('close') is not None):
            blocks.append({'type': 'note',
                           'text': f'A50 期货观察锚（不入触发）：{a50_row["close"]:,.0f}'
                                   f'（{a50_row.get("date", "—")}，锚定日口径隔夜环比见§一）'
                                   '——近连续交易、周末亦报价；盘前可对照实时 A50 读数'
                                   '观察周末/隔夜缺口，P1/P3 触发仍以恒指/纳指为准'
                                   '（Phase 1 观察，积累 2 周后评估是否入触发器）'})
        as_of, next_td = agg.get('as_of'), agg.get('next_trade_date')
        if as_of and next_td:
            try:
                gap = (date.fromisoformat(f'{next_td[:4]}-{next_td[4:6]}-{next_td[6:8]}')
                       - date.fromisoformat(f'{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]}')).days
            except ValueError:
                gap = 1
            if gap > 1:
                blocks.append({'type': 'note',
                               'text': f'距下一交易日 {gap} 天：恒指/纳指期间无新收盘，'
                                       '盘前 P1/P3 重判以两锚现值为准无增量——'
                                       '周末缺口以 A50 观察锚实时读数观察（不入触发）'})

    # --- 盘中验证（V 系列验点：V1 量能 / V1.5 量价联动 / V2 梯队 / V3 溢价 / V4 缠论）---
    blocks.append({'type': 'heading', 'level': 3,
                   'text': '盘中验证：验点表（V1/V1.5/V2/V3/V4，可得几项算几项）'})
    v_rows: List[List[str]] = []
    turnover = ((agg.get('market_env') or {}).get('data') or {}).get('turnover') or {}
    amount_base = turnover.get('today_total')
    if amount_base:
        amount_yi = amount_base / 1e8
        v_rows.append([
            'V1 量能',
            f'转天成交额 vs 今日基准 {amount_yi:,.0f} 亿（±{band:.0f}% 分档）',
            '放量上涨 = 主线确认，可跟随加仓',
            '缩量上涨 = 反弹缺承接，兑现为主；放量下跌 = 防守',
        ])
    # 量价四象限联动（P1 双锚；量价资金方案 V1.1 §3.3③）
    vp = (agg.get('volume_price') or {})
    if vp.get('available'):
        anchors = (vp.get('data') or {}).get('anchors') or []
        quads = '；'.join(f"{a['name']} {a.get('quad') or '—'}" for a in anchors)
        if quads:
            v_rows.append([
                'V1.5 量价联动',
                f'今日四象限（{quads}）——转天象限迁移观察',
                '缩量涨 → 放量涨 = 背离修复，加验证权重；平稳 → 放量涨 = 健康确认',
                '放量涨 → 缩量跌 = 恐慌转折信号，立即防守；缩量跌延续 = 阴跌观望',
            ])
    top_stock = _top_streak_stock(agg)
    if top_stock:
        name, streak = top_stock
        v_rows.append([
            'V2 梯队',
            f'高度股 {name}（{streak} 板）晋级封板与否',
            '晋级 = 情绪延续，关注同梯队跟随与低位补涨',
            '断板/大面 = 梯队断层，转防守（退潮信号）',
        ])
    if m.get('premium_mean') is not None:
        v_rows.append([
            'V3 溢价',
            f'今日涨停 {m.get("limit_up_count", "—")} 只的转天溢价 vs 均值基准 {m["premium_mean"]:+.2f}%',
            '溢价转正/扩大 = 接力修复',
            '续降/转负 = 亏钱效应扩散，降仓位',
        ])
    # V4 缠论验点（P1-8）：缠论池次日开盘相对全市场中位超额——核验闭环
    # （verifier CHAN 池 excess_median）已积累该数据，此处补预案层展示
    chan_n = len(((agg.get('chan') or {}).get('data') or {}).get('items') or [])
    if chan_n:
        v_rows.append([
            'V4 缠论池',
            f'缠论候选 {chan_n} 只的次日开盘相对全市场中位超额'
            '（收盘确认→次日开盘口径，与验证回测撮合一致）',
            '超额 ≥0 = 左侧池互补性初步兑现，维持观察池权重',
            '连续为负 = 左侧池与右侧体系互斥性存疑，降观察权重（联动分级复核）',
        ])
    if v_rows:
        blocks.append({'type': 'table',
                       'headers': ['验点', '观察方法（阈值）', '通过 →', '未通过 →'],
                       'rows': v_rows})

    # --- 方向观察（数据驱动）---
    obs = _direction_watch(agg)
    if obs:
        blocks.append({'type': 'heading', 'level': 3, 'text': '方向观察（数据驱动）'})
        blocks.append({'type': 'bullets', 'items': obs})

    # --- 转天买入候选（观察池 + 买入决策树）---
    blocks.extend(_buy_candidates(agg, rules))

    # --- 口径注记 ---
    blocks.append({'type': 'note',
                   'text': '判据初版（2026-10-05 新增）：阈值见 config review.tomorrow_plan，'
                           '待用户校准定稿（流程同情绪判据 §3）；情景应对为「条件-动作」'
                           '参考框架，非次日涨跌预测（情绪判据校准实测次日区分度 <0.3pct）；'
                           '仓位为倾向描述（防守/中性/跟随），具体仓位由个人风险偏好决定，'
                           '本报告不构成投资建议'})
    return blocks


def _match_positioning(position: str) -> str:
    """情绪定位 → 基准倾向（精确枚举映射，技术方案 V1.0 §8.2）。

    组合态「回暖/震荡」有专属映射，不通过子串包含"回暖"推导为中性偏多；
    未知态（如试用版判据待校准）走震荡兜底。
    """
    if position in _BASE_POSITIONING:
        return _BASE_POSITIONING[position]
    return _FALLBACK_POSITIONING


def _overseas_anchor_rows(agg: Dict[str, Any]) -> List[List[str]]:
    """盘前对照基准行（恒指/纳指锚定日收盘与 5日/位置）；无外围日K时返回空。"""
    indices = (((agg.get('market_env') or {}).get('data') or {}).get('overseas_indices')) or []
    rows = []
    for e in indices:
        if e.get('name') in _OVERSEAS_ANCHORS:
            rows.append([
                f'{e["name"]}（{e.get("date", "—")}）',
                f'{e.get("close"):,.0f}' if e.get('close') is not None else '—',
                f'{e["chg_5d"]:+.1f}' if e.get('chg_5d') is not None else '—',
                f'{e["position_pct"]:.0f}%' if e.get('position_pct') is not None else '—',
            ])
    return rows


def _top_streak_stock(agg: Dict[str, Any]) -> Optional[tuple]:
    """最高板代表股（涨停明细首行，按连板降序）；无明细返回 None。"""
    limit_up = (((agg.get('movers') or {}).get('data') or {}).get('limit_up')) or []
    if not limit_up:
        return None
    top = limit_up[0]
    streak = top.get('streak') or 0
    if streak < 2:
        return None
    return str(top.get('name') or top.get('code')), int(streak)


def _direction_watch(agg: Dict[str, Any]) -> List[str]:
    """方向观察清单：涨停聚集 / 行业榜 / 新高新低（高低切换）/ 机构动向 / 披露。"""
    items: List[str] = []

    sector = ((agg.get('sector') or {}).get('data')) or {}
    clusters = sector.get('limit_up_clusters') or []
    industry_today = sector.get('industry_today') or []
    if clusters:
        items.append('强势延续候选：'
                     + '、'.join(f'{c["concept"]} {c.get("count", "—")} 家' for c in clusters[:3])
                     + '（今日涨停聚集前列）')
    if industry_today:
        items.append('行业领涨对照：'
                     + '、'.join(f'{r["industry"]} {r["mean_change"]:+.1f}%'
                                 for r in industry_today[:3])
                     + '（等权口径）')

    movers = ((agg.get('movers') or {}).get('data')) or {}
    new_high = movers.get('new_high') or []
    new_low = movers.get('new_low') or []
    if new_high or new_low:
        hi = '、'.join(str(r['name']) for r in new_high[:5]) or '—'
        lo = '、'.join(str(r['name']) for r in new_low[:5]) or '—'
        items.append(f'高低切换跟踪：60日新高方向（按成交额前列：{hi}）vs 新低方向（{lo}）'
                     '——验证切换延续或超跌反弹')

    lhb = ((agg.get('lhb') or {}).get('data')) or {}
    institution = lhb.get('institution') or []
    if institution:
        items.append('机构动向：龙虎榜机构净买前列 '
                     + '、'.join(f'{r.get("name") or r["ts_code"]} {r["net"] / 1e8:+.2f} 亿'
                                 for r in institution[:3]))

    disclosure = (((agg.get('fundamentals') or {}).get('data') or {}).get('disclosure')) or {}
    if disclosure.get('next_5'):
        items.append(f'披露扰动：未来 5 个交易日 {disclosure["next_5"]} 家预约披露，'
                     '关注业绩雷/喜对持仓与主线的扰动')
    return items


def _r_pool_prior(agg: Dict[str, Any], ctype: str) -> str:
    """P4-8（评二-7）：R 系列候选先验随行——离线同构回测优先、前向核验补充。

    数据源按可得性组合，均如实标注（不硬造红线）：
    - 离线回测：data/priors/r_pool_offline_prior.json（r_pool_offline_backtest.py
      产出；R3 全历史 / R1 自封板数据起点 20251009；R2/R4 受概念时点记录与
      龙虎榜历史限制**不可行**，见 JSON meta.r2_r4_note）；文件缺失或该池无
      记录 → 跳过；
    - 前向核验：agg['verification'].data.rolling.pools[ctype]（verifier.
      rolling_stats 单一出处：近 30 核验日池超额/胜率滚动）；
    - 两者皆无 → "先验：未统计"如实注明。缠论池先验见 chan_candidates.HIST_NOTE。
    """
    parts: List[str] = []
    offline = _offline_prior().get(ctype)
    if offline:
        parts.append(f"离线同构回测 {offline['window']}（{offline['signals']} 信号/"
                     f"{offline['days']} 日，中位超额 {offline['avg_excess_median']:+.2f}pct，"
                     f"胜率 {offline['win_rate']:.0%}）")
    ver = agg.get('verification') or {}
    st = (((ver.get('data') or {}).get('rolling') or {}).get('pools')
          or {}).get(ctype) or {}
    if st.get('days'):
        fwd = f'前向核验近 {st["days"]} 个核验日'
        if st.get('avg_excess') is not None:
            fwd += f'，池均超额 {st["avg_excess"]:+.2f}pct'
        parts.append(fwd)
    if not parts:
        return (f'{ctype} 先验：未统计（核验体系前向积累中；R2/R4 离线回测受'
                '概念时点记录与龙虎榜历史限制不可行——仅有前向核验）')
    return f'{ctype} 先验：{"；".join(parts)}；历史统计非预测，仅作观察校准参考'


@lru_cache(maxsize=1)
def _offline_prior() -> Dict[str, Any]:
    """离线回测先验 JSON（进程级读盘缓存一次；文件缺失/损坏返回空 dict 不硬造）。"""
    path = (Path(__file__).resolve().parents[4] / 'data' / 'priors'
            / 'r_pool_offline_prior.json')
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _fmt_open_count(oc: Any) -> str:
    """开板次数展示：None=封板数据缺失（is_r1 视为通过），不写数字。"""
    return '封板数据缺失' if oc is None else f'开板{int(oc)}次'


def _r1_weak_veto_text(cand: Dict[str, Any]) -> Optional[str]:
    """R1 弱档否决明细行（V4.3 A2）：消费 aggregator R1_filtered（仅连板达标
    且非 ST 行）。market_weak 日 R1 被剔出 open_types，空池文案不会触发——
    该明细是弱档否决在报告层的唯一逐股落点（20261023 链 3 只 vs 报告 0 的
    对账差异从此看报告行，不用看代码注释）。无明细返回 None（不硬造）。"""
    filtered = [f for f in (cand.get('R1_filtered') or [])
                if f.get('reason') == 'weak_veto']
    if not filtered:
        return None
    top = '、'.join(f"{f.get('name') or f.get('code')}（{f.get('streak')}板/"
                    f"{_fmt_open_count(f.get('open_count'))}）"
                    for f in filtered[:3])
    return (f'R1 弱档否决明细：{len(filtered)} 只连板股整类不开放'
            f'（前 {min(3, len(filtered))}：{top}）——链 CSV R1 列为全量命中'
            '（不带弱档闸门），链/报告差异属设计行为')


def _r1_empty_text(cand: Dict[str, Any]) -> str:
    """R1 空池三态归因（V4.3 A2）：弱档否决/规则筛除/池缺失——替代旧文案
    「池数据缺失或无满足规则的股票」两态混写（10-23 实况为弱档否决，被误归因）。
    仅 R1 消费（R1_filtered 明细只在该池产生）；R2-R4 空池维持旧文案。"""
    filtered = cand.get('R1_filtered') or []
    if filtered and all(f.get('reason') == 'weak_veto' for f in filtered):
        return _r1_weak_veto_text(cand) or '无入选（池数据缺失或无满足规则的股票）'
    if filtered:
        over = [f for f in filtered
                if f.get('reason') in ('reopen_over', 'high_reopen_over')]
        n_high = sum(1 for f in over if f.get('reason') == 'high_reopen_over')
        detail = '、'.join(f"{f.get('name') or f.get('code')}"
                           f"（{_fmt_open_count(f.get('open_count'))}）"
                           for f in over[:3])
        return (f'无入选——{len(filtered)} 只连板股均未过封板规则：开板超限 '
                f'{len(over)} 只（{detail}）、其中高位板更严档 {n_high} 只')
    return '无入选（池数据缺失：当日无连板≥2 截面）'


def _buy_candidates(agg: Dict[str, Any], rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    """转天买入候选（观察池·§3.3 2026-11-24 修订）：消费**发布批次**
    ``agg['candidate_publication']`` 的 published 名单（情绪开放 + ADX 闸门 +
    弱档 + 五维评分排序 + top_n 截取在 candidate_publication 单一实现），
    正文候选表逐项等于实际发布名单——摘要与预测落库同源（缺键时现场构建，
    纯函数同输入同结果，向后兼容旧缓存/测试夹具）。

    候选 = 满足显式规则的观察名单，非推荐；「是否买入」由转天走势对决策树
    的验证决定（确认/否决/风控三分支）。判据初版，阈值 config
    review.tomorrow_plan.buy_rules，待用户校准定稿。
    """
    brules = dict(rules.get('buy_rules') or {})
    if not brules:
        brules = dict(((agg.get('candidates') or {}).get('rules')) or {})
    top_n = int(brules.get('top_n') or 5)

    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 3, 'text': '转天买入候选（观察池·判据初版）'},
    ]

    cand = agg.get('candidates') or {}
    if cand and not cand.get('available', True):
        blocks.append({'type': 'note',
                       'text': '⏳ 待补：候选池数据缺失（截面为空），转天买入候选无法生成'})
        return blocks

    # 发布批次（§3.3）：正文从同一发布名单渲染
    pub = agg.get('candidate_publication')
    if pub is None:
        from ..candidate_publication import build_candidate_publication
        pub = build_candidate_publication(cand, agg.get('sentiment') or {}, {})
    position = pub.get('position') or ''
    adx_info = pub.get('adx_gate') or {}
    gate = str(adx_info.get('gate') or 'closed')
    market_weak = bool(adx_info.get('market_weak'))
    open_types = tuple(pub.get('open_types') or ())

    # 整体关闭文案（冰点/退潮/异常定位/ADX closed——与发布批次 close_reason
    # 同源；正式右侧候选 0 只对读者与次日核验同口径）
    if not open_types:
        reason = pub.get('close_reason') or ''
        if position in ('冰点', '退潮'):
            blocks.append({'type': 'para', 'text': f'情绪定位{position}：本轮不出买入候选'
                            + ('；企稳信号（跌停<涨停 且 昨日涨停溢价>0 且 空间板高度回升）'
                               ' 触发后，下一报告期再开放候选' if position == '冰点'
                               else '（防守降仓期，避开高位股与断板梯队）；高度重建或冰点企稳后再开放')})
        elif gate == 'closed' and position in ('高潮', '回暖/震荡'):
            blocks.append({'type': 'para', 'text': _gate_note(adx_info, market_weak)})
            blocks.append({'type': 'para',
                           'text': '指数趋势闸门关闭（震荡/下行）：本轮不出买入候选——'
                                   'ADX 闸门与情绪开放度叠加生效（任一层关闭即不出）'})
        elif position in ('高潮', '回暖/震荡'):
            # 高潮 × 弱档 / 回暖震荡 × 弱档全关（openness_text 已注明）
            blocks.append({'type': 'para', 'text': pub.get('openness_text') or reason})
            blocks.append({'type': 'para', 'text': _gate_note(adx_info, market_weak)})
            if market_weak:
                weak_txt = _r1_weak_veto_text(cand)
                if weak_txt:
                    blocks.append({'type': 'note', 'text': weak_txt})
        else:
            blocks.append({'type': 'para',
                           'text': f'情绪定位异常（{position or "空"}）：本轮不出买入候选，'
                                   '转人工复核——判据异常时兜底方向为全关而非全开（P4-4）'})
        blocks.append({'type': 'para', 'text': '正式右侧候选 0 只（发布批次：各池均未开放）。'})
        blocks.append({'type': 'note',
                       'text': '候选为规则筛选的观察名单，非推荐；判据初版待校准，'
                               '本报告不构成投资建议'})
        blocks.extend(_theme_watch_blocks(pub))
        return blocks

    # 开放度行 + 闸门说明（文案与发布批次一致）
    openness = pub.get('openness_text') or ''
    blocks.append({'type': 'para', 'text': f'候选开放度：{openness}'})
    blocks.append({'type': 'para', 'text': _gate_note(adx_info, market_weak)})
    # V4.3 A2：弱档否决 R1 明细随行（R1 已被剔出 open_types，空池三态文案
    # 不会触发）——链 CSV R1 列为全量命中（不带弱档闸门），链/报告差异看报告行
    if market_weak and 'R1' not in open_types:
        weak_txt = _r1_weak_veto_text(cand)
        if weak_txt:
            blocks.append({'type': 'note', 'text': weak_txt})

    fmt_amt = lambda a: f'成交额 {a / 1e8:.1f} 亿' if a else '—'
    money_confirm = bool(brules.get('money_confirm'))

    def _money_cell(r):
        """资金确认列（展示恒开；money_confirm=true 时未确认标 ⚠）"""
        net = r.get('big_net')
        if net is None:
            cell = '—'
            flag = '⚠未确认' if money_confirm else ''
            return f'{cell}{flag}'.strip()
        cell = f'{"净流入" if net > 0 else "净流出"}{abs(net) / 1e8:.2f} 亿'
        if money_confirm and net <= 0:
            cell += ' ⚠'
        return cell

    _REMARK = {
        'R1': lambda r: fmt_amt(r.get('amount')),
        'R2': lambda r: fmt_amt(r.get('amount')),
        'R3': lambda r: fmt_amt(r.get('amount')),
        'R4': lambda r: '龙虎榜席位口径（一日榜）',
    }

    for ctype in open_types:
        label, rule_desc = _CANDIDATE_META[ctype]
        rule_desc = rule_desc.format(**{**DEFAULT_BUY_RULES, **brules})
        pool = pub['pools'][ctype]
        # 正文候选表逐项 = 发布名单 published（§3.3：读者看到的名单与次日
        # 评价的名单一致；截断差异见 pool['truncated']，不静默缩池）
        published = pool['published']
        n_eligible = pool['n_eligible']
        rows = [[r.get('code') or '—', r.get('name') or '—', r.get('label') or '—',
                 _REMARK[ctype](r),
                 ('/'.join(r.get('evidence') or []) or '—')
                 + (f"（{r['evidence_count']}证）" if r.get('evidence') else ''),
                 (f"{r['score']:.1f}" if r.get('score') is not None else '—')]
                for r in published]
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f'{label}（开放池全量 {n_eligible} 只，发布前 '
                               f'{min(top_n, len(published))} 只，'
                               f'规则：{rule_desc}）'})
        blocks.append({'type': 'note', 'text': _r_pool_prior(agg, ctype)})  # P4-8 先验随行
        if rows:
            headers = ['代码', '名称', '当日状态', '备注', '证据（五维计数）', '五维评分']
            blocks.append({'type': 'table', 'headers': headers, 'rows': rows})
            tree = _DECISION_TREES[ctype]
            blocks.append({'type': 'bullets',
                           'items': [tree['confirm'], tree['veto'], tree['risk']]})
        else:
            # V4.3 A2：R1 空池三态归因（弱档否决/规则筛除/池缺失）；R2-R4 维持旧文案
            empty = _r1_empty_text(cand) if ctype == 'R1' \
                else '无入选（池数据缺失或无满足规则的股票）'
            blocks.append({'type': 'para', 'text': empty})

    # VETO 出池点名（P1-7 §4.6 B：一票否决命中即出池，禁静默缩池）
    vetoed = cand.get('vetoed') or []
    if vetoed:
        items = [f"{v.get('name') or v.get('code')}（{v.get('type')}）："
                 f"{'、'.join(v.get('reasons') or [])}" for v in vetoed[:10]]
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f"一票否决出池（{len(vetoed)} 只，VETO 独立于加权评分）"})
        blocks.append({'type': 'bullets', 'items': items})
        blocks.append({'type': 'note',
                       'text': 'VETO 命中项（ST/*ST、业绩暴雷、大股东减持、龙虎榜卖一占比超限、'
                               '高位板放量长阴）为规则出池非数据缺失；否决要毙掉的是尾部风险，'
                               '若被毙的恰是高收益股，否决规则本身需 A/B 回测复议'})
    elif cand.get('vetoed_count') == 0:
        blocks.append({'type': 'para', 'text': '一票否决出池：本报告期 0 只'})

    # 题材跟踪（§3.3：未开放池的原始命中单列——注明来源，不附交易条件）
    blocks.extend(_theme_watch_blocks(pub))

    blocks.append({'type': 'note',
                   'text': '候选为规则筛选的观察名单（判据初版 2026-10-05 新增，阈值 config '
                           'review.tomorrow_plan.buy_rules，待用户校准定稿）；全量底表见链 CSV '
                           'data/analysis/daily_review_<锚定日>.csv 的 R1-R4 四列'
                           '（2026-10-18 起候选列合入 daily_review，原独立候选链已删除；'
                           '与报告同规则同序）；'
                           '「是否买入」由转天走势对决策树的验证决定——确认/否决/风控三分支缺一'
                           '不可；竞价与盘中阈值参数由个人风险偏好校准；本报告不构成投资建议'})
    return blocks


def _gate_note(adx_info: Dict[str, Any], market_weak: bool) -> str:
    """ADX 闸门说明行（V4.3 A1 弱档随行——ma60/market_weak 已在 adx_info 算出，
    此前零渲染；gate=full/half 且 market_weak 时触发源必为 MA60 简化规则）。"""
    gate = str(adx_info.get('gate') or 'closed')
    weak_note = ''
    if adx_info.get('ma60') is not None:
        if market_weak:
            src = 'ADX 闸门 closed' if gate == 'closed' else 'MA60 简化规则'
            weak_note = (f"；MA60 {adx_info['ma60']:.0f}"
                         f"{'↓' if gate != 'closed' else ''} vs 收盘 "
                         f"{adx_info.get('close', 0):.0f}"
                         f"——弱档判定生效（R1 不开放；触发源={src}）")
        else:
            weak_note = (f"；MA60 {adx_info['ma60']:.0f} vs 收盘 "
                         f"{adx_info.get('close', 0):.0f}——弱档判定未生效")
    gate_note = (f"指数 ADX 闸门：{gate}"
                 + (f"（{adx_info['index']} ADX {adx_info['adx']}"
                    f"{'↑' if adx_info.get('adx_rising') else '↓'}，"
                    f"收盘 {adx_info['close']:.0f} vs MA20 {adx_info['ma20']:.0f}）"
                    if adx_info.get('adx') is not None
                    else f"（{adx_info.get('reason', '数据不足')}）")) + weak_note
    return gate_note


def _theme_watch_blocks(pub: Dict[str, Any]) -> List[Dict[str, Any]]:
    """题材跟踪段（§3.3）：未开放池的原始命中单列——题材样本，注明来源，
    不附正式候选的交易条件（2026-11-23 三只 R2 原始命中的归处）。"""
    watch = pub.get('theme_watch') or []
    if not watch:
        return []
    blocks = [{'type': 'heading', 'level': 3,
               'text': f'题材跟踪（{len(watch)} 只，非正式候选）'}]
    rows = [[w.get('code') or '—', w.get('name') or '—',
             w.get('label') or '—',
             '/'.join(str(c) for c in (w.get('concepts') or [])) or '—']
            for w in watch]
    blocks.append({'type': 'table',
                   'headers': ['代码', '名称', '命中画像', '所属概念'],
                   'rows': rows})
    blocks.append({'type': 'note',
                   'text': '来源：候选池未开放时的原始命中（发布批次 theme_watch）——'
                           '题材样本仅作跟踪观察（当日涨停聚集与题材延续的检验），'
                           '不附正式候选的买入决策树与交易条件；次日核验不计入'
                           '正式池战绩（研究样本另用 scope）。'})
    return blocks
