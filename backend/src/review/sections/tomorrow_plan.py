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
    '冰点': '观察等企稳——跌停家数收敛与溢价转正前不左侧；企稳信号=跌停<涨停 且 溢价>0',
    '退潮': '防守降仓——避开高位股与断板梯队，等待高度重建或冰点后的企稳信号',
}

#: 兜底倾向（组合定位如「回暖/震荡」或未知状态时，按震荡口径处理）
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
    """组合定位（如「回暖/震荡」）→ 取首个可识别态；未知态走震荡兜底。"""
    for key in ('高潮', '冰点', '退潮', '回暖', '震荡'):
        if key in position:
            return _BASE_POSITIONING[key]
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


def _buy_candidates(agg: Dict[str, Any], rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    """转天买入候选（观察池）：消费 aggregator._build_candidates 全量候选池
    （谓词 candidate_rules 与 daily_review 链 R 列共用，同数据同结果），
    按情绪开放度过滤视图 + top_n 截断展示 + 每类买入决策树。

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

    # 情绪开放度（与基准定位同一匹配顺序：高潮 → 冰点 → 退潮 → 回暖/震荡）
    sentiment = (agg.get('sentiment') or {}).get('data') or {}
    position = str(((sentiment.get('cycle') or {}).get('position')) or '')
    if '高潮' in position:
        openness, open_types = ('高潮（防分歧）：仅开放 R1 梯队接力观察，'
                                '强承接确认前不进'), ('R1',)
    elif '冰点' in position:
        blocks.append({'type': 'para',
                       'text': '情绪定位冰点：本轮不出买入候选；企稳信号（跌停<涨停 且 '
                               '昨日涨停溢价>0 且 空间板高度回升）触发后，下一报告期再开放候选'})
        blocks.append({'type': 'note',
                       'text': '候选为规则筛选的观察名单，非推荐；是否买入由转天走势对决策树'
                               '的验证决定；判据初版待校准，本报告不构成投资建议'})
        return blocks
    elif '退潮' in position:
        blocks.append({'type': 'para',
                       'text': '情绪定位退潮：本轮不出买入候选（防守降仓期，避开高位股与'
                               '断板梯队）；高度重建或冰点企稳后再开放'})
        blocks.append({'type': 'note',
                       'text': '候选为规则筛选的观察名单，非推荐；判据初版待校准，'
                               '本报告不构成投资建议'})
        return blocks
    else:
        openness = '回暖/震荡：四类候选全开'
        open_types = ('R1', 'R2', 'R3', 'R4')
    # ADX 闸门（P1-7 §4.6 E）：指数趋势强度挡位，叠加在情绪开放度之上
    adx_info = cand.get('adx_gate') or {}
    gate = str(adx_info.get('gate') or 'closed')
    gate_note = (f"指数 ADX 闸门：{gate}"
                 + (f"（{adx_info['index']} ADX {adx_info['adx']}"
                    f"{'↑' if adx_info.get('adx_rising') else '↓'}，"
                    f"收盘 {adx_info['close']:.0f} vs MA20 {adx_info['ma20']:.0f}）"
                    if adx_info.get('adx') is not None
                    else f"（{adx_info.get('reason', '数据不足')}）"))
    if gate == 'closed':
        blocks.append({'type': 'para', 'text': gate_note})
        blocks.append({'type': 'para',
                       'text': '指数趋势闸门关闭（震荡/下行）：本轮不出买入候选——'
                               'ADX 闸门与情绪开放度叠加生效（任一层关闭即不出）'})
        blocks.append({'type': 'note',
                       'text': '候选为规则筛选的观察名单，非推荐；ADX 口径 = 通达信 '
                               'SUM+MA 平滑（indicators.adx）；本报告不构成投资建议'})
        return blocks
    if gate == 'half':
        open_types = tuple(t for t in open_types if t in ('R1', 'R2'))
        openness += '；ADX 半开放（仅 R1/R2）'
    else:
        openness += '；ADX 全开放'
    blocks.append({'type': 'para', 'text': f'候选开放度：{openness}'})
    blocks.append({'type': 'para', 'text': gate_note})

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
    _HAS_MONEY = {'R1', 'R2', 'R3'}  # 资金确认列合并进证据列后不再单独输出

    for ctype in open_types:
        label, rule_desc = _CANDIDATE_META[ctype]
        rule_desc = rule_desc.format(**{**DEFAULT_BUY_RULES, **brules})
        pool = cand.get(ctype) or []
        # 展示排序（P1-7 D）：五维加权评分优先（score 非 None），证据计数降序
        # 兜底（评分缺失的旧数据/无维度股票）；两者都缺 → 维持原稳定次序
        ranked = sorted(
            pool,
            key=lambda r: (-(r.get('score') if r.get('score') is not None else -1),
                           -int(r.get('evidence_count') or 0)))
        rows = [[r.get('code') or '—', r.get('name') or '—', r.get('label') or '—',
                 _REMARK[ctype](r),
                 ('/'.join(r.get('evidence') or []) or '—')
                 + (f"（{r['evidence_count']}证）" if r.get('evidence') else ''),
                 (f"{r['score']:.1f}" if r.get('score') is not None else '—')]
                for r in ranked[:top_n]]
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f'{label}（{len(pool)} 只，按五维评分展示前 {min(top_n, len(pool))}，'
                               f'规则：{rule_desc}）'})
        if rows:
            headers = ['代码', '名称', '当日状态', '备注', '证据（五维计数）', '五维评分']
            blocks.append({'type': 'table', 'headers': headers, 'rows': rows})
            tree = _DECISION_TREES[ctype]
            blocks.append({'type': 'bullets',
                           'items': [tree['confirm'], tree['veto'], tree['risk']]})
        else:
            blocks.append({'type': 'para', 'text': '无入选（池数据缺失或无满足规则的股票）'})

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
                               '若被毙的恰是高收益股，否决规则本身需回测复议（P1-6 A/B）'})
    elif cand.get('vetoed_count') == 0:
        blocks.append({'type': 'para', 'text': '一票否决出池：本报告期 0 只'})

    blocks.append({'type': 'note',
                   'text': '候选为规则筛选的观察名单（判据初版 2026-10-05 新增，阈值 config '
                           'review.tomorrow_plan.buy_rules，待用户校准定稿）；全量底表见链 CSV '
                           'data/analysis/daily_review_<锚定日>.csv 的 R1-R4 四列'
                           '（2026-10-18 起候选列合入 daily_review，原独立候选链已删除；'
                           '与报告同规则同序）；'
                           '「是否买入」由转天走势对决策树的验证决定——确认/否决/风控三分支缺一'
                           '不可；竞价与盘中阈值参数由个人风险偏好校准；本报告不构成投资建议'})
    return blocks
