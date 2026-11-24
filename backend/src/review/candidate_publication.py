# -*- coding: utf-8 -*-
"""候选发布名单（每日复盘报告优化技术方案 §3.3，2026-11-24）。

问题（2026-11-23 案例）：情绪规则仅开放 R1 梯队接力，下行的 60 日均线又关闭
R1 → 正文正式右侧候选 **0 只**；而落库预测（``verifier.build_predictions``
读聚合全量名单）与摘要底稿却对三只 R2 原始命中（高德红外/慈文传媒/埃斯顿）
给出跟随条件——正文关闭的候选池在摘要/核验侧仍然出现。

方案：发布时显式保存各阶段名单与排除原因，**正文、摘要、预测落库、次日
核验从同一份发布名单读取**（摘要精选只能是 published 的子集）：

- ``raw``：基础命中（``_build_candidates`` 的 R1-R4 谓词命中 + VETO 分流后；
  R1_filtered 弱档明细与 vetoed 出池明细由 aggregator 侧另行保留）；
- ``eligible``：情绪开放度 + ADX 闸门 + 弱档判定后的开放池全量；
- ``published``：五维评分排序 + top_n 截取后的**实际发布名单**——正文候选
  表与摘要精选逐项等于该名单；
- ``theme_watch``：题材跟踪样本（池未开放时的 R2 原始命中等）——单列展示
  来源与跟踪问题，**不附正式候选的交易条件**；
- ``CHAN``：独立冻结的左侧观察名单（右侧关闭时仍按左侧定义展示）。

排序与截取从 ``sections/tomorrow_plan._buy_candidates`` 迁出至本模块单一
实现（先以既有样例复现 R1 弱档、跨池去重与 VETO 的执行顺序，不改选股结果
——谓词仍在 aggregator ``_build_candidates``，本层只做开放/排序/截取）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

#: 池键（右侧四池）
POOL_KEYS = ('R1', 'R2', 'R3', 'R4')

#: 发布名单版本（写入产物，便于回放对账）
CANDIDATE_PUBLICATION_VERSION = 'v1-20261124'


def open_types_for(position: str,
                   adx_info: Optional[Dict[str, Any]]) -> Tuple[Tuple[str, ...], Optional[str]]:
    """情绪开放度 + ADX 闸门 + 弱档判定 → 开放池类型（单一实现，迁移自
    tomorrow_plan._buy_candidates；执行顺序不变）。

    Returns:
        (开放池类型元组, 整体关闭原因)；open_types 为空即全部关闭。
    """
    adx_info = adx_info or {}
    if position == '高潮':
        open_types: Tuple[str, ...] = ('R1',)
    elif position == '冰点':
        return tuple(), f'情绪定位冰点：不出买入候选（企稳信号触发后下一报告期再开放）'
    elif position == '退潮':
        return tuple(), '情绪定位退潮：不出买入候选（防守降仓期）'
    elif position == '回暖/震荡':
        open_types = ('R1', 'R2', 'R3', 'R4')
    else:
        # P4-4（B6）：异常/未知定位兜底为全关 + 人工复核，不向激进方向兜底
        return tuple(), f'情绪定位异常（{position or "空"}）：全关转人工复核（P4-4）'
    gate = str(adx_info.get('gate') or 'closed')
    if gate == 'closed':
        return tuple(), '指数 ADX 闸门 closed（与情绪开放度叠加，任一层关闭即不出）'
    if gate == 'half':
        open_types = tuple(t for t in open_types if t in ('R1', 'R2'))
    if bool(adx_info.get('market_weak')):
        open_types = tuple(t for t in open_types if t != 'R1')
    return open_types, None


def rank_pool(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """展示排序（迁移自 tomorrow_plan，保持原排序主键）：五维加权评分降序
    （score 非 None），证据计数降序兜底（评分缺失的旧数据/无维度股票），
    两者都缺 → 维持输入稳定次序。"""
    return sorted(
        pool,
        key=lambda r: (-(r.get('score') if r.get('score') is not None else -1),
                       -int(r.get('evidence_count') or 0)))


def openness_text(position: str, adx_info: Optional[Dict[str, Any]],
                  open_types: Tuple[str, ...]) -> str:
    """开放度说明行文案（迁移自 tomorrow_plan._buy_candidates，口径一致；
    高潮 + 弱档全关时不再保留「仅开放 R1」旧文案——文案与实际开放名单一致。
    冰点/退潮/异常定位返回 ''（消费方用 close_reason 文案）。"""
    adx_info = adx_info or {}
    gate = str(adx_info.get('gate') or 'closed')
    market_weak = bool(adx_info.get('market_weak'))
    if position == '高潮':
        if not open_types:
            return ('高潮（防分歧）× 弱档否决：正式候选全关（R1 因弱档不开放）'
                    '——P1-10：收盘<MA60 且 MA60 下行')
        return '高潮（防分歧）：仅开放 R1 梯队接力观察，强承接确认前不进'
    if position != '回暖/震荡':
        return ''
    # 回暖/震荡系
    text = '回暖/震荡：四类候选全开'
    if gate == 'half':
        text += '；ADX 半开放（仅 R1/R2）'
    else:
        text += '；ADX 全开放'
    if market_weak:
        if not open_types:
            return ('回暖/震荡 × 弱档否决：正式候选全关（仅剩 R1 被弱档否决）'
                    + ('；ADX 半开放' if gate == 'half' else '')
                    + '——P1-10：收盘<MA60 且 MA60 下行')
        text = text.replace('四类候选全开',
                            'R2 开放（R1 因弱档否决不开放）' if gate == 'half' else
                            'R2-R4 开放（R1 因弱档否决不开放）')
        text += '——P1-10：收盘<MA60 且 MA60 下行'
    return text


def pool_close_reason(ctype: str, position: str,
                      adx_info: Optional[Dict[str, Any]]) -> str:
    """单池关闭原因（逐股对账用；整池关闭时每池一条）。"""
    adx_info = adx_info or {}
    gate = str(adx_info.get('gate') or 'closed')
    market_weak = bool(adx_info.get('market_weak'))
    if position == '高潮' and ctype != 'R1':
        return '高潮仅开放 R1'
    if position in ('冰点', '退潮'):
        return f'情绪定位{position}：整类不出候选'
    if position not in ('回暖/震荡', '高潮'):
        return f'情绪定位异常（{position or "空"}）：全关（P4-4）'
    if gate == 'closed':
        return '指数 ADX 闸门 closed：全关'
    if gate == 'half' and ctype not in ('R1', 'R2'):
        return 'ADX 半开放（仅 R1/R2）'
    if market_weak and ctype == 'R1':
        return '弱档否决（P1-10：收盘<MA60 且 MA60 下行）'
    return '未开放（原因待核）'


def build_candidate_publication(cand: Dict[str, Any],
                                sentiment: Dict[str, Any],
                                review_cfg: Optional[Dict[str, Any]] = None,
                                chan_items: Optional[List[Dict[str, Any]]] = None
                                ) -> Dict[str, Any]:
    """构建候选发布批次（§3.3：各阶段名单显式保存，三端同源）。

    Args:
        cand: ``agg['candidates']``（_build_candidates 产物：R1-R4 全量 +
            R1_filtered + vetoed + adx_gate + rules）。
        sentiment: ``agg['sentiment']``（AggregationResult 或 dict；
            取 data.cycle.position）。
        review_cfg: config['review']（buy_rules 的 top_n 覆盖）。
        chan_items: 缠论左侧名单（agg['chan'].data.items；独立冻结）。

    Returns:
        发布批次 dict（见模块 docstring；published/eligible/raw 为同一行对象
        的引用列表，消费方只读）。
    """
    review_cfg = review_cfg or {}
    brules = dict(cand.get('rules') or {})
    brules.update(((review_cfg.get('tomorrow_plan') or {}).get('buy_rules') or {}))
    top_n = int(brules.get('top_n') or 5)

    senti_data = (sentiment.get('data') if isinstance(sentiment, dict)
                  else getattr(sentiment, 'data', None)) or {}
    position = str(((senti_data.get('cycle') or {}).get('position')) or '')
    adx_info = cand.get('adx_gate') or {}
    open_types, close_reason = open_types_for(position, adx_info)

    pools: Dict[str, Dict[str, Any]] = {}
    for ctype in POOL_KEYS:
        raw = list(cand.get(ctype) or [])
        pool_open = ctype in open_types
        eligible = list(raw) if pool_open else []
        ranked = rank_pool(eligible) if pool_open else []
        published = ranked[:top_n]
        # 截断排除明细（开放但排不进 top_n 的逐股落点——禁静默缩池）
        truncated = [
            {'ts_code': r.get('ts_code'), 'code': r.get('code'),
             'name': r.get('name'), 'rank': i + 1,
             'reason': f'排序截断（第 {i + 1} 名 > top_n={top_n}）'}
            for i, r in enumerate(ranked[top_n:])]
        pools[ctype] = {
            'open': pool_open,
            'raw': raw,
            'eligible': eligible,
            'published': published,
            'n_raw': len(raw), 'n_eligible': len(eligible),
            'n_published': len(published),
            'truncated': truncated,
            'close_reason': (None if pool_open
                             else pool_close_reason(ctype, position, adx_info)),
        }

    # 题材跟踪（§3.3）：R2 池未开放时的原始命中——题材样本单列，注明来源，
    # 不附正式候选的交易条件（2026-11-23 三只 R2 原始命中的归处）
    theme_watch: List[Dict[str, Any]] = []
    if not pools['R2']['open']:
        theme_watch = [
            {'ts_code': r.get('ts_code'), 'code': r.get('code'),
             'name': r.get('name'), 'label': r.get('label'),
             'concepts': r.get('concepts'), 'amount': r.get('amount'),
             'evidence': r.get('evidence'),
             'source': 'R2 原始命中（池未开放）',
             'watch_reason': 'R2 池未开放——题材跟踪样本，非正式候选，'
                             '不附交易条件'}
            for r in pools['R2']['raw']]

    chan_published = [dict(r) for r in (chan_items or [])]

    return {
        'available': bool(cand.get('available', True)),
        'version': CANDIDATE_PUBLICATION_VERSION,
        'position': position,
        'adx_gate': adx_info,
        'open_types': list(open_types),
        'close_reason': close_reason,
        'openness_text': openness_text(position, adx_info, open_types),
        'top_n': top_n,
        'pools': pools,
        'theme_watch': theme_watch,
        'chan': {'published': chan_published, 'n_published': len(chan_published)},
        'vetoed': list(cand.get('vetoed') or []),
        'vetoed_count': int(cand.get('vetoed_count') or 0),
        'r1_filtered': list(cand.get('R1_filtered') or []),
        'rules': brules,
    }


def published_flat(pub: Dict[str, Any]) -> List[Dict[str, Any]]:
    """右侧四池发布名单扁平化（预测落库 payload 的 candidates 字段，
    verifier.build_predictions 消费——核验按已发布成员计算正式池战绩）。"""
    out: List[Dict[str, Any]] = []
    for ctype in POOL_KEYS:
        for r in (pub.get('pools') or {}).get(ctype, {}).get('published') or []:
            out.append({'ts_code': r.get('ts_code'), 'code': r.get('code'),
                        'name': r.get('name'), 'label': r.get('label'),
                        'pool': ctype,
                        'evidence_count': r.get('evidence_count')})
    return out


def publication_summary(pub: Dict[str, Any]) -> Dict[str, Any]:
    """发布批次摘要（预测 payload 随行——研究样本与正式名单分开，供回放
    对账各阶段计数；不随核验参与战绩）。"""
    return {
        'version': pub.get('version'),
        'position': pub.get('position'),
        'open_types': list(pub.get('open_types') or []),
        'close_reason': pub.get('close_reason'),
        'top_n': pub.get('top_n'),
        'pools': {ctype: {'open': st.get('open'),
                          'n_raw': st.get('n_raw'),
                          'n_eligible': st.get('n_eligible'),
                          'n_published': st.get('n_published'),
                          'close_reason': st.get('close_reason'),
                          'n_truncated': len(st.get('truncated') or [])}
                  for ctype, st in (pub.get('pools') or {}).items()},
        'theme_watch_n': len(pub.get('theme_watch') or []),
        'chan_n': (pub.get('chan') or {}).get('n_published', 0),
        'vetoed_count': pub.get('vetoed_count'),
    }
