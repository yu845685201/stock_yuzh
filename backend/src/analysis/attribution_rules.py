"""涨停 × 公告归因共享规则（P1-11，方案 §3.4；模式同 candidate_rules.py）。

**归因是市场叙事，不是核验过的因果**——关键词/类型命中只说明「时间相邻」，
不构成上涨原因的证据；利好=0 时显式给「题材炒作或跟风上涨」，不硬凑归因。

两层判定（先关键词后类型，方案 §3.4.1）：
1. 标题关键词命中 → ``题材催化(<关键词>)``（利好）；
2. 否则按 ann_type 精确映射（18 类，库内实测）——类型带极性
   （利好/利空/中性），利空类型（解禁/监管问询/风险警示/诉讼仲裁）不得
   作为涨停利好归因；
3. 都无 → ``题材炒作或跟风上涨``（利好=0 的显式结论）。

覆盖断代（2026-10-20 实测）：非定期报告公告自 **2026-09-28** 起才有量
（737/1768/2210 条），此前仅定期报告——锚定日早于断代日的输出显式附
「公告覆盖不全(历史仅定期报告)」。
"""

from __future__ import annotations

from typing import List, Optional, Tuple

#: 利好关键词（19 个；以 stock-programmer/limit-up-review §3.2 为蓝本，
#: 落地时按 A 股公告标题实测增删——「澄清问询」从蓝本清单移除：它是风险
#: 澄清而非催化，列入利好会误导归因）
KEYWORDS: Tuple[str, ...] = (
    '中标候选人', '中标', '签约', '战略合作', '收购', '增资', '扩产', '专利',
    '补贴', '获批', '投产', '预增', '重组', '注入', '定增', '复牌',
    '分红送转', '回购', '员工持股',
)

#: ann_type → 归因标签（18 类，库内 announcement_daily 实测全集）
ANN_TYPE_MAP = {
    '业绩预告': '业绩预增',
    '重大事项': '重大事项',
    '股权融资': '股权融资',
    '募集资金': '募集资金',
    '回购': '回购',
    '分红送转': '分红送转',
    '股权激励': '股权激励',
    '增减持': '增减持',
    '解禁': '解禁',
    '监管问询': '监管问询',
    '风险警示': '风险警示',
    '诉讼仲裁': '诉讼仲裁',
    '担保': '担保',
    '关联交易': '关联交易',
    '股权变动': '股权变动',
    '公司治理': '公司治理',
    '定期报告': '定期报告',
    '其他': '其他',
}

#: 类型极性：利好可作涨停归因；利空/中性只进 detail 不作归因
ANN_TYPE_POLARITY = {
    '业绩预告': '利好', '重大事项': '利好', '股权融资': '利好',
    '募集资金': '利好', '回购': '利好', '分红送转': '利好',
    '股权激励': '利好', '增减持': '利好',
    '解禁': '利空', '监管问询': '利空', '风险警示': '利空', '诉讼仲裁': '利空',
    '担保': '中性', '关联交易': '中性', '股权变动': '中性', '公司治理': '中性',
    '定期报告': '中性', '其他': '中性',
}

#: 利好=0 的显式结论（不硬凑归因）
FALLBACK_LABEL = '题材炒作或跟风上涨'

#: 公告覆盖断代日（YYYYMMDD；此前仅定期报告，非定期类自该日起才有量）
COVERAGE_CUTOFF = '20260928'


def match_keyword(title: str) -> Optional[str]:
    """标题命中首个利好关键词（按 KEYWORDS 顺序；长词优先已内置）。"""
    if not title:
        return None
    for kw in KEYWORDS:
        if kw in title:
            return kw
    return None


def classify(title: str, ann_type: str) -> Tuple[str, str]:
    """单条公告归因：``(标签, 极性)``，极性 ∈ 利好/利空/中性/无。

    先关键词后类型；都无 → (FALLBACK_LABEL, '无')——注意此分支的语义是
    「本条公告无利好」，与「该股无利好公告」的 pick 层结论区分。
    """
    kw = match_keyword(title)
    if kw:
        return f'题材催化({kw})', '利好'
    label = ANN_TYPE_MAP.get(str(ann_type or ''), '其他')
    polarity = ANN_TYPE_POLARITY.get(str(ann_type or ''), '中性')
    return label, polarity


def pick_attribution(announcements: List[dict]) -> Tuple[str, str, Optional[dict]]:
    """从个股公告窗口选最佳归因：``(标签, 极性, 源公告)``。

    优先级：关键词利好 > 利好类型 > 无利好（题材炒作或跟风上涨）；
    同优先级取窗口内最新一条（ann_date 降序兜底）。
    """
    if not announcements:
        return FALLBACK_LABEL, '无', None
    keyword_hits = []
    type_hits = []
    for ann in announcements:
        label, polarity = classify(ann.get('title') or '',
                                   ann.get('ann_type') or '')
        if polarity == '利好' and label.startswith('题材催化'):
            keyword_hits.append((label, ann))
        elif polarity == '利好':
            type_hits.append((label, ann))
    for hits in (keyword_hits, type_hits):
        if hits:
            hits.sort(key=lambda x: str(x[1].get('ann_date') or ''), reverse=True)
            return hits[0][0], '利好', hits[0][1]
    return FALLBACK_LABEL, '无', None


def coverage_note(as_of: str) -> bool:
    """锚定日是否早于公告覆盖断代日（True = 需附「公告覆盖不全」）。"""
    return str(as_of) < COVERAGE_CUTOFF
