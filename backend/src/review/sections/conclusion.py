"""维度6 结论预案章节（§5.3）：情绪周期定位 + 明日关注（主线候选/梯队关键股/
披露日程）+ 转天交易预案（情景应对，子模块 tomorrow_plan）+ 风险提示 +
免责声明（固定文案）。纯函数汇总前五维结果。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import tomorrow_plan

DISCLAIMER = (
    '免责声明：本报告由免费公开数据自动生成，仅供个人复盘研究使用，'
    '不构成任何投资建议；数据存在口径局限（详见复盘框架.MD §4），'
    '据此操作风险自负。'
)


def build(agg: Dict[str, Any], review_cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    # 情绪判据定稿开关（config review.sentiment_calibrated）：定稿后摘除"试用版"水印
    _cycle = ((agg.get('sentiment') or {}).get('data') or {}).get('cycle') or {}
    trial = bool(_cycle.get('trial'))
    heading_suffix = '（判据试用版，见复盘框架.MD §3）' if trial \
        else '（判据定稿口径见复盘框架.MD §3）'
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 2,
         'text': f'六、结论与明日预案{heading_suffix}'},
    ]

    # 情绪周期定位
    sentiment = agg.get('sentiment') or {}
    m = sentiment.get('data', {})
    cycle = m.get('cycle') or {}
    blocks.append({'type': 'heading', 'level': 3, 'text': '情绪周期定位'})
    blocks.append({'type': 'para',
                   'text': f"今日定位：{cycle.get('position', '待校准')}"
                           f"（空间板 {m.get('max_streak', 0)} 板，"
                           f"涨停 {m.get('limit_up_count', 0)} 家，"
                           f"炸板率 "
                           + (f"{m['burst_rate']:.0%}" if m.get('burst_rate') is not None else '—')
                           + '）'})

    # 明日关注
    blocks.append({'type': 'heading', 'level': 3, 'text': '明日关注'})
    items: List[str] = []
    sector = agg.get('sector') or {}
    sdata = sector.get('data') or {}
    clusters = sdata.get('limit_up_clusters') or []
    if clusters:
        items.append('主线方向观察：' + '、'.join(c['concept'] for c in clusters[:3])
                     + '（今日涨停聚集前列）')
    ladder = m.get('ladder') or {}
    if ladder:
        top_height = max(ladder)
        items.append(f'梯队关键位：{top_height} 板高度股明日晋级情况'
                     f'（当前 {top_height} 板 {ladder[top_height]} 家）')
    fundamentals = agg.get('fundamentals') or {}
    fdata = fundamentals.get('data') or {}
    if fdata.get('disclosure'):
        d = fdata['disclosure']
        items.append(f'披露日程：未来 5 个交易日 {d["next_5"]} 家预约披露，'
                     '关注业绩雷/喜对板块的扰动')
    if items:
        blocks.append({'type': 'bullets', 'items': items})
    else:
        blocks.append({'type': 'note', 'text': '⏳ 待补：板块与披露数据不足，明日关注项无法生成'})

    # 转天交易预案（情景应对；判据初版，阈值 config review.tomorrow_plan）
    blocks.extend(tomorrow_plan.build(agg, review_cfg))

    # D4 解禁子块（开发项 D；依赖 L 项 ratio 回填，按解禁日升序）
    unlock = agg.get('unlock')
    if unlock is not None:
        if unlock.get('available', True):
            ud = unlock['data']
            blocks.append({'type': 'heading', 'level': 3, 'text': '未来 10 个交易日解禁日历'})
            blocks.append({'type': 'table',
                           'headers': ['解禁日', '代码', '名称', '解禁股数(亿股)', '占总股本%', '类型'],
                           'rows': [[u['unlock_date'], u['ts_code'], u.get('name') or '—',
                                     round(u['shares'] / 1e8, 2) if u.get('shares') is not None else '—',
                                     f"{u['ratio']:.2f}" if u.get('ratio') is not None else '—',
                                     u.get('type') or '—'] for u in ud.get('items') or []]})
            blocks.append({'type': 'note',
                           'text': '口径：公告制，仅含已披露的近端解禁（一般提前 3-5 个交易日），'
                                   '非完整前瞻；占比列为推导值（解禁股数 ÷ 最新可得总股本），'
                                   '未解析出股数的行占比留空'})
        else:
            blocks.append({'type': 'note',
                           'text': f"⏳ 待补：解禁日历（{unlock.get('reason', '表空')}）"})

    # 风险提示
    blocks.append({'type': 'heading', 'level': 3, 'text': '风险提示'})
    risks: List[str] = []
    burst_rate = m.get('burst_rate')
    if burst_rate is not None and burst_rate >= 0.4:
        risks.append(f'炸板率 {burst_rate:.0%} 偏高（≥40%），情绪分歧加大')
    premium_mean = m.get('premium_mean')
    if premium_mean is not None and premium_mean <= 0:
        risks.append(f'昨日涨停溢价均值 {premium_mean:+.2f}% ≤ 0，打板/接力亏钱效应显现')
    risks.append('数据口径局限：is_st 为同步时点口径、北交所不复权、行业等权口径'
                 '（详见复盘框架.MD §4）'
                 + ('；判据为试用版阈值' if trial else ''))
    blocks.append({'type': 'bullets', 'items': risks})

    # 免责声明（固定文案）
    blocks.append({'type': 'heading', 'level': 3, 'text': '免责声明'})
    blocks.append({'type': 'para', 'text': DISCLAIMER})
    return blocks
