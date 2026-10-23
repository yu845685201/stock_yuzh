"""维度3 板块主线章节（§5.3）：行业涨跌榜（今/5日等权） / 概念涨停聚集度 /
主线候选与高低切换观察。纯函数。"""

from __future__ import annotations

from typing import Any, Dict, List


def build(sector: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 2,
         'text': '三、板块主线（口径说明见复盘框架.MD §2.3；行业为等权口径）'},
    ]
    if not sector.get('available', True):
        blocks.append({'type': 'note',
                       'text': f'⏳ 待补：板块数据（{sector.get("reason", "映射表为空")}）'})
        return blocks

    data = sector['data']
    industry_today = data.get('industry_today') or []
    industry_5d = data.get('industry_5d') or []
    clusters = data.get('limit_up_clusters') or []

    # 行业涨跌榜（今日）
    if industry_today:
        blocks.append({'type': 'heading', 'level': 3, 'text': '行业涨跌榜（今日，等权）'})
        blocks.append(_rank_table(industry_today, top=10, bottom=5))
    else:
        blocks.append({'type': 'note', 'text': '⏳ 待补：行业榜（行业映射为空或面板无行业股）'})

    # 二级行业 Top10（N3 2026-10-04：申万二级 THS 快照口径，置于一级榜后）
    industry_l2_today = data.get('industry_l2_today') or []
    if industry_l2_today:
        blocks.append({'type': 'heading', 'level': 3,
                       'text': '二级行业涨跌榜（今日，等权，Top 10）'})
        blocks.append(_rank_table(industry_l2_today[:10], top=10, bottom=5))
        blocks.append({'type': 'note',
                       'text': '口径：申万二级成分来自 THS 公司概况页当前快照（无历史时点），'
                               '等权涨跌按当前成分计算'})

    # 5 日累计榜
    if industry_5d:
        blocks.append({'type': 'heading', 'level': 3, 'text': '行业涨跌榜（5 日累计，等权）'})
        blocks.append(_rank_table(industry_5d, top=10, bottom=5))
        # 高低切换观察：今日榜前列 vs 5 日榜前列的位次变化
        today_top = {r['industry'] for r in industry_today[:10]}
        d5_top = {r['industry'] for r in industry_5d[:10]}
        fresh = today_top - d5_top
        fade = d5_top - today_top
        if fresh or fade:
            blocks.append({'type': 'heading', 'level': 3, 'text': '高低切换观察'})
            items = []
            if fresh:
                items.append(f'今日新进入前 10（5 日榜未列）: {"、".join(sorted(fresh))}')
            if fade:
                items.append(f'今日跌出前 10（5 日榜仍列）: {"、".join(sorted(fade))}')
            blocks.append({'type': 'bullets', 'items': items})

    # 板块涨停聚集度
    if clusters:
        blocks.append({'type': 'heading', 'level': 3, 'text': '概念涨停聚集度（Top 10）'})
        blocks.append({'type': 'table',
                       'headers': ['概念', '涨停家数', '占成分比'],
                       'rows': [[c['concept'], c['count'],
                                 f"{c['share']:.1%}" if c.get('share') is not None else '—']
                                for c in clusters]})
        conc = data.get('theme_concentration')
        if conc:
            blocks.append({'type': 'para',
                           'text': f'头部集中度：{conc["concept"]} {conc["count"]} 家 ÷ '
                                   f'全市场涨停，占 {conc["rate"]:.0%}'
                                   '（越高越是一枝独秀，散乱轮动期通常 <10%）'})

    # 主线候选：5 日行业榜前列 且 当日涨停聚集前列 的交叉
    if industry_5d and clusters:
        d5_top = {r['industry'] for r in industry_5d[:10]}
        cluster_top = {c['concept'] for c in clusters[:5]}
        # 行业名与概念名可能有出入（不同分类体系），按名称包含关系做宽松交叉
        candidates = [i for i in d5_top if any(i in c or c in i for c in cluster_top)]
        blocks.append({'type': 'heading', 'level': 3, 'text': '主线候选'})
        if candidates:
            blocks.append({'type': 'bullets',
                           'items': [f'{c}（5 日榜前列 ∧ 涨停聚集前列）' for c in candidates]})
        else:
            blocks.append({'type': 'para',
                           'text': '今日无行业同时进入 5 日榜与涨停聚集前列（主线暂不清晰）'})
        blocks.append({'type': 'note',
                       'text': '行业榜为 sw_2021/sina_hy 等权口径；涨停聚集为 sina_gn 概念口径，'
                               '两套分类体系不同，交叉结果仅供观察'})

    # 题材事件树（四期 §3：聚集前列概念的跨日复盘）
    blocks.extend(_theme_tree_blocks(data.get('theme_tree')))
    return blocks


def _theme_tree_blocks(tree: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """题材事件树表：昨日题材强势股今日反馈 → 扩散/收敛 → 炸板/最高板 → 首封范围。"""
    if not tree:
        return []
    rows = []
    for r in tree:
        perf = '—'
        if r.get('prev_mean') is not None:
            perf = f'{r["prev_mean"]:+.2f}%'
            if r.get('red_rate') is not None:
                perf += f' / 翻红 {r["red_rate"]:.0%}'
        seal = ('—' if not r.get('seal_first')
                else (f'{r["seal_first"]}' if r.get('seal_first') == r.get('seal_last')
                      else f'{r["seal_first"]}~{r["seal_last"]}'))
        rows.append([r['concept'], f'{r["prev_up"]} → {r["today_up"]}（{r["flow"]}）',
                     perf, f'{r["relimit"]} 只', r['burst'],
                     f'{r["max_streak"]} 板' if r['max_streak'] else '—', seal])
    return [
        {'type': 'heading', 'level': 3, 'text': '题材事件树（聚集前列概念跨日复盘）'},
        {'type': 'table',
         'headers': ['概念', '涨停家数（昨→今）', '昨日题材股今日表现', '晋级', '今日炸板',
                     '最高板', '首封时间范围'],
         'rows': rows},
        {'type': 'note',
         'text': '口径：按真实事件题材复盘（sina_gn 概念时点匹配）；昨日该题材涨停股今日'
                 '反馈看承接，首封与末封时间拉开=持续发酵；归因是待核对的解释，'
                 '不是已证实因果'},
    ]


def _rank_table(rows: List[Dict[str, Any]], top: int, bottom: int) -> Dict[str, Any]:
    shown = rows[:top] + rows[-bottom:] if len(rows) > top + bottom else rows
    return {'type': 'table',
            'headers': ['行业', '等权涨跌幅%', '样本数'],
            'rows': [[r['industry'], r['mean_change'], r['stocks']] for r in shown]}
