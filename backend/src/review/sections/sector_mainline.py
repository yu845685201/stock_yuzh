"""维度3 板块主线章节（§5.3）：行业涨跌榜（今日等权 / 5 日逐日等权复利） /
概念涨停聚集度 / 主线候选与高低切换观察。纯函数。

2026-11-24 §3.2 口径修订：五日榜改逐日等权 + 复利
（100×[∏(1+r[d])−1]），每行并列五个日收益、每日有效家数、窗口去重股票数
与有效股日数；「新进前十」只比较同周期同公式的今昨五日榜，日榜与五日榜的
差异单列「跨周期排名差」。
"""

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
    industry_5d_prev = data.get('industry_5d_prev') or []
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

    # 5 日累计榜（§3.2 修订：逐日等权 + 复利）
    if industry_5d:
        blocks.append({'type': 'heading', 'level': 3,
                       'text': '行业涨跌榜（5 日累计，逐日等权复利）'})
        blocks.extend(_rank5_blocks(industry_5d, data))
        # 高低切换观察（§3.2：同周期、同公式的今昨五日榜比较）
        if industry_5d_prev:
            today_top = _valid_top(industry_5d, 10)
            prev_top = _valid_top(industry_5d_prev, 10)
            fresh = today_top - prev_top
            fade = prev_top - today_top
            if fresh or fade:
                blocks.append({'type': 'heading', 'level': 3, 'text': '高低切换观察（同周期今昨比较）'})
                items = []
                if fresh:
                    items.append(f'今日新进入 5 日榜前 10（昨日同周期榜未列）: '
                                 f'{"、".join(sorted(fresh))}')
                if fade:
                    items.append(f'今日跌出 5 日榜前 10（昨日同周期榜仍列）: '
                                 f'{"、".join(sorted(fade))}')
                blocks.append({'type': 'bullets', 'items': items})
        # 跨周期排名差（§3.2：今日涨幅榜 vs 五日收益榜单列，不混入新进前十）
        if industry_today:
            day_top = {r['industry'] for r in industry_today[:10]}
            d5_top = _valid_top(industry_5d, 10)
            only_day = day_top - d5_top
            only_5d = d5_top - day_top
            if only_day or only_5d:
                blocks.append({'type': 'heading', 'level': 3, 'text': '今日与五日排名差（跨周期）'})
                items = []
                if only_day:
                    items.append('仅今日涨幅榜前 10（五日收益榜未列）: '
                                 f'{"、".join(sorted(only_day))}')
                if only_5d:
                    items.append('仅五日收益榜前 10（今日涨幅榜未列）: '
                                 f'{"、".join(sorted(only_5d))}')
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

    # 主线候选（§4.2 2026-11-24）：5 日榜前十 × 涨停聚集前五，按 ts_code 求交集
    # （旧实现按名称包含关系宽松匹配——"军工"≠"国防军工"，从根上改为代码口径）
    links = data.get('theme_links')
    if links is not None:
        blocks.extend(_theme_links_blocks(links))
    elif industry_5d and clusters:
        blocks.append({'type': 'heading', 'level': 3, 'text': '主线候选'})
        blocks.append({'type': 'para',
                       'text': '今日无行业同时进入 5 日榜与涨停聚集前列（主线暂不清晰）'})

    # 题材事件树（四期 §3：聚集前列概念的跨日复盘）
    blocks.extend(_theme_tree_blocks(data.get('theme_tree')))
    return blocks


def _theme_links_blocks(links: Dict[str, Any]) -> List[Dict[str, Any]]:
    """行业×题材代码交集 + 题材承接（§4.2；聚合层计算，本函数纯渲染）。

    - 每对行业×题材：交集成员数、交集涨停数、各自有效样本数、重合领涨股
      单独列名；
    - 每题材：可交易成分数 / 未知数 / 涨停占比（可交易且可判定分母）/
      新增首板数 / 昨日涨停今日晋级（昨日分母按昨日成分关系冻结）；
    - 跨题材合计按代码并集去重，共享领涨股（≥2 题材）只占一个代码；
    - 成分映射不足随行标注。
    """
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 3,
         'text': '主线候选（行业×题材代码交集）'},
    ]
    pairs = links.get('pairs') or []
    if pairs:
        shown = pairs[:15]
        blocks.append({'type': 'table',
                       'headers': ['行业（5日榜前十）', '题材（聚集前五）',
                                   '交集成员', '交集涨停', '行业有效样本',
                                   '题材有效样本', '重合领涨股'],
                       'rows': [[p['industry'], p['concept'],
                                 p['n_intersection'], p['n_intersection_limit_up'],
                                 p['industry_valid_n'], p['concept_valid_n'],
                                 '、'.join(p['shared_leaders']) or '—']
                                for p in shown]})
        if len(pairs) > len(shown):
            blocks.append({'type': 'note',
                           'text': f'共 {len(pairs)} 对非零交集（按交集涨停数、'
                                   f'交集规模降序），表列前 {len(shown)} 对。'})
    else:
        blocks.append({'type': 'para',
                       'text': '前十行业与前列题材无代码交集（主线暂不清晰）'})
    blocks.append({'type': 'note',
                   'text': '口径（2026-11-24 修订）：交集按股票代码集合计算'
                           '（sw/sina 行业成分 × sina_gn 概念成分时点匹配），'
                           '替代旧名称包含关系匹配；两套分类体系的成员关系'
                           '均来自当日可确认归属。'})
    themes = links.get('themes') or []
    if themes:
        blocks.append({'type': 'heading', 'level': 3,
                       'text': '题材承接（涨停占比、新增首板与昨日晋级）'})
        rows = []
        for t in themes:
            promoted = (f"{t['promoted']}/{t['prev_up_frozen_n']}"
                        if t['prev_up_frozen_n'] else '—')
            rows.append([
                t['concept'], t['tradable_n'], t['unknown_n'],
                t['limit_up_n'],
                f"{t['limit_rate']:.1%}" if t.get('limit_rate') is not None else '—',
                t['new_first_board_n'], promoted,
                t['prev_up_valid_n'], t['prev_up_missing_n'],
                f"{t['prev_up_median']:+.2f}%" if t.get('prev_up_median') is not None else '—'])
        blocks.append({'type': 'table',
                       'headers': ['题材', '可交易成分', '未知', '涨停', '涨停占比',
                                   '新增首板', '昨日涨停→今日晋级', '晋级组有效',
                                   '晋级组缺行', '昨日涨停股今日中位'],
                       'rows': rows})
        blocks.append({'type': 'note',
                       'text': '口径：涨停占比 = 已确认涨停数 ÷ 可交易且涨停状态'
                               '可判定成分数；未知 = 无行情行（不入占比分母）；'
                               '昨日涨停→今日晋级按**昨日的成分关系**与昨日涨停名单'
                               '冻结分母（跨日改标签不改分母），晋级组缺行单列；'
                               '涨停占比与承接是两个独立判读——占比高只说明当日'
                               '扩散，晋级率才说明跨日承接。'})
    union = links.get('union') or {}
    if union.get('n_sum'):
        shared = union.get('shared') or []
        bits = [f"{s['name']}（{'、'.join(s['concepts'])}）" for s in shared[:6]]
        if len(shared) > 6:
            bits.append(f"等共 {len(shared)} 只")
        text = (f"跨题材涨停合计 {union['n_sum']} 家次，按代码并集去重为 "
                f"{union['n_union']} 只")
        if bits:
            text += f"；共享领涨股：{'、'.join(bits)}"
        blocks.append({'type': 'para', 'text': text + '。'})
    mapping = links.get('mapping_coverage') or {}
    if mapping.get('limit_up_total'):
        unmapped = mapping['limit_up_total'] - mapping.get('with_concept', 0)
        if unmapped:
            blocks.append({'type': 'note',
                           'text': f"成分映射覆盖：当日涨停 {mapping['limit_up_total']} 只中 "
                                   f"行业映射 {mapping.get('with_industry', 0)} 只、"
                                   f"概念映射 {mapping.get('with_concept', 0)} 只——"
                                   f"{unmapped} 只无概念归属（题材读数受此限制）"})
    return blocks


def _valid_top(rows: List[Dict[str, Any]], n: int) -> set:
    """有效榜（return_5d 非 None）前 n 名行业集合——industry_returns 的输出
    有效榜在前、整日缺行段在后，遇首个空值即止。"""
    top: List[str] = []
    for r in rows:
        if r.get('return_5d_pct') is None:
            break
        top.append(r['industry'])
        if len(top) >= n:
            break
    return set(top)


def _rank5_blocks(rows: List[Dict[str, Any]], data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """五日榜（§3.2 新口径）表 + 整日缺行段 + 口径注记。"""
    valid = [r for r in rows if r.get('return_5d_pct') is not None]
    missing = [r for r in rows if r.get('return_5d_pct') is None]
    blocks: List[Dict[str, Any]] = []
    shown = valid[:10]
    if len(valid) > 15:
        shown = valid[:10] + valid[-5:]
    if shown:
        table_rows = []
        for r in shown:
            name = f"{r['industry']}（概念）" if r.get('is_concept') else r['industry']
            daily = '/'.join(
                (f"{d['ret_pct']:+.1f}" if d is not None else '—')
                for d in (r.get('daily') or []))
            counts = '/'.join(str(c) for c in (r.get('daily_counts') or []))
            table_rows.append([
                name, f"{r['return_5d_pct']:+.2f}%", daily, counts,
                r.get('unique_stocks', '—'), r.get('stock_days', '—')])
        blocks.append({'type': 'table',
                       'headers': ['行业', '五日复利%', '逐日收益%（D1→D5）',
                                   '每日有效家数', '窗口去重股票数', '有效股日数'],
                       'rows': table_rows})
    if missing:
        items = [f"{r['industry']}：{r.get('days_valid', 0)}/5 个交易日有有效成员"
                 f"（去重 {r.get('unique_stocks', '—')} 只、股日 {r.get('stock_days', '—')}）"
                 for r in missing[:6]]
        blocks.append({'type': 'bullets', 'items': ['整日缺有效收益（五日值不可算，不入排名）：']
                       + items})
    sessions = data.get('industry_5d_sessions') or []
    note = ('口径（2026-11-24 修订）：逐日先按当日可确认行业成员计算等权日收益 r[d]'
            '（剔除停牌/无成交/收益无效），再按 5 个交易日复利 '
            '100×[∏(1+r[d]/100)−1]；中间过程保留精度，展示时才舍入。'
            + (f"窗口：{sessions[0]}~{sessions[-1]}。" if sessions else '')
            + '旧口径（窗口内全部股日直接均值）已停用于五日榜。')
    memb = data.get('industry_5d_membership') or {}
    if memb.get('snapshot_only'):
        note += (f" 行业成分为快照口径（窗口 {memb.get('window', ['—'])[0]}~"
                 f"{memb.get('window', ['—'])[-1]} 内无成员变更事件，逐日成员等价于"
                 '当前快照）；历史回放受此限制，事后变更的成分不冒充当时已知成员。')
    blocks.append({'type': 'note', 'text': note})
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
    # 评价-3：纯 sina_hy 兜底板（次新股/ST 板等概念板）打（概念）标签，
    # 不再以真实行业身份出现在行业榜
    def _name(r):
        return f"{r['industry']}（概念）" if r.get('is_concept') else r['industry']
    return {'type': 'table',
            'headers': ['行业', '等权涨跌幅%', '样本数'],
            'rows': [[_name(r), r['mean_change'], r['stocks']] for r in shown]}
