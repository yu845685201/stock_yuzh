"""维度5 业绩估值章节（§5.3）：披露日历 / 业绩预告 / 个股估值分位 / 指数估值。
各输入表为空时逐项「待补」，不影响其他项呈现。"""

from __future__ import annotations

from typing import Any, Dict, List

from ..renderer import missing_note


def build(fundamentals: Dict[str, Any], announcement: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """维度5 章节块；announcement 为开发项 D5 公告摘要聚合结果。"""
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 2,
         'text': '五、业绩估值（口径说明见复盘框架.MD §2.5）'},
    ]
    if not fundamentals.get('available', True):
        blocks.append(missing_note('业绩估值数据', fundamentals.get('reason', '各表为空')))
        return blocks

    data = fundamentals['data']
    emitted = False

    # 披露日历
    if data.get('disclosure'):
        d = data['disclosure']
        blocks.append({'type': 'heading', 'level': 3, 'text': '披露日历'})
        blocks.append({'type': 'bullets', 'items': [
            f"未来 5 个交易日预约披露：{d['next_5']} 家",
            f"未来 10 个交易日预约披露：{d['next_10']} 家",
            f"当日实际披露：{d['actual_today']} 家（actual 回填口径）",
        ]})
        emitted = True
    else:
        blocks.append(missing_note('披露日历', 'base_disclosure_calendar 为空'))

    # 业绩预告
    if data.get('forecasts'):
        f = data['forecasts']
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f'当日业绩预告（{f["count"]} 条）'})
        blocks.append({'type': 'bullets', 'items':
                       [f'{t}: {n} 条' for t, n in f['by_type'].items()]})
        if f['items']:
            blocks.append({'type': 'table', 'headers': ['股票', '类型', '摘要'],
                           'rows': [[i['name'], i['type'], i['summary']] for i in f['items']]})
        emitted = True
    else:
        blocks.append(missing_note('当日业绩预告',
                                   'performance_forecast 当日无新增（预告空窗期属正常）'))

    # 个股估值
    if data.get('stock_valuation'):
        v = data['stock_valuation']
        blocks.append({'type': 'heading', 'level': 3, 'text': '个股估值分布（PE-TTM/PB）'})
        blocks.append({'type': 'table',
                       'headers': ['分位', 'P10', 'P25', 'P50', 'P75', 'P90'],
                       'rows': [
                           ['PE'] + [v['pe_quantiles'].get(q) for q in (0.1, 0.25, 0.5, 0.75, 0.9)],
                           ['PB'] + [v['pb_quantiles'].get(q) for q in (0.1, 0.25, 0.5, 0.75, 0.9)],
                       ]})
        blocks.append({'type': 'para',
                       'text': f"样本：PE {v['pe_count']} 只 / PB {v['pb_count']} 只"
                               f"（0<PE<500、0<PB<100，剔除异常值）"})
        emitted = True
    else:
        blocks.append(missing_note('个股估值', 'stock_valuation_daily 为空或当日未自算'))

    # 指数估值
    if data.get('index_valuation'):
        rows = data['index_valuation']
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f'指数估值（{rows[0].get("date", "最新可得")}，pe_static 口径，注明无 TTM）'})
        blocks.append({'type': 'table',
                       'headers': ['指数', 'PE 静态', 'PE TTM'],
                       'rows': [[r['name'], r['pe_static'], r['pe_ttm']] for r in rows]})
        emitted = True
    else:
        blocks.append(missing_note('指数估值', 'index_valuation_daily 为空'))

    # D5 公告摘要子块（开发项 D；库层面全分类，报告层面挑重点类别展示）
    if announcement is not None:
        blocks.extend(_announcement_blocks(announcement))

    if not emitted:
        blocks.append(missing_note('全部业绩估值子项', '各输入表均为空'))
    return blocks


def _announcement_blocks(announcement: Dict[str, Any]) -> List[Dict[str, Any]]:
    """D5 公告摘要：当日 ann_type 计数（剔除"其他"）+ 每类 1~2 条代表条目"""
    if not announcement.get('available', True):
        return [missing_note('公告摘要', announcement.get('reason', '当日无分类公告'))]
    data = announcement['data']
    counts = data.get('counts') or {}
    samples = data.get('samples') or {}
    blocks: List[Dict[str, Any]] = [{'type': 'heading', 'level': 3,
                                    'text': f'当日公告摘要（分类 {len(counts)} 类，'
                                            f'共 {data.get("total", 0)} 条）'}]
    blocks.append({'type': 'bullets', 'items': [f'{t}: {n} 条' for t, n in counts.items()]})
    rows = []
    for t, items in samples.items():
        for i in items:
            rows.append([t, i['ts_code'] or '—', i['name'] or '—', i['title']])
    if rows:
        blocks.append({'type': 'table',
                       'headers': ['类别', '代码', '名称', '标题（截断 40 字）'],
                       'rows': rows})
    return blocks
