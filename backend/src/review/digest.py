"""图文并茂复盘解读 HTML（digest，2026-10-05 用户决策：skill 最终交付物）。

数据流：``ReviewAggregator.aggregate(as_of)``（与六维报告**同口径同数据**）
→ 内联 SVG 图表（零外部依赖、离线可开、同输入逐字节一致）
→ 自包含 HTML（``daily_review_<锚定日>_digest.html``）。

解读文本（agent 撰写的 markdown 子集）经 ``interpretation_path`` 装配进
「复盘解读」章节——只装配不加工（agent 侧计算红线）。

红线：
- 同输入逐字节一致：正文**无时间戳**（耗时等仅进返回值）；
- 图表颜色遵循 A 股惯例：红涨绿跌；
- 免责声明恒在页脚。
"""

from __future__ import annotations

import html as _html
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..analysis.data_reader import DataReader
from .aggregator import ReviewAggregator

logger = logging.getLogger(__name__)

#: A 股色板：红涨绿跌
C_UP = '#c0392b'
C_DOWN = '#1e8449'
C_FLAT = '#8a929c'
C_BAR_BG = '#eef1f4'
C_AXIS = '#57606a'

_PAGE_CSS = """
body { font-family: -apple-system, 'PingFang SC', 'Microsoft YaHei', sans-serif;
       margin: 0; padding: 24px; color: #1f2328; background: #fafbfc; line-height: 1.65; }
#content { max-width: 1080px; margin: 0 auto; background: #fff;
           padding: 32px 40px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
h1 { font-size: 1.5em; border-bottom: 2px solid #d0d7de; padding-bottom: 8px; }
h2 { font-size: 1.2em; border-bottom: 1px solid #d0d7de; padding-bottom: 6px; margin-top: 2em; }
h3 { font-size: 1.05em; margin-top: 1.4em; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 0.92em; }
th, td { border: 1px solid #d0d7de; padding: 6px 10px; text-align: left; }
th { background: #f6f8fa; }
tbody tr:nth-child(even) { background: #f6f8fa; }
blockquote { border-left: 4px solid #d0d7de; margin: 8px 0; padding: 4px 14px; color: #57606a; }
.note { color: #57606a; font-size: 0.88em; }
.up { color: #c0392b; } .down { color: #1e8449; }
.disclaimer { margin-top: 2.5em; padding: 10px 14px; background: #f6f8fa;
              border-radius: 6px; color: #57606a; font-size: 0.88em; }
.interp h2 { border: none; }
.chart { margin: 10px 0 4px; }
"""


# ------------------------------------------------------------------ SVG 工具

def _esc(v: Any) -> str:
    return _html.escape(str(v if v is not None else '—'))


def _svg_open(w: int, h: int) -> str:
    return (f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
            f'xmlns="http://www.w3.org/2000/svg" role="img" font-family="-apple-system,'
            f'PingFang SC,Microsoft YaHei,sans-serif" font-size="12">')


def _fmt(v: Any, nd: int = 2, signed: bool = False) -> str:
    if v is None:
        return '—'
    return f'{float(v):+.{nd}f}' if signed else f'{float(v):.{nd}f}'


def svg_dual_bars(items: List[Tuple[str, Optional[float]]], width: int = 640,
                  unit: str = '%', nd: int = 2) -> str:
    """双向条形图（红涨绿跌）：items=[(名称, 值)]，值可正可负。"""
    row_h, label_w, val_w = 26, 110, 64
    vals = [float(v) for _, v in items if v is not None]
    peak = max((max(abs(x) for x in vals), 1e-9))
    h = row_h * max(len(items), 1) + 8
    zero_x = label_w + (width - label_w - val_w) / 2
    span = (width - label_w - val_w) / 2
    parts = [_svg_open(width, h)]
    for i, (name, v) in enumerate(items):
        y = i * row_h + 5
        parts.append(f'<text x="{label_w - 8}" y="{y + 14}" text-anchor="end" '
                     f'fill="#1f2328">{_esc(name)}</text>')
        if v is None:
            parts.append(f'<text x="{zero_x + span}" y="{y + 14}" fill="#8a929c">—</text>')
            continue
        v = float(v)
        bw = max(abs(v) / peak * span, 1.5)
        color = C_UP if v > 0 else (C_DOWN if v < 0 else C_FLAT)
        x = zero_x if v >= 0 else zero_x - bw
        parts.append(f'<rect x="{x:.1f}" y="{y}" width="{bw:.1f}" height="16" '
                     f'rx="2" fill="{color}" opacity="0.85"/>')
        anchor = 'start' if v >= 0 else 'end'
        tx = (zero_x + bw + 5) if v >= 0 else (zero_x - bw - 5)
        parts.append(f'<text x="{tx:.1f}" y="{y + 13}" text-anchor="{anchor}" '
                     f'fill="{color}">{_fmt(v, nd, signed=True)}{unit}</text>')
    parts.append(f'<line x1="{zero_x}" y1="2" x2="{zero_x}" y2="{h - 4}" '
                 f'stroke="{C_AXIS}" stroke-width="1"/>')
    parts.append('</svg>')
    return ''.join(parts)


def svg_count_bars(items: List[Tuple[str, Optional[int]]], width: int = 640) -> str:
    """计数横条（单色，W4 封板五桶）：items=[(名称, 计数)]，None→'—'。"""
    row_h, label_w, val_w = 26, 70, 64
    vals = [float(v) for _, v in items if v is not None]
    peak = max((max(vals), 1e-9))
    h = row_h * max(len(items), 1) + 8
    span = width - label_w - val_w
    parts = [_svg_open(width, h)]
    for i, (name, v) in enumerate(items):
        y = i * row_h + 5
        parts.append(f'<text x="{label_w - 8}" y="{y + 14}" text-anchor="end" '
                     f'fill="#1f2328">{_esc(name)}</text>')
        if v is None:
            parts.append(f'<text x="{label_w}" y="{y + 14}" fill="#8a929c">—</text>')
            continue
        bw = max(float(v) / peak * span, 1.5)
        parts.append(f'<rect x="{label_w}" y="{y}" width="{bw:.1f}" height="16" '
                     f'rx="2" fill="#57606a" opacity="0.85"/>')
        parts.append(f'<text x="{label_w + bw + 5:.1f}" y="{y + 13}" text-anchor="start" '
                     f'fill="#1f2328">{v} 家</text>')
    parts.append(f'<line x1="{label_w}" y1="2" x2="{label_w}" y2="{h - 4}" '
                 f'stroke="{C_AXIS}" stroke-width="1"/>')
    parts.append('</svg>')
    return ''.join(parts)


def svg_position_bars(items: List[Tuple[str, Optional[float]]], width: int = 640,
                      unit: str = '%') -> str:
    """位置条（0~100）：items=[(名称, 百分位)]，游标 + 底槽（底部区/顶部区参考线）。"""
    row_h, label_w, val_w = 24, 130, 52
    bar_x, bar_w = label_w, width - label_w - val_w
    h = row_h * max(len(items), 1) + 8
    parts = [_svg_open(width, h)]
    for i, (name, v) in enumerate(items):
        y = i * row_h + 4
        parts.append(f'<text x="{label_w - 8}" y="{y + 13}" text-anchor="end" '
                     f'fill="#1f2328">{_esc(name)}</text>')
        parts.append(f'<rect x="{bar_x}" y="{y + 3}" width="{bar_w}" height="12" '
                     f'rx="3" fill="{C_BAR_BG}"/>')
        # 20%/80% 底部区/顶部区参考线（复盘框架口径）
        for p, op in ((20, 0.55), (80, 0.55)):
            px = bar_x + bar_w * p / 100
            parts.append(f'<line x1="{px:.1f}" y1="{y + 1}" x2="{px:.1f}" '
                         f'y2="{y + 17}" stroke="{C_AXIS}" stroke-width="1" '
                         f'stroke-dasharray="3,3" opacity="{op}"/>')
        if v is None:
            parts.append(f'<text x="{bar_x + bar_w + 8}" y="{y + 13}" fill="#8a929c">—</text>')
            continue
        v = max(0.0, min(100.0, float(v)))
        cx = bar_x + bar_w * v / 100
        color = C_DOWN if v <= 20 else (C_UP if v >= 80 else '#d9822b')
        parts.append(f'<circle cx="{cx:.1f}" cy="{y + 9}" r="7" fill="{color}"/>')
        parts.append(f'<text x="{bar_x + bar_w + 8}" y="{y + 13}" '
                     f'fill="{color}">{v:.0f}{unit}</text>')
    parts.append('</svg>')
    return ''.join(parts)


def svg_ladder(ladder: Dict[int, int], width: int = 640) -> str:
    """连板梯队阶梯条（高度升序，家数标注）。"""
    if not ladder:
        return '<p class="note">（梯队为空）</p>'
    heights = sorted(int(k) for k in ladder)
    counts = [int(ladder[k]) for k in heights]
    peak = max(max(counts), 1)
    row_h, label_w, val_w = 26, 70, 60
    h = row_h * len(heights) + 8
    bar_x, bar_w = label_w, width - label_w - val_w
    parts = [_svg_open(width, h)]
    for i, (n, c) in enumerate(zip(heights, counts)):
        y = i * row_h + 5
        bw = max(c / peak * bar_w, 4)
        color = C_UP if n >= 5 else ('#d9822b' if n >= 3 else '#b8b02e')
        parts.append(f'<text x="{label_w - 8}" y="{y + 14}" text-anchor="end" '
                     f'fill="#1f2328">{n} 板</text>')
        parts.append(f'<rect x="{bar_x}" y="{y}" width="{bw:.1f}" height="16" '
                     f'rx="2" fill="{color}" opacity="0.85"/>')
        parts.append(f'<text x="{bar_x + bw + 6:.1f}" y="{y + 13}" fill="#1f2328">{c} 家</text>')
    parts.append('</svg>')
    return ''.join(parts)


def svg_sparkline(name: str, points: List[float], dates: Optional[List[str]] = None,
                  width: int = 640, height: int = 90) -> str:
    """迷你走势折线（升序收盘序列），标注首末值与区间涨跌。"""
    if len(points) < 2:
        return ''
    lo, hi = min(points), max(points)
    span = (hi - lo) or 1.0
    pad = 8
    step = (width - 2 * pad) / (len(points) - 1)
    coords = [(pad + i * step, height - 18 - (p - lo) / span * (height - 30))
              for i, p in enumerate(points)]
    path = ' '.join(f'{"M" if i == 0 else "L"}{x:.1f},{y:.1f}' for i, (x, y) in enumerate(coords))
    chg = (points[-1] / points[0] - 1) * 100
    color = C_UP if chg >= 0 else C_DOWN
    parts = [_svg_open(width, height)]
    parts.append(f'<text x="{pad}" y="14" fill="#1f2328">{_esc(name)}'
                 f'（近 {len(points)} 个交易日）</text>')
    parts.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="1.8"/>')
    lx, ly = coords[-1]
    parts.append(f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="3.5" fill="{color}"/>')
    parts.append(f'<text x="{lx:.1f}" y="{ly - 8:.1f}" text-anchor="end" fill="{color}">'
                 f'{points[-1]:.2f}（区间 {chg:+.1f}%）</text>')
    parts.append(f'<text x="{pad}" y="{height - 4}" fill="{C_AXIS}">'
                 f'{_esc(dates[0]) if dates else ""}</text>')
    parts.append(f'<text x="{width - pad}" y="{height - 4}" text-anchor="end" '
                 f'fill="{C_AXIS}">{_esc(dates[-1]) if dates else ""}</text>')
    parts.append('</svg>')
    return ''.join(parts)


def svg_vbars(items: List[Tuple[str, Optional[float]]], width: int = 640,
              height: int = 150) -> str:
    """竖向条形（假期区间累积% 等，支持 None→'—'）。"""
    n = max(len(items), 1)
    slot = width / n
    vals = [float(v) for _, v in items if v is not None]
    peak = max((max(abs(x) for x in vals), 1e-9))
    base_y, chart_h = height - 26, height - 56
    zero_y = base_y - chart_h / 2
    parts = [_svg_open(width, height)]
    parts.append(f'<line x1="0" y1="{zero_y:.1f}" x2="{width}" y2="{zero_y:.1f}" '
                 f'stroke="{C_AXIS}" stroke-width="1"/>')
    for i, (name, v) in enumerate(items):
        cx = slot * i + slot / 2
        if v is None:
            parts.append(f'<text x="{cx:.1f}" y="{zero_y - 6:.1f}" text-anchor="middle" '
                         f'fill="#8a929c">—</text>')
        else:
            v = float(v)
            bh = max(abs(v) / peak * (chart_h / 2), 2)
            color = C_UP if v > 0 else (C_DOWN if v < 0 else C_FLAT)
            y = zero_y - bh if v >= 0 else zero_y
            parts.append(f'<rect x="{cx - 18:.1f}" y="{y:.1f}" width="36" height="{bh:.1f}" '
                         f'rx="3" fill="{color}" opacity="0.85"/>')
            ty = y - 6 if v >= 0 else y + bh + 14
            parts.append(f'<text x="{cx:.1f}" y="{ty:.1f}" text-anchor="middle" '
                         f'fill="{color}">{_fmt(v, 2, signed=True)}%</text>')
        parts.append(f'<text x="{cx:.1f}" y="{height - 8:.1f}" text-anchor="middle" '
                     f'fill="#1f2328">{_esc(name)}</text>')
    parts.append('</svg>')
    return ''.join(parts)


# ------------------------------------------------------------------ 解读文本装配

_MD_BOLD = re.compile(r'\*\*(.+?)\*\*')
#: 表格分隔行（|---|---| 或 | --- : | :---: | 等变体）
_MD_TABLE_SEP = re.compile(r'^\s*\|?[\s:|-]*-[\s:|-]*\|')


def _inline(text: str) -> str:
    return _MD_BOLD.sub(r'<strong>\1</strong>', _esc(text))


def _md_table_row(line: str) -> List[str]:
    """markdown 表格行 → 单元格列表（去首尾 |，按 | 切分）。"""
    return [c.strip() for c in line.strip().strip('|').split('|')]


def _flush_md_table(buf: List[str], out: List[str]) -> None:
    """缓冲的 | 行 → 表格；无合法分隔行则退化为普通段落（装配不加工）。"""
    if len(buf) >= 2 and _MD_TABLE_SEP.match(buf[1]):
        heads = _md_table_row(buf[0])
        out.append('<table><thead><tr>'
                   + ''.join(f'<th>{_inline(h)}</th>' for h in heads)
                   + '</tr></thead>')
        body = []
        for line in buf[2:]:
            cells = _md_table_row(line)
            cells += [''] * (len(heads) - len(cells))
            body.append('<tr>' + ''.join(f'<td>{_inline(c)}</td>' for c in cells)
                        + '</tr>')
        out.append(f'<tbody>{"".join(body)}</tbody></table>')
    else:
        out.extend(f'<p>{_inline(line.strip())}</p>' for line in buf)


def md_fragment_to_html(md_text: str) -> str:
    """解读文本 markdown 子集 → HTML 片段（#标题/列表/引用/表格/段落/**粗体**）。

    只装配不加工：不改写任何内容，仅做结构转换与转义。
    """
    out: List[str] = []
    list_open: Optional[str] = None
    table_buf: List[str] = []
    for raw in md_text.replace('\r\n', '\n').split('\n'):
        line = raw.rstrip()
        if line.strip().startswith('|'):
            # 表格行：关闭列表态后缓冲，连续 | 行成块
            if list_open:
                out.append(f'</{list_open}>')
                list_open = None
            table_buf.append(line)
            continue
        if table_buf:
            _flush_md_table(table_buf, out)
            table_buf = []
        if not line.strip():
            if list_open:
                out.append(f'</{list_open}>')
                list_open = None
            continue
        m = re.match(r'^(#{1,4})\s+(.*)$', line)
        if m:
            if list_open:
                out.append(f'</{list_open}>')
                list_open = None
            level = min(len(m.group(1)) + 1, 5)
            out.append(f'<h{level}>{_inline(m.group(2))}</h{level}>')
            continue
        if line.startswith('>'):
            if list_open:
                out.append(f'</{list_open}>')
                list_open = None
            out.append(f'<blockquote>{_inline(line.lstrip("> ").strip())}</blockquote>')
            continue
        m = re.match(r'^\s*[-*]\s+(.*)$', line)
        kind = 'ul'
        if not m:
            m = re.match(r'^\s*\d+[.、]\s+(.*)$', line)
            kind = 'ol'
        if m:
            if list_open != kind:
                if list_open:
                    out.append(f'</{list_open}>')
                out.append(f'<{kind}>')
                list_open = kind
            out.append(f'<li>{_inline(m.group(1))}</li>')
            continue
        if list_open:
            out.append(f'</{list_open}>')
            list_open = None
        out.append(f'<p>{_inline(line.strip())}</p>')
    if table_buf:
        _flush_md_table(table_buf, out)
    if list_open:
        out.append(f'</{list_open}>')
    return '\n'.join(out)


# ------------------------------------------------------------------ 数据装配

def _sparkline_series(reader, as_of: str) -> List[Tuple[str, List[float], List[str]]]:
    """两条迷你走势：上证指数（trade_index_info）+ 恒生指数（overseas_kline_day）。"""
    out: List[Tuple[str, List[float], List[str]]] = []
    try:
        rows = reader._query(
            "SELECT trade_date, close FROM trade_index_info "
            "WHERE index_name = '上证指数' AND trade_date <= %s AND close IS NOT NULL "
            "ORDER BY trade_date DESC LIMIT 60", (as_of,))
        if len(rows) >= 2:
            rows = list(reversed(rows))
            out.append(('上证指数', [float(r[1]) for r in rows], [str(r[0]) for r in rows]))
    except Exception as e:  # noqa: BLE001 —— 走势图为增强项，缺数据跳过
        logger.warning('digest 上证走势读取失败: %s', e)
    try:
        rows = reader._query(
            "SELECT trade_date, close FROM overseas_kline_day "
            "WHERE market_code = 'hkHSI' AND trade_date <= %s AND close IS NOT NULL "
            "ORDER BY trade_date DESC LIMIT 60", (as_of,))
        if len(rows) >= 2:
            rows = list(reversed(rows))
            out.append(('恒生指数', [float(r[1]) for r in rows], [str(r[0]) for r in rows]))
    except Exception as e:  # noqa: BLE001
        logger.warning('digest 恒指走势读取失败: %s', e)
    return out


def _pct_cell(v: Any, signed: bool = True) -> str:
    if v is None:
        return '—'
    cls = 'up' if float(v) > 0 else ('down' if float(v) < 0 else '')
    txt = f'{float(v):+.{2}f}' if signed else f'{float(v):.2f}'
    return f'<span class="{cls}">{txt}</span>' if cls else txt


def _fmt_premium(v: Any) -> str:
    """AH 溢价读数（W8）：价差非涨跌方向，中性色不挂 up/down class。"""
    return '—' if v is None else f'{float(v):+.2f}'


def _sentiment_pct_note(senti: Dict[str, Any], pcts: Dict[str, Any]) -> str:
    """§二 分位注释行（W1：炸板率小数 ×100 无符号；分位沿用聚合层小数→百分）。"""
    def _p(key):
        return _esc((pcts.get(key) or {}).get('pct'))

    burst = senti.get('burst_rate')
    burst_txt = '—' if burst is None else f'{float(burst) * 100:.1f}%'
    return (f'涨停 {_esc(senti.get("limit_up_count"))} 家'
            f'（250 日分位 {_p("limit_up_count")}%）/ '
            f'炸板率 当日 {burst_txt}'
            f'（分位 {_p("burst_rate")}%）/ '
            f'溢价均值 {_fmt(senti.get("premium_mean"), 2, True)}%'
            f'（分位 {_p("premium_mean")}%）；分位条为过去 250 交易日历史分位。')


#: 封板五桶短名（W4，渲染层私有）：键=聚合层 bucket 标签全文（aggregator bounds），
#: 未命中回退原标签直显
_SEAL_BUCKET_SHORT = {
    '09:25-09:35': '竞价',
    '09:35-10:00': '早盘',
    '10:00-11:30': '盘中',
    '11:30-14:00': '午后',
    '14:00-15:00': '尾盘',
}


def render_seal_quality(senti: Dict[str, Any]) -> Optional[str]:
    """封板质量块（W4）：五桶计数横条 + 读数注；seal_dist 缺失/无桶→None。"""
    seal = senti.get('seal_dist') or {}
    buckets = seal.get('buckets') or []
    if not buckets:
        return None
    items = [(_SEAL_BUCKET_SHORT.get(b.get('bucket'), b.get('bucket')), b.get('count'))
             for b in buckets]
    parts = ['<div class="chart">' + svg_count_bars(items) + '</div>']
    reads = []
    if seal.get('never_broken_rate') is not None:
        reads.append(f'一次未炸 {seal["never_broken_rate"]:.0%}')
    if seal.get('late_seal_rate') is not None:
        reads.append(f'14:00 后首封 {seal["late_seal_rate"]:.0%}')
    if seal.get('limit_up_count') is not None:
        reads.append(f'涨停 {seal["limit_up_count"]} 家（覆盖 {_esc(seal.get("covered"))}）')
    if reads:
        parts.append(f'<p class="note">{" · ".join(reads)}</p>')
    return ''.join(parts)


def render_promotion(senti: Dict[str, Any]) -> Optional[str]:
    """晋级率小表（W4）：档位|昨日家数|今日家数|晋级率，≤6 行；空→None。"""
    promotion = senti.get('promotion') or {}
    if not promotion:
        return None
    rows = []
    for n, v in sorted(promotion.items(), reverse=True)[:6]:
        rate = v.get('rate')
        rate_txt = f'{rate:.0%}' if rate is not None else '—（昨日基数为 0）'
        rows.append(f'<tr><td>{n - 1}→{n} 板</td><td>{_esc(v.get("prev"))}</td>'
                    f'<td>{_esc(v.get("today"))}</td><td>{_esc(rate_txt)}</td></tr>')
    return ('<table><thead><tr><th>晋级档位</th><th>昨日家数</th><th>今日家数</th>'
            '<th>晋级率</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table>')


def _crit_hit_cell(c: Dict[str, Any]) -> str:
    """W5：参考判据（role=reference）命中列灰字 ✓（参考）；缺 role 视为定位判据。"""
    if not c.get('hit'):
        return '否'
    if c.get('role') == 'reference':
        return '<span class="note">✓（参考）</span>'
    return '✓ 是'


#: 口径字典条目（P1-2，评价-1）：术语|口径一句话定义；与报告/配置单一出处
_GLOSSARY_ROWS = [
    ('涨停家数', '收盘价≥涨停价的非 ST 家数（精确涨停价 = round(昨收×(1+板块涨幅上限),2)）'),
    ('炸板率', '炸板家数 /（封板家数 + 炸板家数）；越高=封板资金越弱'),
    ('溢价均值', '昨日涨停股今日平均涨幅（收盘口径：今日涨跌幅=今收/昨收−1，前复权）'
                 '——接力情绪核心指标'),
    ('score（周期走向）', '近 10 个交易日"涨停家数/高度/炸板率(反向)"归一均值——窗口相对读数，'
                          '样本小仅作方向参考，无绝对含义'),
    ('首封 / 开板 / 一次未炸', '封板时段五桶（09:25-09:35 竞价/09:35-10:00 早盘/'
                               '10:00-11:30 盘中/11:30-14:00 午后/14:00-15:00 尾盘）；'
                               '开板次数=当日开板回封次数；'
                               '一次未炸=全天无开板（封板质量指标，分笔口径）'),
    ('翻红 / 炸板修复（题材反馈）', '昨日炸板股今日收红占比 / 昨日炸板股今日平均涨幅'
                                    '（板块主线章题材反馈指标）'),
    ('R1-R4', '转天候选四类：R1 梯队接力（连板≥2）/ R2 主线跟随 / R3 趋势新高 /'
              ' R4 机构席位；报告层带弱档否决与情绪开放度闸门，链 CSV 列为全量'
              '（链命中≠报告入池）'),
    ('P1-P3', '盘前外围情景三档：P1 外围续弱 / P2 外围平稳 / P3 外围反弹'
              '（恒指/纳指较锚定日收盘 ±1.5% 触发）'),
    ('弱档否决', '情绪弱档（冰点/退潮/ADX closed）时 R 系列候选按规则整类不开放'
                 '或压档——报告层 R1=0 而链命中 N 只即此设计行为'),
    ('一买（缠论）', '下跌两中枢依次下移 + MACD 背驰后的左侧买点；锚定日收盘确认，'
                     '操作口径=次日开盘；历史 60 日费后均值 +6.79%/胜率 53.8%'
                     '（2015-2026 全A验证，门控保留样本口径 2026-10-25 修订，'
                     '见 data/chan_validation/report.md §3.6）'),
    ('二买（缠论）', '一买后回踩不创新低确认的第二个左侧买点；历史 +5.95%/53.6%'),
    ('背驰强度', '|Σhist₂|/|Σhist₁|（两段 MACD 柱面积比），<1 即背驰，越小越强；'
                 '本表按强度升序展示'),
    ('中枢 [ZD,ZG]', '缠论中枢区间（重叠价位区），ZD=中枢下沿 / ZG=中枢上沿'),
    ('富时A50期货', '日K口径优先（overseas_kline_day CHA50CFD，新浪全球期货日K，'
                    '隔夜涨跌幅=最新两根收盘环比，2026-10-25 接入）；日K缺失回退'
                    '新浪快照（本地昨收自算，快照距锚定日 >3 天降级标注、>10 天停显，'
                    'P1-5）'),
]


def _anchor_conf_note(ca: Optional[Dict[str, Any]]) -> Optional[str]:
    """资金锚置信度一行（P1-6，技术方案 §4.6 digest 落点对齐，2026-10-25 补全）。

    标签与综合置信度的推导单一出处 = ``ReviewAggregator._capital_anchors``
    （``anchor_tags``/``confidence``），本函数只渲染不推导；与六维报告 §一
    （market_env 小节资金锚表）同一数据源。数据缺失返回 None（不硬造）。
    """
    data = (ca or {}).get('data') or {}
    tags, conf = data.get('anchor_tags') or {}, data.get('confidence')
    if not tags or not conf:
        return None
    items = ' · '.join(f'{lbl}={tags.get(key, "—")}'
                       for lbl, key in (('北向成交额', 'north_total'),
                                        ('南向净买入', 'south_net'),
                                        ('ETF份额', 'etf')))
    return (f'资金锚置信度：综合 {conf}（{items}；停旧值=快照日期早于锚定日'
            '按停更对待；明细见六维报告 §一）')


def _render_glossary() -> str:
    """折叠式口径字典（P1-2）：模板直拼 HTML（details 默认收起，不干扰阅读流）。"""
    rows = ''.join(f'<tr><td>{_esc(t)}</td><td>{_esc(d)}</td></tr>'
                   for t, d in _GLOSSARY_ROWS)
    return (f'<details><summary>口径字典（点击展开——全文术语定义，'
            f'首次出现不随文重复）</summary>'
            f'<table><thead><tr><th>术语</th><th>口径</th></tr></thead>'
            f'<tbody>{rows}</tbody></table></details>')


def _chan_linkage_state(position: str, review_cfg: Dict[str, Any]) -> str:
    """缠论池情绪联动状态（P1-8；规则表 config review.chan_pool_linkage）。"""
    rules = dict((review_cfg or {}).get('chan_pool_linkage') or {})
    rules.setdefault('回暖', '弱化观察')
    rules.setdefault('震荡', '弱化观察')
    rules.setdefault('冰点', '升主关注')
    rules.setdefault('退潮', '升主关注')
    rules.setdefault('高潮', '仅记录')
    tokens = [t for t in str(position or '').split('/') if t]
    states = []
    for t in tokens:
        s = rules.get(t)
        if s and s not in states:
            states.append(s)
    return '/'.join(states) if states else rules.get('回暖', '弱化观察')


def _render_chan_digest(agg: Dict[str, Any], cycle: Dict[str, Any],
                        industry_map: Dict[str, str],
                        review_cfg: Dict[str, Any]) -> Optional[str]:
    """六、缠论背驰候选（P1-8 digest 固化）：表格（含行业/数据标记列）+
    情绪联动状态 + 三条口径注记 + HIST_NOTE 先验 + 数据质量脚注。

    数据复用 agg['chan']（与 md 报告同一聚合产物，不二次计算）；
    缠论面板失败/缺失 → 不渲染该节（digest 其余部分不受影响）。
    """
    from .sections.chan_candidates import HIST_NOTE
    chan = agg.get('chan')
    if not chan or not getattr(chan, 'available', False):
        return None
    data = getattr(chan, 'data', None) or {}
    items = data.get('items') or []
    parts: List[str] = ['<h2>六、缠论背驰候选（六维§六·左侧观察池）</h2>']
    if not items:
        parts.append('<p class="note">今日无一买/二买信号确认（正常态：全市场年均约'
                     ' 480+250 个，单日 0~16 个常见；冰点/退潮期密度上升）。</p>')
    else:
        rows = []
        for it in items[:10]:
            marks = []
            if it.get('data_flag'):
                marks.append(str(it['data_flag']))
            if it.get('suspect_reason'):
                marks.append('链存疑')
            ind = industry_map.get(it.get('ts_code') or '', '—')
            stop = (f"{it.get('trigger_low', '—')}（止损 {it.get('stop', '—')}）"
                    if it.get('stop') is not None else str(it.get('trigger_low', '—')))
            div = (f"{it['divergence']:.2f}"
                   if it.get('divergence') is not None else '—')
            rows.append(f'<tr><td>{_esc(it.get("code", "—"))}</td>'
                        f'<td>{_esc(it.get("name") or "—")}</td>'
                        f'<td>{_esc(it.get("kind", "—"))}</td>'
                        f'<td>{_esc(div)}</td>'
                        f'<td>{_esc(ind)}</td>'
                        f'<td>{_esc(stop)}</td>'
                        f'<td>{_esc("；".join(marks) or "—")}</td></tr>')
        parts.append('<table><thead><tr><th>代码</th><th>名称</th><th>信号</th>'
                     '<th>背驰强度</th><th>行业</th><th>触发低点(止损)</th>'
                     '<th>数据标记</th></tr></thead><tbody>'
                     + ''.join(rows) + '</tbody></table>')
        state = _chan_linkage_state(cycle.get('position', ''), review_cfg)
        parts.append(f'<p>当前情绪档 <strong>{_esc(cycle.get("position", "—"))}</strong>'
                     f' → 缠论池处理：<strong>{_esc(state)}</strong>'
                     f'（联动规则 config review.chan_pool_linkage；'
                     f'回暖/震荡期新确认一买=逆势新低弱股，降权；冰点/退潮期一买='
                     f'深度洗脱后的左侧点，升主关注）</p>')
        flagged = sum(1 for it in items
                      if it.get('data_flag') or it.get('suspect_reason'))
        dq = (f'数据质量：本日 {len(items)} 只候选中 {flagged} 只带缺日/断裂标记'
              f'（缺日>2% 已剔除不判定）')
        overlap = data.get('suspect_overlap')
        if isinstance(overlap, dict) and overlap.get('available'):
            dq += (f"；缠论候选×存疑清单交叉：命中 {overlap.get('hit_count', 0)} 只"
                   f"（全链存疑股票 {overlap.get('suspect_total', 0)} 只）")
        elif isinstance(overlap, dict):
            dq += f"；缠论候选×存疑清单交叉未执行（{overlap.get('note', '不可得')}）"
        parts.append(f'<p class="note">{_esc(dq)}。</p>')
        more = len(items) - min(len(items), 10)
        if more > 0:
            parts.append(f'<p class="note">仅展示前 10 只，全量 {len(items)} 只见'
                         ' daily_review CSV「缠论背驰买点」列。</p>')
    parts.append('<p class="note">口径：背驰入选 |Σhist₂|/|Σhist₁| &lt; 1 即背驰，'
                 '本表按强度升序取前 10；背驰段须为已完成笔（分型确认），短笔柱面积'
                 '天然偏小（回测口径一致，偏倚已定价）；信号为锚定日收盘确认，'
                 '操作口径=次日开盘，一买止损=触发低点。</p>')
    parts.append(f'<p class="note">{_esc(HIST_NOTE)}</p>')
    return ''.join(parts)


def build_digest(config_manager, as_of: Optional[str] = None,
                 interpretation_path: Optional[str] = None,
                 output_dir: Optional[str] = None) -> Dict[str, Any]:
    """构建图文并茂解读 HTML。

    Args:
        interpretation_path: 解读文本（markdown 子集）路径；None 时先试约定路径
            ``<output_dir>/daily_review_<as_of>_digest_note.md``，仍无则显示占位。

    Returns:
        {'html_path', 'as_of', 'prev_date', 'has_interpretation', 'duration_seconds'}
    """
    started = time.time()
    config = config_manager.load_config()
    review_cfg = (config.get('review') or {})
    configured_dir = output_dir or review_cfg.get('output_dir')
    repo_root = Path(__file__).resolve().parents[3]
    out_dir = Path(configured_dir) if configured_dir and Path(configured_dir).is_absolute() \
        else (repo_root / (configured_dir or 'doc/reports'))
    out_dir.mkdir(parents=True, exist_ok=True)

    with DataReader(config_manager) as reader:
        as_of = reader.resolve_as_of(as_of)
        aggregator = ReviewAggregator(reader, config)
        agg = aggregator.aggregate(as_of)
        sparks = _sparkline_series(reader, as_of)
        fx_note = None

    env = agg['market_env'].get('data') if agg.get('market_env') else {}
    senti = agg['sentiment'].get('data') if agg.get('sentiment') else {}
    sector = agg['sector'].get('data') if agg.get('sector') else {}
    money_effect = agg['money_effect'].get('data') if agg.get('money_effect') else {}
    prev_date = agg.get('prev_date')

    # ---- 图表数据 ----
    a_idx = [(i['name'], i['change_rate']) for i in (env.get('indices') or [])]
    a_idx_pos = [(i['name'], i.get('position_pct')) for i in (env.get('indices') or [])]
    ind_today = [(f"{r['industry']}（概念）" if r.get('is_concept') else r['industry'],
                  r['mean_change'])
                 for r in (sector.get('industry_today') or [])]
    ind_chart = ind_today[:8] + ind_today[-4:] if len(ind_today) > 12 else ind_today
    ov = [(i['name'], i['change_pct'], i.get('chg_5d'), i.get('position_pct'))
          for i in (env.get('overseas_indices') or [])]
    ah_rows = [r for r in ((env.get('ah_mapping') or {}).get('rows') or [])
               if not r.get('unaligned') and r.get('diff') is not None]
    ah_chart = sorted(((r['h_name'], r['diff']) for r in ah_rows),
                      key=lambda x: -abs(x[1]))[:10]
    holiday = env.get('holiday_overseas')

    ladder = senti.get('ladder') or {}
    pcts = senti.get('percentiles') or {}
    pct_items = [(name, (pcts.get(key) or {}).get('pct'))
                 for key, name in (('limit_up_count', '涨停家数'),
                                   ('burst_rate', '炸板率'),
                                   ('max_streak', '空间板高度'),
                                   ('premium_mean', '涨停溢价'))]

    # ---- HTML 装配 ----
    parts: List[str] = []
    title = f'每日复盘 · 图文解读（锚定日 {as_of}）'
    parts.append(f'<h1>{_esc(title)}</h1>')
    parts.append(f'<p class="note">数据口径与 <code>daily_review_{_esc(as_of)}.md</code> '
                 f'六维报告同源（ReviewAggregator 同一次聚合）；外围为市场自身交易日历'
                 f'（qfq/指数原值），A 股锚定日 {as_of}、前一交易日 {prev_date or "—"}。</p>')
    # W2 引用体系统一：两份文档章节编号不同名异指，页首固定对照行
    parts.append('<p class="note">章节对照：图文§一~§三 对应六维§一~§三；'
                 '图文§四（外围）/§五（A-H）属六维§一；图文§六（缠论候选）属六维§六；'
                 '六维§四（个股异动）/§五（基本面）本页不设章，数据由解读区引用。'
                 '解读区「六维§X/图文§X」括注即按此对照。</p>')
    # P1-2 口径字典（评价-1：三源术语不自解释）：折叠式固定渲染，术语与报告口径单一出处
    parts.append(_render_glossary())

    # 〇、昨日核验摘要（四期 §1.4：复盘先对账，置于大盘环境之前）
    ver = agg.get('verification') or {}
    if ver.get('available') and ver.get('data'):
        parts.append('<h2>〇、昨日核验（摘要）</h2>')
        ver_data = ver['data']
        sc = ver_data.get('scenario') or {}
        if sc.get('triggered'):
            open_bit = ''
            if sc.get('expectation') and sc.get('open_gap') is not None:
                open_bit = (f"，今日上证开盘 {_fmt(sc.get('open_gap'), 2, True)}% —— "
                            f"{_esc(sc['expectation'])}预期"
                            f"{_esc(sc.get('fulfilled') or '数据不足')}")
            parts.append(f'<p>情景：<strong>{_esc(sc["triggered"])}</strong>'
                         f'{open_bit}</p>')
        v2 = next((p for p in (ver_data.get('verify_points') or [])
                   if str(p.get('key', '')).startswith('V2')), None)
        if v2:
            parts.append(f'<p>V2 梯队：{_esc(v2.get("base"))} —— 今日 {_esc(v2.get("actual"))}'
                         f'，判定 <strong>{_esc(v2.get("verdict"))}</strong></p>')
        cand = ver_data.get('candidates') or {}
        excess_bits = []
        for key in ('R1', 'R2', 'R3', 'R4'):
            st = ((cand.get('pools') or {}).get(key) or {})
            if st.get('traded') and st.get('excess_median') is not None:
                excess_bits.append(f'{key} {st["excess_median"]:+.2f}pct')
        if excess_bits:
            mkt = cand.get('market_median')
            parts.append('<p>候选池超额（池中位 − 全市场中位'
                         + (f' {_fmt(mkt, 2, True)}%' if mkt is not None else '')
                         + '）：<strong>' + '；'.join(_esc(b) for b in excess_bits)
                         + '</strong></p>')
        if ver_data.get('rolling'):
            parts.append('<p class="note">滚动战绩（近 30 核验日）详见报告'
                         '「〇、昨日核验」章节。</p>')
    elif ver.get('reason'):
        parts.append(f'<p class="note">昨日核验：{_esc(ver["reason"])}</p>')

    parts.append('<h2>一、大盘环境（A 股）</h2>')
    if a_idx:
        parts.append('<div class="chart">' + svg_dual_bars(a_idx) + '</div>')
        parts.append('<div class="chart">' + svg_position_bars(a_idx_pos) + '</div>')
        parts.append('<p class="note">上：当日涨跌幅（红涨绿跌）；下：60 日窗口位置百分位'
                     '（虚线为 20%/80% 底部区/顶部区参考，橙=中性区）。</p>')
    for name, pts, dates in sparks:
        if name == '上证指数':
            parts.append('<div class="chart">' + svg_sparkline(name, pts, dates) + '</div>')

    # P1-6 资金锚置信度一行（技术方案 §4.6 digest 落点对齐）：与六维报告 §一同源
    note_anchor = _anchor_conf_note(agg.get('capital_anchors'))
    if note_anchor:
        parts.append(f'<p class="note">{_esc(note_anchor)}</p>')

    parts.append('<h2>二、市场情绪</h2>')
    cycle = (senti.get('cycle') or {})
    trial_mark = '（判据试用版）' if cycle.get('trial') else ''
    parts.append(f'<p>情绪周期定位：<strong>{_esc(cycle.get("position", "—"))}</strong>'
                 f'{_esc(trial_mark)}'
                 f'（{"；".join(_esc(b) for b in (cycle.get("basis") or [])) or "—"}）</p>')
    # P1-1 判据对照表（评价-2）：判据|阈值|当日值|命中 + 分位四态并列——
    # 直接回答"为什么不是 X"（数据已算出，此前只渲染结论一行）
    criteria = cycle.get('criteria') or []
    if criteria:
        crit_rows = ''.join(
            f'<tr><td>{_esc(c["name"])}</td><td>{_esc(c["expr"])}</td>'
            f'<td>{_esc(c["values"])}</td><td>{_crit_hit_cell(c)}</td></tr>'
            for c in criteria)
        parts.append(f'<table><thead><tr><th>判据</th><th>阈值</th><th>当日值</th>'
                     f'<th>命中</th></tr></thead><tbody>{crit_rows}</tbody></table>')
        parts.append('<p class="note">标注（参考）的判据不参与定位判定。</p>')
    parts.append(f'<p class="note">分位标定四态：{_esc(cycle.get("quantile_position", "—"))}'
                 f'（{"；".join(_esc(b) for b in (cycle.get("quantile_basis") or [])) or "—"}）'
                 + (f'<br>⚠ 规则四态与分位四态分歧（{_esc(cycle.get("position"))} vs '
                    f'{_esc(cycle.get("quantile_position"))}）——以规则判据为主定位，'
                    '分歧本身是观察信息' if cycle.get('quantile_divergence') else '')
                 + '</p>')
    trend = senti.get('cycle_trend')
    if trend:
        complete = '' if trend.get('complete') else f"（窗口仅 {trend['window']} 天，读数不完整）"
        parts.append(f'<p>周期走向：10 日窗口 score <strong>{_fmt(trend.get("score"), 2)}</strong>'
                     f'（窗口分位 {_esc(trend.get("window_pct"))}%），'
                     f'{_esc(trend.get("direction"))}（首尾差 {_fmt(trend.get("delta"), 2, True)}），'
                     f'距窗口最低点第 {_esc(trend.get("days_since_low"))} 天{complete}'
                     f'<span class="note">（score 为 10 日窗口相对读数，样本小仅作方向参考）</span></p>')
    parts.append('<div class="chart">' + svg_ladder(ladder) + '</div>')
    parts.append('<div class="chart">' + svg_position_bars(pct_items) + '</div>')
    # P1-3 图注统一（评价-1）：分位类图注统一"当日值 X（250 日分位 Y%）"格式
    parts.append(f'<p class="note">{_sentiment_pct_note(senti, pcts)}</p>')
    # W4 图文自足：封板质量与晋级块（零新计算，渲染已聚合数据；缺数据逐块降级不硬造）
    parts.append('<h3>封板质量与晋级</h3>')
    parts.append(render_seal_quality(senti)
                 or '<p class="note">封板分笔统计无当日行（limit_seal_stats_daily），'
                    '五桶图不渲染。</p>')
    parts.append(render_promotion(senti)
                 or '<p class="note">昨日无 (N-1) 板基数，晋级率表不渲染。</p>')
    cp = (money_effect or {}).get('consec_premium')
    if cp and cp.get('count'):
        cp_red = f"{cp['red_rate']:.0%}" if cp.get('red_rate') is not None else '—'
        parts.append('<p class="note">连板溢价：昨日 2 板及以上 '
                     f'{_esc(cp.get("count"))} 只（成交 {_esc(cp.get("traded"))}）：'
                     f'均值 {_fmt(cp.get("mean"), 2, True)}% / '
                     f'中位 {_fmt(cp.get("median"), 2, True)}% / 翻红 {cp_red}'
                     '。明细见六维§二（赚钱效应/反馈矩阵全量）。</p>')

    parts.append('<h2>三、板块主线（等权口径）</h2>')
    if ind_chart:
        parts.append('<div class="chart">' + svg_dual_bars(ind_chart, unit='%') + '</div>')
        parts.append('<p class="note">行业等权涨跌幅（今日 Top8 + Bottom4）；两套分类体系'
                     '（sw/sina）交叉结果仅供观察。</p>')
    # W4 图文自足：聚集头名与题材反馈行注（渲染已聚合数据）
    clusters3 = (sector.get('limit_up_clusters') or [])[:3]
    if clusters3:
        cl_txt = ' / '.join(f'{_esc(c.get("concept"))} {_esc(c.get("count"))} 家'
                            for c in clusters3)
        parts.append(f'<p class="note">涨停聚集 Top3：{cl_txt}'
                     '（sina_gn 概念口径，全量见六维§三）。</p>')
    br = (money_effect or {}).get('burst_recovery')
    if br and br.get('count'):
        br_red = f"{br['red_rate']:.0%}" if br.get('red_rate') is not None else '—'
        parts.append('<p class="note">题材反馈：昨日炸板 '
                     f'{_esc(br.get("count"))} 只（成交 {_esc(br.get("traded"))}）今日均值 '
                     f'{_fmt(br.get("mean"), 2, True)}% / 翻红 {br_red}'
                     '（炸板修复，明细见六维§二）。</p>')

    parts.append('<h2>四、外围指数（六维§一·市场自身日历）</h2>')
    if ov:
        parts.append('<div class="chart">'
                     + svg_position_bars([(n, p) for n, _, _, p in ov]) + '</div>')
        rows_html = ''.join(
            f'<tr><td>{_esc(n)}</td><td>{_pct_cell(c)}</td><td>{_pct_cell(d5)}</td>'
            f'<td>{_pct_cell(p, signed=False)}</td></tr>'
            for n, c, d5, p in ov)
        parts.append(f'<table><thead><tr><th>指数</th><th>当日%</th><th>5日%</th>'
                     f'<th>60日位置</th></tr></thead><tbody>{rows_html}</tbody></table>')
    for name, pts, dates in sparks:
        if name == '恒生指数':
            parts.append('<div class="chart">' + svg_sparkline(name, pts, dates) + '</div>')

    parts.append('<h2>五、A-H 映射（六维§一·参考口径）</h2>')
    if ah_chart:
        parts.append('<div class="chart">' + svg_dual_bars(ah_chart, unit='pct') + '</div>')
    if ah_rows:
        top_prem = sorted((r for r in ah_rows if r.get('premium') is not None),
                          key=lambda r: -abs(r['premium']))[:5]
        prem_rows = ''.join(
            f'<tr><td>{_esc(r["h_name"])}</td><td>{_pct_cell(r.get("diff"))}</td>'
            f'<td>{_fmt_premium(r.get("premium"))}</td></tr>' for r in top_prem)
        parts.append(f'<table><thead><tr><th>H股</th><th>AH 5日差(pct)</th>'
                     f'<th>AH溢价%</th></tr></thead><tbody>{prem_rows}</tbody></table>')
        parts.append('<p class="note">差值=A 侧−H 侧 5 日累计（正=A 强于 H）；溢价='
                     'A 收盘/(H 收盘×港元兑人民币快照)−1。两侧复权基准独立滚动、时点错位，'
                     '只作观察不作结论；"未对齐/无A股映射"行不参与。溢价为价差读数非涨跌，'
                     '不作红绿标注。</p>')

    # P1-8 缠论背驰候选（digest 固定渲染位，五节之后、解读区之前）：
    # 复用 agg['chan']（与 md 报告第六章同一聚合数据，不二次计算）
    try:
        industry_map = aggregator.industry_map(as_of)
    except Exception:
        industry_map = {}
    chan_html = _render_chan_digest(agg, cycle, industry_map, review_cfg)
    if chan_html:
        parts.append(chan_html)

    if holiday:
        parts.append(f'<h2>七、假期外围观察（{holiday.get("prev_date")} 后休市区间，'
                     f'截至 {holiday.get("last_date") or "—"}）</h2>')
        hol_items = [(r['name'], r.get('accum_pct')) for r in (holiday.get('rows') or [])]
        parts.append('<div class="chart">' + svg_vbars(hol_items) + '</div>')
        parts.append('<p class="note">区间（前一 A 股交易日, 锚定日）各市场自身交易日'
                     '首尾收盘比（qfq）；锚定日当日变化计入当日%，不重复计入。</p>')

    # 市场要闻（四期 §4；新浪 7x24 当日快讯，importance 优先）
    news = agg.get('news') or {}
    news_items = ((news.get('data') or {}).get('items')) or []
    if news_items:
        parts.append('<h2>市场要闻（新浪 7x24，重要优先）</h2>')
        parts.append('<ul class="news">')
        for it in news_items[:10]:
            star = '★ ' if it.get('importance') else ''
            t = str(it.get('time', ''))
            hm = t[11:16] if len(t) >= 16 else ''
            parts.append(f'<li>{_esc(star)}{_esc(hm)} {_esc(it.get("title"))}</li>')
        parts.append('</ul>')
        parts.append('<p class="note">新闻归因是市场叙事，不是核验过的因果。</p>')
    elif news.get('reason'):
        parts.append(f'<p class="note">市场要闻：{_esc(news["reason"])}</p>')

    parts.append('<div class="interp"><h2>复盘解读</h2>')
    interp_path = Path(interpretation_path) if interpretation_path else \
        out_dir / f'daily_review_{as_of}_digest_note.md'
    interp_html = ''
    if interp_path and Path(interp_path).exists():
        interp_html = md_fragment_to_html(
            Path(interp_path).read_text(encoding='utf-8'))
    if interp_html:
        parts.append(interp_html)
        has_interp = True
    else:
        parts.append('<p class="note">（本次未附解读文本——由复盘 agent 在对话中输出）</p>')
        has_interp = False
    parts.append('</div>')

    parts.append('<div class="disclaimer">免责声明：本页由免费公开数据自动生成'
                 '（图表与六维报告同口径，同数据同结果），仅供个人研究参考，'
                 '不构成任何投资建议；外围数据为腾讯 ifzq/新浪源，qfq 前复权口径'
                 '随公司行动滚动。</div>')

    html_text = ('<!DOCTYPE html>\n<html lang="zh-CN">\n<head>\n<meta charset="utf-8">\n'
                 f'<title>{_esc(title)}</title>\n<style>{_PAGE_CSS}</style>\n</head>\n'
                 '<body><div id="content">\n' + '\n'.join(parts)
                 + '\n</div></body>\n</html>\n')
    html_path = out_dir / f'daily_review_{as_of}_digest.html'
    html_path.write_text(html_text, encoding='utf-8')
    return {'html_path': str(html_path), 'as_of': as_of, 'prev_date': prev_date,
            'has_interpretation': has_interp,
            'duration_seconds': round(time.time() - started, 1)}
