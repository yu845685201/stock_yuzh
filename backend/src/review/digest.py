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
    prev_date = agg.get('prev_date')

    # ---- 图表数据 ----
    a_idx = [(i['name'], i['change_rate']) for i in (env.get('indices') or [])]
    a_idx_pos = [(i['name'], i.get('position_pct')) for i in (env.get('indices') or [])]
    ind_today = [(r['industry'], r['mean_change'])
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

    parts.append('<h2>一、大盘环境（A 股）</h2>')
    if a_idx:
        parts.append('<div class="chart">' + svg_dual_bars(a_idx) + '</div>')
        parts.append('<div class="chart">' + svg_position_bars(a_idx_pos) + '</div>')
        parts.append('<p class="note">上：当日涨跌幅（红涨绿跌）；下：60 日窗口位置百分位'
                     '（虚线为 20%/80% 底部区/顶部区参考，橙=中性区）。</p>')
    for name, pts, dates in sparks:
        if name == '上证指数':
            parts.append('<div class="chart">' + svg_sparkline(name, pts, dates) + '</div>')

    parts.append('<h2>二、市场情绪</h2>')
    cycle = (senti.get('cycle') or {})
    parts.append(f'<p>情绪周期定位：<strong>{_esc(cycle.get("position", "—"))}</strong>'
                 f'（{"；".join(_esc(b) for b in (cycle.get("basis") or [])) or "—"}）</p>')
    parts.append('<div class="chart">' + svg_ladder(ladder) + '</div>')
    parts.append('<div class="chart">' + svg_position_bars(pct_items) + '</div>')
    parts.append(f'<p class="note">涨停 {_esc(senti.get("limit_up_count"))} 家 / '
                 f'炸板率 {_fmt(senti.get("burst_rate"), 1, True)}% / '
                 f'溢价均值 {_fmt(senti.get("premium_mean"), 2, True)}%（分位条为过去 250 '
                 f'交易日历史分位）。</p>')

    parts.append('<h2>三、板块主线（等权口径）</h2>')
    if ind_chart:
        parts.append('<div class="chart">' + svg_dual_bars(ind_chart, unit='%') + '</div>')
        parts.append('<p class="note">行业等权涨跌幅（今日 Top8 + Bottom4）；两套分类体系'
                     '（sw/sina）交叉结果仅供观察。</p>')

    parts.append('<h2>四、外围指数（市场自身日历）</h2>')
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

    parts.append('<h2>五、A-H 映射（参考口径）</h2>')
    if ah_chart:
        parts.append('<div class="chart">' + svg_dual_bars(ah_chart, unit='pct') + '</div>')
    if ah_rows:
        top_prem = sorted((r for r in ah_rows if r.get('premium') is not None),
                          key=lambda r: -abs(r['premium']))[:5]
        prem_rows = ''.join(
            f'<tr><td>{_esc(r["h_name"])}</td><td>{_pct_cell(r.get("diff"))}</td>'
            f'<td>{_pct_cell(r.get("premium"))}</td></tr>' for r in top_prem)
        parts.append(f'<table><thead><tr><th>H股</th><th>AH 5日差(pct)</th>'
                     f'<th>AH溢价%</th></tr></thead><tbody>{prem_rows}</tbody></table>')
        parts.append('<p class="note">差值=A 侧−H 侧 5 日累计（正=A 强于 H）；溢价='
                     'A 收盘/(H 收盘×港元兑人民币快照)−1。两侧复权基准独立滚动、时点错位，'
                     '只作观察不作结论；"未对齐/无A股映射"行不参与。</p>')

    if holiday:
        parts.append(f'<h2>六、假期外围观察（{holiday.get("prev_date")} 后休市区间，'
                     f'截至 {holiday.get("last_date") or "—"}）</h2>')
        hol_items = [(r['name'], r.get('accum_pct')) for r in (holiday.get('rows') or [])]
        parts.append('<div class="chart">' + svg_vbars(hol_items) + '</div>')
        parts.append('<p class="note">区间（前一 A 股交易日, 锚定日）各市场自身交易日'
                     '首尾收盘比（qfq）；锚定日当日变化计入当日%，不重复计入。</p>')

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
