"""报告渲染层（复盘报告层方案 §5.4，M1）。

块模型（sections 产出 → renderer 消费）::

    {'type': 'heading', 'text': str, 'level': 2|3}
    {'type': 'para',    'text': str}
    {'type': 'bullets', 'items': [str]}
    {'type': 'table',   'headers': [str], 'rows': [[str]]}
    {'type': 'note',    'text': str}          # 口径说明/待补标注

约定：
- MD 为源 → 同名 .html（内联 CSS、表格斑马纹、零外部依赖）；
- **正文不出现运行时间戳**（数据时点以 as_of 表达），生成耗时等元信息写
  ``daily_review_YYYYMMDD.meta.json`` → 同锚定日重复生成逐字节一致（§3.10）；
- 缺数据章节渲染「⏳ 待补：{缺什么}（{原因}）」，禁止留空或编造。
"""

from __future__ import annotations

import html
from typing import Any, Dict, List


def blocks_to_markdown(blocks: List[Dict[str, Any]]) -> str:
    """块序列 → Markdown 文本"""
    lines: List[str] = []
    for b in blocks:
        t = b['type']
        if t == 'heading':
            lines.append('#' * int(b.get('level', 3)) + ' ' + b['text'])
            lines.append('')
        elif t == 'para':
            lines.append(b['text'])
            lines.append('')
        elif t == 'bullets':
            for item in b['items']:
                lines.append(f'- {item}')
            lines.append('')
        elif t == 'table':
            headers = [str(h) for h in b['headers']]
            lines.append('| ' + ' | '.join(headers) + ' |')
            lines.append('|' + '|'.join(['---'] * len(headers)) + '|')
            for row in b['rows']:
                lines.append('| ' + ' | '.join(_cell(v) for v in row) + ' |')
            lines.append('')
        elif t == 'note':
            lines.append(f'> {b["text"]}')
            lines.append('')
        else:
            raise ValueError(f'未知块类型: {t}')
    return '\n'.join(lines).rstrip() + '\n'


_HTML_CSS = """
body { font-family: -apple-system, 'PingFang SC', 'Microsoft YaHei', sans-serif;
       margin: 0; padding: 24px; color: #1f2328; background: #fafbfc; line-height: 1.6; }
#content { max-width: 1100px; margin: 0 auto; background: #fff;
           padding: 32px 40px; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
h1 { font-size: 1.5em; border-bottom: 2px solid #d0d7de; padding-bottom: 8px; }
h2 { font-size: 1.25em; border-bottom: 1px solid #d0d7de; padding-bottom: 6px; margin-top: 2em; }
h3 { font-size: 1.1em; margin-top: 1.5em; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 0.92em; }
th, td { border: 1px solid #d0d7de; padding: 6px 10px; text-align: left; }
th { background: #f6f8fa; }
tbody tr:nth-child(even) { background: #f6f8fa; }
blockquote { border-left: 4px solid #d0d7de; margin: 8px 0; padding: 4px 14px; color: #57606a; }
code { background: #f6f8fa; padding: 1px 5px; border-radius: 4px; }
"""


def blocks_to_html(title: str, blocks: List[Dict[str, Any]]) -> str:
    """块序列 → 独立 HTML（内联 CSS，零外部依赖）"""
    parts = [_render_block(b) for b in blocks]
    return ('<!DOCTYPE html>\n<html lang="zh-CN">\n<head>\n<meta charset="utf-8">\n'
            f'<title>{html.escape(title)}</title>\n<style>{_HTML_CSS}</style>\n</head>\n'
            '<body><div id="content">\n'
            + '\n'.join(parts)
            + '\n</div></body>\n</html>\n')


def _render_block(b: Dict[str, Any]) -> str:
    t = b['type']
    if t == 'heading':
        level = min(int(b.get('level', 3)), 6)
        return f'<h{level}>{html.escape(b["text"])}</h{level}>'
    if t == 'para':
        return f'<p>{html.escape(b["text"])}</p>'
    if t == 'bullets':
        lis = ''.join(f'<li>{html.escape(i)}</li>' for i in b['items'])
        return f'<ul>{lis}</ul>'
    if t == 'table':
        heads = ''.join(f'<th>{html.escape(str(h))}</th>' for h in b['headers'])
        body_rows = []
        for row in b['rows']:
            tds = ''.join(f'<td>{html.escape(_cell(v))}</td>' for v in row)
            body_rows.append(f'<tr>{tds}</tr>')
        return (f'<table><thead><tr>{heads}</tr></thead>'
                f'<tbody>{"".join(body_rows)}</tbody></table>')
    if t == 'note':
        return f'<blockquote>{html.escape(b["text"])}</blockquote>'
    raise ValueError(f'未知块类型: {t}')


def _cell(v: Any) -> str:
    """单元格取值：None/NaN → '—'；数字保留展示精度"""
    if v is None:
        return '—'
    try:
        import math
        if isinstance(v, float) and math.isnan(v):
            return '—'
    except (TypeError, ValueError):  # pragma: no cover
        pass
    if isinstance(v, float):
        text = f'{v:,.2f}'.rstrip('0').rstrip('.')
        return text if not text.endswith('.') else text + '0'
    if isinstance(v, bool):
        return '是' if v else '否'
    return str(v)


def missing_note(what: str, why: str = '') -> Dict[str, Any]:
    """缺数据章节的统一「待补」标注（§5.3 缺数据标注规范）"""
    suffix = f'（{why}）' if why else ''
    return {'type': 'note', 'text': f'⏳ 待补：{what}{suffix}'}
