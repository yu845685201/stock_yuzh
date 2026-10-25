#!/usr/bin/env python3
"""digest 数字白名单校验 + 结构校验（P0-3 防幻觉三件套；P2-5 结构规则库）。

输入：``daily_review_<锚定日>_digest_note.md`` + 当日链 CSV + 报告 meta；
可选 ``--digest <digest.html> --config <config.yaml>`` 启用结构校验。
动作：
1. 数字白名单（WARN 级）：note 中全部数字与「CSV 列值集合 ∪ 报告 meta 已知值
   ∪ 已挂来源括注的数字」比对，输出 WARN 清单（行号+数字+上下文）；
   并行 P2-8 语汇扫描（config 键名/时钟）与 T5 骨架兜底扫描（工单号/嵌套括注/
   免责重复/精选池行序自洽——均 WARN 级保险丝，骨架主约束在 SKILL.md）；
2. 结构规则库（P2-5，**硬失败**）：口径字典存在 / 缠论候选×存疑交叉披露 /
   背驰入选口径注记 / 判据表阈值与 config 逐值一致 / 操作池表头规范 /
   主干行（**主干：…** 独立行，速览框提取契约 T1） / 基准倾向行
   （**基准倾向**：，T1）——任一失败退出码 1，阻断 digest 发布。

纪律（SKILL.md 路径 A 第三层）：生成 digest 前必跑；agent 必须逐个人工过
WARN 清单并在最终输出点名（「WARN 已逐条核对」或逐条说明）。

用法::

    python check_digest_numbers.py --note <digest_note.md> --csv <daily_review.csv> \
        [--meta <meta.json>] [--digest <digest.html>] [--config <config.yaml>] [--strict]

退出码：0（全绿）；1（结构规则失败，或 --strict 时有 WARN）；2（输入缺失）。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

#: 数字形状（含千分位与小数；单位由后续正则单独捕获）
_NUM = r'-?\d+(?:,\d{3})*(?:\.\d+)?'
#: 来源括注/推断标注（命中即视为已接地，不告警；「（推断」兼容「（推断：」形态；
#: W2 2026-10-25 起新 note 统一（六维§X/图文§X），（报告§X 旧格式保留接受兼容历史存量）
_ANNOTATION = re.compile(
    r'（推断|（CSV:|（报告§|（六维§|（图文§|【推断】|【假设·待验证】|（链CSV|（报告meta')
#: 结构性数字豁免：ev 编号 / 日期 / 时间 / 章节号 / 序号
_STRUCTURAL = re.compile(
    r'ev-\d{8}-\d+|\d{8}|\d{6}|\d{4}-\d{2}-\d{2}|§\s*\d+(?:\.\d+)?|第\s*\d+\s*[节条]')
#: 注记行（> 开头）中的数字豁免（口径说明非结论数字）
_NOTE_LINE = re.compile(r'^\s*>')

#: 数字 + 可选单位（金额/百分比/倍数/家数/板数/年/秒）
_NUM_WITH_UNIT = re.compile(
    rf'({_NUM})\s*(%|x|×|倍|亿|万|家|板|年|秒|个百分点|pp|元|手)?')


def _normalize(num_text: str) -> str:
    """归一化：去千分位、去尾随零（147.0 → 147），便于与 CSV 单元格比对。"""
    t = num_text.replace(',', '')
    try:
        v = float(t)
    except ValueError:
        return t
    if v == int(v):
        return str(int(v))
    return ('%f' % v).rstrip('0').rstrip('.')


def _collect_numbers(text: str) -> Set[str]:
    """文本中全部数字的归一化集合（CSV/meta 白名单构建用）。"""
    return {_normalize(m.group(1))
            for m in _NUM_WITH_UNIT.finditer(text or '')}


def _csv_whitelist(csv_path: Path) -> Set[str]:
    """CSV 列值集合：全部单元格数字（归一化）。"""
    white: Set[str] = set()
    try:
        text = csv_path.read_text(encoding='utf-8-sig')
    except OSError as e:
        print(f'[warn] CSV 读取失败: {e}', file=sys.stderr)
        return white
    for line in text.splitlines():
        white |= _collect_numbers(line)
    return white


def _meta_whitelist(meta_path: Path) -> Set[str]:
    """报告 meta 已知值集合（递归收集全部数字）。"""
    try:
        data = json.loads(meta_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as e:
        print(f'[warn] meta 读取失败: {e}', file=sys.stderr)
        return set()
    return _collect_numbers(json.dumps(data, ensure_ascii=False))


def check_note(note_path: Path, whitelist: Set[str]) -> List[Dict[str, str]]:
    """逐行扫描 note，返回 WARN 清单（未接地数字）。

    豁免：来源括注上下文（±30 字符）/ 结构性数字 / 注记行。
    """
    warns: List[Dict[str, str]] = []
    try:
        lines = note_path.read_text(encoding='utf-8').splitlines()
    except OSError as e:
        return [{'line': '0', 'num': '-', 'context': f'note 读取失败: {e}'}]
    for ln, line in enumerate(lines, 1):
        if _NOTE_LINE.match(line):
            continue
        # 行内任一位置命中结构性模式 → 该行跳过（章节头/编号行）
        if _STRUCTURAL.search(line):
            continue
        for m in _NUM_WITH_UNIT.finditer(line):
            num = _normalize(m.group(1))
            if not num or num in ('0',):        # 纯 0 多为序号/占位，豁免
                continue
            ctx_start = max(m.start() - 30, 0)
            ctx_end = min(m.end() + 30, len(line))
            context = line[ctx_start:ctx_end]
            if _ANNOTATION.search(context):
                continue                        # 已挂来源/推断标注
            if num in whitelist:
                continue                        # CSV 列值或 meta 已知值
            warns.append({'line': str(ln), 'num': m.group(0).strip(),
                          'value': num, 'context': context.strip()})
    return warns


#: P2-8 语汇边界（W6 2026-10-25）：实现细节形状——config 键名引用 / 时钟时间戳
_RE_IMPL_LEAK = re.compile(r'config\s+[\w.]+|\b\d{1,2}:\d{2}\b')
#: 行情时刻词（实施修正）：紧邻（±6 字符）出现时时钟数字视为行情事实而非实现
#: 细节时间戳（如"首封 14:30""14:00 后首封"），不告警
_MARKET_TIME_WORDS = ('首封', '封板', '开板', '回封', '炸板', '竞价', '开盘',
                      '收盘', '涨停', '尾盘', '午后', '早盘', '盘中')


def check_impl_leak(note_path: Path) -> List[Dict[str, str]]:
    """正文实现细节外漏扫描（P2-8，WARN 级不阻断）。

    豁免：表格行（| 开头——实现信息允许进质量点名表说明列）、注记行（> 开头）、
    行情时刻词紧邻的时钟数字。每行至多报一处（定位足够，避免刷屏）。
    """
    leaks: List[Dict[str, str]] = []
    try:
        lines = note_path.read_text(encoding='utf-8').splitlines()
    except OSError:
        return leaks
    for ln, line in enumerate(lines, 1):
        if line.lstrip().startswith(('|', '>')):
            continue
        for m in _RE_IMPL_LEAK.finditer(line):
            ctx = line[max(m.start() - 6, 0):m.end() + 6]
            if ':' in m.group(0) and any(w in ctx for w in _MARKET_TIME_WORDS):
                continue
            leaks.append({'line': str(ln), 'num': m.group(0),
                          'value': m.group(0), 'context': line.strip()[:60]})
            break
    return leaks


# ---------------------------------------------------------------- T5 骨架兜底（WARN 级保险丝，2026-10-26）

#: 工单号形状（T5.4/P2-8）：X 系列与 P/S/A-短编号；词边界+连字符后须数字，
#: 已核对不误伤 P1-P3（连字符后非数字）/V 系列/R1-R4/A50/CHA50CFD/日期
_RE_TICKET = re.compile(r'\b(?:X\d+|[PSA]\d{1,2}-\d{1,2})\b')
#: 嵌套括注（T5.1）：全角括号内再出现全角开括号
_RE_NESTED_PAREN = re.compile(r'（[^（）]*（')
#: 池表行「N 证，X 分」类别列形状（T5.2 行序自洽）
_RE_POOL_EVID = re.compile(r'(\d+)\s*证[，,]\s*(\d+(?:\.\d+)?)\s*分')


def _scan_lines(note_path: Path, keep_table: bool) -> List[Tuple[int, str]]:
    """note 逐行读取为 (行号, 行文本)；keep_table=False 时剔除表格/引用行。"""
    try:
        lines = note_path.read_text(encoding='utf-8').splitlines()
    except OSError:
        return []
    return [(ln, line) for ln, line in enumerate(lines, 1)
            if keep_table or not line.lstrip().startswith(('|', '>'))]


def check_ticket_leak(note_path: Path) -> List[Dict[str, str]]:
    """工单号扫描（T5.4，WARN 级）：note 全文禁 X/P/A/S 系列编号（含表格行）。

    与 check_impl_leak 的每行一条不同：一行多枚工单号常见（"X2 修复、P4-8 回测"），
    漏报比刷屏危害大，逐枚全量报告。"""
    hits = []
    for ln, line in _scan_lines(note_path, keep_table=True):
        for m in _RE_TICKET.finditer(line):
            hits.append({'line': str(ln), 'num': m.group(0),
                         'value': m.group(0), 'context': line.strip()[:60]})
    return hits


def check_nested_paren(note_path: Path) -> List[Dict[str, str]]:
    """嵌套括注扫描（T5.1，WARN 级）：正文行全角括号嵌套（表格/引用行豁免）。"""
    hits = []
    for ln, line in _scan_lines(note_path, keep_table=False):
        m = _RE_NESTED_PAREN.search(line)
        if m:
            hits.append({'line': str(ln), 'num': '嵌套括注',
                         'value': m.group(0)[-20:], 'context': line.strip()[:60]})
    return hits


def check_disclaimer(note_path: Path) -> List[Dict[str, str]]:
    """免责单出处（T5.5，WARN 级）：note 不写免责（digest 页脚恒带为单一出处）。"""
    hits = []
    for ln, line in _scan_lines(note_path, keep_table=True):
        if '免责声明' in line:
            hits.append({'line': str(ln), 'num': '免责声明', 'value': '免责声明',
                         'context': line.strip()[:60]})
    return hits


def _note_pool_rows(note_path: Path) -> List[Tuple[str, int, float]]:
    """解析 note 精选池表行 → [(代码, 证据数, 评分)]。

    仅识别含「N 证，X 分」的 | 表格行（表头/分隔行无此形状自然跳过）；
    格式漂移行不报错跳过（骨架规则是主约束，本函数是保险丝）。
    """
    rows = []
    for _, line in _scan_lines(note_path, keep_table=True):
        s = line.strip()
        if not s.startswith('|'):
            continue
        m = _RE_POOL_EVID.search(s)
        if not m:
            continue
        code_m = re.search(r'\d{6}', s)
        try:
            rows.append((code_m.group(0) if code_m else '',
                         int(m.group(1)), float(m.group(2))))
        except ValueError:
            continue
    return rows


def _csv_codes(csv_path: Path) -> Set[str]:
    """CSV 文本内全部 6 位数字串集合（代码存在性核对用；读失败返回空集=跳过）。"""
    try:
        text = csv_path.read_text(encoding='utf-8-sig')
    except OSError:
        return set()
    return set(re.findall(r'\d{6}', text))


def check_pool_order(note_path: Path, csv_path: Path) -> List[Dict[str, str]]:
    """精选池行序自洽校验（T5.2，WARN 级）：证据数降序 + 同证据数档内评分降序；
    另核池内代码存在于当日 CSV（骨架「照抄报告序/只挑选不新增」的机器锚）。
    行数 <2 或解析不出行 → 不校验（静默跳过，WARN 保险丝不阻断）。"""
    rows = _note_pool_rows(note_path)
    hits: List[Dict[str, str]] = []
    if len(rows) >= 2:
        for prev, cur in zip(rows, rows[1:]):
            if cur[1] > prev[1] or (cur[1] == prev[1] and cur[2] > prev[2]):
                hits.append({'line': '-', 'value': '池序逆行',
                             'num': f'{cur[0] or "?"}({cur[1]}证{cur[2]}分)',
                             'context': f'前一行 {prev[0] or "?"}'
                                        f'({prev[1]}证{prev[2]}分)——'
                                        '骨架：证据数降序→档内评分降序'})
                break
    if rows:
        codes = _csv_codes(csv_path)
        for code, *_ in rows:
            if code and codes and code not in codes:
                hits.append({'line': '-', 'value': '池内代码不在 CSV',
                             'num': code,
                             'context': '精选池应为当日 CSV 子集（agent 只挑选不新增）'})
                break
    return hits


# ---------------------------------------------------------------- 结构规则库（P2-5）

def _cfg_sentiment_thresholds(cfg_text: str) -> Dict[str, Optional[str]]:
    """从 config.yaml 文本提取 sentiment_rules 段阈值（正则，不引 PyYAML）。"""
    m = re.search(r'sentiment_rules:\s*\n(.*?)(?=\n\S|\n\s*#|\Z)', cfg_text, re.S)
    seg = m.group(1) if m else ''

    def _f(key: str) -> Optional[str]:
        mm = re.search(rf'{key}:\s*([\d.]+)', seg)
        return mm.group(1) if mm else None

    return {'max_streak_gte': _f('max_streak_gte'),
            'premium_mean_gte': _f('premium_mean_gte'),
            'max_streak_lte': _f('max_streak_lte'),
            'burst_rate_gte': _f('burst_rate_gte'),
            'premium_mean_lte': _f('premium_mean_lte'),
            'breadth_ratio_gte': _f('breadth_ratio_gte')}


def _expect_thresholds(th: Dict[str, Optional[str]]) -> List[str]:
    """判据表渲染时应出现的阈值子串（与 digest.py criteria expr 同构）。"""
    exp: List[str] = []
    if th.get('max_streak_gte'):
        exp.append(f"高度≥{th['max_streak_gte']}")
    if th.get('premium_mean_gte'):
        exp.append(f"溢价≥{th['premium_mean_gte']}%")
    if th.get('max_streak_lte'):
        exp.append(f"高度≤{th['max_streak_lte']}")
    if th.get('burst_rate_gte'):
        exp.append(f"炸板率≥{th['burst_rate_gte']}")
    if th.get('premium_mean_lte') is not None:
        exp.append(f"溢价均值≤{th['premium_mean_lte']}%")
    if th.get('breadth_ratio_gte'):
        exp.append(f"涨跌家数比≥{th['breadth_ratio_gte']}")
    return exp


#: 规则表：(名称, 作用目标, 检查函数(text, cfg_text) -> (ok, 说明))
#: T1 速览框提取契约：主干行/基准倾向行（digest._extract_tldr_fields 同式）
_RE_TLDR_TRUNK = re.compile(r'\*\*主干：.+\*\*')
STRUCTURAL_RULES = [
    ('glossary', 'digest',
     lambda t, c: ('<details>' in t and '口径字典' in t,
                   'digest.html 缺折叠式口径字典（P1-2）')),
    ('chan_cross', 'note',
     lambda t, c: ('缠论候选×存疑' in t,
                   'digest_note 缺"缠论候选×存疑"交叉披露行（P0-7 报告层）')),
    ('chan_criteria', 'digest',
     lambda t, c: (re.search(r'&lt;\s*1\s*即背驰|<\s*1\s*即背驰', t) is not None,
                   'digest.html 缺背驰入选口径注记（|Σhist₂|/|Σhist₁| < 1 即背驰，P1-8）')),
    ('criteria_cfg', 'digest+config',
     lambda t, c: _check_criteria_cfg(t, c)),
    ('pool_style', 'note',
     lambda t, c: ('观察池（非推荐）' in t,
                   'digest_note 操作池表头须含"观察池（非推荐）"（P2-1）')),
    ('note_trunk', 'note',
     lambda t, c: (any(_RE_TLDR_TRUNK.match(l.strip())
                       for l in (t or '').splitlines()),
                   'digest_note 缺「**主干：…**」独立行（页首速览框提取契约，T1）')),
    ('note_bias', 'note',
     lambda t, c: (any('**基准倾向**：' in l for l in (t or '').splitlines()),
                   'digest_note 缺「**基准倾向**：」行（页首速览框提取契约，T1）')),
]


def _check_criteria_cfg(digest_text: str, cfg_text: Optional[str]) -> Tuple[bool, str]:
    """判据表阈值与 config sentiment_rules 逐值一致（P1-1 验收的程序化锚）。"""
    if not cfg_text:
        return False, 'criteria_cfg 规则需要 --config <config.yaml>'
    th = _cfg_sentiment_thresholds(cfg_text)
    missing = [e for e in _expect_thresholds(th) if e not in digest_text]
    if missing:
        return False, f'digest.html 判据表阈值与 config 不一致，缺: {missing}'
    return True, '判据表阈值与 config 逐值一致'


def run_structural_rules(note_path: Optional[Path], digest_path: Optional[Path],
                         config_path: Optional[Path]) -> List[str]:
    """执行结构规则库，返回失败说明清单（空 = 全绿）。

    **--digest 提供时整体启用**（P2-5 交付门以完整入参调用）；未提供 --digest
    时全部跳过（兼容旧调用与 B7 库函数复用）。digest 规则需要 --digest；
    note 规则同批启用；criteria_cfg 另需 --config（缺 config 视为失败，
    由 SKILL.md 流程纪律保证完整入参）。
    """
    if digest_path is None:
        return []
    note_text = note_path.read_text(encoding='utf-8') if note_path else None
    digest_text = digest_path.read_text(encoding='utf-8')
    cfg_text = config_path.read_text(encoding='utf-8') if config_path else None
    failures: List[str] = []
    for name, target, fn in STRUCTURAL_RULES:
        if target == 'digest+config' and cfg_text is None:
            failures.append('[criteria_cfg] 需要 --config <config.yaml> '
                            '（判据表阈值与 config 逐值比对）')
            continue
        ok, why = fn(digest_text if 'digest' in target else note_text, cfg_text)
        if not ok:
            failures.append(f'[{name}] {why}')
    return failures


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='digest 数字白名单 + 结构校验')
    ap.add_argument('--note', required=True, help='digest_note.md 路径')
    ap.add_argument('--csv', required=True, help='当日 daily_review CSV 路径')
    ap.add_argument('--meta', default=None, help='报告 meta.json 路径（可选）')
    ap.add_argument('--digest', default=None,
                    help='digest.html 路径（提供则执行 P2-5 结构规则）')
    ap.add_argument('--config', default=None,
                    help='config.yaml 路径（criteria_cfg 规则需要）')
    ap.add_argument('--strict', action='store_true',
                    help='存在 WARN 时退出码 1（默认 0）')
    args = ap.parse_args(argv)

    note_path = Path(args.note)
    csv_path = Path(args.csv)
    for p, name in ((note_path, 'note'), (csv_path, 'csv')):
        if not p.exists():
            print(f'[error] {name} 不存在: {p}', file=sys.stderr)
            return 2

    # ---- 结构规则（P2-5，硬失败，先于数字白名单） ----
    digest_path = Path(args.digest) if args.digest else None
    config_path = Path(args.config) if args.config else None
    if digest_path and not digest_path.exists():
        print(f'[error] digest 不存在: {digest_path}', file=sys.stderr)
        return 2
    if config_path and not config_path.exists():
        print(f'[error] config 不存在: {config_path}', file=sys.stderr)
        return 2
    failures = run_structural_rules(note_path, digest_path, config_path)
    if failures:
        print(f'✗ 结构校验失败 {len(failures)} 条（P2-5 规则库，硬失败阻断发布）：')
        for f in failures:
            print(f'  {f}')
        print('\n处置：按规则补齐对应内容后重跑；不得以 --strict 规避结构失败。')
        return 1
    if digest_path:
        print(f'✓ 结构校验通过（{len(STRUCTURAL_RULES)} 条规则，'
              f'digest={digest_path.name}, config={"有" if config_path else "未提供"}）')

    whitelist = _csv_whitelist(csv_path)
    if args.meta:
        whitelist |= _meta_whitelist(Path(args.meta))
    warns = check_note(note_path, whitelist)
    leaks = check_impl_leak(note_path)      # P2-8 语汇边界（W6，WARN 级）
    # T5 骨架兜底（WARN 级保险丝）：工单号/嵌套括注/免责单出处/池表行序自洽
    t5_hits = (check_ticket_leak(note_path) + check_nested_paren(note_path)
               + check_disclaimer(note_path))
    pool_hits = check_pool_order(note_path, csv_path)

    if not warns and not leaks and not t5_hits and not pool_hits:
        print(f'✓ 数字白名单校验通过：{note_path.name} 全部数字已接地'
              f'（白名单 {len(whitelist)} 个值）；语汇边界与骨架扫描通过（P2-8/T5）')
        return 0
    if warns:
        print(f'⚠ WARN {len(warns)} 个数字未接地（不在 CSV 列值/meta 已知值/'
              f'已标注推断），必须逐个人工核对并在输出点名：')
        for w in warns:
            print(f"  L{w['line']}: 「{w['num']}」 …{w['context']}…")
    if leaks:
        print(f'⚠ WARN {len(leaks)} 处实现细节疑似外漏正文（P2-8 语汇边界：'
              'config 键名/时钟时间戳不入用户面正文；表格行与 > 注记行豁免）：')
        for lk in leaks:
            print(f"  L{lk['line']}: 「{lk['num']}」 …{lk['context']}…")
    if t5_hits:
        print(f'⚠ WARN {len(t5_hits)} 处骨架语汇违例（T5：工单号/嵌套括注/'
              '免责重复——新 note 起目标恒 0，存量 note 属预期）：')
        for lk in t5_hits:
            print(f"  L{lk['line']}: 「{lk['num']}」 …{lk['context']}…")
    if pool_hits:
        print(f'⚠ WARN {len(pool_hits)} 处精选池行序/子集违例（T5.2 表内自洽）：')
        for lk in pool_hits:
            print(f"  L{lk['line']}: 「{lk['num']}」 …{lk['context']}…")
    print('\n纪律：数字逐条确认是「合法推断」则补（推断）标注，无法确认则改写或删除；'
          '实现细节移入质量点名表说明列或删除；骨架违例按 SKILL.md T5 修订后重写。')
    return 1 if args.strict else 0


if __name__ == '__main__':
    sys.exit(main())
