"""A50 触发准入回测报告生成（V4.0-A4/X3）。

只读库；产物 data/calibration/a50_trigger_calibration_<ts>.md（不入库）。
A5 准入门三门读数口径固定（calibration_stats.a50_gate_reads）：① 周一子样本
同向率 ≥60%；② 周一子样本越 ±1.5% 频度 ≥10%；③ 前向案例人工登记。
报告不预设立场——三门读数如实呈现，A5 与否由用户决策。

用法：backend/ 下  python scripts/run_a50_trigger_calibration.py
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pandas as pd  # noqa: E402

from src.analysis import calibration_stats as cals  # noqa: E402
from src.config.config_manager import ConfigManager  # noqa: E402

#: 对照锚（A50 相对增量的来源；恒指/纳指为现行 P1/P3 触发锚）
_ANCHORS = (('富时A50期货', 'CHA50CFD'), ('恒生指数', 'hkHSI'), ('纳斯达克', 'usIXIC'))


def _load(reader) -> tuple:
    sh_rows = reader._query(
        "SELECT trade_date, open, preclose FROM trade_index_info "
        "WHERE index_code = '000001' AND open IS NOT NULL "
        "AND preclose IS NOT NULL ORDER BY trade_date")
    sh = pd.DataFrame(sh_rows, columns=['trade_date', 'open', 'preclose'])
    anchors = {}
    for name, code in _ANCHORS:
        rows = reader._query(
            "SELECT trade_date, close FROM overseas_kline_day "
            "WHERE market_code = %s AND close IS NOT NULL ORDER BY trade_date",
            (code,))
        anchors[name] = pd.DataFrame(rows, columns=['trade_date', 'close'])
    return sh, anchors


def _md_row(cells) -> str:
    return '| ' + ' | '.join(str(c) for c in cells) + ' |'


def _monday(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df[df['trade_date'].map(lambda d: pd.Timestamp(d).weekday() == 0)]


def render(sh: pd.DataFrame, anchors: Dict[str, pd.DataFrame]) -> str:
    L: list = []
    L.append('# A50 触发准入回测（周末缝度量工具，A5 准入证据）\n')
    L.append(f'- 生成：{datetime.now().strftime("%Y-%m-%d %H:%M")}；来源 '
             f'overseas_kline_day（CHA50CFD/hkHSI/usIXIC）+ trade_index_info 000001。')
    L.append('- 对齐口径：逐 A 股交易日 D 取锚「最后一根 trade_date ≤ D−1」的收盘'
             '为盘前可得值（D 自身 bar 于 D+1 凌晨或 D 日盘后收，对 D 开盘不可用）；'
             '前向填充覆盖锚休市（周一读数含周五夜盘 → 周末缺口进入周一）。')
    L.append('- **乐观界声明**：日K 为「全天收」代理，D−1 bar 含 D−1 日盘后信息，'
             '同向率是乐观界；盘前实时读数的可得性历史不可回测，由前向观察补。\n')

    cons_all, cons_mon, avail = {}, {}, {}
    for name, _ in _ANCHORS:
        ov = anchors[name]
        merged = cals.align_overseas(sh, ov)
        cons_all[name] = cals.consistency(merged)
        cons_mon[name] = cals.consistency(_monday(merged))
        avail[name] = cals.trigger_availability(merged, thresholds=(1.0, 1.5, 2.0),
                                                weekday=0)
        if name == '富时A50期货':
            merged_a50 = merged

    L.append('## 一、同向率（sign(锚日变动) == sign(上证开盘gap)）\n')
    L.append(_md_row(['锚', '样本 n', '全样本同向率%', '零值剔除', '周一子样本 n',
                      '周一同向率%']))
    L.append(_md_row(['---'] * 6))
    for name, _ in _ANCHORS:
        a, m = cons_all[name], cons_mon[name]
        L.append(_md_row([name, a['n'], a['agree_rate'] if a['agree_rate'] is not None else '—',
                          a['zero_excluded'], m['n'],
                          m['agree_rate'] if m['agree_rate'] is not None else '—']))
    L.append('')

    L.append('## 一(b)、A50 同向率分年\n')
    if merged_a50.empty:
        L.append('（A50 无有效对齐样本）\n')
    else:
        L.append(_md_row(['年份', 'n', '同向率%']))
        L.append(_md_row(['---'] * 3))
        for year, g in merged_a50.assign(
                year=merged_a50['trade_date'].str[:4]).groupby('year'):
            c = cals.consistency(g)
            L.append(_md_row([year, c['n'],
                              c['agree_rate'] if c['agree_rate'] is not None else '—']))
        L.append('')

    L.append('## 二、触发可用性（周一子样本：周末缝场景）\n')
    L.append(_md_row(['锚', '阈值', '周一锚日数', '越阈天数', '越阈频度%',
                      'P1(≤−thr)天数', 'P1 低开兑现', 'P3(≥+thr)天数', 'P3 高开兑现']))
    L.append(_md_row(['---'] * 9))
    for name, _ in _ANCHORS:
        for o in avail[name]:
            L.append(_md_row([name, o['threshold'], o['anchor_days'],
                              o['cross_days'], o['cross_pct'],
                              o['p1_n'], o['p1_open_down'], o['p3_n'],
                              o['p3_open_up']]))
    L.append('')

    L.append('## 三、对照与增量解读\n')
    rates = {n: cons_mon[n]['agree_rate'] for n, _ in _ANCHORS}
    L.append(f"- 周一子样本同向率并列：A50 {rates['富时A50期货']}% / 恒指 "
             f"{rates['恒生指数']}% / 纳指 {rates['纳斯达克']}%（越阈频度见 §二）。")
    L.append('- A50 的「相对增量」= 在恒指/纳指周末无新收盘时（周一场景恒成立），'
             'A50 是否提供方向信息——即 A50 周一子样本同向率与越阈频度是否'
             '达到门槛；三锚并列值供对照，严格增量需多变量回归（超出本轮口径）。\n')

    a50_bars = len(anchors['富时A50期货'])
    avail_15 = next((o for o in avail['富时A50期货'] if o['threshold'] == 1.5), None)
    gates = cals.a50_gate_reads(cons_mon['富时A50期货'], avail_15, a50_bars)

    L.append('## 四、A5 准入门三门读数\n')
    L.append(_md_row(['门', '口径', '当前读数', '门槛', '判过']))
    L.append(_md_row(['---'] * 5))
    g1, g2, g3 = gates['gate1_same_direction'], gates['gate2_cross_freq'], gates['gate3_forward_cases']
    L.append(_md_row(['① 同向率', '周一子样本', g1['value'], f"≥{g1['threshold']}%",
                      '通过' if g1['pass'] else '未过' if gates['sample_sufficient'] else '样本不足']))
    L.append(_md_row(['② 越阈频度', '周一子样本 ±1.5%', g2['value'], f"≥{g2['threshold']}%",
                      '通过' if g2['pass'] else '未过' if gates['sample_sufficient'] else '样本不足']))
    L.append(_md_row(['③ 前向案例', '≥2 例方向一致（人工登记）', '待积累', '≥2 例', '进行中']))
    L.append('')
    if not gates['sample_sufficient']:
        L.append(f"> 结论：A50 历史不足 {cals.A50_MIN_BARS} 根（现 {a50_bars} 根），"
                 "样本不足不产结论。")
    elif g1['pass'] and g2['pass']:
        L.append('> 结论：回测侧门①②通过——待门③前向案例积累（Phase 1 观察锚，'
                 '观察期至约 2026-11-08）齐备后，可议 A5（A50 入第三锚触发，'
                 '届时单独授权并登记核验口径切换）。')
    else:
        fails = [n for n, g in (('①', g1), ('②', g2)) if not g['pass']]
        L.append(f'> 结论：回测侧门 {"、".join(fails)} 未过（乐观界口径下仍未达），'
                 'A5 无需再议；除非修订触发口径（如分档阈值）后重新回测。')
    return '\n'.join(L) + '\n'


def main() -> None:
    import src.review.aggregator as agg_mod
    with agg_mod.DataReader(ConfigManager()) as reader:
        sh, anchors = _load(reader)
    if sh.empty or anchors['富时A50期货'].empty:
        print('✗ 数据不足（上证或 A50 日K 为空），不产报告')
        sys.exit(2)
    report = render(sh, anchors)
    out_dir = BACKEND.parent / 'data' / 'calibration'
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    path = out_dir / f'a50_trigger_calibration_{ts}.md'
    path.write_text(report, encoding='utf-8')
    print(f'✓ A50 回测报告已生成: {path}')


if __name__ == '__main__':
    main()
