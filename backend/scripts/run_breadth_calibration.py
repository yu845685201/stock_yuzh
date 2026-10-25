"""涨跌家数比（高潮第二证据 breadth_ratio_gte）校准数据包生成（V4.0-A3/X4）。

只读库；产物 data/calibration/breadth_calibration_<ts>.md（不入库，同
chan_validation 先例）。报告不预设立场——三态结论选项（调值/升格/维持）
由用户基于数据决策；本脚本只把「决策所需信息」摆齐。

用法：backend/ 下  python scripts/run_breadth_calibration.py
"""

import sys
from datetime import datetime
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pandas as pd  # noqa: E402

from src.analysis import calibration_stats as cals  # noqa: E402
from src.config.config_manager import ConfigManager  # noqa: E402


def _load(reader) -> tuple:
    m_rows = reader._query(
        "SELECT trade_date, up_count, down_count, limit_up_count, max_streak, "
        "premium_mean, burst_rate FROM sentiment_metrics_daily "
        "WHERE up_count IS NOT NULL AND down_count IS NOT NULL "
        "ORDER BY trade_date")
    sh_rows = reader._query(
        "SELECT trade_date, open, preclose, close FROM trade_index_info "
        "WHERE index_code = '000001' AND open IS NOT NULL "
        "AND preclose IS NOT NULL ORDER BY trade_date")
    m = pd.DataFrame(m_rows, columns=['trade_date', 'up_count', 'down_count',
                                      'limit_up_count', 'max_streak',
                                      'premium_mean', 'burst_rate'])
    sh = pd.DataFrame(sh_rows, columns=['trade_date', 'open', 'preclose', 'close'])
    return m, sh


def _md_row(cells) -> str:
    return '| ' + ' | '.join(str(c) for c in cells) + ' |'


def render(m: pd.DataFrame, sh: pd.DataFrame) -> str:
    br = cals.breadth_series(m)
    dist = cals.breadth_distribution(br)
    cond = cals.conditional_next_day(m, sh)
    inc = cals.incremental_power(m, sh)
    opts = cals.threshold_options(m, sh)

    L: list = []
    L.append('# 高潮第二证据（涨跌家数比）阈值校准数据包\n')
    L.append(f'- 生成：{datetime.now().strftime("%Y-%m-%d %H:%M")}；'
             f'来源 sentiment_metrics_daily（up/down 全非空样本）+ '
             f'trade_index_info 000001（次日上证）。')
    L.append('- 口径：breadth = up_count/down_count；down=0 → inf 档（普涨无下跌'
             '的极端宽度，计入 ≥ 阈值档）；分位为有限值分位。次日结果按 '
             'trade_date 对齐，缺日剔除并计数（不硬造）。')
    L.append('- 定位：本报告只生产决策证据；阈值调值/升格定位判据由用户决策。\n')

    L.append('## 一、分布\n')
    L.append(_md_row(['样本天数', 'inf 档天数', 'P50', 'P75', 'P90', 'P95', 'P99']))
    L.append(_md_row(['---'] * 7))
    q = dist['quantiles']
    L.append(_md_row([dist['sample_days'], dist['inf_days'], q['P50'], q['P75'],
                      q['P90'], q['P95'], q['P99']]) + '\n')
    L.append(_md_row(['阈值', '命中天数', '占比%']))
    L.append(_md_row(['---'] * 3))
    for k, v in dist['above'].items():
        L.append(_md_row([k, v['days'], v['pct']]))
    L.append('')

    L.append('## 二、分桶 × 次日结果\n')
    L.append(f'（样本 {cond["rows_count"]} 日；缺次日剔除 {cond["dropped_no_next"]}；'
             f'上证缺日剔除 {cond["dropped_no_sh"]}）\n')
    L.append(_md_row(['分桶', 'n', '次日溢价均值%', '涨停家数变化中位', '上证开盘gap中位%',
                      '上证收盘涨幅中位%']))
    L.append(_md_row(['---'] * 6))
    for b in cond['buckets']:
        L.append(_md_row([b['bucket'], b['n'],
                          b['next_premium_mean'] if b['next_premium_mean'] is not None else '—',
                          b.get('next_lu_chg_median') if b.get('next_lu_chg_median') is not None else '—',
                          b['sh_open_gap_median'], b['sh_close_chg_median']]))
    L.append('')

    L.append('## 三、条件增量（第二证据的存在意义）\n')
    L.append(f"core（高潮主判据：高度≥5 且 溢价≥4）命中 {inc['core_days']} 日。\n")
    L.append('**(a) core 命中日内：宽度 ≥2 vs <2 的次日差异**\n')
    L.append(_md_row(['组', 'n', '次日溢价均值%', '上证开盘gap中位%', '上证收盘涨幅中位%']))
    L.append(_md_row(['---'] * 5))
    for k, v in inc['in_core'].items():
        L.append(_md_row([k, v['n'], v['next_premium_mean'] or '—',
                          v['sh_open_gap_median'] or '—',
                          v['sh_close_chg_median'] or '—']))
    L.append('\n**(b) core 未命中日：宽度越档 vs 基线（极端宽度的单独预告力）**\n')
    L.append(_md_row(['组', 'n', '次日溢价均值%', '上证开盘gap中位%', '上证收盘涨幅中位%']))
    L.append(_md_row(['---'] * 5))
    for k, v in inc['out_core'].items():
        L.append(_md_row([k, v['n'], v['next_premium_mean'] or '—',
                          v['sh_open_gap_median'] or '—',
                          v['sh_close_chg_median'] or '—']))
    L.append('')

    L.append('## 四、阈值选项表\n')
    L.append(_md_row(['阈值', '命中天数', '占比%', '次日上证收盘涨幅中位%',
                      '次日溢价均值%', '与 core 交叠%']))
    L.append(_md_row(['---'] * 6))
    for o in opts:
        L.append(_md_row([o['threshold'], o['days'], o['pct'],
                          o['next_sh_close_median'] if o['next_sh_close_median'] is not None else '—',
                          o['next_premium_mean'] if o['next_premium_mean'] is not None else '—',
                          o['overlap_core_pct'] if o['overlap_core_pct'] is not None else '—']))
    L.append('')

    L.append('## 五、结论选项（三态，由用户决策）\n')
    L.append('1. **调阈值、维持参考判据**——若某阈值档的次日区分度显著优于 2.0 '
             '且样本充分；改动 = config `breadth_ratio_gte` 单键。')
    L.append('2. **升格定位判据**（role reference→position）——若宽度对次日有'
             '独立增量（§三 (b) 路径）；⚠ 触碰「不动情绪定位规则与阈值」红线，'
             '需单独说明并登记 verifier 核验口径变更。')
    L.append('3. **维持 2.0**——若 §二/§三 显示宽度无稳定次日增量。\n')
    L.append('> 决策所需信息已齐平（分布/判别力/选项表）；本报告不预设立场。')
    return '\n'.join(L) + '\n'


def main() -> None:
    import src.review.aggregator as agg_mod
    with agg_mod.DataReader(ConfigManager()) as reader:
        m, sh = _load(reader)
    if m.empty or sh.empty:
        print('✗ 数据不足（sentiment_metrics_daily 或 trade_index_info 为空），不产报告')
        sys.exit(2)
    report = render(m, sh)
    out_dir = BACKEND.parent / 'data' / 'calibration'
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M')
    path = out_dir / f'breadth_calibration_{ts}.md'
    path.write_text(report, encoding='utf-8')
    print(f'✓ 校准报告已生成: {path}')


if __name__ == '__main__':
    main()
