"""技术分析链 CLI 命令组（方案 §3.2 F9 / §3.6）。

子命令：
- ``analyze run``          执行分析链（**必须显式指定 --chain 或 --modules**，无默认链）
- ``analyze list-chains``  列出可用分析链
- ``analyze list-modules`` 列出可用分析模块（含 README.MD 登记校验告警）
- ``analyze progress``     读取进度文件并格式化输出（供 agent 定时汇报）
- ``analyze rankic``       因子 RankIC 评估（P1-6：R1-R4 候选信号 × 前瞻收益）
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from ..analysis import (
    ChainEngine,
    Lookback,
    build_output_dir,
    format_progress,
    list_chains,
    load_chain,
    load_registry,
    options_from_config,
    parse_modules_option,
    read_progress,
    registration_warnings,
)
from ..analysis.chain_config import ChainConfig


@click.group()
def analyze():
    """股票技术分析链（只读，不触发数据同步）"""


# ---------------------------------------------------------------- analyze run
@analyze.command("run")
@click.option("--chain", "chain_name", help="链名（chains/<链名>.yaml）")
@click.option("--modules", "modules_option", help="临时链：模块英文名列表，逗号分隔（免落文件）")
@click.option("--as-of", "as_of", help="锚定日 yyyyMMdd，默认取库内最新交易日")
@click.option("--lookback", "lookback", default=None,
              help="分析窗口覆盖（默认用链 YAML 的 lookback，通常为 1y）："
                   "1y/2y（自然年）、6m/3m（自然月）、250d（交易日）、0/all（全历史）")
@click.option("--missing-day-mode", type=click.Choice(["gap", "all", "off"]), default=None,
              help="缺行分流模式：gap=仅孤立单日缺行计入存疑（默认）；all=所有缺行；off=关闭")
@click.option("--output-dir", "output_dir", help="CSV/进度文件输出目录（覆盖配置）")
@click.option("--quiet-progress", is_flag=True, default=False, help="不输出 stdout 进度行")
@click.pass_context
def run_chain(ctx, chain_name, modules_option, as_of, lookback, missing_day_mode,
              output_dir, quiet_progress):
    """执行分析链，输出 CSV（含进度文件与元信息）"""
    if not chain_name and not modules_option:
        raise click.UsageError(
            "必须显式指定 --chain <链名> 或 --modules <模块列表>（本系统无默认链）。"
            "可用链见 analyze list-chains，可用模块见 analyze list-modules。"
        )
    if chain_name and modules_option:
        raise click.UsageError("--chain 与 --modules 不能同时指定")

    config_manager = ctx.obj["config_manager"]

    if modules_option:
        chain = ChainConfig(chain="adhoc", description="CLI 临时链（--modules）",
                            modules=parse_modules_option(modules_option))
        # 临时链同样要做模块/参数校验
        for spec in chain.modules:
            from ..analysis import build as build_module

            build_module(spec.name_en, spec.params)
    else:
        chain = load_chain(chain_name)

    options = options_from_config(config_manager)
    options.as_of = as_of
    if lookback is not None:
        try:
            options.lookback = Lookback.parse(lookback)
        except ValueError as exc:
            raise click.BadParameter(str(exc), param_hint="--lookback")
    if missing_day_mode is not None:
        options.missing_day_mode = missing_day_mode
    if output_dir:
        options.output_dir = Path(output_dir).expanduser()
    if quiet_progress:
        options.progress_interval_stocks = 10 ** 9
        options.progress_interval_percent = 1000.0

    effective = options.lookback or chain.lookback
    click.echo(f"分析链: {chain.chain}")
    if chain.description:
        click.echo(f"说明: {chain.description}")
    click.echo(f"模块: {', '.join(s.name_en for s in chain.modules)}")
    click.echo(f"分析窗口: {effective}"
               + ("" if options.lookback is None else "（CLI 覆盖）"))
    click.echo("开始加载数据并执行（长任务，可用 analyze progress 查看进度）...")

    engine = ChainEngine(config_manager, options)
    try:
        result = engine.run(chain)
    except ValueError as exc:
        click.echo(f"\n✗ 执行失败: {exc}")
        sys.exit(1)
    except Exception as exc:  # 未预期异常也保持退出码非 0
        click.echo(f"\n✗ 执行异常: {exc!r}")
        sys.exit(1)

    click.echo("\n✓ 分析链执行完成")
    click.echo(f"  - 锚定日: {result.as_of}")
    scope = f"  - 分析窗口: {result.lookback}（起点 {result.window_start or '全历史'}，" \
            f"窗口内 {result.loaded_trading_days} 个交易日"
    if result.window_expanded:
        scope += "，因模块回看需求扩展"
    click.echo(scope + "）")
    click.echo(f"  - 股票数: {result.stock_count}")
    click.echo(f"  - 存疑股票: {result.suspect_count}")
    click.echo(f"  - 疑似停牌(不计入存疑): {result.suspended_count}")
    for name, hits in result.hit_summary.items():
        if isinstance(hits, dict):
            # 分桶/样本数展示（方案 §2.5）：如「大涨大跌: 大涨 147 / 大跌 130」
            parts = " / ".join(f"{k} {v}" for k, v in hits.items())
            ins = result.insufficient_summary.get(name, 0)
            suffix = f"（数据不足 {ins}）" if ins else ""
            click.echo(f"  - {name}: {parts}{suffix}")
        else:
            click.echo(f"  - {name} 命中: {hits}")
    click.echo(f"  - 耗时: {result.duration_seconds:.1f} 秒")
    click.echo(f"  - CSV: {result.csv_path}")
    click.echo(f"  - 进度: {result.progress_path}")
    click.echo(f"  - 元信息: {result.meta_path}")


# --------------------------------------------------------- analyze list-chains
@analyze.command("list-chains")
def list_chains_cmd():
    """列出可用分析链"""
    chains = list_chains()
    if not chains:
        click.echo("（chains/ 下暂无链配置）")
        return
    click.echo(f"可用分析链（{len(chains)} 条）：")
    for chain in chains:
        click.echo(f"\n  链名: {chain.chain}")
        if chain.description:
            click.echo(f"    说明: {chain.description}")
        click.echo(f"    数据域: {chain.domain}"
                   + ("（A 股）" if chain.domain == "a_share" else "（外围·市场自身日历）"))
        click.echo(f"    分析窗口: {chain.lookback}")
        click.echo(f"    模块: {', '.join(s.name_en + (f' {s.params}' if s.params else '') for s in chain.modules)}")
        click.echo(f"    文件: {chain.source_path}")
    if len(chains) == 1:
        click.echo("\n提示: 本系统无默认链，执行时需显式指定 --chain。")


# -------------------------------------------------------- analyze list-modules
@analyze.command("list-modules")
def list_modules_cmd():
    """列出可用分析模块（含 README.MD 登记校验）"""
    registry = load_registry()
    if not registry:
        click.echo("（未发现任何分析模块）")
        return
    click.echo(f"可用分析模块（{len(registry)} 个）：")
    for name_en, cls in sorted(registry.items()):
        click.echo(f"\n  英文名: {name_en}")
        click.echo(f"    中文名: {cls.name}")
        click.echo(f"    返回值: 见 src/analysis/README.MD")
        click.echo(f"    回看天数: {cls.max_lookback or '全历史'}")
        if cls.default_params:
            click.echo(f"    参数默认值: {json.dumps(cls.default_params, ensure_ascii=False)}")
        click.echo(f"    描述: {cls.description}")

    warnings = registration_warnings()
    if warnings:
        click.echo("\n⚠ README.MD 登记校验告警：")
        for item in warnings:
            click.echo(f"  - {item}")


# ----------------------------------------------------------- analyze progress
@analyze.command("progress")
@click.option("--chain", "chain_name", required=True, help="链名")
@click.option("--as-of", "as_of", help="锚定日 yyyyMMdd，缺省取该链最新的进度文件")
@click.option("--output-dir", "output_dir", help="输出目录（覆盖配置）")
@click.option("--json", "as_json", is_flag=True, default=False, help="输出原始 JSON")
@click.pass_context
def progress_cmd(ctx, chain_name, as_of, output_dir, as_json):
    """查看分析链运行进度"""
    config_manager = ctx.obj["config_manager"]
    out_dir = Path(output_dir).expanduser() if output_dir else build_output_dir(config_manager)

    if as_of:
        value = str(as_of).replace("-", "")
        path = out_dir / f"{chain_name}_{value}.progress.json"
    else:
        candidates = sorted(
            out_dir.glob(f"{chain_name}_*.progress.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            click.echo(f"未找到链 {chain_name} 的进度文件（目录: {out_dir}）")
            sys.exit(1)
        path = candidates[0]

    payload = read_progress(path)
    if payload is None:
        click.echo(f"进度文件不存在或不可解析: {path}")
        sys.exit(1)

    if as_json:
        click.echo(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        click.echo(format_progress(payload))
        click.echo(f"（进度文件: {path}）")


# --------------------------------------------------------------- analyze rankic
@analyze.command("rankic")
@click.option("--signal", "signal_option", default="r1,r2,r3,r4",
              help="信号列表，逗号分隔（r1/r2/r3/r4；连板数为自检信号恒算）")
@click.option("--horizon", "horizon_option", default="1,5,20",
              help="前瞻 horizon（交易日），逗号分隔，如 1,5,20")
@click.option("--start", "start", default=None, help="样本开始日 yyyyMMdd（默认 20240101）")
@click.option("--end", "end", default=None, help="样本结束日 yyyyMMdd（默认 20260930）")
@click.option("--min-names", type=int, default=50, help="每日有效样本下限（默认 50）")
@click.option("--no-prefix-check", is_flag=True, default=False,
              help="跳过后缀不变性自检（全量 vs 截断重算，默认执行）")
@click.option("--output-dir", "output_dir", help="输出目录（默认 config review.output_dir）")
@click.pass_context
def rankic_cmd(ctx, signal_option, horizon_option, start, end, min_names,
               no_prefix_check, output_dir):
    """因子 RankIC 评估（P1-6）：R1-R4 候选信号 × T+h 前瞻收益的逐截面 Spearman。

    信号源为 candidate_rules 谓词 + cross_section 向量化面板库内重算（不逐日跑链）；
    输出 IC 汇总 CSV + 逐日 IC 序列 CSV + meta（含前缀不变性自检）。
    """
    import pandas as pd

    from ..analysis.data_reader import DataReader
    from ..review import factor_eval

    config_manager = ctx.obj["config_manager"]
    signals = [s.strip() for s in signal_option.split(',') if s.strip()]
    horizons = [int(h) for h in horizon_option.split(',') if h.strip()]
    valid = {'r1', 'r2', 'r3', 'r4'}
    bad = [s for s in signals if s not in valid]
    if bad:
        raise click.UsageError(f"未知信号: {bad}（可用: r1,r2,r3,r4）")
    if not horizons or any(h <= 0 for h in horizons):
        raise click.UsageError("--horizon 须为正整数列表，如 1,5,20")

    start = start or factor_eval.DEFAULT_START
    end = end or factor_eval.DEFAULT_END
    review_cfg = (config_manager.load_config().get('review') or {})
    # 相对路径落到仓库根（与 generator._repo_root 同口径，勿用 cwd——CLI 常在
    # backend/ 下执行，cwd 解析会把报告写进 backend/doc/reports）
    from ..sync.report_writer import resolve_repo_root
    root = resolve_repo_root()
    out_dir = Path(output_dir) if output_dir else Path(
        review_cfg.get('output_dir') or 'doc/reports')
    if not out_dir.is_absolute():
        out_dir = root / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    reader = DataReader(config_manager)
    try:
        click.echo(f"RankIC 评估：信号 {signals} × horizon {horizons}，"
                   f"样本期 {start}~{end}（库内重算，约数分钟）...")
        result = factor_eval.evaluate(signals, horizons, start, end, reader,
                                      config_manager, min_names=min_names,
                                      prefix_check=not no_prefix_check)
    finally:
        reader.close()
    if not result.get('ok'):
        click.echo(f"\n✗ 评估失败: {result.get('reason')}")
        sys.exit(1)

    rows = []
    ic_series_frames = []
    for key, r in result['rankic'].items():
        if 'error' in r:
            click.echo(f"  - {key}: {r['error']}")
            continue
        rows.append({'signal_horizon': key, 'rank_ic': r['rank_ic'],
                     'ic_mean': r['ic_mean'], 'ic_std': r['ic_std'],
                     'icir': r['icir'], 'n_days': r['n_days'],
                     'n_names': r['n_names']})
        series = result['per_day_ic'].get(key)
        if series is not None and len(series):
            ic_series_frames.append(pd.DataFrame(
                {'signal_horizon': key, 'trade_date': series.index,
                 'ic': series.values}))
    csv_path = out_dir / f'rankic_{start}_{end}.csv'
    pd.DataFrame(rows).to_csv(csv_path, index=False, encoding='utf-8-sig')
    ic_csv = out_dir / f'rankic_{start}_{end}_ic_series.csv'
    if ic_series_frames:
        pd.concat(ic_series_frames).to_csv(ic_csv, index=False, encoding='utf-8-sig')

    prefix = result.get('prefix_invariance') or {}
    meta = {'start': start, 'end': end, 'signals': signals, 'horizons': horizons,
            'min_names': min_names,
            'prefix_invariance_ok': result.get('prefix_invariance_ok'),
            'prefix_invariance': {k: {kk: vv for kk, vv in v.items()}
                                  for k, v in prefix.items()},
            'factor_eval_version': '1.0'}
    meta_path = out_dir / f'rankic_{start}_{end}.meta.json'
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                         encoding='utf-8')

    click.echo("\n✓ RankIC 评估完成")
    for r in rows:
        click.echo(f"  - {r['signal_horizon']}: IC mean {r['ic_mean']} "
                   f"(std {r['ic_std']}, ICIR {r['icir']}, {r['n_days']} 日)")
    for sig, cov in (result.get('coverage') or {}).items():
        gap = cov['total_days'] - cov['computable_days']
        if gap > 0:
            click.echo(f"  - ⚠ {sig} 数据覆盖: {cov['computable_days']}/"
                       f"{cov['total_days']} 日可算（缺口日已出局，见日志）")
    if result.get('prefix_invariance_ok') is not None:
        ok = result['prefix_invariance_ok']
        click.echo(f"  - 前缀不变性自检: {'✓ 通过' if ok else '✗ 未通过'}"
                   f"（{len(prefix)} 个信号）")
    click.echo(f"  - CSV: {csv_path}")
    if ic_series_frames:
        click.echo(f"  - IC 序列: {ic_csv}")
    click.echo(f"  - 元信息: {meta_path}")


# ------------------------------------------------------------ 单独调试入口
def main():  # pragma: no cover
    analyze()


if __name__ == "__main__":  # pragma: no cover
    main()
