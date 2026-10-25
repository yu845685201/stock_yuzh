"""
命令行主入口
"""

import click
import sys
import os
import logging
from datetime import date, datetime

# Add the backend directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.config import ConfigManager
from src.sync import SyncManager
from src.database import DatabaseConnection
from src.cli.analysis_cli import analyze
from src.cli.common import csv_db_options, kline_range_options, parse_codes, render_sync_result
from src.cli.review_sync_cli import review_commands, review_group


def setup_logging():
    """设置日志配置，确保进度条能够正常显示"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
        force=True  # 强制重新配置，避免重复调用问题
    )


@click.group()
@click.pass_context
def cli(ctx):
    """A股盘后静态分析系统命令行工具"""
    ctx.ensure_object(dict)
    ctx.obj['config_manager'] = ConfigManager()


# 技术分析链命令组（analyze run / list-chains / list-modules / progress）
cli.add_command(analyze)

# 每日复盘数据采集命令组（方案 2.6：编排器 + 独立子命令，均支持 --dry-run）
for _review_cmd in review_commands:
    cli.add_command(_review_cmd)

# 复盘报告生成命令组（复盘报告层方案 §5.5：review generate）
cli.add_command(review_group)


@cli.command()
@click.option('--init-db', is_flag=True, help='是否初始化数据库表')
@click.pass_context
def init(ctx, init_db):
    """初始化系统"""
    click.echo("正在初始化A股盘后静态分析系统...")

    # 初始化数据库
    if init_db:
        click.echo("初始化数据库表...")
        db_conn = DatabaseConnection(ctx.obj['config_manager'])
        try:
            db_conn.initialize_tables()
            click.echo("✓ 数据库表初始化成功")
        except Exception as e:
            click.echo(f"✗ 数据库表初始化失败: {e}")
            sys.exit(1)

    # 检查数据目录
    config_manager = ctx.obj['config_manager']
    data_paths = config_manager.get_data_paths()

    import os
    for path_name, path_value in data_paths.items():
        if not os.path.exists(path_value):
            os.makedirs(path_value, exist_ok=True)
            click.echo(f"✓ 创建目录: {path_value}")

    click.echo("✓ 系统初始化完成")


@cli.command()
@csv_db_options
@click.pass_context
def sync_all(ctx, no_csv, no_db):
    """同步所有数据"""
    click.echo("开始同步所有数据...")

    # 默认都保存，除非明确指定不保存
    save_to_csv = not no_csv
    save_to_db = not no_db

    sync_manager = SyncManager(ctx.obj['config_manager'])
    result = sync_manager.sync_all(save_to_csv=save_to_csv, save_to_db=save_to_db)

    if result['success']:
        click.echo("\n✓ 数据同步完成!")
        click.echo(f"  - 股票数量: {result['stocks_count']}")
        click.echo(f"  - 耗时: {result['duration']:.2f} 秒")
    else:
        click.echo("\n✗ 数据同步失败!")
        for error in result['errors']:
            click.echo(f"  错误: {error}")


@cli.command('sync-stocks')
@click.option('--apply', is_flag=True, default=False,
              help='落库（新股/改名/退市/list_date 回填）；缺省仅产出 diff 报告')
@click.pass_context
def sync_stocks(ctx, apply):
    """股票列表维护（tdx-api /api/codes 快照；baostock 下线换源 20261025）"""
    from src.sync.stock_list_manager import StockListManager

    click.echo("股票列表快照维护（tdx-api /api/codes）...")
    report = StockListManager(ctx.obj['config_manager']).execute(apply=apply)

    click.echo(f"\n✓ 股票列表维护完成（{'已落库' if apply else '仅报告，--apply 落库'}）")
    click.echo(f"  - 快照: {report['snapshot']} 只 | 库内 type='1': {report['db_stocks']} 只")
    click.echo(f"  - 新股: {len(report['new_stocks'])} | 改名: {len(report['renamed'])} | "
               f"退市判定: {len(report['delisted'])} | list_date 回填候选: {len(report['list_date_backfill'])}")
    click.echo(f"  - 报告: {report['report_path']}")
    click.echo(f"  - 耗时: {report['duration']}s")


@cli.command()
@click.option('--start-year', type=int, required=True, help='开始年份 (yyyy)')
@click.option('--end-year', type=int, required=True, help='结束年份 (yyyy)')
@click.pass_context
def sync_trade_calendar(ctx, start_year, end_year):
    """同步交易日历"""
    if start_year > end_year:
        click.echo(f"\n✗ 错误: 开始年份不能大于结束年份: {start_year} > {end_year}")
        return

    click.echo("开始同步交易日历数据...")
    click.echo(f"  - 年份范围: {start_year} ~ {end_year}")

    sync_manager = SyncManager(ctx.obj['config_manager'])
    result = sync_manager.sync_trade_calendar(start_year=start_year, end_year=end_year, save_to_db=True)

    render_sync_result(
        result,
        success_title="✓ 交易日历同步完成!",
        fail_title="✗ 交易日历同步失败!",
        show_duration=True,
        show_report=False
    )


@cli.command()
@csv_db_options
@kline_range_options
@click.pass_context
def sync_kline_1min(ctx, no_csv, no_db, init_mode, start_date, end_date, codes):
    """同步1分钟K线数据"""
    save_to_csv = not no_csv
    save_to_db = not no_db

    if (start_date and not end_date) or (end_date and not start_date):
        click.echo("\n✗ 错误: --start-date和--end-date必须同时指定")
        return

    click.echo("开始同步1分钟K线数据...")
    click.echo(f"  - 保存到CSV: {'是' if save_to_csv else '否'}")
    click.echo(f"  - 保存到数据库: {'是' if save_to_db else '否'}")

    if init_mode:
        click.echo("  - 采集模式: 数据初始化（全量股票全时段）")
    elif start_date and end_date:
        click.echo(f"  - 采集模式: 指定日期范围（{start_date} ~ {end_date}）")
    else:
        click.echo("  - 采集模式: 增量更新（当天）")

    ts_codes = parse_codes(codes)
    if ts_codes:
        click.echo(f"  - 指定股票: {len(ts_codes)}只")

    sync_manager = SyncManager(ctx.obj['config_manager'])
    result = sync_manager.sync_kline_1min(
        init_mode=init_mode,
        start_date=start_date,
        end_date=end_date,
        ts_codes=ts_codes,
        save_to_csv=save_to_csv,
        save_to_db=save_to_db
    )

    render_sync_result(
        result,
        success_title="✓ 1分钟K线同步完成!",
        fail_title="✗ 1分钟K线同步失败!",
        show_duration=True,
        show_report=True
    )


@cli.command()
@csv_db_options
@kline_range_options
@click.pass_context
def sync_kline_day(ctx, no_csv, no_db, init_mode, start_date, end_date, codes):
    """同步日K线数据"""
    save_to_csv = not no_csv
    save_to_db = not no_db

    if (start_date and not end_date) or (end_date and not start_date):
        click.echo("\n✗ 错误: --start-date和--end-date必须同时指定")
        return

    click.echo("开始同步日K线数据...")
    click.echo(f"  - 保存到CSV: {'是' if save_to_csv else '否'}")
    click.echo(f"  - 保存到数据库: {'是' if save_to_db else '否'}")

    if init_mode:
        click.echo("  - 采集模式: 数据初始化（全量股票全时段）")
    elif start_date and end_date:
        click.echo(f"  - 采集模式: 指定日期范围（{start_date} ~ {end_date}）")
    else:
        click.echo("  - 采集模式: 增量更新（当天）")

    ts_codes = parse_codes(codes)
    if ts_codes:
        click.echo(f"  - 指定股票: {len(ts_codes)}只")

    sync_manager = SyncManager(ctx.obj['config_manager'])
    result = sync_manager.sync_kline_day(
        init_mode=init_mode,
        start_date=start_date,
        end_date=end_date,
        ts_codes=ts_codes,
        save_to_csv=save_to_csv,
        save_to_db=save_to_db
    )

    render_sync_result(
        result,
        success_title="✓ 日K线同步完成!",
        fail_title="✗ 日K线同步失败!",
        show_duration=True,
        show_report=True
    )


@cli.command()
@click.option('--init', 'init_mode', is_flag=True, help='数据初始化模式，采集全量股票全时段')
@click.option('--codes', help='指定股票ts_code列表，逗号分隔（如: sz.000001,sh.600000）')
@click.pass_context
def sync_anal_kline_rise_25pre(ctx, init_mode, codes):
    """生成立体K线数据"""
    if init_mode:
        click.echo("开始生成立体K线数据（初始化模式）...")
    else:
        click.echo("开始生成立体K线数据（日常更新）...")

    ts_codes = parse_codes(codes)
    if ts_codes:
        click.echo(f"  - 指定股票: {len(ts_codes)}只")

    sync_manager = SyncManager(ctx.obj['config_manager'])
    result = sync_manager.sync_anal_kline_rise_25pre(
        init_mode=init_mode,
        ts_codes=ts_codes
    )

    render_sync_result(
        result,
        success_title="✓ 立体K线生成完成!",
        fail_title="✗ 立体K线生成失败!",
        show_duration=False,
        show_report=True
    )


@cli.command()
@click.pass_context
def sync_kline_1min_migrate(ctx):
    """迁移1分钟K线为按天分区表"""
    click.echo("开始迁移1分钟K线分区表...")
    db_conn = DatabaseConnection(ctx.obj['config_manager'])
    try:
        db_conn.migrate_his_kline_1min_partitions()
        click.echo("✓ 迁移完成")
    except Exception as e:
        click.echo(f"✗ 迁移失败: {e}")
        sys.exit(1)


@cli.command()
@click.pass_context
def sync_kline_1min_rollback(ctx):
    """回滚1分钟K线分区表"""
    click.echo("开始回滚1分钟K线分区表...")
    db_conn = DatabaseConnection(ctx.obj['config_manager'])
    try:
        db_conn.rollback_his_kline_1min_partitions()
        click.echo("✓ 回滚完成")
    except Exception as e:
        click.echo(f"✗ 回滚失败: {e}")
        sys.exit(1)


@cli.command('sync-shares-xdxr')
@click.option('--codes', help='指定股票ts_code列表，逗号分隔（默认全市场 type=1 含退市）')
@click.option('--no-resume', is_flag=True, default=False, help='忽略 manifest 断点，从头开始')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.option('--manifest-path', default=None, help='manifest 路径（默认 <仓库根>/tmp/xdxr_share_manifest.json）')
@click.pass_context
def sync_shares_xdxr(ctx, codes, no_resume, dry_run, manifest_path):
    """股本数据同步（XDXR 源，baostock 下线换源 20261025；全量约 15 分钟）"""
    from src.sync.xdxr_share_manager import sync_shares_xdxr as run

    ts_codes = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    options = {
        'codes': ts_codes,
        'resume': not no_resume,
        'dry_run': dry_run,
    }
    if manifest_path:
        options['manifest_path'] = manifest_path

    click.echo("开始 XDXR 股本同步（tdx-api /api/xdxr）...")
    result = run(ctx.obj['config_manager'], **options)

    click.echo("\n✓ XDXR 股本同步完成!" if not result.get('failed') else "\n✗ 部分失败（断点续跑同命令）")
    click.echo(f"  - 目标: {result.get('total', 0)}，成功: {result.get('ok', 0)}，"
               f"无事件: {result.get('no_data', 0)}，失败: {result.get('failed', 0)}，"
               f"跳过断点: {result.get('skipped', 0)}")
    click.echo(f"  - 写入行数: {result.get('rows', 0)}")
    click.echo(f"  - 耗时: {result.get('duration', 0)} 秒")


@cli.command('sync-financial-pdf')
@click.option('--init', 'init_mode', is_flag=True, help='全历史初始化（含早年报告，断点分晚跑）')
@click.option('--codes', default=None, help='指定股票代码列表，逗号分隔（如: 600519,000001，兼容 sh.600519 写法）')
@click.option('--year', type=int, default=None, help='指定报告期年份（如 2023）')
@click.option('--year-from', type=int, default=None, help='报告期起始年份（含，与--year-to配合）')
@click.option('--year-to', type=int, default=None, help='报告期结束年份（含，与--year-from配合）')
@click.option('--dry-run', is_flag=True, default=False, help='只建清单不下载（输出份数与容量估算，不写进度）')
@click.pass_context
def sync_financial_pdf(ctx, init_mode, codes, year, year_from, year_to, dry_run):
    """采集财报PDF（巨潮资讯网）：默认增量（无进度股票回补近5年）"""
    if init_mode and (year or year_from or year_to):
        click.echo("\n✗ 错误: --init 与 --year/--year-from/--year-to 互斥")
        return
    if year_to is not None and year_from is None:
        click.echo("\n✗ 错误: --year-to 必须与 --year-from 配合使用")
        return
    if year is not None:
        year_from, year_to = year, year

    if init_mode:
        click.echo("开始采集财报PDF（全历史初始化）...")
    elif year_from is not None:
        click.echo(f"开始采集财报PDF（报告期 {year_from}~{year_to or year_from}）...")
    else:
        click.echo("开始采集财报PDF（增量：有进度只采新增，无进度回补近5年）...")

    if codes:
        click.echo(f"  - 指定股票: {codes}")
    if dry_run:
        click.echo("  - dry-run 模式: 只建清单不下载，不写进度")

    from src.sync.financial_pdf_manager import FinancialPdfManager
    manager = FinancialPdfManager(ctx.obj['config_manager'])
    stock_codes = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    result = manager.execute(init_mode=init_mode, year_from=year_from, year_to=year_to,
                             codes=stock_codes, dry_run=dry_run)

    stats = result['stats']
    if dry_run:
        click.echo(f"\n✓ dry-run 完成（{stats['mode']}）")
        click.echo(f"  - 股票: {stats['stocks_total']} 只")
        click.echo(f"  - 计划下载: {stats['docs_planned']} 份")
        click.echo(f"  - 容量估算: {stats['estimated_mb']:.0f} MB（adjunctSize 口径）")
    elif result['success']:
        click.echo(f"\n✓ 财报PDF采集完成（{stats['mode']}）!")
    elif result['blocked']:
        click.echo("\n✗ 财报PDF采集因巨潮风控急停（进度/断点已保存，稍后重跑即可续）")
    else:
        click.echo("\n✗ 财报PDF采集完成但存在失败（manifest 失败清单已记录，重跑可自动重试）")
    click.echo(f"  - 股票: {stats['stocks_total']} 只（完成 {stats['stocks_done']}）")
    click.echo(f"  - 计划 {stats['docs_planned']} 份，下载 {stats['docs_downloaded']}，"
               f"跳过已有 {stats['docs_skipped_existing']}，非PDF跳过 {stats['docs_skipped_non_pdf']}，"
               f"重试找回 {stats['docs_retried_ok']}，版本覆盖 {stats['docs_replaced']}，"
               f"失败 {stats['docs_failed']}")
    click.echo(f"  - 进度推进股票数: {stats['progress_advanced']}")
    if stats['failed_stocks']:
        click.echo(f"  - 失败股票: {stats['failed_stocks']}")
    if result.get('errors'):
        for err in result['errors']:
            click.echo(f"  - 错误: {err}")
    if result.get('duration') is not None:
        click.echo(f"  - 耗时: {result['duration'] / 60:.1f} 分钟")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@cli.command()
@click.pass_context
def status(ctx):
    """查看系统状态"""
    click.echo("系统状态:")

    config_manager = ctx.obj['config_manager']

    # 检查数据库连接
    db_conn = DatabaseConnection(config_manager)
    try:
        stock_count = db_conn.get_table_count('base_stock_info')
        click.echo(f"  - 数据库连接: 正常")
        click.echo(f"  - 股票数量: {stock_count}")

        # 检查基本面数据表
        try:
            fundamentals_count = db_conn.get_table_count('base_fundamentals_info')
            click.echo(f"  - 基本面数据: {fundamentals_count} 条")
        except Exception:
            click.echo(f"  - 基本面数据表: 未初始化")
    except Exception as e:
        click.echo(f"  - 数据库连接: 失败 ({e})")

    # 检查数据目录
    import os
    data_paths = config_manager.get_data_paths()
    for path_name, path_value in data_paths.items():
        exists = os.path.exists(path_value)
        click.echo(f"  - {path_name}目录: {'存在' if exists else '不存在'} ({path_value})")


def main():
    """命令行入口函数"""
    # 设置日志配置，确保进度条能够正常显示
    setup_logging()
    cli()


if __name__ == '__main__':
    main()
