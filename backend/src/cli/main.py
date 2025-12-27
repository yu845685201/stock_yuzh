"""
命令行主入口
"""

import click
import sys
import logging
from datetime import date, datetime
from src.config import ConfigManager
from src.sync import SyncManager
from src.database import DatabaseConnection


def setup_logging():
    """设置日志配置，确保进度条能够正常显示"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S',
        force=True  # 强制重新配置，避免重复调用问题
    )


@click.group()
@click.option('--env', default='uat', help='运行环境 (uat/prod)')
@click.pass_context
def cli(ctx, env):
    """A股盘后静态分析系统命令行工具"""
    ctx.ensure_object(dict)
    ctx.obj['env'] = env
    ctx.obj['config_manager'] = ConfigManager(env=env)


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
@click.option('--csv', is_flag=True, default=True, help='保存到CSV文件')
@click.option('--db', is_flag=True, default=True, help='保存到数据库')
@click.pass_context
def sync_all(ctx, csv, db):
    """同步所有数据"""
    click.echo("开始同步所有数据...")

    sync_manager = SyncManager(ctx.obj['config_manager'])
    result = sync_manager.sync_all(save_to_csv=csv, save_to_db=db)

    if result['success']:
        click.echo("\n✓ 数据同步完成!")
        click.echo(f"  - 股票数量: {result['stocks_count']}")
        click.echo(f"  - 日K线数据: {result['daily_data_count']} 条")
        click.echo(f"  - 5分钟K线数据: {result['min5_data_count']} 条")
        click.echo(f"  - 耗时: {result['duration']:.2f} 秒")
    else:
        click.echo("\n✗ 数据同步失败!")
        for error in result['errors']:
            click.echo(f"  错误: {error}")


@cli.command()
@click.option('--csv', is_flag=True, help='保存到CSV文件')
@click.option('--db', is_flag=True, help='保存到数据库')
@click.pass_context
def sync_stocks(ctx, csv, db):
    """同步股票列表"""
    click.echo("同步股票列表...")

    # 如果没有指定任何标志，默认同时保存CSV和数据库
    if not csv and not db:
        csv = True
        db = True

    sync_manager = SyncManager(ctx.obj['config_manager'])
    stocks = sync_manager.sync_stocks(save_to_csv=csv, save_to_db=db)

    click.echo(f"\n✓ 股票列表同步完成，共 {len(stocks)} 只股票")


@cli.command()
@click.option('--csv', is_flag=True, default=True, help='保存到CSV文件')
@click.option('--db', is_flag=True, default=True, help='保存到数据库')
@click.option('--collect-only', is_flag=True, default=False, help='只采集不保存')
@click.option('--silent', is_flag=True, default=False, help='静默模式，隐藏实时进度日志')
@click.option('--no-anomaly-report', is_flag=True, default=False, help='不生成异常报告')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--codes', help='股票代码列表，逗号分隔')
@click.pass_context
def sync_daily(ctx, csv, db, collect_only, silent, no_anomaly_report, start_date, end_date, codes):
    """同步日K线数据"""
    # 如果是只采集模式，自动设置不保存
    if collect_only:
        csv = False
        db = False
        click.echo("🔍 只采集模式：不会保存到CSV文件或数据库")

    # 解析日期
    start_dt = None
    end_dt = None
    if start_date:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    # 解析股票代码
    codes_list = None
    if codes:
        codes_list = [code.strip() for code in codes.split(',')]

    if not silent:
        click.echo("同步日K线数据...")
        if start_dt:
            click.echo(f"  - 开始日期: {start_dt}")
        if end_dt:
            click.echo(f"  - 结束日期: {end_dt}")
        if codes_list:
            click.echo(f"  - 股票数量: {len(codes_list)}")
        if collect_only:
            click.echo(f"  - 模式: 只采集（不保存）")
        if silent:
            click.echo(f"  - 模式: 静默运行")
        if no_anomaly_report:
            click.echo(f"  - 异常报告: 已禁用")

    sync_manager = SyncManager(ctx.obj['config_manager'])

    # 使用参数值
    save_to_csv = csv
    save_to_db = db

    count = sync_manager.sync_daily_data(
        save_to_csv=save_to_csv,
        save_to_db=save_to_db,
        start_date=start_dt,
        end_date=end_dt,
        codes=codes_list,
        silent_mode=silent,
        generate_anomaly_report=not no_anomaly_report
    )

    if not silent:
        click.echo(f"\n✓ 日K线数据同步完成，共 {count} 条数据")






@cli.command()
@click.option('--csv', is_flag=True, default=True, help='保存到CSV文件')
@click.option('--db', is_flag=True, default=True, help='保存到数据库')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--codes', help='股票代码列表，逗号分隔')
@click.pass_context
def sync_1min(ctx, csv, db, start_date, end_date, codes):
    """同步1分钟K线数据"""
    # 解析日期
    start_dt = None
    end_dt = None
    if start_date:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    # 解析股票代码
    codes_list = None
    if codes:
        codes_list = [code.strip() for code in codes.split(',')]

    click.echo("同步1分钟K线数据...")
    if start_dt:
        click.echo(f"  - 开始日期: {start_dt}")
    if end_dt:
        click.echo(f"  - 结束日期: {end_dt}")
    if codes_list:
        click.echo(f"  - 股票数量: {len(codes_list)}")

    sync_manager = SyncManager(ctx.obj['config_manager'])
    count = sync_manager.sync_1min_data(
        save_to_csv=csv,
        save_to_db=db,
        start_date=start_dt,
        end_date=end_dt,
        codes=codes_list
    )

    click.echo(f"\n✓ 1分钟K线数据同步完成，共 {count} 条数据")


@cli.command()
@click.option('--csv', is_flag=True, default=True, help='保存到CSV文件')
@click.option('--db', is_flag=True, default=True, help='保存到数据库')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--codes', help='股票代码列表，逗号分隔')
@click.pass_context
def sync_5min(ctx, csv, db, start_date, end_date, codes):
    """同步5分钟K线数据"""
    # 解析日期
    start_dt = None
    end_dt = None
    if start_date:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

    # 解析股票代码
    codes_list = None
    if codes:
        codes_list = [code.strip() for code in codes.split(',')]

    click.echo("同步5分钟K线数据...")
    if start_dt:
        click.echo(f"  - 开始日期: {start_dt}")
    if end_dt:
        click.echo(f"  - 结束日期: {end_dt}")
    if codes_list:
        click.echo(f"  - 股票数量: {len(codes_list)}")

    sync_manager = SyncManager(ctx.obj['config_manager'])
    count = sync_manager.sync_5min_data(
        save_to_csv=csv,
        save_to_db=db,
        start_date=start_dt,
        end_date=end_dt,
        codes=codes_list
    )

    click.echo(f"\n✓ 5分钟K线数据同步完成，共 {count} 条数据")


@cli.command()
@click.option('--batch-size', type=int, default=150, help='批次大小，默认150（性能优化后）')
@click.option('--dry-run', is_flag=True, help='试运行模式，不实际写入数据')
@click.option('--list-status', type=click.Choice(['L', 'D', 'P']), default='L',
              help='股票上市状态过滤: L=上市, D=退市, P=暂停上市 (默认: L)')
@click.option('--qps-limit', type=int, help='QPS限制，设置为0或负数表示不限流（覆盖配置文件设置）')
@click.option('--max-workers', type=int, default=6, help='最大并发线程数，默认6（第三阶段优化）')
@click.option('--concurrent', is_flag=True, default=True, help='启用并发处理（第三阶段优化）')
@click.pass_context
def sync_fundamentals(ctx, batch_size, dry_run, list_status, qps_limit, max_workers, concurrent):
    """同步股票基本面数据"""
    click.echo("开始同步股票基本面数据...")
    click.echo(f"  - 批次大小: {batch_size}")
    click.echo(f"  - 试运行模式: {'是' if dry_run else '否'}")
    click.echo(f"  - 上市状态: {list_status}")

    # 处理QPS限制
    if qps_limit is not None:
        click.echo(f"  - QPS限制: {'不限流' if qps_limit <= 0 else qps_limit}")
        # 临时更新配置
        config_manager = ctx.obj['config_manager']
        rate_limit_config = config_manager.get('rate_limit', {})
        rate_limit_config['calls_per_period'] = qps_limit
        # 更新到配置中
        config_manager.load_config()['rate_limit'] = rate_limit_config

    sync_manager = SyncManager(ctx.obj['config_manager'])

    # 构建选项字典
    options = {
        'batch_size': batch_size,
        'dry_run': dry_run,
        'list_status': list_status,
        'max_workers': max_workers,
        'concurrent': concurrent
    }

    # 根据并发参数选择处理方式
    if concurrent:
        from ..sync.concurrent_fundamentals_manager import sync_fundamentals_data_concurrent
        result = sync_fundamentals_data_concurrent(ctx.obj['config_manager'], **options)
    else:
        result = sync_manager.sync_fundamentals_data(**options)

    if 'error' in result:
        click.echo(f"\n✗ 基本面数据同步失败: {result['error']}")
        return

    click.echo("\n✓ 基本面数据同步完成!")
    click.echo(f"  - 总股票数量: {result.get('total_stocks', 0)}")
    click.echo(f"  - 成功采集: {result.get('successful', 0)}")
    click.echo(f"  - 失败数量: {result.get('failed', 0)}")
    click.echo(f"  - 处理批次数: {result.get('batch_count', 0)}")

    if 'duration' in result:
        click.echo(f"  - 耗时: {result['duration']:.2f} 秒")

    if 'success_rate' in result:
        click.echo(f"  - 成功率: {result['success_rate']:.2%}")


@cli.command()
@click.pass_context
def status(ctx):
    """查看系统状态"""
    click.echo("系统状态:")

    config_manager = ctx.obj['config_manager']
    click.echo(f"  - 环境: {config_manager.env}")

    # 检查数据库连接
    db_conn = DatabaseConnection(config_manager)
    try:
        result = db_conn.execute_query("SELECT COUNT(*) as count FROM base_stock_info")
        stock_count = result[0]['count'] if result else 0
        click.echo(f"  - 数据库连接: 正常")
        click.echo(f"  - 股票数量: {stock_count}")

        # 检查基本面数据表
        try:
            result = db_conn.execute_query("SELECT COUNT(*) as count FROM base_fundamentals_info")
            fundamentals_count = result[0]['count'] if result else 0
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