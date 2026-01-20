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
@click.option('--no-csv', is_flag=True, default=False, help='不保存到CSV文件')
@click.option('--no-db', is_flag=True, default=False, help='不保存到数据库')
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


@cli.command()
@click.option('--no-csv', is_flag=True, default=False, help='不保存到CSV文件')
@click.option('--no-db', is_flag=True, default=False, help='不保存到数据库')
@click.pass_context
def sync_stocks(ctx, no_csv, no_db):
    """同步股票列表"""
    click.echo("同步股票列表...")

    # 默认都保存，除非明确指定不保存
    save_to_csv = not no_csv
    save_to_db = not no_db

    sync_manager = SyncManager(ctx.obj['config_manager'])
    stocks = sync_manager.sync_stocks(save_to_csv=save_to_csv, save_to_db=save_to_db)

    click.echo(f"\n✓ 股票列表同步完成，共 {len(stocks)} 只股票")


@cli.command()
@click.option('--no-csv', is_flag=True, default=False, help='不保存到CSV文件')
@click.option('--no-db', is_flag=True, default=False, help='不保存到数据库')
@click.option('--batch-size', type=int, default=150, help='批次大小，默认150（性能优化后）')
@click.option('--dry-run', is_flag=True, help='试运行模式，不实际写入数据')
@click.option('--list-status', type=click.Choice(['L', 'D', 'P']), default='L',
              help='股票上市状态过滤: L=上市, D=退市, P=暂停上市 (默认: L)')
@click.option('--qps-limit', type=int, help='QPS限制，设置为0或负数表示不限流（覆盖配置文件设置）')
@click.option('--max-workers', type=int, default=6, help='最大并发线程数，默认6（第三阶段优化）')
@click.option('--concurrent', is_flag=True, default=True, help='启用并发处理（第三阶段优化）')
@click.option('--init', 'init_mode', is_flag=True, help='数据初始化模式，采集全量股票全时段(1992Q3至今)')
@click.option('--year', type=int, help='指定年份（与--quarter配合使用）')
@click.option('--quarter', type=click.Choice(['1', '2', '3', '4']), help='指定季度1-4（与--year配合使用）')
@click.option('--codes', help='指定股票ts_code列表，逗号分隔（如: sz.000001,sh.600000）')
@click.option('--batch', type=int, help='指定批次编号（仅在init模式下有效，从1开始）')
@click.pass_context
def sync_fundamentals(ctx, no_csv, no_db, batch_size, dry_run, list_status, qps_limit, max_workers, concurrent, init_mode, year, quarter, codes, batch):
    """同步股票基本面数据 - 支持三种采集方式：数据初始化/增量更新/指定股票和时间"""

    # 默认都保存，除非明确指定不保存
    save_to_csv = not no_csv
    save_to_db = not no_db

    click.echo("开始同步股票基本面数据...")
    click.echo(f"  - 保存到CSV: {'是' if save_to_csv else '否'}")
    click.echo(f"  - 保存到数据库: {'是' if save_to_db else '否'}")
    click.echo(f"  - 批次大小: {batch_size}")
    click.echo(f"  - 试运行模式: {'是' if dry_run else '否'}")
    click.echo(f"  - 上市状态: {list_status}")

    # 采集模式提示
    if init_mode:
        if batch:
            click.echo(f"  - 采集模式: 数据初始化（批次{batch}）")
        else:
            click.echo(f"  - 采集模式: 数据初始化（全量股票全时段）")
    elif year and quarter:
        click.echo(f"  - 采集模式: 指定时间（{year}年Q{quarter}）")
    else:
        click.echo(f"  - 采集模式: 增量更新（最近两个季度）")

    # batch参数验证
    if batch is not None:
        if not init_mode:
            click.echo("\n✗ 错误: --batch参数仅在init模式(--init)下有效")
            return
        if batch < 1:
            click.echo(f"\n✗ 错误: 批次编号必须从1开始，当前值: {batch}")
            return

    # 解析股票代码列表
    ts_codes = None
    if codes:
        ts_codes = [code.strip() for code in codes.split(',')]
        click.echo(f"  - 指定股票: {len(ts_codes)}只")

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
        'concurrent': concurrent,
        'init_mode': init_mode,
        'year': year,
        'quarter': int(quarter) if quarter else None,
        'ts_codes': ts_codes,
        'save_to_csv': save_to_csv,
        'save_to_db': save_to_db,
        'batch': batch
    }

    # 根据并发参数选择处理方式
    if concurrent:
        from src.sync.concurrent_fundamentals_manager import sync_fundamentals_data_concurrent
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
@click.option('--no-csv', is_flag=True, default=False, help='不保存到CSV文件')
@click.option('--no-db', is_flag=True, default=False, help='不保存到数据库')
@click.option('--batch-size', type=int, default=50, help='批次大小，默认50')
@click.option('--dry-run', is_flag=True, help='试运行模式，不实际写入数据')
@click.option('--list-status', type=click.Choice(['L', 'D', 'P']), default='L',
              help='股票上市状态过滤: L=上市, D=退市, P=暂停上市 (默认: L)')
@click.option('--max-workers', type=int, default=6, help='最大并发线程数，默认6')
@click.option('--init', 'init_mode', is_flag=True, help='数据初始化模式，采集1990-12-19至今')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--codes', help='指定股票ts_code列表，逗号分隔（如: sz.000001,sh.600000）')
@click.option('--batch', type=int, help='指定批次编号（仅在init模式下有效，从1开始）')
@click.pass_context
def sync_daily_kline(ctx, no_csv, no_db, batch_size, dry_run, list_status, max_workers, init_mode, start_date, end_date, codes, batch):
    """同步日线K线数据 - 支持初始化/增量更新/指定范围"""

    # 默认都保存，除非明确指定不保存
    save_to_csv = not no_csv
    save_to_db = not no_db

    click.echo("开始同步日线K线数据...")
    click.echo(f"  - 保存到CSV: {'是' if save_to_csv else '否'}")
    click.echo(f"  - 保存到数据库: {'是' if save_to_db else '否'}")
    click.echo(f"  - 批次大小: {batch_size}")
    click.echo(f"  - 试运行模式: {'是' if dry_run else '否'}")
    click.echo(f"  - 上市状态: {list_status}")

    # 采集模式提示
    if init_mode:
        if batch:
            click.echo(f"  - 采集模式: 数据初始化（批次{batch}）")
        else:
            click.echo(f"  - 采集模式: 数据初始化（全量股票全时段）")
    elif start_date and end_date:
        click.echo(f"  - 采集模式: 指定范围（{start_date}至{end_date}）")
    else:
        click.echo(f"  - 采集模式: 日增量更新")

    # batch参数验证
    if batch is not None:
        if not init_mode:
            click.echo("\n✗ 错误: --batch参数仅在init模式(--init)下有效")
            return
        if batch < 1:
            click.echo(f"\n✗ 错误: 批次编号必须从1开始，当前值: {batch}")
            return

    # 解析股票代码列表
    ts_codes = None
    if codes:
        ts_codes = [code.strip() for code in codes.split(',')]
        click.echo(f"  - 指定股票: {len(ts_codes)}只")

    # 导入并执行
    from src.sync.daily_kline_manager import sync_daily_kline_concurrent

    options = {
        'batch_size': batch_size,
        'dry_run': dry_run,
        'list_status': list_status,
        'max_workers': max_workers,
        'init_mode': init_mode,
        'start_date': start_date,
        'end_date': end_date,
        'ts_codes': ts_codes,
        'save_to_csv': save_to_csv,
        'save_to_db': save_to_db,
        'batch': batch
    }

    result = sync_daily_kline_concurrent(ctx.obj['config_manager'], **options)

    if 'error' in result:
        click.echo(f"\n✗ 日线K线数据同步失败: {result['error']}")
        return

    click.echo("\n✓ 日线K线数据同步完成!")
    click.echo(f"  - 总股票数量: {result.get('total_stocks', 0)}")
    click.echo(f"  - 成功采集: {result.get('successful', 0)}")
    click.echo(f"  - 失败数量: {result.get('failed', 0)}")
    click.echo(f"  - 处理批次数: {result.get('batch_count', 0)}")
    click.echo(f"  - 异常波动记录: {result.get('anomalies_count', 0)}")

    if 'duration' in result:
        click.echo(f"  - 耗时: {result['duration']:.2f} 秒")

    if 'success_rate' in result:
        click.echo(f"  - 成功率: {result['success_rate']:.2%}")


@cli.command()
@click.option('--no-csv', is_flag=True, default=False, help='不保存到CSV文件')
@click.option('--no-db', is_flag=True, default=False, help='不保存到数据库')
@click.option('--batch-size', type=int, default=20, help='批次大小，默认20')
@click.option('--dry-run', is_flag=True, help='试运行模式，不实际写入数据')
@click.option('--list-status', type=click.Choice(['L', 'D', 'P']), default='L',
              help='股票上市状态过滤: L=上市, D=退市, P=暂停上市 (默认: L)')
@click.option('--max-workers', type=int, default=6, help='最大并发线程数，默认6')
@click.option('--init', 'init_mode', is_flag=True, help='数据初始化模式，采集2019-01-05至今')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--codes', help='指定股票ts_code列表，逗号分隔（如: sz.000001,sh.600000）')
@click.option('--batch', type=int, help='指定批次编号（仅在init模式下有效，从1开始）')
@click.pass_context
def sync_5min_kline(ctx, no_csv, no_db, batch_size, dry_run, list_status, max_workers, init_mode, start_date, end_date, codes, batch):
    """同步5分钟K线数据 - 支持初始化/增量更新/指定范围"""

    # 默认都保存，除非明确指定不保存
    save_to_csv = not no_csv
    save_to_db = not no_db

    click.echo("开始同步5分钟K线数据...")
    click.echo(f"  - 保存到CSV: {'是' if save_to_csv else '否'}")
    click.echo(f"  - 保存到数据库: {'是' if save_to_db else '否'}")
    click.echo(f"  - 批次大小: {batch_size}")
    click.echo(f"  - 试运行模式: {'是' if dry_run else '否'}")
    click.echo(f"  - 上市状态: {list_status}")

    # 采集模式提示
    if init_mode:
        if batch:
            click.echo(f"  - 采集模式: 数据初始化（批次{batch}）")
        else:
            click.echo(f"  - 采集模式: 数据初始化（全量股票 2019-01-05至今）")
    elif start_date and end_date:
        click.echo(f"  - 采集模式: 指定范围（{start_date}至{end_date}）")
    else:
        click.echo(f"  - 采集模式: 日增量更新")

    # batch参数验证
    if batch is not None:
        if not init_mode:
            click.echo("\n✗ 错误: --batch参数仅在init模式(--init)下有效")
            return
        if batch < 1:
            click.echo(f"\n✗ 错误: 批次编号必须从1开始，当前值: {batch}")
            return

    # 解析股票代码列表
    ts_codes = None
    if codes:
        ts_codes = [code.strip() for code in codes.split(',')]
        click.echo(f"  - 指定股票: {len(ts_codes)}只")

    # 导入并执行
    from src.sync.minute_kline_manager import sync_minute_kline_concurrent

    options = {
        'batch_size': batch_size,
        'dry_run': dry_run,
        'list_status': list_status,
        'max_workers': max_workers,
        'init_mode': init_mode,
        'start_date': start_date,
        'end_date': end_date,
        'ts_codes': ts_codes,
        'save_to_csv': save_to_csv,
        'save_to_db': save_to_db,
        'batch': batch
    }

    result = sync_minute_kline_concurrent(ctx.obj['config_manager'], **options)

    if 'error' in result:
        click.echo(f"\n✗ 5分钟K线数据同步失败: {result['error']}")
        return

    click.echo("\n✓ 5分钟K线数据同步完成!")
    click.echo(f"  - 总股票数量: {result.get('total_stocks', 0)}")
    click.echo(f"  - 成功采集: {result.get('successful', 0)}")
    click.echo(f"  - 失败数量: {result.get('failed', 0)}")
    click.echo(f"  - 处理批次数: {result.get('batch_count', 0)}")
    click.echo(f"  - 异常波动记录: {result.get('anomalies_count', 0)}")

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
