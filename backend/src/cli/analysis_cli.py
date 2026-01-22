import click
import logging
from ..analysis.three_dimension_kline import ThreeDimensionKlineGenerator

logger = logging.getLogger(__name__)

@click.command('generate_3d_kline')
@click.option('--ts-code', help='指定股票代码 (例如: sh.600000)')
@click.option('--start-date', help='开始日期 (YYYY-MM-DD)')
@click.option('--end-date', help='结束日期 (YYYY-MM-DD)')
@click.option('--source-type', default='5min', type=click.Choice(['1min', '5min']), help='源K线类型 (默认: 5min)')
@click.pass_context
def generate_3d_kline(ctx, ts_code, start_date, end_date, source_type):
    """生成基于价格涨幅的3D K线数据"""
    click.echo(f"开始生成3D K线数据...")
    click.echo(f"  - 源数据类型: {source_type}")
    if ts_code:
        click.echo(f"  - 股票代码: {ts_code}")
    if start_date:
        click.echo(f"  - 开始日期: {start_date}")
    if end_date:
        click.echo(f"  - 结束日期: {end_date}")

    config_manager = ctx.obj.get('config_manager')
    generator = ThreeDimensionKlineGenerator(config_manager)

    try:
        result = generator.generate(ts_code, start_date, end_date, source_type)

        click.echo("\n✓ 生成完成!")
        click.echo(f"  - 处理股票数: {result.get('processed_stocks', 0)}")
        click.echo(f"  - 生成记录数: {result.get('total_records', 0)}")

        perf = result.get('performance', {})
        if perf:
            click.echo("\n性能统计:")
            click.echo(f"  - 查询耗时: {perf.get('query_time', 0):.3f}s")
            click.echo(f"  - 计算耗时: {perf.get('calc_time', 0):.3f}s")
            click.echo(f"  - 写库耗时: {perf.get('db_write_time', 0):.3f}s")
            click.echo(f"  - 总耗时  : {perf.get('total_time', 0):.3f}s")

    except Exception as e:
        click.echo(f"\n✗ 生成失败: {e}")
        logger.error(f"3D K线生成失败: {e}", exc_info=True)
