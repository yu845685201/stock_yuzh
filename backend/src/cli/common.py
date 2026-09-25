"""
CLI 公共件（R-08 抽取）：click 装饰器组、结果渲染、codes 解析

- 参数名/默认值/help 文案逐字取自原 main.py 各命令定义
- render_sync_result 成功/失败输出文案逐字取自原各命令输出块
"""

from typing import Callable, List, Optional

import click


def csv_db_options(fn: Callable) -> Callable:
    """--no-csv / --no-db 选项组（原各同步命令的重复定义）"""
    return click.option('--no-csv', is_flag=True, default=False, help='不保存到CSV文件')(
        click.option('--no-db', is_flag=True, default=False, help='不保存到数据库')(fn)
    )


def kline_range_options(fn: Callable) -> Callable:
    """--init / --start-date / --end-date / --codes 选项组（原 1min/day 两命令的近复制）"""
    return click.option('--init', 'init_mode', is_flag=True, help='数据初始化模式，采集全量股票全时段')(
        click.option('--start-date', help='指定开始日期(yyyyMMdd)')(
            click.option('--end-date', help='指定结束日期(yyyyMMdd)')(
                click.option('--codes', help='指定股票ts_code列表，逗号分隔（如: sz.000001,sh.600000）')(fn)
            )
        )
    )


def parse_codes(codes: Optional[str]) -> Optional[List[str]]:
    """逗号分隔 ts_code 列表解析（原 1min/day/anal 三命令的相同实现）"""
    if not codes:
        return None
    ts_codes = [code.strip() for code in codes.split(',') if code.strip()]
    return ts_codes


def render_sync_result(
    result,
    success_title: str,
    fail_title: str,
    show_duration: bool = True,
    show_report: bool = True,
) -> None:
    """统一成功/失败输出块（文案逐字取自原各命令实现）。

    - 成功：标题 + 记录数/写库行数 + 可选 耗时 + 可选 报告
    - 失败：标题 + errors 逐行
    """
    if result['success']:
        click.echo(f"\n{success_title}")
        click.echo(f"  - 记录数: {result.get('records', 0)}")
        click.echo(f"  - 写库行数: {result.get('db_rows', 0)}")
        if show_duration:
            click.echo(f"  - 耗时: {result.get('duration', 0):.2f} 秒")
        if show_report and result.get('report_path'):
            click.echo(f"  - 报告: {result['report_path']}")
    else:
        click.echo(f"\n{fail_title}")
        for error in result['errors']:
            click.echo(f"  错误: {error}")
