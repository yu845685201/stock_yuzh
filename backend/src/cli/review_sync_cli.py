"""
每日复盘数据采集 CLI 子命令（方案 2.6；注册方式照 sync-financial-pdf，延迟 import manager）

命令清单：
  sync-review-data          盘后编排器（默认 11 源串行，单源失败不阻塞）
  sync-index-kline          指数日K（tdx-api）
  sync-adjust-factor        复权因子（tdx-api /api/ths-factor；全量 --init / 增量读触发清单）
  sync-money-flow           分笔资金流（异动子集，近似口径）
  sync-announcement         公告清单 + 业绩预告（巨潮，同请求双用途）
  sync-disclosure-calendar  财报披露日历（交易所官网）
  sync-index-valuation      指数估值（中证官网）
  sync-ths-finance          THS 财报指标（全量 --init / 增量当日披露）
  sync-lhb                  龙虎榜（交易所官网）
  sync-margin-trade         两融（交易所官网，T-1）
  sync-industry             行业分类（新浪行业板块，2026-09-28 新增）
  sync-concept              概念板块成分（新浪概念接口，2026-09-28 换源）
  sync-unlock-calendar      解禁日历（巨潮公告+PDF 解析，2026-09-28 换源）
  sync-overseas-market      外围市场（新浪）
  calc-stock-valuation      个股 PE/PB 自算（本地计算）
"""

import click


@click.command('sync-review-data')
@click.option('--sources', default='all',
              help='逗号分隔子集：index,factor,overseas,valuation_idx,announcement,'
                   'disclosure,lhb,margin,ths_finance,money_flow,valuation_calc'
                   '（周频 concept/unlock 显式指定；默认 all）')
@click.option('--as-of', default=None, help='yyyyMMdd，缺省当日')
@click.option('--dry-run', is_flag=True, default=False, help='试运行：只做前置校验与目标清单')
@click.pass_context
def sync_review_data(ctx, sources, as_of, dry_run):
    """每日复盘数据采集编排器（盘后一条命令，单源失败不阻塞）"""
    from src.sync.review_sync_orchestrator import ReviewSyncOrchestrator
    try:
        orchestrator = ReviewSyncOrchestrator(ctx.obj['config_manager'])
    except ValueError as e:
        click.echo(f"\n✗ 参数错误: {e}")
        return
    click.echo(f"开始每日复盘数据采集（as_of={as_of or '当日'}, sources={sources}）...")
    result = orchestrator.execute(sources=sources, as_of=as_of, dry_run=dry_run)

    if result['kline_check']:
        click.echo(f"  - 日K前置校验: {result['kline_check']}")
    if result['errors'] and not result['summary']:
        click.echo("\n✗ 编排中止:")
        for err in result['errors']:
            click.echo(f"  - {err}")
    else:
        click.echo(f"\n{'✓' if result['success'] else '✗'} 编排完成（"
                   f"{result['duration'] / 60:.1f} 分钟）:")
        for e in result['summary']:
            click.echo(f"  - {e['label']}: {e['status']}（行数 {e['rows']}，"
                       f"{e['duration']}s）{(' ' + e['detail']) if e['detail'] else ''}")
    if result.get('report_path'):
        click.echo(f"  - 汇总报告: {result['report_path']}")


@click.command('sync-index-kline')
@click.option('--init', 'init_mode', is_flag=True, default=False, help='全量历史（默认尾部增量）')
@click.option('--codes', default=None, help='指定指数代码，逗号分隔（如 000001,399006）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_index_kline(ctx, init_mode, codes, dry_run):
    """同步指数日K线（tdx-api，含 up_count/down_count）"""
    from src.sync.index_kline_manager import IndexKlineManager
    manager = IndexKlineManager(ctx.obj['config_manager'])
    code_list = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    result = manager.execute(init_mode=init_mode, codes=code_list, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 指数日K同步"
               f"（成功 {stats['indices_ok']}/{stats['indices_total']}，行数 {stats['rows']}）")
    for item in stats['skipped'] + stats['failed']:
        click.echo(f"  - {item}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-adjust-factor')
@click.option('--init', 'init_mode', is_flag=True, default=False,
              help='全量初始化（全市场，manifest 断点跨天）')
@click.option('--trade-date', default=None, help='增量：读取该日除权触发清单（yyyyMMdd，缺省当日）')
@click.option('--codes', default=None, help='指定股票，逗号分隔')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，只列目标清单')
@click.pass_context
def sync_adjust_factor(ctx, init_mode, trade_date, codes, dry_run):
    """同步复权因子（tdx-api /api/ths-factor；增量依赖日K除权触发清单）"""
    from src.sync.adjust_factor_manager import AdjustFactorManager
    manager = AdjustFactorManager(ctx.obj['config_manager'])
    code_list = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    result = manager.execute(init_mode=init_mode, trade_date=trade_date,
                             codes=code_list, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 复权因子同步（{stats['mode']}）")
    click.echo(f"  - 目标 {stats['targets']} 只，成功 {stats['stocks_ok']}，行数 {stats['rows']}，"
               f"xdxr 校验不一致 {stats['xdxr_mismatch']}")
    if stats['failed']:
        click.echo(f"  - 失败: {stats['failed'][:20]}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-money-flow')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省当日；历史日走历史分笔路径补抓）')
@click.option('--codes', default=None, help='指定股票（跳过异动子集筛选）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行：只输出异动子集清单')
@click.pass_context
def sync_money_flow(ctx, trade_date, codes, dry_run):
    """同步分笔资金流（异动子集，近似口径：分笔分桶）"""
    from src.sync.money_flow_manager import MoneyFlowManager
    manager = MoneyFlowManager(ctx.obj['config_manager'])
    code_list = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    try:
        result = manager.execute(trade_date=trade_date, codes=code_list, dry_run=dry_run)
    except Exception as e:
        click.echo(f"\n✗ 分笔资金流失败: {e}")
        return
    stats = result['stats']
    sub = stats.get('subset', {})
    click.echo(f"\n{'✓' if result['success'] else '✗'} 分笔资金流（{stats['trade_date']}）")
    click.echo(f"  - 子集: 涨跌停 {sub.get('limit_hits', 0)}，|涨跌幅| {sub.get('pct_hits', 0)}，"
               f"Top成交额 {sub.get('top_amount', 0)} -> 选中 {sub.get('selected', 0)} 只")
    if not dry_run:
        click.echo(f"  - 抓取: 成功 {stats['stocks_ok']}，失败 {stats['stocks_failed']}，"
                   f"分笔 {stats['total_trades']} 笔；降速触发: "
                   f"{'是' if stats['timeout_throttle'] else '否'}")
        if stats['failed']:
            click.echo(f"  - 失败（次日历史路径补抓）: {stats['failed'][:20]}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-announcement')
@click.option('--as-of', default=None, help='yyyyMMdd（缺省当日；窗口下界取游标/昨日）')
@click.option('--days-back', type=int, default=1, help='无游标时回看天数（默认 1）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库不推游标')
@click.pass_context
def sync_announcement(ctx, as_of, days_back, dry_run):
    """同步公告清单 + 业绩预告（巨潮全市场窗口检索，同请求双用途）"""
    from src.sync.announcement_manager import AnnouncementManager
    manager = AnnouncementManager(ctx.obj['config_manager'])
    result = manager.execute(as_of=as_of, days_back=days_back, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 公告采集（窗口 {stats['window']}）")
    click.echo(f"  - 公告 {stats['announcements']} 条入库 {stats['announcements_new']}，"
               f"业绩预告 {stats['forecast_rows']} 条")
    if result['blocked']:
        click.echo("  - ⚠ 巨潮风控急停（游标未推进，稍后重跑自动补）")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-disclosure-calendar')
@click.option('--year', type=int, default=None, help='预约年份（缺省当年，Q1 自动含上年年报）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_disclosure_calendar(ctx, year, dry_run):
    """同步财报披露日历（上交所预约+实际；深交所目录待核实标待补）"""
    from src.sync.disclosure_manager import DisclosureManager
    manager = DisclosureManager(ctx.obj['config_manager'])
    result = manager.execute(year=year, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 披露日历（{stats['years']}）"
               f"上交所 {stats['sse_rows']} 行（预约 {stats['plan_rows']}/实际 {stats['actual_rows']}）")
    click.echo(f"  - 深交所: {stats['szse_status']}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-index-valuation')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_index_valuation(ctx, dry_run):
    """同步指数估值 PE（中证官网每日 indicator 文件）"""
    from src.sync.index_valuation_manager import IndexValuationManager
    manager = IndexValuationManager(ctx.obj['config_manager'])
    result = manager.execute(dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 指数估值"
               f"（成功 {stats['indices_ok']}/{stats['indices_total']}，行数 {stats['rows']}）")
    if stats['missing']:
        click.echo(f"  - 未发布: {stats['missing']}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-ths-finance')
@click.option('--init', 'init_mode', is_flag=True, default=False,
              help='全量初始化（全市场，manifest 断点跨天）')
@click.option('--as-of', default=None, help='增量：当日披露日期（yyyyMMdd，缺省当日）')
@click.option('--codes', default=None, help='指定股票，逗号分隔')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，只列目标清单')
@click.pass_context
def sync_ths_finance(ctx, init_mode, as_of, codes, dry_run):
    """同步同花顺财报指标（探路已通过；增量只抓当日披露股）"""
    from src.sync.ths_finance_manager import ThsFinanceManager
    manager = ThsFinanceManager(ctx.obj['config_manager'])
    code_list = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    result = manager.execute(init_mode=init_mode, codes=code_list, as_of=as_of, dry_run=dry_run)
    if result.get('skipped_disabled'):
        click.echo(f"\n⚠ THS 财报配置停用（待补）: {result['errors']}")
        return
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} THS 财报（{stats['mode']}）")
    click.echo(f"  - 目标 {stats['targets']} 只，成功 {stats['stocks_ok']}，期数行 {stats['rows']}")
    if stats['failed']:
        click.echo(f"  - 失败: {stats['failed'][:20]}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-lhb')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省当日；17:30 后发布）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_lhb(ctx, trade_date, dry_run):
    """同步龙虎榜（沪深交易所官网，含席位明细）"""
    from src.sync.lhb_manager import LhbManager
    manager = LhbManager(ctx.obj['config_manager'])
    result = manager.execute(trade_date=trade_date, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 龙虎榜（{stats['trade_date']}）"
               f"深 {stats['szse_stocks']}/沪 {stats['sse_stocks']} 条，"
               f"席位明细 {stats['detail_rows']}")
    for err in result['errors']:
        click.echo(f"  - {err}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-margin-trade')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省 T-1，交易所次日盘前发布）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_margin_trade(ctx, trade_date, dry_run):
    """同步融资融券（沪深交易所官网，T-1 全市场明细）"""
    from src.sync.margin_manager import MarginManager
    manager = MarginManager(ctx.obj['config_manager'])
    result = manager.execute(trade_date=trade_date, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 两融（数据日期 {stats['data_date']}，T-1）"
               f"深 {stats['szse_rows']}/沪 {stats['sse_rows']} 行")
    for err in result['errors']:
        click.echo(f"  - {err}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-concept')
@click.option('--dry-run', is_flag=True, default=False, help='试运行')
@click.pass_context
def sync_concept(ctx, dry_run):
    """同步概念板块成分（新浪概念接口，2026-09-28 换源；原 THS 翻页被拦弃用）"""
    from src.sync.concept_manager import ConceptManager
    manager = ConceptManager(ctx.obj['config_manager'])
    result = manager.execute(dry_run=dry_run)
    stats = result['stats']
    if result.get('skipped_disabled'):
        for err in result['errors']:
            click.echo(f"  - {err}")
    else:
        click.echo(f"\n{'✓' if result['success'] else '✗'} 概念板块（新浪口径 sina_gn）"
                   f"清单 {stats.get('nodes_total', 0)} 个（抓全 {stats.get('nodes_ok', 0)}），"
                   f"在册成分 {stats.get('members_active', 0)}"
                   f"（新进 {stats.get('members_new', 0)}，退出 {stats.get('members_out', 0)}）")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-industry')
@click.option('--dry-run', is_flag=True, default=False, help='试运行')
@click.pass_context
def sync_industry(ctx, dry_run):
    """同步行业分类（新浪行业板块，source=sina_hy；补齐 base_stock_info 行业字段缺失缺口）"""
    from src.sync.industry_manager import IndustryManager
    manager = IndustryManager(ctx.obj['config_manager'])
    result = manager.execute(dry_run=dry_run)
    stats = result['stats']
    if result.get('skipped_disabled'):
        for err in result['errors']:
            click.echo(f"  - {err}")
    else:
        click.echo(f"\n{'✓' if result['success'] else '✗'} 行业分类（新浪口径 sina_hy）"
                   f"清单 {stats.get('nodes_total', 0)} 个（抓全 {stats.get('nodes_ok', 0)}），"
                   f"在册成分 {stats.get('members_active', 0)}"
                   f"（新进 {stats.get('members_new', 0)}，退出 {stats.get('members_out', 0)}）")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-unlock-calendar')
@click.option('--as-of', default=None, help='yyyyMMdd（缺省当日）')
@click.option('--days-back', type=int, default=None,
              help='回看天数（缺省取 config review_sync.unlock.days_back=3；'
                   '首次回补建议 --days-back=35）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行（只检索+过滤，不下载不落库）')
@click.pass_context
def sync_unlock_calendar(ctx, as_of, days_back, dry_run):
    """同步解禁日历（巨潮解禁公告+PDF 正文解析，2026-09-28 换源；原 THS 页 404 弃用）"""
    from src.sync.unlock_manager import UnlockManager
    manager = UnlockManager(ctx.obj['config_manager'])
    result = manager.execute(as_of=as_of, days_back=days_back, dry_run=dry_run)
    stats = result['stats']
    if result.get('skipped_disabled'):
        for err in result['errors']:
            click.echo(f"  - {err}")
    elif dry_run:
        click.echo(f"\n✓ 解禁公告试运行（窗口 {stats['window']}）: "
                   f"检索去重 {stats['deduped']}，标题过滤后 {stats['kept']} 条")
    else:
        click.echo(f"\n{'✓' if result['success'] else '✗'} 解禁日历（窗口 {stats['window']}）")
        click.echo(f"  - 漏斗: 检索去重 {stats['deduped']} → 过滤 {stats['kept']} → "
                   f"下载 {stats['downloaded']} → 解析入库 {stats['rows']}")
        if stats['parse_failed']:
            click.echo(f"  - 解析失败 {len(stats['parse_failed'])} 条（如实不入库）")
    if result['blocked']:
        click.echo("  - ⚠ 巨潮风控急停（游标未推进，稍后重跑自动补）")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-overseas-market')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省当日，美股为 T-1 收盘）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_overseas_market(ctx, trade_date, dry_run):
    """同步外围市场（新浪：美股指数/汇率）"""
    from src.sync.overseas_manager import OverseasManager
    manager = OverseasManager(ctx.obj['config_manager'])
    result = manager.execute(trade_date=trade_date, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 外围市场（{stats['trade_date']}）"
               f"成功 {stats['markets_ok']}/{stats['markets_total']}")
    if stats['missing']:
        click.echo(f"  - 缺失（待补）: {stats['missing']}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('calc-stock-valuation')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省当日）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def calc_stock_valuation(ctx, trade_date, dry_run):
    """自算个股 PE/PB（本地计算：财报+股本+收盘价，无网络请求）"""
    from src.sync.valuation_calc_manager import ValuationCalcManager
    manager = ValuationCalcManager(ctx.obj['config_manager'])
    result = manager.execute(trade_date=trade_date, dry_run=dry_run)
    stats = result['stats']
    if result['success']:
        click.echo(f"\n✓ PE/PB 自算（{stats['trade_date']}）: 全市场 {stats['stocks']} 只，"
                   f"PE 可算 {stats['pe_ok']}，PB 可算 {stats['pb_ok']}，输入不足 {stats['insufficient']}")
    else:
        click.echo(f"\n✗ PE/PB 自算失败: {result['errors']}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


# CLI 注册清单（main.py 汇总 add_command）
review_commands = [
    sync_review_data,
    sync_index_kline,
    sync_adjust_factor,
    sync_money_flow,
    sync_announcement,
    sync_disclosure_calendar,
    sync_index_valuation,
    sync_ths_finance,
    sync_lhb,
    sync_margin_trade,
    sync_industry,
    sync_concept,
    sync_unlock_calendar,
    sync_overseas_market,
    calc_stock_valuation,
]
