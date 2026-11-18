"""
每日复盘数据采集 CLI 子命令（方案 2.6；注册方式照 sync-financial-pdf，延迟 import manager）

命令清单：
  sync-review-all           盘后一键快捷接口（全市场日K增量 + 全部复盘源波次并发，2026-09-29 新增）
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
  sync-ths-industry-l2      申万二级行业成分（THS 公司概况页，2026-10-04 新增）
  sync-overseas-market      外围市场（新浪）
  sync-overseas-kline       外围日K（腾讯 ifzq：港美股指数+港股个股池，2026-10-04 新增）
  calc-stock-valuation      个股 PE/PB 自算（本地计算）
  sync-bj-kline             北交所日K（通达信官网盘后包，P0-2 方案 B，2026-09-30 新增）
  sync-macro-indicator      宏观指标（东财+金十直连，第三阶段-1，2026-10-23 新增）
  sync-option-daily         ETF 期权（上交所官方接口直连，第三阶段-2，2026-10-23 新增）
  sync-north-flow-detail    北向个股明细（已退役：交易所 2024-08-19 起停披露，仅历史留存；F1 巡检豁免）
  review generate           复盘报告生成（六维 MD+HTML，复盘报告层方案 §5.5）
"""

import fcntl
import os
import sys

import click


def _acquire_szse_backfill_lock():
    """深交所回补互斥锁：多客户端并发请求会触发官网 IP 级封禁（2026-10-04 实测）。

    仅历史回补模式使用；日常增量为编排器进程内串行调用，不经过此入口。
    拿不到锁以退出码 2 结束；fd 保持打开，进程退出时锁自动释放。
    """
    fd = os.open('/tmp/szse_backfill.lock', os.O_CREAT | os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        click.echo("✗ 已有深交所回补进程在运行（/tmp/szse_backfill.lock）；"
                   "并发请求会触发官网封 IP，请等待其完成后再试")
        sys.exit(2)
    return fd


@click.command('sync-review-data')
@click.option('--sources', default='all',
              help='逗号分隔子集：index,factor,overseas,overseas_kline,valuation_idx,'
                   'announcement,'
                   'disclosure,lhb,margin,ths_finance,money_flow,valuation_calc,'
                   'sentiment_metrics'
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


@click.command('sync-review-all')
@click.option('--sources', default='all',
              help='逗号分隔子集，缺省 all=全市场日K+12 日频源（周日缺省自动并入周频 '
                   'concept/unlock；显式指定则以显式为准）')
@click.option('--as-of', default=None, help='yyyyMMdd，缺省当日')
@click.option('--skip-kline', is_flag=True, default=False,
              help='日K已单独跑过时跳过日K同步（行为同 sync-review-data）')
@click.option('--dry-run', is_flag=True, default=False,
              help='试运行：日K不同步、门控不生效，只展示执行计划')
@click.pass_context
def sync_review_all(ctx, sources, as_of, skip_kline, dry_run):
    """盘后一键采集：全市场日K增量 + 全部复盘数据源（源间波次并发，日K依赖源后置门控）"""
    from src.sync.review_sync_orchestrator import ReviewSyncOrchestrator
    try:
        orchestrator = ReviewSyncOrchestrator(ctx.obj['config_manager'])
    except ValueError as e:
        click.echo(f"\n✗ 参数错误: {e}")
        return
    kline_mode = '跳过（--skip-kline）' if skip_kline else '内置同步（与独立源并发）'
    click.echo(f"开始盘后一键采集（as_of={as_of or '当日'}, sources={sources}, 日K={kline_mode}）...")
    result = orchestrator.execute(sources=sources, as_of=as_of, dry_run=dry_run,
                                  include_kline=not skip_kline)

    if result['kline_check']:
        click.echo(f"  - 日K门控: {result['kline_check']}")
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


@click.command('sync-freshness-check')
@click.pass_context
def sync_freshness_check(ctx):
    """数据新鲜度巡检（F1）：全部日频/周频表 max(日期) vs 最近已收盘交易日，红黄绿灯报告"""
    from src.sync.freshness_check_manager import FreshnessCheckManager
    manager = FreshnessCheckManager(ctx.obj['config_manager'])
    result = manager.execute()
    for r in result['rows']:
        status = r['status']
        marker = '🔴' if status == '红灯' else ('🟡' if status == '黄灯' else ('⚪' if status == '豁免' else '🟢'))
        lag = r.get('lag_days')
        lag_txt = f"{'超前' if (lag or 0) < 0 else '滞后'} {abs(lag)} 天" if lag is not None else '-'
        click.echo(f"  {marker} {r['label']}（{r['table']}）: 最新 {r.get('max_date') or '-'}，"
                   f"{lag_txt}，阈值 {r['max_lag_days']} [{status}]"
                   + (f"（{r.get('detail')}）" if r.get('detail') else ''))
    click.echo(f"\n{'✓' if result['success'] else '✗'} 巡检完成: 红灯 {len(result['red'])} 项 / "
               f"黄灯 {len(result['yellow'])} 项（基准日 {result['base_date']}）")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


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


@click.command('sync-hkex-connect')
@click.option('--start-date', default=None, help='yyyyMMdd 起始（缺省=库内最新+1；空库回补 backfill_days）')
@click.option('--end-date', default=None, help='yyyyMMdd 结束（缺省当日）')
@click.pass_context
def sync_hkex_connect(ctx, start_date, end_date):
    """互联互通日度（北向总额+南向净额；港交所官方日度 JSON，滚动保留约2个月）"""
    from src.sync.hkex_connect_manager import HkexConnectManager
    manager = HkexConnectManager(ctx.obj['config_manager'])
    result = manager.execute(start_date=start_date, end_date=end_date)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 互联互通（{stats['start']}~{stats['end']}）")
    click.echo(f"  - 抓到 {stats['days_fetched']} 个交易日 / 空日 {stats['days_empty']}；入库 {stats['rows']} 行；耗时 {stats['duration']}s")
    click.echo('  - 口径：北向仅成交总额（禁止解读净额）；南向净买入=Buy−Sell')
    for e in result['errors'][:3]:
        click.echo(f'  - 错误: {e}')
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-etf-scale')
@click.option('--start-date', default=None, help='yyyyMMdd（上交所 STAT_DATE 回补起点；缺省=库内最新+1，空库回补 250 日）')
@click.option('--end-date', default=None, help='yyyyMMdd（缺省当日）')
@click.pass_context
def sync_etf_scale(ctx, start_date, end_date):
    """ETF 份额日度（沪深两所官方 API；单位万份；深交所仅当日快照）"""
    from src.sync.etf_scale_manager import EtfScaleManager
    manager = EtfScaleManager(ctx.obj['config_manager'])
    result = manager.execute(start_date=start_date, end_date=end_date)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} ETF 份额（{stats['start']}~{stats['end']}）")
    click.echo(f"  - 上交所 {stats['sse_days']} 日 {stats['sse_rows']} 行；深交所快照 {stats['szse_rows']} 行；耗时 {stats['duration']}s")
    click.echo('  - 口径：份额万份；日变化=申赎方向代理')
    for e in stats.get('errors', [])[:3]:
        click.echo(f'  - 错误: {e}')
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-news-telegraph')
@click.option('--pages', default=None, type=int, help='抓取页数（缺省=增量追平游标，上限 config max_pages）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行：不请求不写库')
@click.pass_context
def sync_news_telegraph(ctx, pages, dry_run):
    """市场要闻（四期方案 §4）：新浪 7x24 快讯 → news_telegraph（增量游标幂等）"""
    from src.sync.news_telegraph_manager import NewsTelegraphManager
    manager = NewsTelegraphManager(ctx.obj['config_manager'])
    result = manager.execute(pages=pages, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 市场要闻（新浪 7x24，dry_run={dry_run}）")
    click.echo(f"  - 页数 {stats['pages']}，条目 {stats['items']}，新入库 {stats['rows']}；"
               f"游标 {stats['cursor_before'] or '—'} → {stats['cursor_after'] or '—'}；"
               f"耗时 {result.get('duration', 0):.1f}s")
    click.echo('  - 口径：新闻归因是市场叙事，不是核验过的因果；财联社签名接口按合规红线不采')
    for e in result.get('errors', [])[:3]:
        click.echo(f'  - 错误: {e}')
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-money-flow')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省当日；历史日走历史分笔路径补抓）')
@click.option('--codes', default=None, help='指定股票（跳过异动子集筛选）')
@click.option('--full-market/--no-full-market', default=None,
              help='覆盖 config full_market（P2 全市场扩面；缺省用配置值）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行：只输出异动子集清单')
@click.option('--yes', is_flag=True, default=False, help='确认继续（15:30 前缺省当日会被拦）')
@click.pass_context
def sync_money_flow(ctx, trade_date, codes, full_market, dry_run, yes):
    """同步分笔资金流（P2 全市场扩面后为全池；口径：分笔分桶）"""
    from src.sync.money_flow_manager import MoneyFlowManager
    # F14（方案 V1.0 §F14）：缺省日期陷阱保护——补历史必须显式 --trade-date
    # （2026-11-18 实测：不带 --trade-date 默认当日，289 只历史补抓全失败）
    if not trade_date:
        from datetime import datetime as _dt
        if _dt.now().time() < _dt.strptime('15:30', '%H:%M').time() and not yes:
            click.echo("✗ 未传 --trade-date 且当前未收盘（15:30 前）：缺省'当日'将采不到数据。"
                       "补历史请显式 --trade-date yyyyMMdd；确要采当日请加 --yes。")
            ctx.exit(2)
    manager = MoneyFlowManager(ctx.obj['config_manager'])
    if full_market is not None:
        manager.full_market = bool(full_market)
    code_list = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    try:
        result = manager.execute(trade_date=trade_date, codes=code_list, dry_run=dry_run)
    except Exception as e:
        click.echo(f"\n✗ 分笔资金流失败: {e}")
        return
    stats = result['stats']
    sub = stats.get('subset', {})
    click.echo(f"\n{'✓' if result['success'] else '✗'} 分笔资金流（{stats['trade_date']}）")
    if sub.get('full_market'):
        click.echo(f"  - 全市场模式: 快照 {sub.get('snapshot_total', 0)} -> 选中 {sub.get('selected', 0)} 只")
    else:
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
@click.option('--start-date', default=None, help='回补区间起 yyyyMMdd（与 --end-date 同给走回补模式）')
@click.option('--end-date', default=None, help='回补区间止 yyyyMMdd（含当日）')
@click.option('--category', default='', help="巨潮分类过滤：periodic=四类定期报告（披露日回补），空=全量")
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库不推游标')
@click.pass_context
def sync_announcement(ctx, as_of, days_back, start_date, end_date, category, dry_run):
    """同步公告清单 + 业绩预告（巨潮全市场窗口检索，同请求双用途）

    显式区间模式（开发项 K 披露日回补）：--start-date/--end-date 同给时按周切窗回补，
    不推进游标、只写一份汇总报告，结束后触发披露日 COALESCE 回填。
    """
    from src.sync.announcement_manager import AnnouncementManager
    manager = AnnouncementManager(ctx.obj['config_manager'])
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    if start_date:
        result = manager.backfill_range(start_date, end_date, category=category, dry_run=dry_run)
        stats = result['stats']
        click.echo(f"\n{'✓' if result['success'] else '✗'} 公告历史回补"
                   f"（{stats['range']}，category={stats['category']}）")
        click.echo(f"  - 窗口 {stats['windows_done']}/{stats['windows_total']} 完成，"
                   f"公告检索 {stats['announcements']} 条入库 {stats['announcements_new']}，"
                   f"业绩预告 {stats['forecast_rows']} 条")
        if stats.get('disclosure_backfill_rows') is not None:
            click.echo(f"  - 披露日 COALESCE 回填: {stats['disclosure_backfill_rows']} 行")
        if result['blocked']:
            click.echo("  - ⚠ 巨潮风控急停（已完成窗口已入库，重跑自动续）")
        if result['errors']:
            click.echo(f"  - 错误 {len(result['errors'])} 条（详见回补报告）")
        if result.get('report_path'):
            click.echo(f"  - 报告: {result['report_path']}")
        return
    result = manager.execute(as_of=as_of, days_back=days_back, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 公告采集（窗口 {stats['window']}）")
    click.echo(f"  - 公告 {stats['announcements']} 条入库 {stats['announcements_new']}，"
               f"业绩预告 {stats['forecast_rows']} 条")
    if result['blocked']:
        click.echo("  - ⚠ 巨潮风控急停（游标未推进，稍后重跑自动补）")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('reclassify-announcement')
@click.option('--days-back', type=int, default=7, help='重分类近 N 日已入库行（默认 7）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，只统计变更不写库')
@click.pass_context
def reclassify_announcement(ctx, days_back, dry_run):
    """按当前词表重跑近 N 日公告分类（开发项 H；纯本地仅 update ann_type，幂等可重跑）"""
    from src.sync.announcement_manager import AnnouncementManager
    manager = AnnouncementManager(ctx.obj['config_manager'])
    result = manager.reclassify(days_back=days_back, dry_run=dry_run)
    stats = result['stats']
    other = stats['other_ratio']
    other_txt = f"{other:.1%}" if other is not None else '—'
    click.echo(f"\n{'✓' if result['success'] else '✗'} 公告重分类（{stats['window']}）"
               f"行数 {stats['rows']}，变更 {stats['changed']} 行，\"其他\"占比 {other_txt}")
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
@click.option('--init-perf', is_flag=True, default=False,
              help='历史回补：index-perf API 按年切窗拉全历史日K（N2，分钟级；'
                   '缺省模式含每日 perf 增量，无需手动回补）')
@click.pass_context
def sync_index_valuation(ctx, dry_run, init_perf):
    """同步指数估值 PE（中证官网每日 indicator 文件）+ 指数历史日K增量（index-perf）"""
    from src.sync.index_valuation_manager import IndexValuationManager
    manager = IndexValuationManager(ctx.obj['config_manager'])
    if init_perf:
        if not manager.perf_indices:
            click.echo("\n✗ config review_sync.csindex_perf_indices 未配置，无可回补指数")
            return
        click.echo(f"开始指数历史日K回补（指数 "
                   f"{[i.get('name') or i['code'] for i in manager.perf_indices]}）...")
    result = manager.execute(dry_run=dry_run, init_perf=init_perf)
    stats = result['stats']
    if init_perf:
        click.echo(f"\n{'✓' if result['success'] else '✗'} 指数历史日K回补"
                   f"（成功 {stats['perf_ok']}/{stats['perf_indices_total']}，"
                   f"行数 {stats['perf_rows']}）")
        if result['errors']:
            click.echo(f"  - 错误: {result['errors']}")
    else:
        click.echo(f"\n{'✓' if result['success'] else '✗'} 指数估值"
                   f"（成功 {stats['indices_ok']}/{stats['indices_total']}，行数 {stats['rows']}）")
        if stats['missing']:
            click.echo(f"  - 未发布: {stats['missing']}")
        if stats.get('perf_rows') is not None and stats.get('perf_indices_total'):
            click.echo(f"  - 指数日K增量: 成功 {stats['perf_ok']}/"
                       f"{stats['perf_indices_total']}，行数 {stats['perf_rows']}")
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
@click.option('--start-date', default=None, help='回填区间起 yyyyMMdd（与 --end-date 同给走回填模式）')
@click.option('--end-date', default=None, help='回填区间止 yyyyMMdd（含当日）')
@click.option('--daily-reports', is_flag=True, default=False,
              help='调试用：回填模式逐日落盘报告（默认抑制，只写一份汇总，X-19）')
@click.option('--yes', is_flag=True, default=False, help='确认继续（15:30 前缺省当日会被拦）')
@click.pass_context
def sync_lhb(ctx, trade_date, start_date, end_date, daily_reports, dry_run, yes):
    """同步龙虎榜（沪深交易所官网，含席位明细）

    历史回填模式（开发项 C）：--start-date/--end-date 同给时按交易日历逐日回填，
    断点续跑只信查库；上交所回补限速提至 3s（X-5，config lhb_backfill_interval 可覆盖）。
    """
    from src.sync.lhb_manager import LhbManager
    manager = LhbManager(ctx.obj['config_manager'])
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    # F14（方案 V1.0 §F14）：龙虎榜缺省只抓当日（17:30 后发布），补历史必须显式日期
    if not trade_date and not start_date:
        from datetime import datetime as _dt
        if _dt.now().time() < _dt.strptime('15:30', '%H:%M').time() and not yes:
            click.echo("✗ 未传 --trade-date/--start-date 且当前未收盘（15:30 前）：缺省'当日'"
                       "将采不到数据（17:30 后发布）。补历史请显式指定日期；确要采当日请加 --yes。")
            ctx.exit(2)
    if start_date:
        if not dry_run:
            _acquire_szse_backfill_lock()
        result = manager.backfill_range(start_date, end_date, dry_run=dry_run,
                                        suppress_report=not daily_reports)
        stats = result['stats']
        click.echo(f"\n{'✓' if result['success'] else '✗'} 龙虎榜回填（{stats['range']}）"
                   f"交易日 {stats['days_total']}：回填 {stats['days_done']}、"
                   f"跳过 {stats['days_skipped']}、失败 {len(stats['days_failed'])}；"
                   f"榜单 {stats['lhb_rows']} 行 / 席位 {stats['detail_rows']} 行")
        if result['blocked']:
            click.echo("  - ⚠ 风控急停（已完成日已入库，重跑自动续）")
        if stats['days_failed']:
            click.echo(f"  - 失败日期: {[f['date'] for f in stats['days_failed'][:30]]}")
        if result.get('report_path'):
            click.echo(f"  - 报告: {result['report_path']}")
        return
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
@click.option('--start-date', default=None, help='回填区间起 yyyyMMdd（与 --end-date 同给走回填模式）')
@click.option('--end-date', default=None, help='回填区间止 yyyyMMdd（含当日）')
@click.option('--daily-reports', is_flag=True, default=False,
              help='调试用：回填模式逐日落盘报告（默认抑制，只写一份汇总，X-19）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_margin_trade(ctx, trade_date, start_date, end_date, daily_reports, dry_run):
    """同步融资融券（沪深交易所官网，T-1 全市场明细）

    历史回填模式（开发项 C）：--start-date/--end-date 同给时按交易日历逐日回填，
    断点续跑只信查库（X-14：回看机制会静默采到 d-1，不信任单次 stats）。
    """
    from src.sync.margin_manager import MarginManager
    manager = MarginManager(ctx.obj['config_manager'])
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    if start_date:
        if not dry_run:
            _acquire_szse_backfill_lock()
        result = manager.backfill_range(start_date, end_date, dry_run=dry_run,
                                        suppress_report=not daily_reports)
        stats = result['stats']
        click.echo(f"\n{'✓' if result['success'] else '✗'} 两融回填（{stats['range']}）"
                   f"交易日 {stats['days_total']}：回填 {stats['days_done']}、"
                   f"跳过 {stats['days_skipped']}、失败 {len(stats['days_failed'])}；"
                   f"累计 {stats['rows']} 行")
        if stats['date_notes']:
            click.echo(f"  - 数据日期核对（请求日≠数据日，回看行为）{len(stats['date_notes'])} 日"
                       f"（详见回填报告）")
        if result['blocked']:
            click.echo("  - ⚠ 风控急停（已完成日已入库，重跑自动续）")
        if stats['days_failed']:
            click.echo(f"  - 失败日期: {[f['date'] for f in stats['days_failed'][:30]]}")
        if result.get('report_path'):
            click.echo(f"  - 报告: {result['report_path']}")
        return
    result = manager.execute(trade_date=trade_date, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 两融（数据日期 {stats['data_date']}，T-1）"
               f"深 {stats['szse_rows']}/沪 {stats['sse_rows']} 行"
               f"（实际数据日 深 {stats.get('szse_date')} / 沪 {stats.get('sse_date')}）")
    for err in result['errors']:
        click.echo(f"  - {err}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-seal-stats')
@click.option('--start-date', required=True, help='回填区间起 yyyyMMdd（如 20251001）')
@click.option('--end-date', required=True, help='回填区间止 yyyyMMdd（含当日）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_seal_stats(ctx, start_date, end_date, dry_run):
    """封板指标历史回补（开发项 F：涨停∪炸板∪跌停子集，分笔自算首封/开板）

    断点续跑按 limit_seal_stats_daily 逐股粒度跳过；历史抓取含 X-4 预热重试与
    X-9 断连等待；约 250 交易日 × ~90 只/日 ≈ 2.2 万次请求，分晚执行。
    """
    from src.sync.money_flow_manager import MoneyFlowManager
    manager = MoneyFlowManager(ctx.obj['config_manager'])
    result = manager.backfill_seal_stats(start_date, end_date, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 封板指标回补（{stats['range']}）"
               f"交易日 {stats['days_total']}：回填 {stats['days_done']}、"
               f"无子集/已完成 {stats['days_no_subset']}；"
               f"seal {stats['seal_rows']} 行 / money_flow {stats['money_rows']} 行，"
               f"个股失败 {stats['stocks_failed']}")
    if result['errors']:
        click.echo(f"  - 错误: {result['errors']}")
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


@click.command('sync-ths-industry-l2')
@click.option('--init', 'init_mode', is_flag=True, default=False,
              help='全量初始化（全市场 ~5,300 只 × 2s 限速 ≈ 3 小时，建议 nohup；'
                   '缺省增量：仅新上市 + 在册缺失）')
@click.option('--codes', default=None, help='指定股票，逗号分隔（调试用）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，只列目标清单不写库')
@click.pass_context
def sync_ths_industry_l2(ctx, init_mode, codes, dry_run):
    """同步申万二级行业成分（THS 公司概况页"所属申万行业"，N3；当前快照口径）"""
    from src.sync.ths_industry_l2_manager import ThsIndustryL2Manager
    manager = ThsIndustryL2Manager(ctx.obj['config_manager'])
    code_list = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    result = manager.execute(init_mode=init_mode, codes=code_list, dry_run=dry_run)
    if result.get('skipped_disabled'):
        click.echo(f"\n⚠ 申万二级行业配置停用: {result['errors']}")
        return
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 申万二级行业（{stats['mode']}）"
               f"目标 {stats['targets']} 只，成功 {stats['ok']}"
               f"（二级 {stats['concepts']} 个），无申万分类 {len(stats['missing'])}")
    if stats['failed']:
        click.echo(f"  - 失败 {len(stats['failed'])} 只（次轮增量自动补）: {stats['failed'][:20]}")
    if result['blocked']:
        click.echo("  - ⚠ 同花顺风控急停（已完成批次已入库，重跑自动续）")
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


@click.command('sync-overseas-kline')
@click.option('--init', 'init_mode', is_flag=True, default=False,
              help='全量初始化：近 10 年分窗拉历史（4 请求/代码，~5 分钟，建议 nohup）')
@click.option('--codes', default=None, help='指定代码，逗号分隔（如 hkHSI,hk00700；调试/补采）')
@click.option('--start-date', default=None, help='区间补采起 yyyyMMdd（与 --end-date 同给走回补模式）')
@click.option('--end-date', default=None, help='区间补采止 yyyyMMdd（含当日）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行：只列目标清单与请求计划')
@click.pass_context
def sync_overseas_kline(ctx, init_mode, codes, start_date, end_date, dry_run):
    """同步外围日K（腾讯 ifzq：港股3指数+美股3指数+港股个股池；市场自身交易日历）"""
    from src.sync.overseas_kline_manager import OverseasKlineManager
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    manager = OverseasKlineManager(ctx.obj['config_manager'])
    code_list = [c.strip() for c in codes.split(',') if c.strip()] if codes else None
    try:
        result = manager.execute(init_mode=init_mode, codes=code_list,
                                 start_date=start_date, end_date=end_date, dry_run=dry_run)
    except ValueError as e:
        click.echo(f"\n✗ 参数错误: {e}")
        return
    if dry_run:
        plan = result.get('plan') or {}
        click.echo(f"\n✓ 试运行计划（{plan.get('plan', {}).get('mode')}，"
                   f"预计请求 {plan.get('plan', {}).get('requests')} 次）:")
        for t in plan.get('targets', []):
            a = f"，a_code={t['a_code']}" if t.get('a_code') else ''
            click.echo(f"  - {t['code']} {t['name']}（{t['type']}，{t['adj_type']}{a}）"
                       f"库内最新: {t['latest_in_db'] or '无'}")
        for w in plan.get('plan', {}).get('windows', []):
            click.echo(f"  - 窗口: {w}")
        return
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 外围日K（{stats['mode']}）"
               f"代码 {stats['codes_ok']}/{stats['codes_total']} 成功，入库 {stats['rows']} 行"
               f"（解析跳过 {stats['skipped_rows']}）")
    if stats['failed']:
        click.echo(f"  - 失败: {stats['failed']}")
    if stats['qfq_refresh']:
        click.echo(f"  - qfq 重述全历史重拉: {stats['qfq_refresh']}")
    if stats['frozen_suspect']:
        click.echo(f"  - ⚠ 疑似冻结: {stats['frozen_suspect']}")
    for w in result.get('warnings', []):
        click.echo(f"  - ⚠ {w}")
    if result['blocked']:
        click.echo("  - ⚠ 风控急停（已完成代码已入库，稍后重跑自动续）")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('calc-stock-valuation')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省当日）')
@click.option('--start-date', default=None, help='回填区间起 yyyyMMdd（与 --end-date 同给走历史回填模式）')
@click.option('--end-date', default=None, help='回填区间止 yyyyMMdd（含当日）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def calc_stock_valuation(ctx, trade_date, start_date, end_date, dry_run):
    """自算个股 PE/PB（本地计算：财报+股本+收盘价，无网络请求）

    历史回填模式（开发项 A）：--start-date/--end-date 同给时按交易日历逐日 as-of 回填；
    前置依赖开发项 K（披露日回补），否则 as-of 过滤会筛空历史财报。
    """
    from src.sync.valuation_calc_manager import ValuationCalcManager
    manager = ValuationCalcManager(ctx.obj['config_manager'])
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    result = manager.execute(trade_date=trade_date, dry_run=dry_run,
                             start_date=start_date, end_date=end_date)
    stats = result['stats']
    if result.get('backfill'):
        if result['success']:
            click.echo(f"\n✓ PE/PB 历史回填（{stats['start_date']}~{stats['end_date']}）: "
                       f"交易日 {stats['days_total']}，回填 {stats['days_done']}、"
                       f"断点跳过 {stats['days_skipped_done']}、无K线 {stats['days_no_kline']}；"
                       f"累计 {stats['rows']} 行（PE 可算 {stats['pe_ok']}）")
        else:
            click.echo(f"\n✗ PE/PB 历史回填失败: {result['errors']}")
    elif result['success']:
        click.echo(f"\n✓ PE/PB 自算（{stats['trade_date']}）: 全市场 {stats['stocks']} 只，"
                   f"PE 可算 {stats['pe_ok']}，PB 可算 {stats['pb_ok']}，输入不足 {stats['insufficient']}")
    else:
        click.echo(f"\n✗ PE/PB 自算失败: {result['errors']}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-bj-kline')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省当日）')
@click.option('--days-back', default=0, type=int, help='自起始日再向前回补的交易日数（回补历史用）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行：只解析目标交易日清单，不取包不写库')
@click.pass_context
def sync_bj_kline(ctx, trade_date, days_back, dry_run):
    """北交所日K（通达信官网盘后包，P0-2 方案 B）+ 沪深对拍审计"""
    from src.sync.bj_kline_manager import BjKlineManager
    manager = BjKlineManager(ctx.obj['config_manager'])
    if dry_run:
        dates = manager._resolve_trade_dates(trade_date, days_back)
        click.echo(f"dry-run：目标交易日（{len(dates)} 天）: {dates}")
        click.echo("  - 每交易日 1 次盘后包请求（间隔见 review_sync.tdx_official_interval）")
        return
    click.echo(f"开始北交所日K同步（trade_date={trade_date or '当日'}, days_back={days_back}）...")
    result = manager.execute(trade_date=trade_date, days_back=days_back, dry_run=False)
    stats = result['stats']
    if result['success']:
        click.echo(f"\n✓ 北交所日K同步完成: {stats['days']} 天入库 {stats['rows']} 行，"
                   f"包未发布跳过 {stats['unpublished']} 天")
    else:
        click.echo(f"\n✗ 北交所日K同步失败: {result['errors']}")
    click.echo(f"  - 对拍差异股数: {stats['diff_stocks']}，僵尸码疑似: {stats['zombie_stocks']}")
    if result.get('report_path'):
        click.echo(f"  - 审计报告: {result['report_path']}")


# ------------------------------------------------ 复盘报告生成命令组（§5.5）
@click.group('review')
def review_group():
    """复盘报告层（report generate：六维报告 MD+HTML，只读）"""


@review_group.command('generate')
@click.option('--as-of', default=None, help='锚定日 yyyyMMdd，默认取库内最新交易日')
@click.option('--output-dir', default=None, help='输出目录（覆盖配置 review.output_dir）')
@click.pass_context
def review_generate(ctx, as_of, output_dir):
    """生成每日复盘报告（MD+HTML；取数→六维聚合→渲染→落盘）"""
    from src.review import ReviewReportGenerator
    click.echo(f"开始生成复盘报告（as_of={as_of or '库内最新交易日'}）...")
    generator = ReviewReportGenerator(ctx.obj['config_manager'])
    try:
        result = generator.generate(as_of=as_of, output_dir=output_dir)
    except ValueError as e:
        click.echo(f"\n✗ 生成失败: {e}")
        sys.exit(1)
    except Exception as e:  # 未预期异常保持退出码非 0
        click.echo(f"\n✗ 生成异常: {e!r}")
        sys.exit(1)
    click.echo(f"\n✓ 报告已生成（耗时 {result['duration_seconds']:.1f} 秒）:")
    click.echo(f"  - MD  : {result['md_path']}")
    click.echo(f"  - HTML: {result['html_path']}")
    click.echo(f"  - 元信息: {result['meta_path']}")


@review_group.command('digest')
@click.option('--as-of', default=None, help='锚定日 yyyyMMdd，默认取库内最新交易日')
@click.option('--interpretation', 'interpretation', default=None,
              help='解读文本 md 路径（复盘 agent 撰写；缺省先试约定路径 '
                   '<output_dir>/daily_review_<锚定日>_digest_note.md）')
@click.option('--output-dir', default=None, help='输出目录（覆盖配置 review.output_dir）')
@click.pass_context
def review_digest(ctx, as_of, interpretation, output_dir):
    """生成图文并茂复盘解读 HTML（SVG 图表+解读文本，自包含可离线打开）"""
    from src.review.digest import build_digest
    click.echo(f"开始生成复盘解读 HTML（as_of={as_of or '库内最新交易日'}）...")
    try:
        result = build_digest(ctx.obj['config_manager'], as_of=as_of,
                              interpretation_path=interpretation,
                              output_dir=output_dir)
    except ValueError as e:
        click.echo(f"\n✗ 生成失败: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"\n✗ 生成异常: {e!r}")
        sys.exit(1)
    interp = '已装配解读文本' if result['has_interpretation'] else '未附解读文本（占位）'
    click.echo(f"\n✓ 解读 HTML 已生成（耗时 {result['duration_seconds']:.1f} 秒，{interp}）:")
    click.echo(f"  - HTML: {result['html_path']}")


@review_group.command('backfill-sentiment')
@click.option('--start', 'start_date', default=None,
              help='回填区间起 yyyyMMdd（全量历史如 20210101；缺省=最近 10 个交易日增量）')
@click.option('--end', 'end_date', default=None, help='回填区间止 yyyyMMdd（缺省=库内最新）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def review_backfill_sentiment(ctx, start_date, end_date, dry_run):
    """回填情绪指标日频表（开发项 B；与报告层逐字节同口径，幂等可重跑）"""
    from src.sync.sentiment_metrics_manager import SentimentMetricsManager
    manager = SentimentMetricsManager(ctx.obj['config_manager'])
    result = manager.execute(start_date=start_date, end_date=end_date, dry_run=dry_run)
    stats = result['stats']
    mode = '显式区间' if start_date else '缺省增量（最近 10 个交易日）'
    if result['success']:
        click.echo(f"\n✓ 情绪指标回填（{mode}，{stats['range']}）: "
                   f"交易日 {stats['days_total']}，回填 {stats['days_done']}，"
                   f"无K线 {stats['days_no_kline']}")
    else:
        click.echo(f"\n✗ 情绪指标回填失败: {result['errors']}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@review_group.command('calibrate-sentiment')
@click.option('--start', 'start_date', required=True, help='样本区间起 yyyyMMdd（须已回填，如 20210101）')
@click.option('--end', 'end_date', required=True, help='样本区间止 yyyyMMdd')
@click.pass_context
def review_calibrate_sentiment(ctx, start_date, end_date):
    """情绪判据校准（开发项 B；只读产出阈值提案，定稿由用户改 config）"""
    from src.review.sentiment_calibration import run_calibration
    click.echo(f"开始校准（{_plain(start_date)}~{_plain(end_date)}，只读）...")
    try:
        result = run_calibration(ctx.obj['config_manager'], start_date, end_date)
    except ValueError as e:
        click.echo(f"\n✗ 校准失败: {e}")
        sys.exit(1)
    click.echo(f"\n✓ 校准提案已产出（样本 {result['samples']} 天；只读，未改配置）:")
    click.echo(f"  - 报告: {result['report_path']}")


def _plain(v):
    return str(v).replace('-', '')[:8]


@click.command('sync-macro-indicator')
@click.option('--start-date', default=None, help='回填区间起 yyyyMMdd（与 --end-date 同给走回填模式）')
@click.option('--end-date', default=None, help='回填区间止 yyyyMMdd（含当日）')
@click.option('--indicators', default=None,
              help='指标 code 列表逗号分隔（缺省 config review_sync.macro.indicators 全量）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_macro_indicator(ctx, start_date, end_date, indicators, dry_run):
    """同步宏观指标（LPR/SHIBOR/PMI/M1/M2/社融/CPI/PPI；东财+金十直连）

    低频指标整段拉取按发布日过滤；日/周跑一次足矣，不进 sync-review-all。
    区间回填模式：--start-date/--end-date 同给（断点查库跳过已有）。
    """
    from src.sync.macro_manager import MacroManager
    manager = MacroManager(ctx.obj['config_manager'])
    codes = ([c.strip() for c in indicators.split(',') if c.strip()]
             if indicators else None)
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    if start_date:
        result = manager.backfill_range(start_date, end_date, dry_run=dry_run)
        stats = result['stats']
        total = stats.pop('_total_rows', 0)
        click.echo(f"\n{'✓' if result['success'] else '✗'} 宏观指标回填"
                   f"（{start_date}~{end_date}）累计 {total} 行")
        for code, st in stats.items():
            click.echo(f"  - {code}: {st['status']} rows={st.get('rows', 0)}")
        return
    result = manager.execute(dry_run=dry_run, indicators=codes)
    click.echo(f"\n{'✓' if result['success'] else '✗'} 宏观指标同步")
    for code, st in result['stats'].items():
        line = f"  - {code}: {st['status']}"
        if st.get('latest_date'):
            line += f" latest={st['latest_date']}"
        if st.get('rows'):
            line += f" rows={st['rows']}"
        if st.get('error'):
            line += f" ({st['error']})"
        click.echo(line)
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-option-daily')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省 T-1；SSE 数据 T 日盘后发布）')
@click.option('--start-date', default=None, help='回填区间起 yyyyMMdd（与 --end-date 同给走回填模式）')
@click.option('--end-date', default=None, help='回填区间止 yyyyMMdd（含当日）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_option_daily(ctx, trade_date, start_date, end_date, dry_run):
    """同步 ETF 期权（上交所官方接口：逐合约 IV/希腊字母 + 逐标的统计 + 合约主数据）

    历史回填模式：--start-date/--end-date 同给按交易日历逐日回填，
    断点续跑只信查库（当日 option_daily 行数 > 0 跳过）。
    """
    from src.sync.option_manager import OptionManager
    manager = OptionManager(ctx.obj['config_manager'])
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    if start_date:
        result = manager.backfill_range(start_date, end_date, dry_run=dry_run)
        stats = result['stats']
        click.echo(f"\n{'✓' if result['success'] else '✗'} ETF 期权回填"
                   f"（{stats['range']}）交易日 {stats['days_total']}：回填 "
                   f"{stats['days_done']}、跳过 {stats['days_skipped']}、失败 "
                   f"{len(stats['days_failed'])}；累计 {stats['risk_rows']} 行")
        if stats['days_failed']:
            click.echo(f"  - 失败日期: {[f['date'] for f in stats['days_failed'][:30]]}")
        if result.get('report_path'):
            click.echo(f"  - 报告: {result['report_path']}")
        return
    result = manager.execute(trade_date=trade_date, dry_run=dry_run)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} ETF 期权（数据日期 "
               f"{stats['trade_date']}）风险指标 {stats['risk_rows']} 行 / "
               f"标的统计 {stats['stats_rows']} 行 / 主数据 {stats['master_rows']} 行")
    if stats['missing']:
        click.echo(f"  - 校验提示: {stats['missing']}")
    for err in result['errors']:
        click.echo(f"  - {err}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


@click.command('sync-north-flow-detail')
@click.option('--trade-date', default=None, help='yyyyMMdd（缺省对库内有持仓股票重拉；披露止于 20240816）')
# F15：源侧已停披露（2024-08-19），本命令退役——仅保留历史数据与 CLI 入口，不再入盘后编排
@click.option('--start-date', default=None, help='回填区间起 yyyyMMdd（与 --end-date 同给走回填模式）')
@click.option('--end-date', default=None, help='回填区间止 yyyyMMdd（含当日）')
@click.option('--symbols', default=None,
              help='股票代码列表逗号分隔（缺省日增量=库内有持仓股票；回填=全部在市股票）')
@click.option('--dry-run', is_flag=True, default=False, help='试运行，不写库')
@click.pass_context
def sync_north_flow_detail(ctx, trade_date, start_date, end_date, symbols, dry_run):
    """同步北向个股明细（东财直连：个股北向持股历史，沪/深双通道聚合）

    口径边界：交易所 2024-08-19 起停止披露个股每日明细，日增量重拉自然无新
    数据（如实记录非故障）；总额口径见 sync-hkex-connect（港交所日统计）。
    """
    from src.sync.hkex_connect_manager import NorthFlowDetailManager
    manager = NorthFlowDetailManager(ctx.obj['config_manager'])
    codes = ([c.strip() for c in symbols.split(',') if c.strip()]
             if symbols else None)
    if bool(start_date) != bool(end_date):
        raise click.UsageError('--start-date 与 --end-date 必须同时给出')
    if start_date:
        result = manager.backfill_range(start_date, end_date, symbols=codes,
                                        dry_run=dry_run)
        stats = result['stats']
        click.echo(f"\n{'✓' if result['success'] else '✗'} 北向个股明细回填"
                   f"（{stats['range']}）股票 {stats['symbols_total']} 只：完成 "
                   f"{stats['symbols_done']}、失败 {len(stats['symbols_failed'])}；"
                   f"累计 {stats['rows']} 行")
        if stats['symbols_failed']:
            click.echo(f"  - 失败股票: {stats['symbols_failed'][:30]}")
        if result.get('report_path'):
            click.echo(f"  - 报告: {result['report_path']}")
        return
    result = manager.execute(trade_date=trade_date, dry_run=dry_run, symbols=codes)
    stats = result['stats']
    click.echo(f"\n{'✓' if result['success'] else '✗'} 北向个股明细同步"
               f"（目标 {stats['requested']} 只，实拉 {stats['pulled']} 只）")
    click.echo(f"  - 库内最新数据日：{stats.get('max_date_before')} → "
               f"{stats.get('max_date_after')}（披露止于 20240816）")
    if stats['failed']:
        click.echo(f"  - 失败股票: {stats['failed'][:20]}")
    if result.get('report_path'):
        click.echo(f"  - 报告: {result['report_path']}")


# CLI 注册清单（main.py 汇总 add_command）
review_commands = [
    sync_review_all,
    sync_review_data,
    sync_freshness_check,
    sync_index_kline,
    sync_adjust_factor,
    sync_hkex_connect,
    sync_etf_scale,
    sync_news_telegraph,
    sync_money_flow,
    sync_seal_stats,
    sync_announcement,
    reclassify_announcement,
    sync_disclosure_calendar,
    sync_index_valuation,
    sync_ths_finance,
    sync_lhb,
    sync_margin_trade,
    sync_industry,
    sync_concept,
    sync_unlock_calendar,
    sync_ths_industry_l2,
    sync_overseas_market,
    sync_overseas_kline,
    calc_stock_valuation,
    sync_bj_kline,
    sync_macro_indicator,
    sync_option_daily,
    sync_north_flow_detail,
]
