"""
数据同步管理器
"""

import time
import logging
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..config import ConfigManager
from ..data_sources import TdxApiSource
from ..database import DatabaseConnection
from .csv_writer import CsvWriter
from ..utils.data_transformer import DataTransformer
from ..utils.kline_normalize import (
    normalize_date_str,
    normalize_time_str,
    split_datetime,
    to_float,
    scale_price,
    scale_amount,
    scale_volume,
    get_kline_limit_rate,
    build_fundamentals_records_map,
    match_fundamentals,
)
from ..kline_1min.adapters import (
    CsvAsyncWriter,
    DatabaseKline1MinPartitionAdapter,
    DatabaseKline1MinRepository,
    FundamentalsPortAdapter,
    ReportPortAdapter,
    StockPortAdapter,
    TdxSourceAdapter,
    TradeCalendarPortAdapter
)
from ..kline_1min.usecase import Kline1MinSyncUseCase
from .kline_rise_25pre import generate_kline_rise_25pre
from .report_writer import write_markdown_report
from .kline_day_pipeline import KlineDaySharedState, KlineDayDeps, RawFallbackBudget, process_one_stock, safe_process_one_stock, merge_stock_result

class SyncManager:
    """数据同步管理器"""

    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化同步管理器

        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager or ConfigManager()
        self.db_conn = DatabaseConnection(self.config_manager)
        self.csv_writer = CsvWriter(self.config_manager)
        self.logger = logging.getLogger(__name__)

        # 初始化数据源
        self.tdx_api_source = None

        self._init_data_sources()

    def _init_data_sources(self) -> None:
        """初始化数据源（baostock 已于 2026-10-25 下线：日历/股本/列表全部走 tdx-api）"""
        # 初始化Tdx API数据源
        if self.config_manager.get('data_sources.tdx_api.enabled', True):
            tdx_api_config = self.config_manager.get('data_sources.tdx_api', {})
            # 中间件增强路径开关（V3.0 M-C），false 时保持 V2.0 直连路径
            tdx_api_config['use_enhanced_api'] = self.config_manager.get('sync.kline_day_use_enhanced_api', False)
            self.tdx_api_source = TdxApiSource(tdx_api_config)

    def sync_all(self, save_to_csv: bool = True, save_to_db: bool = True) -> Dict[str, Any]:
        """
        同步所有数据

        Args:
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库

        Returns:
            同步结果统计
        """
        result = {
            'start_time': datetime.now(),
            'success': False,
            'stocks_count': 0,
            'errors': []
        }

        try:
            # 1. 同步股票列表
            stocks = self.sync_stocks(save_to_csv, save_to_db)
            result['stocks_count'] = len(stocks)

            result['success'] = True
        except Exception as e:
            result['errors'].append(str(e))
            print(f"同步数据失败: {e}")

        result['end_time'] = datetime.now()
        result['duration'] = (result['end_time'] - result['start_time']).total_seconds()

        return result

    def sync_stocks(self, save_to_csv: bool = True, save_to_db: bool = True) -> List[Dict[str, Any]]:
        """同步股票列表（baostock 下线换源 20261025：tdx-api /api/codes 快照维护）

        快照 + 新股/改名/退市标记/list_date 回填，见 StockListManager；
        save_to_db=False 时仅产出 diff 报告不落库（save_to_csv 保留参数兼容旧调用）

        Returns:
            快照股票列表（ts_code/name/exchange）
        """
        from .stock_list_manager import StockListManager
        report = StockListManager(self.config_manager).execute(apply=save_to_db)

        stocks = [{'ts_code': s['ts_code'], 'stock_name': s['name'], 'market': s['exchange']}
                  for s in report.get('_snapshot_rows', [])]
        print(f"股票列表维护完成：快照 {report['snapshot']} 只 | 新股 {len(report['new_stocks'])} | "
              f"改名 {len(report['renamed'])} | 退市 {len(report['delisted'])} | "
              f"{'已落库' if report['apply'] else '仅报告'} | 报告: {report['report_path']}")
        return stocks

    def sync_trade_calendar(self, start_year: int, end_year: int, save_to_db: bool = True) -> Dict[str, Any]:
        """
        同步交易日历数据 - 按年范围同步

        Args:
            start_year: 开始年份 (yyyy)
            end_year: 结束年份 (yyyy)
            save_to_db: 是否保存到数据库

        Returns:
            同步结果统计
        """
        result = {
            'start_year': start_year,
            'end_year': end_year,
            'success': False,
            'records': 0,
            'db_rows': 0,
            'errors': []
        }

        start_time = time.time()
        try:
            if start_year > end_year:
                raise ValueError(f"开始年份不能大于结束年份: {start_year} > {end_year}")

            # 数据源：tdx-api /api/workday/range（本地中间件；历史源自上证指数日K，
            # 未来日期为中间件推断，故 end_year 限当年+1，baostock 已下线）
            if not self.tdx_api_source:
                raise RuntimeError("Tdx API数据源未初始化")

            max_year = datetime.now().year + 1
            if end_year > max_year:
                print(f"⚠️  结束年份 {end_year} 超出上限（未来日期为中间件推断），截断为 {max_year}")
                end_year = max_year
            result['end_year'] = end_year

            if not self.tdx_api_source.connect():
                raise RuntimeError("Tdx API连接失败")

            calendar_data = self.tdx_api_source.get_trade_calendar(start_year, end_year)
            result['records'] = len(calendar_data)

            if save_to_db and calendar_data:
                result['db_rows'] = self.db_conn.upsert_trade_calendar(calendar_data)

            # F7（数据采集链路风险修复方案 V1.0 §F7）：跨年覆盖校验——end_year 为
            # "当年+1"而库内 max(calendar_date) 未到次年 12-31 时告警（2027-01 初
            # 日历缺失会让增量日K停在旧日期空跑；F1 巡检的日历专项规则兜底监控）
            if save_to_db and end_year >= datetime.now().year + 1:
                r = self.db_conn.execute_query(
                    'SELECT MAX(calendar_date) AS mx FROM base_trade_calendar')
                mx = str(r[0]['mx']) if r and r[0].get('mx') else ''
                expected = f'{end_year}-12-31'
                if not mx or mx < expected:
                    warn = (f"次年日历未覆盖: 库内 max(calendar_date)={mx or '空'} "
                            f"未到 {expected}（等指数K线到达后重跑 sync-trade-calendar "
                            f"{end_year} {end_year}；中间件 workday 节假日表需含 {end_year} 年）")
                    result['errors'].append(warn)
                    print(f"⚠️  {warn}")

            result['success'] = True
        except Exception as e:
            result['errors'].append(str(e))
            print(f"同步交易日历失败: {e}")
        finally:
            if self.tdx_api_source:
                self.tdx_api_source.disconnect()

        end_time = time.time()
        result['duration'] = round(end_time - start_time, 2)

        return result

    def sync_kline_1min(
        self,
        init_mode: bool = False,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        save_to_csv: bool = True,
        save_to_db: bool = True
    ) -> Dict[str, Any]:
        """
        同步1分钟K线数据 - 支持数据初始化/增量更新/指定日期范围
        """
        if not self.tdx_api_source:
            return {
                'success': False,
                'records': 0,
                'db_rows': 0,
                'errors': ['Tdx API数据源未初始化'],
                'report_path': None
            }

        raw_csv_workers = self.config_manager.get('sync.kline_1min_csv_max_workers', 2)
        raw_csv_queue = self.config_manager.get('sync.kline_1min_csv_max_queue', 20000)
        try:
            csv_workers = max(1, int(raw_csv_workers or 2))
        except (TypeError, ValueError):
            self.logger.warning(f"kline_1min_csv_max_workers配置无效({raw_csv_workers})，已回退为2")
            csv_workers = 2
        try:
            csv_queue = max(1000, int(raw_csv_queue or 20000))
        except (TypeError, ValueError):
            self.logger.warning(f"kline_1min_csv_max_queue配置无效({raw_csv_queue})，已回退为20000")
            csv_queue = 20000

        usecase = Kline1MinSyncUseCase(
            source=TdxSourceAdapter(self.tdx_api_source),
            repository=DatabaseKline1MinRepository(self.db_conn),
            partition_port=DatabaseKline1MinPartitionAdapter(self.db_conn),
            csv_writer=CsvAsyncWriter(self.csv_writer, max_workers=csv_workers, max_queue=csv_queue),
            report=ReportPortAdapter(self),
            fundamentals=FundamentalsPortAdapter(self),
            trade_calendar=TradeCalendarPortAdapter(self),
            stock_port=StockPortAdapter(self),
            config=self.config_manager.load_config(),
            logger=self.logger
        )

        return usecase.execute(
            init_mode=init_mode,
            start_date=start_date,
            end_date=end_date,
            ts_codes=ts_codes,
            save_to_csv=save_to_csv,
            save_to_db=save_to_db
        )

    def sync_kline_day(
        self,
        init_mode: bool = False,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        save_to_csv: bool = True,
        save_to_db: bool = True
    ) -> Dict[str, Any]:
        """
        同步日K线数据 - 支持数据初始化/增量更新/指定日期范围

        R-07c 拆分后本方法为编排壳：参数解析、数据源连接、批循环与线程池、汇总、报告；
        单股处理流程见 kline_day_pipeline.py（process_one_stock / merge_stock_result）。
        签名与返回键集合完全不变（外部契约：failed_stocks/detection/source_missing）。
        """
        result = {
            'success': False,
            'records': 0,
            'db_rows': 0,
            'errors': [],
            'report_path': None
        }

        if not self.tdx_api_source:
            result['errors'].append('Tdx API数据源未初始化')
            return result

        ctx = KlineDaySharedState(result=result)

        start_ts = time.time()
        max_workers: Optional[int] = None
        stock_batch_size: Optional[int] = None
        try:
            if not self.tdx_api_source.connect():
                raise RuntimeError("Tdx API连接失败")

            stocks = self.db_conn.fetch_stock_basic(ts_codes)
            if not stocks:
                result['success'] = True
                return result

            stock_ts_codes = [s['ts_code'] for s in stocks if s.get('ts_code')]

            trade_dates: Optional[List[str]] = None
            is_explicit_range = bool(start_date and end_date)
            if not init_mode:
                if is_explicit_range:
                    trade_dates = self._get_trade_dates(start_date, end_date)
                else:
                    # 增量目标日：交易日历中 ≤ 今天的最近交易日；若为"今天"且未收盘
                    # （<15:30）再退一日。直接用 date.today() 会在跨零点补跑时把
                    # 全市场数据过滤丢弃（见问题记录 P-11）
                    target = self._latest_closed_trade_date()
                    trade_dates = self._get_trade_dates(target, target)

                if not trade_dates:
                    result['success'] = True
                    return result

            fundamentals_map = self._build_fundamentals_map(
                init_mode=init_mode,
                ts_codes=stock_ts_codes,
                start_date=start_date,
                end_date=end_date
            )

            total_stocks = len(stocks)

            # 双源拉取模式：init=双全量；日常/近期区间=双尾部；远期区间=双全量
            # 判定原则：只要请求区间的起点落在最近 tail_window 个交易日内，就用 tail 覆盖，
            # 不再拉全历史（~1MB/只），避免"补 1~2 天却全量拉取"压垮中间件。
            tail_limit = int(self.config_manager.get('sync.kline_day_tail_limit', 5))
            tail_window = int(self.config_manager.get('sync.kline_day_tail_window', 10))
            if init_mode:
                fetch_full = True
            else:
                span_len = self._trade_span_to_today(min(trade_dates)) if trade_dates else 10 ** 6
                if span_len <= tail_window:
                    fetch_full = False
                    # tail 需覆盖 起点→今日 的全部交易日，另留 2 根缓冲（今日 bar 可能尚未就绪）
                    tail_limit = max(tail_limit, span_len + 2)
                else:
                    fetch_full = True
            result['detection'] = ctx.detection
            result['detection_details'] = ctx.detection_details
            result['source_missing'] = ctx.source_missing
            result['failed_stocks'] = 0

            # raw 兜底（方案 V1.0）：台账表幂等创建 + 单次运行兜底股票数上限
            raw_fb_budget = None
            if bool(self.config_manager.get('sync.kline_day_raw_fallback.enabled', True)):
                try:
                    self.db_conn.ensure_raw_fallback_table()
                    raw_fb_limit = self.config_manager.get('sync.kline_day_raw_fallback.max_stocks_per_run', 1500)
                    raw_fb_budget = RawFallbackBudget(int(raw_fb_limit or 1500))
                except Exception as e:
                    self.logger.warning(f"raw 兜底台账表初始化失败，本次运行停用兜底: {e}")

            deps = KlineDayDeps(
                tdx_api_source=self.tdx_api_source,
                csv_writer=self.csv_writer,
                db_conn=self.db_conn,
                logger=self.logger,
                fundamentals_map=fundamentals_map,
                trade_dates=trade_dates,
                init_mode=init_mode,
                is_explicit_range=is_explicit_range,
                save_to_csv=save_to_csv,
                save_to_db=save_to_db,
                fetch_full=fetch_full,
                tail_limit=tail_limit,
                total_stocks=total_stocks,
                log_progress=self._log_progress,
                merge_sources=self._merge_kline_day_sources,
                detect_refetch=self._detect_kline_day_refetch,
                filter_raw=self._filter_raw_kline_by_dates,
                normalize_records=self._normalize_kline_day_records,
                raw_fallback_budget=raw_fb_budget,
                circuit_breaker_threshold=int(self.config_manager.get('sync.kline_day_circuit_breaker', 20) or 20)
            )

            max_workers = self.config_manager.get('sync.kline_max_workers', 1)
            raw_batch_size = self.config_manager.get('sync.kline_stock_batch_size', 1000)
            try:
                stock_batch_size = max(1, int(raw_batch_size or 1000))
            except (TypeError, ValueError):
                self.logger.warning(f"kline_stock_batch_size配置无效({raw_batch_size})，已回退为1000")
                stock_batch_size = 1000
            total_batches = (total_stocks + stock_batch_size - 1) // stock_batch_size

            for batch_no, batch_start in enumerate(range(0, total_stocks, stock_batch_size), start=1):
                if ctx.circuit_tripped:
                    # F5 整批熔断：上一批已触发，剩余批次不再提交
                    self.logger.error(f"熔断生效，跳过批次 {batch_no}/{total_batches}")
                    break
                stock_batch = stocks[batch_start:batch_start + stock_batch_size]
                batch_end = batch_start + len(stock_batch)
                self.logger.info(
                    f"日K线批次开始: 第{batch_no}/{total_batches}批, 股票[{batch_start + 1}-{batch_end}]"
                )

                if max_workers and max_workers > 1 and len(stock_batch) > 1:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = [
                            executor.submit(
                                safe_process_one_stock, s, batch_start + i + 1, ctx=ctx, deps=deps
                            )
                            for i, s in enumerate(stock_batch)
                        ]
                        batch_aborted = False
                        for future in as_completed(futures):
                            res = future.result()
                            merge_stock_result(res, ctx)
                            if res.get('circuit_tripped'):
                                # F5：本批内触发熔断——取消未开始的 future 并中止后续批次
                                batch_aborted = True
                                for f in futures:
                                    f.cancel()
                                break
                        if batch_aborted:
                            self.logger.error("熔断触发，中止当前批次剩余股票与后续批次")
                            break
                else:
                    for i, stock in enumerate(stock_batch):
                        res = safe_process_one_stock(stock, batch_start + i + 1, ctx=ctx, deps=deps)
                        merge_stock_result(res, ctx)
                        if res.get('circuit_tripped'):
                            self.logger.error("熔断触发，中止剩余股票与后续批次")
                            break

                if ctx.circuit_tripped:
                    break

                self.logger.info(
                    f"日K线批次完成: 第{batch_no}/{total_batches}批, 累计记录{result['records']}条, 累计写库{result['db_rows']}条"
                )

            result['success'] = True
            # F5：熔断 → 全场失败显式化
            if ctx.circuit_tripped:
                result['success'] = False
                result['errors'].append(ctx.circuit_error)
            # F8：全场级故障显式化——失败股超过一半即判失败（strict 模式下中间件
            # 宕机会使绝大多数股票计 failed，不再"成功 0 记录"）
            if total_stocks and result.get('failed_stocks', 0) > total_stocks * 0.5:
                result['success'] = False
                result['errors'].append(
                    f"全场级故障: 失败 {result['failed_stocks']}/{total_stocks} 只（>50%），"
                    f"疑中间件不可用或数据源异常")
            result['circuit_tripped'] = ctx.circuit_tripped
            # 除权触发清单落盘（方案 4.2）：日常增量检出 qfq_rebase 的股票清单，
            # 供复权因子增量同步（adjust_factor_manager）读取；与当日已有清单合并，幂等。
            # 文件名用本次同步的目标交易日（非 wall clock，跨零点补跑防错位，见 P-10）
            if not init_mode and not is_explicit_range and ctx.xdxr_hit_codes:
                try:
                    self._write_adjust_trigger(ctx.xdxr_hit_codes,
                                               trade_date=max(trade_dates) if trade_dates else None)
                except Exception as e:
                    self.logger.warning(f"除权触发清单落盘失败（不影响日K同步）: {e}")

            # raw 兜底恢复阶段（方案 V1.0 §3.4）：台账 open 记录重拉 qfq 覆盖销账
            try:
                self._recover_raw_fallback(trade_dates, result)
            except Exception as e:
                self.logger.warning(f"raw 兜底恢复阶段异常（不影响日K同步）: {e}")
        except Exception as e:
            result['errors'].append(str(e))
            print(f"同步日K线失败: {e}")
        finally:
            if self.tdx_api_source:
                self.tdx_api_source.disconnect()

        end_ts = time.time()
        timing = {
            'api_time': round(ctx.api_time, 2),
            'csv_time': round(ctx.csv_time, 2),
            'db_time': round(ctx.db_time, 2),
            'total_time': round(end_ts - start_ts, 2),
            'parallel': max_workers if max_workers is not None else self.config_manager.get('sync.kline_max_workers', 1),
            'stock_batch_size': stock_batch_size if stock_batch_size is not None else self.config_manager.get('sync.kline_stock_batch_size', 1000),
            'api_wall': round(ctx.api_span[1] - ctx.api_span[0], 2) if ctx.api_span else 0,
            'csv_wall': round(ctx.csv_span[1] - ctx.csv_span[0], 2) if ctx.csv_span else 0,
            'db_wall': round(ctx.db_span[1] - ctx.db_span[0], 2) if ctx.db_span else 0
        }

        result['duration'] = timing['total_time']
        result['fallback_written'] = ctx.fallback_written
        result['fallback_skipped_exrights'] = ctx.fallback_skipped_exrights
        result['fallback_recovered'] = result.get('fallback_recovered', 0)
        result['report_path'] = self._write_kline_day_report(result, ctx.anomalies, timing)

        return result

    def _merge_kline_day_sources(self, qfq_list: List[Dict[str, Any]],
                                 raw_list: List[Dict[str, Any]],
                                 trade_dates: Optional[List[str]] = None,
                                 ts_code: Optional[str] = None,
                                 raw_fallback_budget: Any = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        合并双源日K：前复权序列（同花顺源）为主表，原始序列（通达信源）提供
        amount 与 raw_close。以 trade_date 对齐，原始域缺失时留空。
        字段保持中间件原始单位（价格/成交额=厘，成交量=手），由 normalize 统一换算。

        【raw 兜底（日K线raw兜底与自动恢复方案 V1.0）】qfq 主表缺失、而 raw 存在的
        "尾部缺口"日期（td > qfq 序列最后日期，即 THS 前复权尚未发布的当日/近日），
        在除权校验通过后用 raw 值构建兜底行：行带 raw_fallback=True 标记
        （normalize 后 source='TDXAPI_RAWFB'），由台账 kline_day_raw_fallback 追踪、
        恢复阶段（_recover_raw_fallback）在 THS 发布后用 qfq 覆盖销账。
        仅兜底"尾部缺口"：qfq 序列中段/早期的缺失属结构性差异（如两源上市日不同），
        维持原跳过行为。init 模式（trade_dates=None）不限制日期。

        Returns:
            (merged, raw_skips)：raw_skips 为除权校验未通过而跳过的明细
            （trade_date/reason/raw_close），由调用方写入台账。
        """
        merged: Dict[str, Dict[str, Any]] = {}
        for item in qfq_list or []:
            if not isinstance(item, dict):
                continue
            td = self._normalize_date_str(item.get('Time') or item.get('time'))
            if not td:
                continue
            merged[td] = {
                'Time': item.get('Time'),
                'Open': item.get('Open'),
                'High': item.get('High'),
                'Low': item.get('Low'),
                'Close': item.get('Close'),
                'Last': item.get('Last'),
                'Volume': item.get('Volume'),
                # 同花顺源 amount 恒为 0，置空由原始域补充
                'Amount': None,
                'RawClose': None,
            }
        raw_skips: List[Dict[str, Any]] = []
        fallback_enabled = bool(self.config_manager.get('sync.kline_day_raw_fallback.enabled', True))
        last_qfq_date = max(merged.keys()) if merged else None
        allowed_td = set(trade_dates) if trade_dates else None
        stock_budget_acquired = False
        for item in raw_list or []:
            if not isinstance(item, dict):
                continue
            td = self._normalize_date_str(item.get('Time') or item.get('time'))
            if not td:
                continue
            row = merged.get(td)
            if row is None:
                # 前复权主表没有的日期：仅对"尾部缺口"做 raw 兜底（除权校验通过后）
                if not (fallback_enabled and last_qfq_date and td > last_qfq_date):
                    continue
                if allowed_td is not None and td not in allowed_td:
                    # 请求区间外（P-11 日期过滤语义），不兜底
                    continue
                ok, fail_reason = self._raw_fallback_no_exrights(item, raw_list, td, ts_code)
                if not ok:
                    raw_skips.append({
                        'trade_date': td,
                        'reason': fail_reason or 'exrights_suspected',
                        'raw_close': self._to_float(item.get('Close')),
                    })
                    continue
                if raw_fallback_budget is not None and not stock_budget_acquired:
                    # 一股一额度：本股首条兜底行时占用；额度耗尽则本股不再兜底
                    if not raw_fallback_budget.try_acquire():
                        continue
                    stock_budget_acquired = True
                merged[td] = {
                    'Time': item.get('Time'),
                    'Open': item.get('Open'),
                    'High': item.get('High'),
                    'Low': item.get('Low'),
                    'Close': item.get('Close'),
                    'Last': item.get('Last'),
                    'Volume': item.get('Volume'),
                    'Amount': item.get('Amount'),
                    'RawClose': item.get('Close'),
                    'raw_fallback': True,
                }
            else:
                row['Amount'] = item.get('Amount')
                row['RawClose'] = item.get('Close')
        return [merged[key] for key in sorted(merged.keys())], raw_skips

    def _raw_fallback_no_exrights(self, raw_item: Dict[str, Any],
                                  raw_list: List[Dict[str, Any]],
                                  td: str, ts_code: Optional[str]) -> Tuple[bool, Optional[str]]:
        """
        raw 兜底的除权校验（唯一风险门）：raw 本日 Last（上一收盘）必须精确等于
        前一交易日 raw 收盘——除权除息日交易所会把"上一收盘"调整为除权基准价，
        必不相等；无除权日两者为同源整数厘，精确相等。校验数据不全时保守跳过。

        前一日取值：优先 raw_list 内时间序紧邻前条；不在 raw_list 内（尾部窗口
        恰好从兜底日开始）时回落查库 fetch_prev_his_kline_day_close（元口径，
        与 raw 厘口径经 _scale_price 归一后比较）。

        Returns:
            (通过?, 失败原因)：通过时 (True, None)；失败原因
            'missing_last'（Last 缺失/非法，校验数据不全）| 'exrights_suspected'（Last 与前日收盘不一致）
        """
        last_v = self._to_float(raw_item.get('Last'))
        if not last_v or last_v <= 0:
            return False, 'missing_last'

        prev_v = None
        best_prev_td = None
        for item in raw_list or []:
            if not isinstance(item, dict):
                continue
            d = self._normalize_date_str(item.get('Time') or item.get('time'))
            if d and d < td and (best_prev_td is None or d > best_prev_td):
                best_prev_td = d
                prev_v = self._to_float(item.get('Close'))

        last_yuan = self._scale_price(last_v)
        if prev_v is not None:
            prev_yuan = self._scale_price(prev_v)
        elif ts_code:
            prev_yuan = self._to_float(self.db_conn.fetch_prev_his_kline_day_close(ts_code, td))
        else:
            return False, 'missing_last'

        if not prev_yuan or prev_yuan == 0 or not last_yuan:
            return False, 'missing_last'
        if abs(last_yuan - prev_yuan) < 1e-6:
            return True, None
        return False, 'exrights_suspected'

    def _recover_raw_fallback(self, trade_dates: Optional[List[str]],
                              result: Dict[str, Any]) -> int:
        """
        raw 兜底恢复阶段（方案 V1.0 §3.4）：每次 sync_kline_day 末尾自动执行。
        对台账中仍为 fallback 的记录逐股重拉 qfq 尾部，THS 已发布该日的用 qfq 值
        定点覆盖兜底行（source 回归 TDXAPI）并销账；仍缺的保留 open 下次再试。
        定点更新只动价格/preclose/change_rate/source，volume/amount/基本面保持原值
        （无除权时 qfq 与 raw 数值相同；若期间发生除权重基，由既有除权检测触发
        整股重拉彻底修复）。任何异常不影响主同步结果。
        """
        open_rows = self.db_conn.fetch_open_raw_fallback()
        if not open_rows:
            return 0

        by_stock: Dict[str, List[str]] = {}
        for r in open_rows:
            by_stock.setdefault(r['ts_code'], []).append(r['trade_date'])

        target = max(trade_dates) if trade_dates else date.today().strftime('%Y%m%d')
        recovered_keys: List[tuple] = []
        for ts_code, dates in by_stock.items():
            api_code = ts_code.replace('.', '').upper()
            oldest = min(dates)
            try:
                span_rows = self._get_trade_dates(oldest, target)
                tail_limit = max(len(span_rows) + 3, 5)
            except Exception:
                tail_limit = 30
            try:
                qfq_list = self.tdx_api_source.get_kline_qfq_tail(api_code, tail_limit)
            except Exception as e:
                self.logger.warning(f"raw 兜底恢复 qfq 拉取失败: {ts_code} {e}")
                continue
            qmap = {}
            for item in qfq_list or []:
                if isinstance(item, dict):
                    d = self._normalize_date_str(item.get('Time') or item.get('time'))
                    if d:
                        qmap[d] = item
            for d in dates:
                item = qmap.get(d)
                if not item:
                    continue  # THS 仍未发布 → 保留 open
                close_v = self._scale_price(self._to_float(item.get('Close')))
                if not close_v:
                    continue
                open_v = self._scale_price(self._to_float(item.get('Open')))
                high_v = self._scale_price(self._to_float(item.get('High')))
                low_v = self._scale_price(self._to_float(item.get('Low')))
                preclose_v = self._scale_price(self._to_float(item.get('Last')))
                change_rate_v = None
                if preclose_v and preclose_v != 0:
                    change_rate_v = (close_v - preclose_v) / preclose_v * 100
                updated = self.db_conn.update_raw_fallback_row_prices(
                    ts_code, d, open_v, high_v, low_v, close_v, preclose_v, change_rate_v)
                if updated:
                    recovered_keys.append((ts_code, d))

        if recovered_keys:
            self.db_conn.mark_raw_fallback_recovered(recovered_keys, recover_source='recovery_stage')
            self.logger.info(
                f"raw 兜底恢复完成: {len(recovered_keys)}/{len(open_rows)} 条"
                f"（{','.join(f'{c}@{d}' for c, d in recovered_keys[:10])}"
                f"{'...' if len(recovered_keys) > 10 else ''}）"
            )
        result['fallback_recovered'] = len(recovered_keys)
        return len(recovered_keys)

    def _latest_closed_trade_date(self) -> str:
        """增量目标日：交易日历中 ≤ 今天的最近交易日；若为"今天"且未收盘（15:30 前）再退一日。

        目的是让"今天"语义收敛为"最近已收盘的数据交易日"（问题记录 P-11）。
        F1 起逻辑抽为 review_common.latest_closed_trade_date 公共函数（巡检/编排复用），
        本方法保留为薄委托以维持既有调用面。
        """
        from .review_common import latest_closed_trade_date
        return latest_closed_trade_date(self.db_conn)

    def _write_adjust_trigger(self, ts_codes: List[str], trade_date: Optional[str] = None) -> str:
        """落除权触发清单（tmp/adjust_trigger_YYYYMMDD.json，与已有清单合并幂等）。

        trade_date 用本次同步的目标交易日（跨零点补跑时 wall clock 会错位一天，
        见问题记录 P-10）；缺省回退系统当日。

        消费方：adjust_factor_manager.read_adjust_trigger（复权因子增量，方案 4.2）。
        """
        import json
        from .adjust_factor_manager import adjust_trigger_path
        from .review_common import save_manifest, tmp_dir

        trade_date = trade_date or date.today().strftime('%Y%m%d')
        path = adjust_trigger_path(trade_date)
        merged: List[str] = list(dict.fromkeys(ts_codes))
        try:
            existing = json.loads(path.read_text(encoding='utf-8'))
            merged = list(dict.fromkeys(merged + [str(c) for c in existing.get('ts_codes', [])]))
        except Exception:
            pass
        payload = {'trade_date': trade_date, 'ts_codes': merged,
                   'updated_at': datetime.now().isoformat(timespec='seconds')}
        save_manifest(tmp_dir() / f'adjust_trigger_{trade_date}.json', payload)
        self.logger.info(f"除权触发清单已落盘: {len(merged)} 只（{path.name}）")
        return str(path)

    def _detect_kline_day_refetch(self, ts_code: str, merged: List[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
        """
        日常增量的除权/修订检测（跨快照）：
        1) 新拉最旧 bar 的 Last（同花顺前复权昨收）vs 库存上一交易日 close —— 命中即除权重基
           【F11①】库内 prev_close 缺失（上一交易日同步失败/新股）时，降级用
           "最旧 bar 的 fresh Close vs 库内同日 close" 比对（重叠日期跨快照可比）
        2) 重叠日期 fresh raw_close vs 库存 raw_close —— 命中即源数据修订
           【F11⑤】同批次增加 fresh qfq Close vs 库存 close 比对——覆盖 qfq 中段
           历史修订（002083 案例：前一日价格被源端重算，原检测仅比 raw_close 漏检）

        返回 (是否命中, 明细)
        """
        if not merged:
            return False, {}

        abs_threshold = float(self.config_manager.get('sync.kline_day_detect_threshold_abs', 0.01))
        pct_threshold = float(self.config_manager.get('sync.kline_day_detect_threshold_pct', 0.002))
        raw_tolerance = float(self.config_manager.get('sync.kline_day_raw_match_tolerance', 0.005))

        oldest = merged[0]
        oldest_date = self._normalize_date_str(oldest.get('Time') or oldest.get('time'))
        fresh_last = self._scale_price(self._to_float(oldest.get('Last')))
        fresh_oldest_close = self._scale_price(self._to_float(oldest.get('Close')))

        # 1) 前复权重基检测
        if oldest_date and fresh_last:
            prev_close = self.db_conn.fetch_prev_his_kline_day_close(ts_code, oldest_date)
            if prev_close:
                threshold = max(abs_threshold, abs(prev_close) * pct_threshold)
                if abs(fresh_last - prev_close) > threshold:
                    return True, {
                        'type': 'qfq_rebase',
                        'date': oldest_date,
                        'stored_close': round(prev_close, 4),
                        'fresh_last': round(fresh_last, 4),
                        'diff': round(fresh_last - prev_close, 4)
                    }
            elif fresh_oldest_close:
                # F11①：库内 prev_close 缺失 → 降级用重叠日期同日 close 跨快照比对
                stored_same = self.db_conn.fetch_one(
                    "SELECT close FROM his_kline_day WHERE ts_code = %s AND trade_date = %s",
                    (ts_code, oldest_date))
                stored_close = self._to_float((stored_same or {}).get('close'))
                if stored_close:
                    threshold = max(abs_threshold, abs(stored_close) * pct_threshold)
                    if abs(fresh_oldest_close - stored_close) > threshold:
                        return True, {
                            'type': 'qfq_rebase',
                            'date': oldest_date,
                            'stored_close': round(stored_close, 4),
                            'fresh_close': round(fresh_oldest_close, 4),
                            'diff': round(fresh_oldest_close - stored_close, 4),
                            'note': 'prev_close 缺失，降级同日 close 比对（F11①）'
                        }

        # 2) 原始域不可变校验（重叠日期）+ F11⑤ qfq 域修订比对
        dates = [self._normalize_date_str(r.get('Time') or r.get('time')) for r in merged]
        dates = [d for d in dates if d]
        if dates:
            min_d, max_d = min(dates), max(dates)
            stored_rows = self.db_conn.execute_query(
                "SELECT trade_date, close, raw_close FROM his_kline_day "
                "WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s",
                (ts_code, min_d, max_d))
            if stored_rows:
                fresh_map = {}
                for r in merged:
                    d = self._normalize_date_str(r.get('Time') or r.get('time'))
                    fresh_map[d] = {
                        'raw': self._scale_price(self._to_float(r.get('RawClose'))),
                        'qfq': self._scale_price(self._to_float(r.get('Close'))),
                    }
                for row in stored_rows:
                    d = row['trade_date']
                    fresh = fresh_map.get(d, {})
                    stored_close = self._to_float(row.get('close'))
                    stored_raw = self._to_float(row.get('raw_close'))
                    # 2a) raw 域不可变校验（原逻辑）
                    fresh_close_raw = fresh.get('raw')
                    if stored_raw is not None and fresh_close_raw is not None:
                        if abs(fresh_close_raw - stored_raw) > max(raw_tolerance, abs(stored_raw) * pct_threshold):
                            return True, {
                                'type': 'raw_revision',
                                'date': d,
                                'stored_raw_close': round(stored_raw, 4),
                                'fresh_raw_close': round(fresh_close_raw, 4)
                            }
                    # 2b) F11⑤ qfq 域修订比对（raw 无变化而 qfq 变 → 源端前复权重算）
                    fresh_qfq = fresh.get('qfq')
                    if stored_close is not None and fresh_qfq is not None:
                        threshold = max(abs_threshold, abs(stored_close) * pct_threshold)
                        if abs(fresh_qfq - stored_close) > threshold:
                            return True, {
                                'type': 'qfq_revision',
                                'date': d,
                                'stored_close': round(stored_close, 4),
                                'fresh_qfq_close': round(fresh_qfq, 4),
                                'diff': round(fresh_qfq - stored_close, 4),
                                'note': 'raw 域一致而 qfq 变动 → 源端前复权修订（F11⑤）'
                            }
        return False, {}

    def sync_anal_kline_rise_25pre(
        self,
        init_mode: bool = False,
        ts_codes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        生成立体K线数据（基于1分钟K线）
        """
        result = {
            'success': False,
            'records': 0,
            'db_rows': 0,
            'errors': [],
            'report_path': None
        }

        query_time = 0.0
        gen_time = 0.0
        db_time = 0.0

        start_ts = time.time()
        try:
            self.db_conn.ensure_anal_kline_rise_25pre_constraints()
            stocks = self.db_conn.fetch_stock_basic(ts_codes)
            if not stocks:
                result['success'] = True
                return result

            total_stocks = len(stocks)
            for idx, stock in enumerate(stocks, start=1):
                ts_code = stock.get('ts_code')
                stock_code = stock.get('stock_code') or ''
                stock_name = stock.get('stock_name') or ''
                if not ts_code:
                    continue

                query_start = time.time()
                if init_mode:
                    kline_rows = self.db_conn.fetch_his_kline_1min_by_ts_code(ts_code)
                else:
                    last_end = self.db_conn.fetch_last_anal_kline_rise_25pre_end_time(ts_code)
                    if last_end:
                        kline_rows = self.db_conn.fetch_his_kline_1min_after(ts_code, last_end)
                    else:
                        kline_rows = self.db_conn.fetch_his_kline_1min_by_ts_code(ts_code)
                query_time += time.time() - query_start

                if not kline_rows:
                    continue

                gen_start = time.time()
                anal_rows = self._generate_kline_rise_25pre(kline_rows)
                gen_elapsed = time.time() - gen_start
                gen_time += gen_elapsed

                self._log_progress('立体 k 线', idx, total_stocks, stock_code, stock_name, gen_elapsed)

                if not anal_rows:
                    continue

                db_start = time.time()
                result['db_rows'] += self.db_conn.upsert_anal_kline_rise_25pre(anal_rows)
                db_time += time.time() - db_start

                result['records'] += len(anal_rows)

            result['success'] = True
        except Exception as e:
            result['errors'].append(str(e))
            print(f"生成立体K线失败: {e}")

        end_ts = time.time()
        timing = {
            'query_time': round(query_time, 2),
            'gen_time': round(gen_time, 2),
            'db_time': round(db_time, 2),
            'total_time': round(end_ts - start_ts, 2)
        }

        result['report_path'] = self._write_anal_kline_report(result, timing)
        return result


    def _get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        start_date = self._normalize_calendar_date(start_date)
        end_date = self._normalize_calendar_date(end_date)
        if not start_date or not end_date:
            return []

        rows = self.db_conn.fetch_trade_calendar(start_date, end_date)
        trade_dates = []
        for row in rows:
            calendar_date = row.get('calendar_date')
            if calendar_date:
                trade_dates.append(calendar_date.replace('-', ''))
        return trade_dates

    def _trade_span_to_today(self, earliest: str) -> int:
        """返回 earliest(YYYYMMDD) 到今日（含）之间的交易日数量。

        用于判断某日期区间能否由 tail（最近 N 个交易日）覆盖。解析失败返回一个大数，
        使调用方退化为全量拉取（安全兜底）。
        """
        d = (earliest or '').replace('-', '')
        if len(d) != 8 or not d.isdigit():
            return 10 ** 6
        earliest_dash = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

        today_str = date.today().strftime('%Y-%m-%d')
        if earliest_dash > today_str:
            return 0
        return len(self._get_trade_dates(earliest_dash, today_str))

    def _build_fundamentals_map(
        self,
        init_mode: bool,
        ts_codes: List[str],
        start_date: Optional[str],
        end_date: Optional[str]
    ) -> Dict[str, Dict[str, Any]]:
        if init_mode:
            data = self.db_conn.fetch_fundamentals_all(ts_codes)
        elif start_date and end_date:
            end_str = self._normalize_calendar_date(end_date)
            start_str = self._normalize_calendar_date(start_date)
            if not end_str or not start_str:
                data = []
            else:
                data = self.db_conn.fetch_fundamentals_range_with_prev(ts_codes, start_str, end_str)
        else:
            data = self.db_conn.fetch_fundamentals_latest(ts_codes)

        return build_fundamentals_records_map(data)

    def _match_fundamentals(
        self,
        ts_code: str,
        trade_date: str,
        fundamentals_map: Dict[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        return match_fundamentals(ts_code, trade_date, fundamentals_map)

    def _normalize_kline_day_records(
        self,
        raw_records: List[Any],
        stock: Dict[str, Any],
        fundamentals_map: Dict[str, Dict[str, Any]],
        allowed_dates: Optional[set] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        normalized: List[Dict[str, Any]] = []
        anomalies: List[Dict[str, Any]] = []

        for raw in raw_records:
            record = self._normalize_kline_day_record(raw, stock)
            if record:
                normalized.append(record)

        if not normalized:
            return [], []

        if allowed_dates:
            normalized = [item for item in normalized if item.get('trade_date') in allowed_dates]

        normalized.sort(key=lambda x: x['trade_date'])

        prev_close = None
        for record in normalized:
            last_value = self._to_float(record.pop('_raw_last', None))
            is_raw_fallback = bool(record.pop('_raw_fallback', False))
            if last_value is not None and last_value != 0:
                preclose = last_value
            else:
                if prev_close is not None and prev_close != 0:
                    preclose = prev_close
                else:
                    prev_db_close = self._to_float(self.db_conn.fetch_prev_his_kline_day_close(
                        record.get('ts_code'),
                        record.get('trade_date')
                    ))
                    if prev_db_close is not None and prev_db_close != 0:
                        preclose = prev_db_close
                    else:
                        preclose = self._to_float(record.get('open'))

            record['preclose'] = preclose

            close = self._to_float(record.get('close'))
            record['close'] = close
            if preclose and close is not None:
                record['change_rate'] = (close - preclose) / preclose * 100
            else:
                record['change_rate'] = None

            fundamentals = self._match_fundamentals(record['ts_code'], record['trade_date'], fundamentals_map)
            if fundamentals:
                record['fundamentals_disclosure_date'] = fundamentals.get('disclosure_date')
                record['total_share'] = self._to_float(fundamentals.get('total_share'))
                record['float_share'] = self._to_float(fundamentals.get('float_share'))
            else:
                record['fundamentals_disclosure_date'] = None
                record['total_share'] = None
                record['float_share'] = None

            float_share = record.get('float_share')
            volume = record.get('volume')
            if float_share and volume is not None and float_share != 0:
                record['turnover_rate'] = volume / float_share * 100
            else:
                record['turnover_rate'] = None

            # raw 兜底行标记 TDXAPI_RAWFB（方案 V1.0），由台账追踪、恢复阶段回归 TDXAPI
            record['source'] = 'TDXAPI_RAWFB' if is_raw_fallback else 'TDXAPI'

            limit_rate = self._get_kline_limit_rate(record.get('stock_code'), record.get('stock_name'))
            change_rate = record.get('change_rate')
            if change_rate is not None and abs(change_rate) > limit_rate:
                anomalies.append({
                    'ts_code': record.get('ts_code'),
                    'trade_date': record.get('trade_date'),
                    'change_rate': round(change_rate, 6),
                    'limit_rate': limit_rate
                })

            prev_close = close

        return normalized, anomalies

    def _normalize_kline_day_record(self, raw: Any, stock: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        trade_date = self._normalize_date_str(
            raw.get('trade_date') or raw.get('date') or raw.get('tradeDate') or raw.get('Time') or raw.get('time')
        )

        if not trade_date:
            trade_datetime = raw.get('Time') or raw.get('time')
            if trade_datetime:
                trade_date, _ = self._split_datetime(trade_datetime)

        if not trade_date:
            return None

        open_v = self._scale_price(self._to_float(raw.get('open') or raw.get('Open')))
        high_v = self._scale_price(self._to_float(raw.get('high') or raw.get('High')))
        low_v = self._scale_price(self._to_float(raw.get('low') or raw.get('Low')))
        close_v = self._scale_price(self._to_float(raw.get('close') or raw.get('Close')))
        last_v = self._scale_price(self._to_float(raw.get('last') or raw.get('Last')))
        raw_close_v = self._scale_price(self._to_float(raw.get('raw_close') or raw.get('RawClose')))
        volume_v = self._scale_volume(self._to_float(raw.get('volume') or raw.get('Volume')))
        amount_v = self._scale_amount(self._to_float(raw.get('amount') or raw.get('Amount')))

        return {
            'ts_code': stock.get('ts_code'),
            'stock_code': stock.get('stock_code'),
            'stock_name': stock.get('stock_name'),
            'trade_date': trade_date,
            'open': open_v,
            'high': high_v,
            'low': low_v,
            'close': close_v,
            'preclose': None,
            '_raw_last': last_v,
            'raw_close': raw_close_v,
            'volume': volume_v,
            'amount': amount_v,
            # raw 兜底标记随行传递（merge 层打标，normalize 出口转 source 列）
            '_raw_fallback': bool(raw.get('raw_fallback'))
        }

    def _filter_raw_kline_by_dates(self, raw_data: List[Dict[str, Any]], trade_dates: Optional[List[str]]) -> List[Dict[str, Any]]:
        if not raw_data or not trade_dates:
            return raw_data

        allowed = set(trade_dates)
        filtered: List[Dict[str, Any]] = []
        seen_keys = set()
        for item in raw_data:
            if not isinstance(item, dict):
                continue
            time_value = item.get('Time') or item.get('time') or item.get('trade_date') or item.get('date')
            trade_date = self._normalize_date_str(time_value)
            if trade_date and trade_date in allowed:
                key = time_value or trade_date
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                filtered.append(item)
        return filtered

    def _generate_kline_rise_25pre(self, kline_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """立体K线算法已下沉至 kline_rise_25pre.generate_kline_rise_25pre（R-07a），保留委托以缩小影响面"""
        return generate_kline_rise_25pre(kline_rows)

    def _write_anal_kline_report(self, result: Dict[str, Any], timing: Dict[str, Any]) -> Optional[str]:
        lines = [
            "# 立体K线数据生成报告",
            "",
            "## 同步概览",
            f"- 成功: {'是' if result.get('success') else '否'}",
            f"- 记录数: {result.get('records', 0)}",
            f"- 写库行数: {result.get('db_rows', 0)}",
            "",
            "## 性能信息",
            f"- K线数据查询耗时: {timing.get('query_time', 0)} 秒",
            f"- 立体K线生成耗时: {timing.get('gen_time', 0)} 秒",
            f"- 写库耗时: {timing.get('db_time', 0)} 秒",
            f"- 总耗时: {timing.get('total_time', 0)} 秒",
            ""
        ]
        return write_markdown_report(lines, 'anal_kline_rise_25pre_report', log_label='立体K线报告')

    def _log_progress(
        self,
        stage: str,
        current: int,
        total: int,
        stock_code: str,
        stock_name: str,
        elapsed: float
    ) -> None:
        if total <= 0:
            return
        percent = (current / total) * 100
        message = (
            f"{stage}-[{current}/{total}]"
            f"{stock_code}-{stock_name}，"
            f"进度{percent:.2f}%，"
            f"阶段耗时 {elapsed:.2f} s"
        )
        self.logger.info(message)

    def _normalize_calendar_date(self, date_str: str) -> Optional[str]:
        if not date_str:
            return None
        date_str = str(date_str).strip()
        if '-' in date_str and len(date_str) == 10:
            return date_str
        if len(date_str) == 8 and date_str.isdigit():
            return f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return None

    # R-07d：normalize 工具族已下沉至 utils/kline_normalize.py，以下为同名薄委托
    def _normalize_date_str(self, value: Any) -> Optional[str]:
        return normalize_date_str(value)

    def _normalize_time_str(self, value: Any) -> Optional[str]:
        return normalize_time_str(value)

    def _split_datetime(self, value: Any) -> Tuple[Optional[str], Optional[str]]:
        return split_datetime(value)

    def _to_float(self, value: Any) -> Optional[float]:
        return to_float(value)

    def _scale_price(self, value: Optional[float]) -> Optional[float]:
        return scale_price(value)

    def _scale_amount(self, value: Optional[float]) -> Optional[float]:
        return scale_amount(value)

    def _scale_volume(self, value: Optional[float]) -> Optional[float]:
        return scale_volume(value)

    def _get_kline_limit_rate(self, stock_code: Optional[str], stock_name: Optional[str]) -> float:
        return get_kline_limit_rate(stock_code, stock_name)

    def _write_kline_1min_report(
        self,
        result: Dict[str, Any],
        anomalies: List[Dict[str, Any]],
        timing: Dict[str, Any]
    ) -> Optional[str]:
        lines = [
            "# 1分钟K线数据同步报告",
            "",
            "## 同步概览",
            f"- 成功: {'是' if result.get('success') else '否'}",
            f"- 记录数: {result.get('records', 0)}",
            f"- 写库行数: {result.get('db_rows', 0)}",
            "",
            "## 性能信息",
            f"- tdx-api累计耗时: {timing.get('api_time', 0)} 秒",
            f"- CSV累计耗时: {timing.get('csv_time', 0)} 秒",
            f"- 写库累计耗时: {timing.get('db_time', 0)} 秒",
            f"- tdx-api实际耗时(墙钟): {timing.get('api_wall', 0)} 秒",
            f"- CSV实际耗时(墙钟): {timing.get('csv_wall', 0)} 秒",
            f"- 写库实际耗时(墙钟): {timing.get('db_wall', 0)} 秒",
            f"- 流水线墙钟耗时: {timing.get('pipeline_wall', 0)} 秒",
            f"- 总耗时(墙钟): {timing.get('total_time', 0)} 秒",
            f"- COPY耗时: {timing.get('copy_time', 0)} 秒",
            f"- COPY写入行数: {timing.get('copy_rows', 0)}",
            f"- COPY是否启用: {'是' if timing.get('copy_used') else '否'}",
            f"- 分区清理模式: {timing.get('partition_cleanup_mode', '')}",
            f"- 流水线是否启用: {'是' if timing.get('pipeline_enabled') else '否'}",
            f"- 流水线采集线程数: {timing.get('pipeline_fetch_workers', 0)}",
            f"- 流水线归一化线程数: {timing.get('pipeline_normalize_workers', 0)}",
            f"- 流水线写入线程数: {timing.get('pipeline_write_workers', 0)}",
            f"- db_time/records(秒/条): {timing.get('db_time_per_record', 0)}",
            f"- db_wall/total_time: {timing.get('db_wall_ratio', 0)}",
            f"- 并发线程数: {timing.get('parallel', 1)}",
            f"- 股票批次大小: {timing.get('stock_batch_size', 1000)}",
            f"- 入库并发写数: {timing.get('db_max_writers', 2)}",
            f"- 异常样本上限: {timing.get('anomaly_limit', 0)}",
            f"- 异常样本省略数: {timing.get('anomaly_omitted', 0)}",
            ""
        ]

        failed_dates = result.get('failed_dates') or []
        if failed_dates:
            lines.extend([
                "## 失败日期",
                "",
                "| trade_date |",
                "| --- |"
            ])
            for item in failed_dates:
                lines.append(f"| {item} |")
        else:
            lines.append("## 失败日期")
            lines.append("")
            lines.append("- 无失败日期")

        if anomalies:
            lines.extend([
                "## 异常数据",
                "",
                "| ts_code | trade_date | trade_time | change_rate | limit_rate |",
                "| --- | --- | --- | --- | --- |"
            ])
            for item in anomalies:
                lines.append(
                    f"| {item.get('ts_code')} | {item.get('trade_date')} | {item.get('trade_time')} | "
                    f"{item.get('change_rate')} | {item.get('limit_rate')} |"
                )
        else:
            lines.append("## 异常数据")
            lines.append("")
            lines.append("- 无异常数据")

        return write_markdown_report(lines, 'kline_1min_sync_report', log_label='1分钟K线同步报告')

    def _write_kline_day_report(
        self,
        result: Dict[str, Any],
        anomalies: List[Dict[str, Any]],
        timing: Dict[str, Any]
    ) -> Optional[str]:
        lines = [
            "# 日K线数据同步报告",
            "",
            "## 同步概览",
            f"- 成功: {'是' if result.get('success') else '否'}",
            f"- 记录数: {result.get('records', 0)}",
            f"- 写库行数: {result.get('db_rows', 0)}",
            f"- 失败股票: {result.get('failed_stocks', 0)}",
            "",
            "## 双源与除权检测（V3.0）",
            f"- 除权检测: 检查 {result.get('detection', {}).get('checked', 0)} 只，"
            f"命中 {result.get('detection', {}).get('hits', 0)}，"
            f"整股重拉 {result.get('detection', {}).get('refetched', 0)}",
            f"- 数据源缺失: 前复权空 {result.get('source_missing', {}).get('qfq_empty', 0)} 只，"
            f"原始域空 {result.get('source_missing', {}).get('raw_empty', 0)} 只",
            "",
            "## raw 兜底（V1.0）",
            f"- 兜底写入: {result.get('fallback_written', 0)} 行（source=TDXAPI_RAWFB，台账追踪）",
            f"- 除权跳过: {result.get('fallback_skipped_exrights', 0)} 行（等 THS 恢复后正常入库）",
            f"- 恢复销账: {result.get('fallback_recovered', 0)} 条（qfq 覆盖，source 回归 TDXAPI）",
            "",
            "## 性能信息",
            f"- tdx-api累计耗时: {timing.get('api_time', 0)} 秒",
            f"- CSV累计耗时: {timing.get('csv_time', 0)} 秒",
            f"- 写库累计耗时: {timing.get('db_time', 0)} 秒",
            f"- tdx-api实际耗时(墙钟): {timing.get('api_wall', 0)} 秒",
            f"- CSV实际耗时(墙钟): {timing.get('csv_wall', 0)} 秒",
            f"- 写库实际耗时(墙钟): {timing.get('db_wall', 0)} 秒",
            f"- 总耗时(墙钟): {timing.get('total_time', 0)} 秒",
            f"- 并发线程数: {timing.get('parallel', 1)}",
            f"- 股票批次大小: {timing.get('stock_batch_size', 1000)}",
            ""
        ]

        if anomalies:
            lines.extend([
                "## 异常数据",
                "",
                "| ts_code | trade_date | change_rate | limit_rate |",
                "| --- | --- | --- | --- |"
            ])
            for item in anomalies:
                lines.append(
                    f"| {item.get('ts_code')} | {item.get('trade_date')} | "
                    f"{item.get('change_rate')} | {item.get('limit_rate')} |"
                )
        else:
            lines.append("## 异常数据")
            lines.append("")
            lines.append("- 无异常数据")

        return write_markdown_report(lines, 'kline_day_sync_report', log_label='日K线同步报告')
