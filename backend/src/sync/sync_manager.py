"""
数据同步管理器
"""

import time
import logging
import os
import threading
from typing import List, Dict, Any, Optional, Tuple
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..config import ConfigManager
from ..data_sources import BaostockSource, TdxApiSource
from ..database import DatabaseConnection, Stock
from .csv_writer import CsvWriter
from .fundamentals_manager import FundamentalsManager
from ..utils.log_aggregator import LogAggregator
from ..utils.data_transformer import DataTransformer
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
        self.db = self.db_conn  # 简化数据库访问
        self.logger = logging.getLogger(__name__)

        # 数据库日志汇总器
        self._db_log_aggregator = LogAggregator()

        # 初始化数据源
        self.baostock_source = None
        self.tdx_api_source = None

        self._init_data_sources()

    def _init_data_sources(self) -> None:
        """初始化数据源"""
        # 初始化Baostock数据源
        if self.config_manager.get('data_sources.baostock.enabled', True):
            baostock_config = {
                'data_path': self.config_manager.get_data_paths().get('csv')
            }
            self.baostock_source = BaostockSource(baostock_config)

        # 初始化Tdx API数据源
        if self.config_manager.get('data_sources.tdx_api.enabled', True):
            tdx_api_config = self.config_manager.get('data_sources.tdx_api', {})
            # 中间件增强路径开关（V3.0 M-C），false 时保持 V2.0 直连路径
            tdx_api_config['use_enhanced_api'] = self.config_manager.get('sync.kline_day_use_enhanced_api', False)
            self.tdx_api_source = TdxApiSource(tdx_api_config)

        # 初始化基本面数据管理器
        self.fundamentals_manager = FundamentalsManager(self.config_manager)

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
        """同步股票列表 - 严格按照要求使用纯baostock方案

        Returns:
            同步结果字典，包含stocks和耗时统计
        """
        import time
        result = {
            'stocks': [],
            'timing': {
                'baostock_fetch': 0.0,
                'csv_generation': 0.0,
                'database_write': 0.0
            }
        }

        # 1. 从Baostock获取股票列表（记录耗时）
        baostock_start = time.time()
        all_stocks = []
        if self.baostock_source:
            if self.baostock_source.connect():
                stocks = self.baostock_source.get_stock_list()
                all_stocks.extend(stocks)
                self.baostock_source.disconnect()
        baostock_end = time.time()
        result['timing']['baostock_fetch'] = round(baostock_end - baostock_start, 2)

        # 去重
        unique_stocks = {}
        for stock in all_stocks:
            code = stock.get('stock_code')
            ts_code = stock.get('ts_code')
            unique_key = ts_code if ts_code else code
            if unique_key and unique_key not in unique_stocks:
                unique_stocks[unique_key] = stock
            else:
                print(f"⚠️  跳过重复股票: {unique_key}")

        stocks_list = list(unique_stocks.values())
        result['stocks'] = stocks_list

        # 2. 生成CSV文件（记录耗时）
        csv_start = time.time()
        if save_to_csv:
            self.csv_writer.write_stocks(stocks_list)
        csv_end = time.time()
        result['timing']['csv_generation'] = round(csv_end - csv_start, 2)

        # 3. 写入数据库（记录耗时）
        db_start = time.time()
        if save_to_db:
            self._save_stocks_to_db(stocks_list)
        db_end = time.time()
        result['timing']['database_write'] = round(db_end - db_start, 2)

        # 打印汇总信息
        total_time = sum(result['timing'].values())
        print(f"同步股票列表完成，共 {len(stocks_list)} 只股票")
        print(f"  - Baostock采集耗时: {result['timing']['baostock_fetch']:.2f}s")
        print(f"  - CSV生成耗时: {result['timing']['csv_generation']:.2f}s")
        print(f"  - 数据库写入耗时: {result['timing']['database_write']:.2f}s")
        print(f"  - 总耗时: {total_time:.2f}s")

        return stocks_list

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

            if not self.baostock_source:
                raise RuntimeError("Baostock数据源未初始化")

            if not self.baostock_source.connect():
                raise RuntimeError("Baostock连接失败")

            calendar_data = self.baostock_source.get_trade_calendar(start_year, end_year)
            result['records'] = len(calendar_data)

            if save_to_db and calendar_data:
                result['db_rows'] = self.db_conn.upsert_trade_calendar(calendar_data)

            result['success'] = True
        except Exception as e:
            result['errors'].append(str(e))
            print(f"同步交易日历失败: {e}")
        finally:
            if self.baostock_source:
                self.baostock_source.disconnect()

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

        api_time = 0.0
        csv_time = 0.0
        db_time = 0.0
        api_span: Optional[Tuple[float, float]] = None
        csv_span: Optional[Tuple[float, float]] = None
        db_span: Optional[Tuple[float, float]] = None
        anomalies: List[Dict[str, Any]] = []

        start_ts = time.time()
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
                    today_str = date.today().strftime('%Y-%m-%d')
                    trade_dates = self._get_trade_dates(today_str, today_str)

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
            detection = {'checked': 0, 'hits': 0, 'refetched': 0}
            detection_details = []
            source_missing = {'qfq_empty': 0, 'raw_empty': 0}
            result['detection'] = detection
            result['detection_details'] = detection_details
            result['source_missing'] = source_missing
            result['failed_stocks'] = 0

            def process_stock(stock_item: Dict[str, Any], stock_index: int) -> Dict[str, Any]:
                ts_code = stock_item.get('ts_code')
                stock_code = stock_item.get('stock_code')
                stock_name = stock_item.get('stock_name') or ''
                if not ts_code or not stock_code:
                    return {
                        'records': 0,
                        'db_rows': 0,
                        'api_time': 0.0,
                        'csv_time': 0.0,
                        'db_time': 0.0,
                        'api_span': None,
                        'csv_span': None,
                        'db_span': None,
                        'anomalies': []
                    }

                per_stock_records: List[Dict[str, Any]] = []
                raw_for_csv: List[Dict[str, Any]] = []
                local_api_time = 0.0
                local_csv_time = 0.0
                local_db_time = 0.0
                local_anomalies: List[Dict[str, Any]] = []
                api_start_wall = None
                api_end_wall = None
                csv_start_wall = None
                csv_end_wall = None
                db_start_wall = None
                db_end_wall = None

                # ---- 双源拉取与合并（init/区间=双全量；日常=双尾部）----
                merged_records: List[Dict[str, Any]] = []
                refetched = False
                stock_detection_details: List[Dict[str, Any]] = []
                fetch_start = time.time()
                if api_start_wall is None:
                    api_start_wall = fetch_start

                if fetch_full:
                    qfq_list = self.tdx_api_source.get_kline_qfq_full(stock_code)
                    raw_list = self.tdx_api_source.get_kline_raw_full(stock_code)
                else:
                    qfq_list = self.tdx_api_source.get_kline_qfq_tail(stock_code, tail_limit)
                    raw_list = self.tdx_api_source.get_kline_raw_tail(stock_code, tail_limit)
                api_end_wall = time.time()
                local_api_time += api_end_wall - fetch_start
                self._log_progress('数据采集', stock_index, total_stocks, stock_code, stock_name, api_end_wall - fetch_start)

                local_source_missing = {'qfq_empty': 0, 'raw_empty': 0}
                if not qfq_list:
                    local_source_missing['qfq_empty'] = 1
                if not raw_list:
                    local_source_missing['raw_empty'] = 1

                merged_records = self._merge_kline_day_sources(qfq_list, raw_list)

                # ---- 日常增量的跨快照除权检测（命中→整股重拉自愈）----
                # 仅日常增量执行：显式区间补齐不做整股重拉，避免触发全量回源
                stock_detection = {'checked': 0, 'hits': 0, 'refetched': 0}
                if not fetch_full and not is_explicit_range and merged_records:
                    hit, detail = self._detect_kline_day_refetch(ts_code, merged_records)
                    stock_detection['checked'] = 1
                    if hit:
                        stock_detection['hits'] = 1
                        stock_detection_details.append({'ts_code': ts_code, **detail})
                        logger.warning(f"除权/修订检测命中，整股重拉: {ts_code} {detail}")
                        refetch_start = time.time()
                        # 增强路径下必须 refresh=True 绕过缓存回源，否则从同一份失效缓存取回同样错位的数据
                        qfq_list = self.tdx_api_source.get_kline_qfq_full(stock_code, refresh=True)
                        raw_list = self.tdx_api_source.get_kline_raw_full(stock_code)
                        local_api_time += time.time() - refetch_start
                        merged_records = self._merge_kline_day_sources(qfq_list, raw_list)
                        stock_detection['refetched'] = 1
                        refetched = True

                if not merged_records:
                    return {
                        'records': 0,
                        'db_rows': 0,
                        'api_time': local_api_time,
                        'csv_time': local_csv_time,
                        'db_time': local_db_time,
                        'api_span': (api_start_wall, api_end_wall) if api_start_wall and api_end_wall else None,
                        'csv_span': None,
                        'db_span': None,
                        'anomalies': [],
                        'detection': stock_detection,
                        'detection_details': stock_detection_details,
                        'source_missing': local_source_missing,
                        'error': None
                    }

                # ---- CSV（合并后的原始口径，按允许日期过滤）----
                if save_to_csv:
                    for item in merged_records:
                        if isinstance(item, dict):
                            item_with_code = {'ts_code': ts_code}
                            item_with_code.update(item)
                            raw_for_csv.append(item_with_code)
                    csv_start = time.time()
                    if csv_start_wall is None:
                        csv_start_wall = csv_start
                    filtered_raw = self._filter_raw_kline_by_dates(raw_for_csv, trade_dates)
                    if filtered_raw:
                        self.csv_writer.write_his_kline_day_raw(ts_code, filtered_raw)
                    csv_end_wall = time.time()
                    local_csv_time += csv_end_wall - csv_start
                    self._log_progress('csv 生成', stock_index, total_stocks, stock_code, stock_name, csv_end_wall - csv_start)

                # ---- normalize（preclose 链/change_rate/基本面匹配/turnover）----
                allowed_dates = set(trade_dates) if (trade_dates and not init_mode) else None
                if init_mode or refetched:
                    # 全量口径：整股替换写库，不做日期过滤
                    records, record_anomalies = self._normalize_kline_day_records(
                        merged_records, stock_item, fundamentals_map
                    )
                else:
                    records, record_anomalies = self._normalize_kline_day_records(
                        merged_records, stock_item, fundamentals_map, allowed_dates=allowed_dates
                    )
                for record in records:
                    record['adjust_flag'] = 2  # 前复权
                per_stock_records.extend(records)
                local_anomalies.extend(record_anomalies)

                # ---- 写库：init/重拉=整股原子替换；区间/日常=upsert ----
                if per_stock_records and save_to_db:
                    db_start = time.time()
                    if db_start_wall is None:
                        db_start_wall = db_start
                    if init_mode or refetched:
                        db_rows = self.db_conn.replace_his_kline_day(ts_code, per_stock_records)
                    else:
                        db_rows = self.db_conn.upsert_his_kline_day(per_stock_records)
                    db_end_wall = time.time()
                    local_db_time += db_end_wall - db_start
                    self._log_progress('数据入库-', stock_index, total_stocks, stock_code, stock_name, db_end_wall - db_start)
                else:
                    db_rows = 0

                return {
                    'records': len(per_stock_records),
                    'db_rows': db_rows,
                    'api_time': local_api_time,
                    'csv_time': local_csv_time,
                    'db_time': local_db_time,
                    'api_span': (api_start_wall, api_end_wall) if api_start_wall and api_end_wall else None,
                    'csv_span': (csv_start_wall, csv_end_wall) if csv_start_wall and csv_end_wall else None,
                    'db_span': (db_start_wall, db_end_wall) if db_start_wall and db_end_wall else None,
                    'anomalies': local_anomalies,
                    'detection': stock_detection,
                    'detection_details': stock_detection_details,
                    'source_missing': local_source_missing,
                    'error': None
                }

            def merge_stock_result(res: Dict[str, Any]) -> None:
                nonlocal api_time, csv_time, db_time, api_span, csv_span, db_span

                if res.get('error'):
                    result['errors'].append(res['error'])
                    result['failed_stocks'] += 1

                det = res.get('detection') or {}
                detection['checked'] += det.get('checked', 0)
                detection['hits'] += det.get('hits', 0)
                detection['refetched'] += det.get('refetched', 0)
                details = res.get('detection_details') or []
                if details:
                    detection_details.extend(details[:200 - len(detection_details)])
                for k, v in (res.get('source_missing') or {}).items():
                    source_missing[k] = source_missing.get(k, 0) + v

                result['records'] += res['records']
                result['db_rows'] += res['db_rows']
                api_time += res['api_time']
                csv_time += res['csv_time']
                db_time += res['db_time']
                anomalies.extend(res['anomalies'])

                if res['api_span']:
                    api_span = (
                        min(api_span[0], res['api_span'][0]) if api_span else res['api_span'][0],
                        max(api_span[1], res['api_span'][1]) if api_span else res['api_span'][1]
                    )
                if res['csv_span']:
                    csv_span = (
                        min(csv_span[0], res['csv_span'][0]) if csv_span else res['csv_span'][0],
                        max(csv_span[1], res['csv_span'][1]) if csv_span else res['csv_span'][1]
                    )
                if res['db_span']:
                    db_span = (
                        min(db_span[0], res['db_span'][0]) if db_span else res['db_span'][0],
                        max(db_span[1], res['db_span'][1]) if db_span else res['db_span'][1]
                    )

            def _safe_process_stock(stock_item: Dict[str, Any], stock_index: int) -> Dict[str, Any]:
                """单股异常隔离：一只股票失败不影响整场同步"""
                try:
                    return process_stock(stock_item, stock_index)
                except Exception as e:
                    self.logger.exception(f"单股日K采集失败: {stock_item.get('ts_code')} {e}")
                    return {
                        'records': 0, 'db_rows': 0, 'api_time': 0.0, 'csv_time': 0.0, 'db_time': 0.0,
                        'api_span': None, 'csv_span': None, 'db_span': None,
                        'anomalies': [], 'detection': {'checked': 0, 'hits': 0, 'refetched': 0},
                        'detection_details': [], 'source_missing': {},
                        'error': f"{stock_item.get('ts_code')}: {e}"
                    }

            max_workers = self.config_manager.get('sync.kline_max_workers', 1)
            raw_batch_size = self.config_manager.get('sync.kline_stock_batch_size', 1000)
            try:
                stock_batch_size = max(1, int(raw_batch_size or 1000))
            except (TypeError, ValueError):
                self.logger.warning(f"kline_stock_batch_size配置无效({raw_batch_size})，已回退为1000")
                stock_batch_size = 1000
            total_batches = (total_stocks + stock_batch_size - 1) // stock_batch_size

            for batch_no, batch_start in enumerate(range(0, total_stocks, stock_batch_size), start=1):
                stock_batch = stocks[batch_start:batch_start + stock_batch_size]
                batch_end = batch_start + len(stock_batch)
                self.logger.info(
                    f"日K线批次开始: 第{batch_no}/{total_batches}批, 股票[{batch_start + 1}-{batch_end}]"
                )

                if max_workers and max_workers > 1 and len(stock_batch) > 1:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = [
                            executor.submit(_safe_process_stock, s, batch_start + i + 1)
                            for i, s in enumerate(stock_batch)
                        ]
                        for future in as_completed(futures):
                            merge_stock_result(future.result())
                else:
                    for i, stock in enumerate(stock_batch):
                        merge_stock_result(_safe_process_stock(stock, batch_start + i + 1))

                self.logger.info(
                    f"日K线批次完成: 第{batch_no}/{total_batches}批, 累计记录{result['records']}条, 累计写库{result['db_rows']}条"
                )

            result['success'] = True
        except Exception as e:
            result['errors'].append(str(e))
            print(f"同步日K线失败: {e}")
        finally:
            if self.tdx_api_source:
                self.tdx_api_source.disconnect()

        end_ts = time.time()
        timing = {
            'api_time': round(api_time, 2),
            'csv_time': round(csv_time, 2),
            'db_time': round(db_time, 2),
            'total_time': round(end_ts - start_ts, 2),
            'parallel': max_workers if 'max_workers' in locals() else self.config_manager.get('sync.kline_max_workers', 1),
            'stock_batch_size': stock_batch_size if 'stock_batch_size' in locals() else self.config_manager.get('sync.kline_stock_batch_size', 1000),
            'api_wall': round(api_span[1] - api_span[0], 2) if api_span else 0,
            'csv_wall': round(csv_span[1] - csv_span[0], 2) if csv_span else 0,
            'db_wall': round(db_span[1] - db_span[0], 2) if db_span else 0
        }

        result['duration'] = timing['total_time']
        result['report_path'] = self._write_kline_day_report(result, anomalies, timing)

        return result

    def _merge_kline_day_sources(self, qfq_list: List[Dict[str, Any]],
                                 raw_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        合并双源日K：前复权序列（同花顺源）为主表，原始序列（通达信源）提供
        amount 与 raw_close。以 trade_date 对齐，原始域缺失时留空。
        字段保持中间件原始单位（价格/成交额=厘，成交量=手），由 normalize 统一换算。
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
        for item in raw_list or []:
            if not isinstance(item, dict):
                continue
            td = self._normalize_date_str(item.get('Time') or item.get('time'))
            if not td:
                continue
            row = merged.get(td)
            if row is None:
                # 前复权主表没有的日期（两源早期历史不一致），跳过
                continue
            row['Amount'] = item.get('Amount')
            row['RawClose'] = item.get('Close')
        return [merged[key] for key in sorted(merged.keys())]

    def _detect_kline_day_refetch(self, ts_code: str, merged: List[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
        """
        日常增量的除权/修订检测（跨快照）：
        1) 新拉最旧 bar 的 Last（同花顺前复权昨收）vs 库存上一交易日 close —— 命中即除权重基
        2) 重叠日期 fresh raw_close vs 库存 raw_close —— 命中即源数据修订

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

        # 2) 原始域不可变校验（重叠日期）
        dates = [self._normalize_date_str(r.get('Time') or r.get('time')) for r in merged]
        dates = [d for d in dates if d]
        if dates:
            min_d, max_d = min(dates), max(dates)
            stored_rows = self.db_conn.execute_query(
                "SELECT trade_date, raw_close FROM his_kline_day "
                "WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s AND raw_close IS NOT NULL",
                (ts_code, min_d, max_d))
            if stored_rows:
                fresh_raw_map = {}
                for r in merged:
                    d = self._normalize_date_str(r.get('Time') or r.get('time'))
                    fresh_raw_map[d] = self._scale_price(self._to_float(r.get('RawClose')))
                for row in stored_rows:
                    d = row['trade_date']
                    stored_close = self._to_float(row['raw_close'])
                    fresh_close = fresh_raw_map.get(d)
                    if stored_close is None or fresh_close is None:
                        continue
                    if abs(fresh_close - stored_close) > max(raw_tolerance, abs(stored_close) * pct_threshold):
                        return True, {
                            'type': 'raw_revision',
                            'date': d,
                            'stored_raw_close': round(stored_close, 4),
                            'fresh_raw_close': round(fresh_close, 4)
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


    def _save_stocks_to_db(self, stocks: List[Dict[str, Any]]) -> None:
        """保存股票数据到数据库 - 修复表名和字段映射"""
        batch_size = self.config_manager.get('sync.batch_size', 1000)  # 优化批次大小
        for i in range(0, len(stocks), batch_size):
            batch = stocks[i:i+batch_size]
            values = []
            for stock in batch:
                # 处理日期格式转换
                list_date = stock.get('list_date')
                delist_date = stock.get('delist_date')

                # 将日期对象转换为yyyyMMdd格式的字符串
                if list_date and hasattr(list_date, 'strftime'):
                    list_date = list_date.strftime('%Y%m%d')
                elif list_date is None:
                    list_date = None

                if delist_date and hasattr(delist_date, 'strftime'):
                    delist_date = delist_date.strftime('%Y%m%d')
                elif delist_date is None:
                    delist_date = None

                # 直接从字典获取数据，避免模型转换问题
                values.append((
                    stock.get('ts_code'),
                    stock.get('stock_code') or stock.get('code'),  # 兼容不同字段名
                    stock.get('stock_name') or stock.get('name'),
                    stock.get('cnspell'),
                    stock.get('market_code') or stock.get('market'),
                    stock.get('market_name'),
                    stock.get('exchange_code'),
                    stock.get('sector_code'),
                    stock.get('sector_name'),
                    stock.get('industry_code'),
                    stock.get('industry_name') or stock.get('industry'),
                    stock.get('list_status') or stock.get('status'),
                    list_date,
                    delist_date,
                    stock.get('type')  # 新增type字段
                ))

            query = """
                INSERT INTO base_stock_info (
                    ts_code, stock_code, stock_name, cnspell, market_code, market_name,
                    exchange_code, sector_code, sector_name, industry_code, industry_name,
                    list_status, list_date, delist_date, type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_code) DO UPDATE SET
                    ts_code = EXCLUDED.ts_code,
                    stock_name = EXCLUDED.stock_name,
                    cnspell = EXCLUDED.cnspell,
                    market_code = EXCLUDED.market_code,
                    market_name = EXCLUDED.market_name,
                    exchange_code = EXCLUDED.exchange_code,
                    sector_code = EXCLUDED.sector_code,
                    sector_name = EXCLUDED.sector_name,
                    industry_code = EXCLUDED.industry_code,
                    industry_name = EXCLUDED.industry_name,
                    list_status = EXCLUDED.list_status,
                    list_date = EXCLUDED.list_date,
                    delist_date = EXCLUDED.delist_date,
                    type = EXCLUDED.type,
                    update_time = CURRENT_TIMESTAMP
            """
            self.db_conn.execute_batch(query, values)

    def sync_fundamentals_data(self, **options) -> Dict[str, Any]:
        """
        同步基本面数据 - 新增方法

        Args:
            **options: 同步选项
                - batch_size: 批次大小，默认50
                - dry_run: 是否试运行，默认False
                - list_status: 股票上市状态过滤，默认'L'（仅上市）

        Returns:
            同步统计信息
        """
        return self.fundamentals_manager.execute_sync(**options)

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

        fundamentals_map: Dict[str, Dict[str, Any]] = {}
        for item in data:
            ts_code = item.get('ts_code')
            if not ts_code:
                continue
            disclosure_date = self._normalize_date_str(item.get('disclosure_date'))
            if not disclosure_date:
                # 无真实披露日的记录不参与时点匹配（时点不明宁缺毋滥）
                continue
            record = {
                'disclosure_date': disclosure_date,
                'stat_date': self._normalize_date_str(item.get('stat_date')),
                'total_share': item.get('total_share'),
                'float_share': item.get('float_share')
            }
            entry = fundamentals_map.setdefault(ts_code, {'dates': [], 'records': []})
            entry['dates'].append(disclosure_date)
            entry['records'].append(record)

        for ts_code, entry in fundamentals_map.items():
            # 排序键 (disclosure_date, stat_date)：同日披露年报+一季报时，
            # bisect 取报告期更新的一份（股本更新）
            combined = sorted(zip(entry['dates'], entry['records']),
                              key=lambda x: (x[0], x[1].get('stat_date') or ''))
            entry['dates'] = [x[0] for x in combined]
            entry['records'] = [x[1] for x in combined]

        return fundamentals_map

    def _match_fundamentals(
        self,
        ts_code: str,
        trade_date: str,
        fundamentals_map: Dict[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if not trade_date:
            return None
        entry = fundamentals_map.get(ts_code)
        if not entry:
            return None
        dates = entry.get('dates', [])
        if not dates:
            return None
        idx = bisect_right(dates, trade_date)
        if idx <= 0:
            return None
        return entry['records'][idx - 1]

    def _normalize_kline_1min_records(
        self,
        raw_records: List[Any],
        stock: Dict[str, Any],
        fundamentals_map: Dict[str, Dict[str, Any]],
        previous_preclose: Optional[float],
        allowed_dates: Optional[set] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        normalized: List[Dict[str, Any]] = []
        anomalies: List[Dict[str, Any]] = []

        for raw in raw_records:
            record = self._normalize_kline_1min_record(raw, stock)
            if record:
                normalized.append(record)

        if not normalized:
            return [], []

        if allowed_dates:
            normalized = [item for item in normalized if item.get('trade_date') in allowed_dates]

        normalized.sort(key=lambda x: (x['trade_date'], x['trade_time']))

        prev_close = self._to_float(previous_preclose)
        for idx, record in enumerate(normalized):
            last_value = self._to_float(record.pop('_raw_last', None))
            if last_value is not None and last_value != 0:
                preclose = last_value
            else:
                if prev_close is not None and prev_close != 0:
                    preclose = prev_close
                else:
                    prev_db_close = self._to_float(self.db_conn.fetch_prev_his_kline_1min_close(
                        record.get('ts_code'),
                        record.get('trade_date'),
                        record.get('trade_time')
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

            record['source'] = 'TDXAPI'

            limit_rate = self._get_kline_limit_rate(record.get('stock_code'), record.get('stock_name'))
            change_rate = record.get('change_rate')
            if change_rate is not None and abs(change_rate) > limit_rate:
                anomalies.append({
                    'ts_code': record.get('ts_code'),
                    'trade_date': record.get('trade_date'),
                    'trade_time': record.get('trade_time'),
                    'change_rate': round(change_rate, 6),
                    'limit_rate': limit_rate
                })

            prev_close = close

        return normalized, anomalies

    def _normalize_kline_1min_record(self, raw: Any, stock: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        trade_date = self._normalize_date_str(
            raw.get('trade_date') or raw.get('date') or raw.get('tradeDate')
        )
        trade_time = self._normalize_time_str(
            raw.get('trade_time') or raw.get('time') or raw.get('tradeTime')
        )

        trade_datetime = (
            raw.get('trade_datetime') or raw.get('datetime') or raw.get('Time') or raw.get('time')
        )
        if (not trade_date or not trade_time) and trade_datetime:
            trade_date, trade_time = self._split_datetime(trade_datetime)

        if not trade_date or not trade_time:
            return None

        trade_datetime = f"{trade_date}{trade_time}"

        open_v = self._scale_price(self._to_float(raw.get('open') or raw.get('Open')))
        high_v = self._scale_price(self._to_float(raw.get('high') or raw.get('High')))
        low_v = self._scale_price(self._to_float(raw.get('low') or raw.get('Low')))
        close_v = self._scale_price(self._to_float(raw.get('close') or raw.get('Close')))
        last_v = self._scale_price(self._to_float(raw.get('last') or raw.get('Last')))
        volume_v = self._scale_volume(self._to_float(raw.get('volume') or raw.get('Volume')))
        amount_v = self._scale_amount(self._to_float(raw.get('amount') or raw.get('Amount')))

        return {
            'ts_code': stock.get('ts_code'),
            'stock_code': stock.get('stock_code'),
            'stock_name': stock.get('stock_name'),
            'trade_date': trade_date,
            'trade_time': trade_time,
            'trade_datetime': trade_datetime,
            'open': open_v,
            'high': high_v,
            'low': low_v,
            'close': close_v,
            'preclose': None,
            '_raw_last': last_v,
            'volume': volume_v,
            'amount': amount_v
        }

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

            record['source'] = 'TDXAPI'

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
            'amount': amount_v
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
        if not kline_rows:
            return []

        rows = sorted(kline_rows, key=lambda x: x.get('trade_datetime'))
        results: List[Dict[str, Any]] = []

        threshold_rate = Decimal('0.025')
        current: Optional[Dict[str, Any]] = None
        reference_price: Optional[Decimal] = None
        segment_sequence: Dict[Tuple[Optional[str], Any, Any], int] = {}

        def _to_decimal(value: Any) -> Optional[Decimal]:
            if value is None:
                return None
            if isinstance(value, Decimal):
                return value
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                return None

        def _sign(value: Decimal) -> int:
            if value > 0:
                return 1
            if value < 0:
                return -1
            return 0

        def start_segment(
            row: Dict[str, Any],
            open_price: Decimal
        ) -> Dict[str, Any]:
            return {
                'ts_code': row.get('ts_code'),
                'stock_code': row.get('stock_code'),
                'stock_name': row.get('stock_name'),
                'trade_begin_date': row.get('trade_date'),
                'trade_begin_time': row.get('trade_time'),
                'trade_begin_datetime': row.get('trade_datetime'),
                'trade_date': row.get('trade_date'),
                'trade_time': row.get('trade_time'),
                'trade_datetime': row.get('trade_datetime'),
                'segment_index': None,
                'open': open_price,
                'high': open_price,
                'low': open_price,
                'close': open_price,
                'volume': Decimal('0'),
                'amount': Decimal('0'),
                'change_rate': None,
                'turnover_rate': Decimal('0')
            }

        def apply_slice(
            segment: Dict[str, Any],
            slice_open: Decimal,
            slice_close: Decimal,
            slice_fraction: Decimal,
            row: Dict[str, Any],
            volume_value: Decimal,
            amount_value: Decimal,
            turnover_value: Decimal
        ) -> None:
            if slice_fraction > 0:
                segment['volume'] += volume_value * slice_fraction
                segment['amount'] += amount_value * slice_fraction
                segment['turnover_rate'] += turnover_value * slice_fraction

            slice_high = max(slice_open, slice_close)
            slice_low = min(slice_open, slice_close)
            segment['high'] = slice_high if segment['high'] is None else max(segment['high'], slice_high)
            segment['low'] = slice_low if segment['low'] is None else min(segment['low'], slice_low)
            segment['close'] = slice_close
            segment['trade_date'] = row.get('trade_date')
            segment['trade_time'] = row.get('trade_time')
            segment['trade_datetime'] = row.get('trade_datetime')

        def get_threshold_ratio(
            open_price_value: Decimal,
            close_price_value: Decimal,
            threshold_price_value: Decimal
        ) -> Optional[Decimal]:
            if close_price_value == open_price_value:
                return Decimal('1') if threshold_price_value == open_price_value else None
            ratio = (threshold_price_value - open_price_value) / (close_price_value - open_price_value)
            if ratio <= 0 or ratio > 1:
                return None
            return ratio

        for row in rows:
            open_price = _to_decimal(row.get('open'))
            close_price = _to_decimal(row.get('close'))
            if open_price is None or close_price is None:
                continue

            volume = _to_decimal(row.get('volume')) or Decimal('0')
            amount = _to_decimal(row.get('amount')) or Decimal('0')
            turnover_rate = _to_decimal(row.get('turnover_rate')) or Decimal('0')

            if current is None:
                current = start_segment(row, open_price)
                reference_price = open_price

            if reference_price is None:
                continue

            remaining_fraction = Decimal('1')
            consumed_ratio = Decimal('0')

            while True:
                direction = _sign(close_price - reference_price)
                if direction == 0:
                    break

                threshold_price = reference_price * (1 + direction * threshold_rate)
                ratio = get_threshold_ratio(open_price, close_price, threshold_price)
                if ratio is None:
                    break

                if ratio < consumed_ratio:
                    break

                slice_fraction = ratio - consumed_ratio
                if slice_fraction < 0:
                    break

                slice_start_price = open_price + (close_price - open_price) * consumed_ratio
                slice_open = slice_start_price if slice_fraction > 0 else threshold_price
                apply_slice(current, slice_open, threshold_price, slice_fraction, row, volume, amount, turnover_rate)
                current['change_rate'] = Decimal(direction) * Decimal('2.5')
                segment_key = (
                    current.get('ts_code'),
                    current.get('trade_date'),
                    current.get('trade_time')
                )
                current['segment_index'] = segment_sequence.get(segment_key, 0)
                segment_sequence[segment_key] = current['segment_index'] + 1
                results.append(current)

                reference_price = threshold_price
                current = start_segment(row, open_price)

                consumed_ratio = ratio
                remaining_fraction = Decimal('1') - consumed_ratio
                if remaining_fraction <= 0:
                    break

            if remaining_fraction > 0 and current is not None:
                remaining_open = open_price + (close_price - open_price) * consumed_ratio
                apply_slice(current, remaining_open, close_price, remaining_fraction, row, volume, amount, turnover_rate)

        return results

    def _write_anal_kline_report(self, result: Dict[str, Any], timing: Dict[str, Any]) -> Optional[str]:
        try:
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            report_dir = os.path.join(repo_root, 'doc', 'reports')
            os.makedirs(report_dir, exist_ok=True)

            filename = f"anal_kline_rise_25pre_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            report_path = os.path.join(report_dir, filename)

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

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))

            return report_path
        except Exception as e:
            self.logger.error(f"生成立体K线报告失败: {e}")
            return None

    def _chunk_list(self, items: List[str], size: int) -> List[List[str]]:
        if not items or size <= 0:
            return []
        return [items[i:i + size] for i in range(0, len(items), size)]

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

    def _normalize_date_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.strftime('%Y%m%d')
        if isinstance(value, date):
            return value.strftime('%Y%m%d')

        s = str(value).strip()
        if len(s) >= 8 and s[0:8].isdigit():
            if '-' in s and len(s) >= 10:
                return s[0:10].replace('-', '')
            return s[0:8]
        if '-' in s and len(s) >= 10:
            return s[0:10].replace('-', '')
        return None

    def _normalize_time_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        s = str(value).strip()
        if ':' in s:
            parts = s.split(':')
            if len(parts) >= 2:
                hh = parts[0].zfill(2)
                mm = parts[1].zfill(2)
                return f"{hh}{mm}"
        if s.isdigit():
            if len(s) == 4:
                return s
            if len(s) == 3:
                return s.zfill(4)
            if len(s) >= 5:
                return s[0:4]
        return None

    def _split_datetime(self, value: Any) -> Tuple[Optional[str], Optional[str]]:
        s = str(value).strip()
        try:
            dt = datetime.fromisoformat(s)
            return dt.strftime('%Y%m%d'), dt.strftime('%H%M')
        except Exception:
            pass
        if len(s) >= 12 and s[0:12].isdigit():
            return s[0:8], s[8:12]
        if ' ' in s:
            date_part, time_part = s.split(' ', 1)
            date_str = self._normalize_date_str(date_part)
            time_str = self._normalize_time_str(time_part)
            return date_str, time_str
        return None, None

    def _to_float(self, value: Any) -> Optional[float]:
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _scale_price(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return value / 1000.0

    def _scale_amount(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return value / 1000.0

    def _scale_volume(self, value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return value * 100.0

    def _get_kline_limit_rate(self, stock_code: Optional[str], stock_name: Optional[str]) -> float:
        if stock_name and DataTransformer.check_is_st(stock_name):
            return 5.1
        if stock_code:
            # 创业板（300/301开头）和科创板（68开头）涨跌幅20.1%
            if stock_code.startswith(('300', '301')):
                return 20.1
            if stock_code.startswith('68'):
                return 20.1
        return 10.1

    def _write_kline_1min_report(
        self,
        result: Dict[str, Any],
        anomalies: List[Dict[str, Any]],
        timing: Dict[str, Any]
    ) -> Optional[str]:
        try:
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            report_dir = os.path.join(repo_root, 'doc', 'reports')
            os.makedirs(report_dir, exist_ok=True)

            filename = f"kline_1min_sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            report_path = os.path.join(report_dir, filename)

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

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))

            return report_path
        except Exception as e:
            self.logger.error(f"生成1分钟K线同步报告失败: {e}")
            return None

    def _write_kline_day_report(
        self,
        result: Dict[str, Any],
        anomalies: List[Dict[str, Any]],
        timing: Dict[str, Any]
    ) -> Optional[str]:
        try:
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            report_dir = os.path.join(repo_root, 'doc', 'reports')
            os.makedirs(report_dir, exist_ok=True)

            filename = f"kline_day_sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            report_path = os.path.join(report_dir, filename)

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

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))

            return report_path
        except Exception as e:
            self.logger.error(f"生成日K线同步报告失败: {e}")
            return None
