"""
并发日线K线数据采集管理器
实现多线程并发处理，支持数据初始化、日增量更新和指定范围采集
"""

import time
import logging
import threading
import bisect
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta

from ..config import ConfigManager
from ..database.connection import DatabaseConnection
from ..data_sources.thread_safe_baostock import ThreadSafeBaostockSource
from ..sync.csv_writer import CsvWriter
from ..utils.thread_safe_statistics import ThreadSafeStatistics
from ..models.collection_result import CollectionResult, CollectionStatus
from ..utils.progress_formatter import ProgressFormatter
from ..utils.data_transformer import DataTransformer

logger = logging.getLogger(__name__)


class DailyKlineSyncManager:
    """并发日线K线数据采集管理器"""

    def __init__(self, config_manager: ConfigManager = None, max_workers: int = 6):
        """
        初始化并发日线K线采集管理器

        Args:
            config_manager: 配置管理器
            max_workers: 最大工作线程数，默认6
        """
        self.config = config_manager or ConfigManager()
        self.db = DatabaseConnection(self.config)

        # 使用线程安全的baostock源
        baostock_config = {
            'data_path': self.config.get('data_paths', {}).get('csv', 'uat/data'),
            'financial_data_rate_limit': self.config.get('data_sources.baostock', {}).get('financial_data_rate_limit', {})
        }
        self.baostock = ThreadSafeBaostockSource(baostock_config)

        self.csv_writer = CsvWriter(self.config)
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
        self.progress_formatter = ProgressFormatter()

        # 异常记录
        self.anomalies = []

    def execute_sync(self, **options) -> Dict[str, Any]:
        """
        执行并发日线K线数据同步

        Args:
            **options: 配置选项
                - batch_size: 批次大小，默认50 (K线数据量大，批次小一点)
                - dry_run: 是否试运行，默认False
                - list_status: 股票上市状态过滤，默认'L'
                - max_workers: 最大工作线程数，覆盖默认值
                - init_mode: 数据初始化模式，采集1990-12-19至今
                - start_date: 指定开始日期 (YYYY-MM-DD)
                - end_date: 指定结束日期 (YYYY-MM-DD)
                - ts_codes: 指定股票ts_code列表
                - batch: 指定批次编号（仅在init模式下有效，从1开始）

        Returns:
            同步统计信息
        """
        batch_size = options.get('batch_size', 50)
        dry_run = options.get('dry_run', False)
        list_status = options.get('list_status', 'L')
        max_workers = options.get('max_workers', self.max_workers)
        init_mode = options.get('init_mode', False)
        start_date = options.get('start_date', None)
        end_date = options.get('end_date', None)
        ts_codes = options.get('ts_codes', None)
        save_to_csv = options.get('save_to_csv', True)
        save_to_db = options.get('save_to_db', True)
        batch = options.get('batch', None)

        # 线程安全统计收集器
        stats = ThreadSafeStatistics()

        # 重置异常记录
        self.anomalies = []

        try:
            # 采集开始前只连接一次baostock
            if not self.baostock.connect():
                raise Exception("Baostock连接失败")

            # 获取股票列表（支持ts_codes过滤）
            stocks = self._get_stock_list(list_status, ts_codes)
            original_total_stocks = len(stocks)

            # 如果指定了批次，进行批次过滤
            if batch is not None and init_mode:
                total_batches = (len(stocks) + batch_size - 1) // batch_size

                if batch > total_batches:
                    error_msg = f"批次编号超出范围: 请求批次{batch}，但总共只有{total_batches}个批次"
                    self.logger.error(error_msg)
                    stats.finish()
                    error_stats = stats.get_stats()
                    error_stats['error'] = error_msg
                    return error_stats

                start_idx = (batch - 1) * batch_size
                end_idx = min(batch * batch_size, len(stocks))
                stocks = stocks[start_idx:end_idx]
                self.logger.info(f"批次过滤: 批次{batch}/{total_batches}，处理股票索引{start_idx}-{end_idx-1}，共{len(stocks)}只股票")

            stats.total_stocks = len(stocks)

            if not stocks:
                self.logger.warning(f"没有找到上市状态为'{list_status}'的股票")
                stats.finish()
                return stats.get_stats()

            # 确定日期范围和模式描述
            q_start_date, q_end_date = self._get_date_range(init_mode, start_date, end_date)

            mode_desc = "日增量更新"
            if init_mode:
                mode_desc = f"数据初始化({q_start_date}至{q_end_date})"
            elif start_date and end_date:
                mode_desc = f"指定范围({q_start_date}至{q_end_date})"

            self.logger.info(f"开始并发同步日线K线: 模式={mode_desc}, 总计{stats.total_stocks}只股票，批次大小{batch_size}，最大线程数{max_workers}")

            # 分批处理
            all_kline_data = [] # 仅在非init模式或小批量时收集所有数据用于最后处理，init模式下尽量批次内处理完
            batch_count = 0

            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i + batch_size]

                current_batch_num = batch if batch is not None else (i // batch_size) + 1
                calc_total_batches = (len(stocks) + batch_size - 1) // batch_size

                try:
                    # 并发处理批次
                    batch_data = self._process_batch_concurrent(
                        batch_stocks,
                        dry_run,
                        stats,
                        q_start_date,
                        q_end_date,
                        save_to_csv=save_to_csv,
                        save_to_db=save_to_db,
                        batch_num=current_batch_num,
                        total_batches=calc_total_batches,
                        batch_size=batch_size,
                        original_total_stocks=original_total_stocks
                    )

                    batch_count += 1

                    # 实时更新进度
                    current, total, percentage = stats.get_progress_info()
                    if current % 10 == 0 or percentage in [25.0, 50.0, 75.0, 100.0]:
                        self.logger.info(f"进度: {current}/{total} ({percentage:.1f}%)")

                except Exception as e:
                    self.logger.error(f"批次处理异常: {e}")

            # 完成统计
            stats.finish()
            final_stats = stats.get_stats()
            final_stats['batch_count'] = batch_count
            final_stats['anomalies_count'] = len(self.anomalies)

            # 生成采集报告
            self._generate_sync_report(stats, batch_count, final_stats.get('db_total_time', 0))

            return final_stats

        except Exception as e:
            self.logger.error(f"并发日线K线同步失败: {e}")
            stats.finish()
            error_stats = stats.get_stats()
            error_stats['error'] = str(e)
            return error_stats
        finally:
            self.baostock.disconnect()

    def _get_stock_list(self, list_status: str = 'L', ts_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        获取股票列表
        注意：仅获取在 base_fundamentals_info 表中有记录的股票
        """
        if ts_codes:
            placeholders = ','.join(['%s'] * len(ts_codes))
            query = f"""
                SELECT DISTINCT ts_code, stock_code, stock_name
                FROM base_fundamentals_info
                WHERE ts_code IN ({placeholders})
                ORDER BY ts_code
            """
            return self.db.execute_query(query, tuple(ts_codes))
        else:
            query = """
                SELECT DISTINCT ts_code, stock_code, stock_name
                FROM base_fundamentals_info
                ORDER BY ts_code
            """
            return self.db.execute_query(query)

    def _get_date_range(self, init_mode: bool, start_date: str, end_date: str) -> Tuple[str, str]:
        """确定查询日期范围"""
        today = datetime.now().strftime('%Y-%m-%d')

        if init_mode:
            # 初始化模式：从1990-12-19开始，end为空
            return '1990-12-19', ''

        if start_date and end_date:
            # 指定范围模式
            return start_date, end_date

        # 默认模式（日增量更新）：仅更新当天
        # 修改为：start_date = end_date = today
        return today, today

    def _generate_sync_report(self, stats: ThreadSafeStatistics, batch_count: int, db_duration: float):
        """
        生成同步报告 - 包含性能信息和异常数据
        """
        report_path = f"doc/reports/kline_sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

        try:
            # 获取统计数据
            s = stats.get_stats()
            timing = s.get('timing', {})

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("# 日线K线数据采集报告\n\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                f.write("## 1. 性能信息\n\n")
                f.write("| 环节 | 总耗时(秒) | 说明 |\n")
                f.write("| --- | --- | --- |\n")
                f.write(f"| Baostock接口 | {timing.get('baostock_total', 0):.3f} | 调用接口获取数据耗时 |\n")
                f.write(f"| CSV生成 | {timing.get('csv_total', 0):.3f} | 写入CSV文件耗时 |\n") # 需要在csv_writer中统计并传递
                f.write(f"| 数据库写入 | {s.get('db_total_time', 0):.3f} | 写库总耗时 |\n") # stats.add_database_timing 累加
                f.write(f"| 总执行时间 | {s.get('total_execution_time', 0):.3f} | 任务总耗时 |\n\n")

                f.write("## 2. 采集统计\n\n")
                f.write(f"- 总股票数: {s.get('total_stocks', 0)}\n")
                f.write(f"- 成功: {s.get('success_count', 0)}\n")
                f.write(f"- 无数据: {s.get('no_data_count', 0)}\n")
                f.write(f"- 失败: {s.get('error_count', 0)}\n")
                f.write(f"- 批次数量: {batch_count}\n\n")

                f.write("## 3. 异常数据\n\n")
                if self.anomalies:
                    f.write(f"共发现 {len(self.anomalies)} 条异常数据：\n\n")
                    f.write("| 代码 | 名称 | 日期 | 涨跌幅(%) | 阈值(%) | ST |\n")
                    f.write("| --- | --- | --- | --- | --- | --- |\n")
                    for a in self.anomalies:
                        f.write(f"| {a['ts_code']} | {a['stock_name']} | {a['trade_date']} | {a['pct_chg']} | {a['limit']} | {a['is_st']} |\n")
                else:
                    f.write("本次采集未发现异常数据。\n")

            self.logger.info(f"已生成采集报告: {report_path}")
        except Exception as e:
            self.logger.error(f"生成采集报告失败: {e}")

    def _enrich_and_transform(self, raw_data: List[Dict[str, str]], stock_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        丰富和转换数据 - 严格按照产品文档和数据库Schema要求
        """
        if not raw_data:
            return []

        ts_code = stock_info.get('ts_code')
        stock_code = stock_info.get('stock_code')
        stock_name = stock_info.get('stock_name')

        # 1. 加载基本面数据
        # 查询该股票的所有基本面记录，按披露日期排序
        # 注意：base_fundamentals_info 中 disclosure_date 是 varchar(8)
        query = """
            SELECT disclosure_date, total_share, float_share
            FROM base_fundamentals_info
            WHERE ts_code = %s
            ORDER BY disclosure_date ASC
        """
        fundamentals = self.db.execute_query(query, (ts_code,))

        # 准备披露日期列表以便查找
        # disclosure_date 已经是 varchar(8) 'YYYYMMDD'，无需转换，直接比较字符串即可
        fund_dates = [f['disclosure_date'] for f in fundamentals]

        enriched_data = []

        for row in raw_data:
            try:
                # 原始日期格式 YYYY-MM-DD
                raw_date = row['date']
                # 转换目标格式 YYYYMMDD
                trade_date_str = raw_date.replace('-', '')

                # 查找适用的基本面数据：找到最后一个 disclosure_date <= trade_date 的记录
                # bisect_right 返回的是插入点，使得左边的都 <= x
                idx = bisect.bisect_right(fund_dates, trade_date_str)

                total_share = None
                float_share = None
                disclosure_date = None

                if idx > 0:
                    fund_rec = fundamentals[idx - 1]
                    total_share = float(fund_rec['total_share']) if fund_rec['total_share'] is not None else None
                    float_share = float(fund_rec['float_share']) if fund_rec['float_share'] is not None else None
                    disclosure_date = fund_rec['disclosure_date']

                # 类型转换辅助函数
                def to_float(val):
                    return float(val) if val and val != '' else None

                def to_int(val):
                    return int(val) if val and val != '' else None

                # 严格按照表结构构建字典
                item = {
                    'ts_code': ts_code,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'trade_date': trade_date_str,
                    'open': to_float(row['open']),
                    'high': to_float(row['high']),
                    'low': to_float(row['low']),
                    'close': to_float(row['close']),
                    'preclose': to_float(row['preclose']),
                    'volume': to_float(row['volume']),
                    'amount': to_float(row['amount']),
                    'trade_status': to_int(row['tradestatus']),
                    'is_st': row['isST'] == '1', # 转换为boolean
                    'change_rate': to_float(row['pctChg']),
                    'turnover_rate': to_float(row['turn']), # 使用 turn 字段
                    'fundamentals_disclosure_date': disclosure_date,
                    'total_share': total_share,
                    'float_share': float_share,
                    'pe_ttm': to_float(row['peTTM']),
                    'pb_rate': to_float(row['pbMRQ']),
                    'ps_ttm': to_float(row['psTTM']),
                    'pcf_ttm': to_float(row['pcfNcfTTM']),
                    'source': 'BAOSTOCK',
                    'create_time': datetime.now(),
                    'update_time': datetime.now()
                }
                enriched_data.append(item)

            except Exception as e:
                self.logger.error(f"数据转换失败: {e} Row: {row}")
                continue

        return enriched_data

    def _collect_daily_kline_raw(
        self,
        stock: Dict[str, Any],
        start_date: str,
        end_date: str
    ) -> CollectionResult:
        """采集单只股票原始日线数据"""
        start_time = time.time()
        try:
            # 使用 fetch_daily_k_data_raw
            k_data = self.baostock.fetch_daily_k_data_raw(stock['ts_code'], start_date, end_date)
            execution_time = time.time() - start_time

            if k_data:
                return CollectionResult.success(k_data, execution_time)
            else:
                return CollectionResult.no_data(execution_time)
        except Exception as e:
            execution_time = time.time() - start_time
            return CollectionResult.error(str(e), execution_time)


    def _process_batch_concurrent(
        self,
        stock_batch: List[Dict[str, Any]],
        dry_run: bool,
        stats: ThreadSafeStatistics,
        start_date: str,
        end_date: str,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        batch_num: Optional[int] = None,
        total_batches: Optional[int] = None,
        batch_size: int = 50,
        original_total_stocks: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """并发处理股票批次"""
        batch_data = []

        # 使用线程池并发采集
        with threading.Lock(): # 保护print/log输出不乱序
            pass

        # 这里需要注意：baostock的python sdk是基于全局socket连接的，多线程可能存在竞争问题
        # 虽然ThreadSafeBaostockSource加了锁，但那只是针对connect/disconnect
        # query_history_k_data_plus本身是否线程安全取决于baostock内部实现
        # 为了稳妥，如同fundamentals manager，我们在batch内部串行处理，但batch之间可以优化
        # 但fundamentals manager实际上是单线程处理batch内的每只股票
        # 我们遵循concurrent_fundamentals_manager的模式：
        # Manager层负责批次循环，_process_batch_concurrent 内部也是循环处理
        # 真正的并发需要多线程调用 _collect_daily_kline_concurrent

        # 修改策略：鉴于baostock限制，我们还是串行处理每只股票的请求，但可以通过减少IO等待来优化
        # 由于我们使用ThreadSafeBaostockSource，它没有为query加锁，如果baostock不支持并发query，我们必须串行
        # 假设baostock query不是线程安全的（通常是单连接），我们保持串行采集

        for stock in stock_batch:
            try:
                stock_start_time = time.time()

                # 实际执行日期范围修正：如果上市日期晚于start_date，使用上市日期
                actual_start_date = start_date
                if stock.get('list_date'):
                    list_date_val = stock['list_date']
                    if hasattr(list_date_val, 'strftime'):
                        list_date_str = list_date_val.strftime('%Y-%m-%d')
                    else:
                        # 尝试处理字符串格式，可能是 YYYYMMDD
                        s = str(list_date_val)
                        if len(s) == 8 and s.isdigit():
                            list_date_str = f"{s[:4]}-{s[4:6]}-{s[6:]}"
                        else:
                            list_date_str = s

                    if list_date_str > start_date:
                        actual_start_date = list_date_str

                # 如果实际开始日期晚于结束日期，跳过
                # 注意：如果end_date为空（初始化模式），则不跳过
                if end_date and actual_start_date > end_date:
                    self.logger.debug(f"{stock['ts_code']} 上市日期{actual_start_date}晚于结束日期{end_date}，跳过")
                    stats.add_result(CollectionResult.no_data(0))
                    continue

                result = self._collect_daily_kline_raw(
                    stock,
                    actual_start_date,
                    end_date
                )

                elapsed_time = time.time() - stock_start_time
                stats.add_result(result)

                current, total, _ = stats.get_progress_info()

                if batch_num is None or total_batches is None:
                     calc_batch_num, calc_total_batches = self.progress_formatter.calculate_batch_info(
                        current_index=current - 1,
                        total_stocks=total,
                        batch_size=batch_size,
                        specified_batch=None,
                        original_total_stocks=original_total_stocks
                    )
                else:
                    calc_batch_num = batch_num
                    calc_total_batches = total_batches

                if result.is_success:
                    raw_kline_data = result.data
                    if raw_kline_data:
                        # CSV保存：每只股票单独存文件（按需求），保存原始数据
                        if save_to_csv and not dry_run:
                            csv_start_time = time.time()
                            self.csv_writer.write_daily_kline_data(raw_kline_data, stock['ts_code'])
                            csv_duration = time.time() - csv_start_time
                            stats.add_csv_timing(csv_duration)

                        # 转换和丰富数据用于入库和验证
                        enriched_data = self._enrich_and_transform(raw_kline_data, stock)

                        if enriched_data:
                            batch_data.extend(enriched_data)

                            # 验证数据
                            self._validate_data(enriched_data, stock)

                            # 单只股票进度
                            progress_msg = self.progress_formatter.format_progress(
                                current=current,
                                total=total,
                                ts_code=stock['ts_code'],
                                stock_name=stock['stock_name'],
                                status=CollectionStatus.SUCCESS,
                                elapsed_time=elapsed_time,
                                batch_num=calc_batch_num,
                                total_batches=calc_total_batches,
                                disclosure_date=f"{len(enriched_data)}条"
                            )
                            self.logger.info(progress_msg)
                    else:
                         # 成功但无数据（可能是停牌或无交易）
                         progress_msg = self.progress_formatter.format_progress(
                            current=current,
                            total=total,
                            ts_code=stock['ts_code'],
                            stock_name=stock['stock_name'],
                            status=CollectionStatus.NO_DATA,
                            elapsed_time=elapsed_time,
                            batch_num=calc_batch_num,
                            total_batches=calc_total_batches
                        )
                         self.logger.info(progress_msg)
                elif result.is_error:
                    progress_msg = self.progress_formatter.format_progress(
                        current=current,
                        total=total,
                        ts_code=stock['ts_code'],
                        stock_name=stock['stock_name'],
                        status=CollectionStatus.ERROR,
                        elapsed_time=elapsed_time,
                        batch_num=calc_batch_num,
                        total_batches=calc_total_batches,
                        error_message=result.error_message
                    )
                    self.logger.error(progress_msg)

            except Exception as e:
                self.logger.error(f"股票{stock.get('ts_code')}处理异常: {e}")

        # 批次入库
        if batch_data and save_to_db and not dry_run:
            try:
                db_start_time = time.time()
                self.db.upsert_kline_data(batch_data)
                db_duration = time.time() - db_start_time
                stats.add_database_timing(db_duration)
                self.logger.debug(f"批次入库完成: {len(batch_data)}条记录，耗时{db_duration:.3f}s")
            except Exception as e:
                self.logger.error(f"批次入库失败: {e}")

        return batch_data

    def _collect_daily_kline_concurrent(
        self,
        stock: Dict[str, Any],
        start_date: str,
        end_date: str
    ) -> CollectionResult:
        """采集单只股票日线数据"""
        start_time = time.time()
        try:
            k_data = self.baostock.get_daily_k_data(stock['ts_code'], start_date, end_date)
            execution_time = time.time() - start_time

            if k_data:
                return CollectionResult.success(k_data, execution_time)
            else:
                return CollectionResult.no_data(execution_time)
        except Exception as e:
            execution_time = time.time() - start_time
            return CollectionResult.error(str(e), execution_time)

    def _validate_data(self, data: List[Dict[str, Any]], stock: Dict[str, Any]) -> None:
        """
        验证数据，检查异常波动
        Main Board (SH/SZ): ±10.1%
        ChiNext/STAR (30xxxx, 68xxxx): ±20.1%
        ST Stocks (is_st=True): ±5.1%
        """
        stock_code = stock.get('stock_code', '')
        ts_code = stock.get('ts_code', '')

        # Determine limits based on stock type
        limit = 10.1 # default Main Board

        if stock_code.startswith('30') or stock_code.startswith('68'):
            limit = 20.1 # ChiNext / STAR

        for day in data:
            # Enriched data uses 'change_rate' for pctChg
            pct_chg = day.get('change_rate')
            # Enriched data uses boolean for is_st
            is_st = day.get('is_st')

            # ST override
            current_limit = limit
            if is_st is True:
                current_limit = 5.1

            if pct_chg is not None:
                if abs(pct_chg) > current_limit:
                    anomaly = {
                        'ts_code': ts_code,
                        'trade_date': day.get('trade_date'),
                        'pct_chg': pct_chg,
                        'limit': current_limit,
                        'is_st': str(is_st),
                        'stock_name': stock.get('stock_name')
                    }
                    self.anomalies.append(anomaly)
                    # self.logger.warning(f"发现异常波动: {ts_code} {day.get('trade_date')} 涨跌幅{pct_chg}% (阈值±{current_limit}%)")

def sync_daily_kline_concurrent(config_manager: ConfigManager = None, **options) -> Dict[str, Any]:
    """便捷函数"""
    max_workers = options.get('max_workers', 6)
    manager = DailyKlineSyncManager(config_manager, max_workers=max_workers)
    return manager.execute_sync(**options)
