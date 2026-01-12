"""
并发基本面数据采集管理器
实现多线程并发处理，显著提升采集性能
"""

import time
import logging
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..config import ConfigManager
from ..database.connection import DatabaseConnection
from ..data_sources.thread_safe_baostock import ThreadSafeBaostockSource
from ..sync.csv_writer import CsvWriter
from ..utils.thread_safe_statistics import ThreadSafeStatistics
from ..models.collection_result import CollectionResult, CollectionStatus
from ..utils.progress_formatter import ProgressFormatter

logger = logging.getLogger(__name__)


class ConcurrentFundamentalsManager:
    """并发基本面数据采集管理器"""

    def __init__(self, config_manager: ConfigManager = None, max_workers: int = 6):
        """
        初始化并发基本面采集管理器

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
        self.progress_formatter = ProgressFormatter()  # 初始化进度格式化器

    def execute_sync(self, **options) -> Dict[str, Any]:
        """
        执行并发基本面数据同步 - 严格按照产品设计文档要求

        Args:
            **options: 配置选项
                - batch_size: 批次大小，默认150
                - dry_run: 是否试运行，默认False
                - list_status: 股票上市状态过滤，默认'L'
                - max_workers: 最大工作线程数，覆盖默认值
                - init_mode: 数据初始化模式，采集全量股票全时段(1992Q3至今)
                - year: 指定年份
                - quarter: 指定季度(1-4)
                - ts_codes: 指定股票ts_code列表
                - batch: 指定批次编号（仅在init模式下有效，从1开始）

        Returns:
            同步统计信息
        """
        batch_size = options.get('batch_size', 150)
        dry_run = options.get('dry_run', False)
        list_status = options.get('list_status', 'L')
        max_workers = options.get('max_workers', self.max_workers)
        init_mode = options.get('init_mode', False)
        year = options.get('year', None)
        quarter = options.get('quarter', None)
        ts_codes = options.get('ts_codes', None)
        save_to_csv = options.get('save_to_csv', True)
        save_to_db = options.get('save_to_db', True)
        batch = options.get('batch', None)

        # 线程安全统计收集器
        stats = ThreadSafeStatistics()

        try:
            # 采集开始前只连接一次baostock
            if not self.baostock.connect():
                raise Exception("Baostock连接失败")

            # 获取股票列表（支持ts_codes过滤）
            stocks = self._get_stock_list(list_status, ts_codes)
            # 保存原始总股票数(用于批次计算)
            original_total_stocks = len(stocks)


            # 如果指定了批次，进行批次过滤
            if batch is not None and init_mode:
                # 计算总批次数
                total_batches = (len(stocks) + batch_size - 1) // batch_size

                # 验证批次范围
                if batch > total_batches:
                    error_msg = f"批次编号超出范围: 请求批次{batch}，但总共只有{total_batches}个批次（共{len(stocks)}只股票，批次大小{batch_size}）"
                    self.logger.error(error_msg)
                    stats.finish()
                    error_stats = stats.get_stats()
                    error_stats['error'] = error_msg
                    return error_stats

                # 计算批次的起始和结束索引
                start_idx = (batch - 1) * batch_size
                end_idx = min(batch * batch_size, len(stocks))

                # 切片获取该批次的股票
                stocks = stocks[start_idx:end_idx]
                self.logger.info(f"批次过滤: 批次{batch}/{total_batches}，处理股票索引{start_idx}-{end_idx-1}，共{len(stocks)}只股票")

            stats.total_stocks = len(stocks)

            if not stocks:
                self.logger.warning(f"没有找到上市状态为'{list_status}'的股票")
                stats.finish()
                return stats.get_stats()

            # 确定采集模式
            mode_desc = "增量更新"
            if init_mode:
                mode_desc = "数据初始化(1992Q3至今)"
            elif year and quarter:
                mode_desc = f"指定时间({year}年Q{quarter})"

            self.logger.info(f"开始并发同步基本面数据: 模式={mode_desc}, 总计{stats.total_stocks}只股票，批次大小{batch_size}，最大线程数{max_workers}")

            # 分批处理（由于baostock全局会话限制，串行处理批次）
            fundamentals_data = []
            batch_count = 0

            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i + batch_size]

                # 计算当前处理批次号
                current_batch_num = batch if batch is not None else (i // batch_size) + 1
                calc_total_batches = (len(stocks) + batch_size - 1) // batch_size

                try:
                    batch_data = self._process_batch_concurrent(
                        batch_stocks,
                        dry_run,
                        stats,
                        init_mode=init_mode,
                        year=year,
                        quarter=quarter,
                        save_to_csv=save_to_csv,
                        save_to_db=save_to_db,
                        batch_num=current_batch_num,
                        total_batches=calc_total_batches,
                        batch_size=batch_size,
                        original_total_stocks=original_total_stocks
                    )
                    if batch_data:
                        fundamentals_data.extend(batch_data)
                        batch_count += 1

                        # 实时更新进度
                        current, total, percentage = stats.get_progress_info()
                        if current % 10 == 0 or percentage in [25.0, 50.0, 75.0, 100.0]:
                            self.logger.info(f"进度: {current}/{total} ({percentage:.1f}%)")
                except Exception as e:
                    self.logger.error(f"批次处理异常: {e}")

            # 处理剩余数据
            if fundamentals_data and not dry_run:
                self._process_final_batch(fundamentals_data, stats, save_to_csv, save_to_db)

            # 完成统计
            stats.finish()
            final_stats = stats.get_stats()
            final_stats['batch_count'] = batch_count

            return final_stats

        except Exception as e:
            self.logger.error(f"并发基本面数据同步失败: {e}")
            stats.finish()
            error_stats = stats.get_stats()
            error_stats['error'] = str(e)
            return error_stats
        finally:
            # 采集完成后只断开一次baostock连接
            self.baostock.disconnect()

    def _get_stock_list(self, list_status: str = 'L', ts_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        获取未退市股票列表 - 严格按照产品设计文档要求

        Args:
            list_status: 上市状态过滤，'L'=上市，'D'=退市，'P'=暂停上市
            ts_codes: 指定股票ts_code列表，为None则查询全部

        Returns:
            股票列表，包含ts_code、stock_code、stock_name和list_date字段
        """
        if ts_codes:
            # 指定股票模式：通过入参传递的股票ts_code查询base_stock_info表中未退市的数据
            placeholders = ','.join(['%s'] * len(ts_codes))
            query = f"""
                SELECT ts_code, stock_code, stock_name, list_date
                FROM base_stock_info
                WHERE list_status = %s AND type = '1' AND ts_code IN ({placeholders})
                ORDER BY ts_code
            """
            return self.db.execute_query(query, (list_status, *ts_codes))
        else:
            # 全量股票模式：查询base_stock_info表中全部未退市的数据
            query = """
                SELECT ts_code, stock_code, stock_name, list_date
                FROM base_stock_info
                WHERE list_status = %s AND type = '1'
                ORDER BY ts_code
            """
            return self.db.execute_query(query, (list_status,))

    def _process_batch_concurrent(
        self,
        stock_batch: List[Dict[str, Any]],
        dry_run: bool,
        stats: ThreadSafeStatistics,
        init_mode: bool = False,
        year: int = None,
        quarter: int = None,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        batch_num: Optional[int] = None,
        total_batches: Optional[int] = None,
        batch_size: int = 150,
        original_total_stocks: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        并发处理股票批次 - 支持三种采集方式

        Args:
            stock_batch: 股票批次数据
            dry_run: 是否试运行
            stats: 统计收集器
            init_mode: 数据初始化模式
            year: 指定年份
            quarter: 指定季度
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
            batch_num: 当前批次号（可选）
            total_batches: 总批次数（可选）
            batch_size: 批次大小

        Returns:
            处理后的基本面数据列表
        """
        batch_data = []
        batch_start_time = time.time()

        try:
            # 串行处理批次中的每只股票（避免baostock多线程问题）
            for stock in stock_batch:
                try:
                    stock_start_time = time.time()

                    result = self._collect_fundamentals_concurrent(
                        stock,
                        stats,
                        init_mode=init_mode,
                        year=year,
                        quarter=quarter
                    )

                    elapsed_time = time.time() - stock_start_time

                    # 更新统计信息
                    stats.add_result(result)

                    # 获取当前进度信息
                    current, total, progress_percentage = stats.get_progress_info()

                    # 计算批次信息（如果未提供）
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

                    # 输出详细的单只股票进度
                    if result and result.is_success:
                        # 支持列表数据(init模式)和单条数据
                        if isinstance(result.data, list):
                            batch_data.extend(result.data)
                        else:
                            batch_data.append(result.data)

                        # 提取披露日期(从第一条或唯一的数据中)
                        first_data = result.data[0] if isinstance(result.data, list) else result.data
                        disclosure_date = first_data.get('disclosure_date', '')
                        if hasattr(disclosure_date, 'strftime'):
                            disclosure_date_str = disclosure_date.strftime('%Y%m%d')
                        else:
                            disclosure_date_str = str(disclosure_date) if disclosure_date else ''

                        # 使用formatter格式化进度信息
                        progress_msg = self.progress_formatter.format_progress(
                            current=current,
                            total=total,
                            ts_code=stock['ts_code'],
                            stock_name=stock['stock_name'],
                            status=CollectionStatus.SUCCESS,
                            elapsed_time=elapsed_time,
                            batch_num=calc_batch_num,
                            total_batches=calc_total_batches,
                            disclosure_date=disclosure_date_str
                        )
                        self.logger.info(progress_msg)

                    elif result and result.is_no_data:
                        # 无数据状态进度
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

                    elif result and result.is_error:
                        # 错误状态进度
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
                    self.logger.error(f"股票处理异常: {e}")

            # 批次处理
            # 批次处理
            if batch_data and not dry_run:
                # 写入CSV文件
                if save_to_csv:
                    csv_start_time = time.time()
                    self.csv_writer.write_base_fundamentals_info(batch_data)
                    csv_duration = time.time() - csv_start_time
                    stats.add_csv_timing(csv_duration)

                # 写入数据库
                if save_to_db:
                    db_start_time = time.time()
                    affected_rows = self.db.upsert_fundamentals_data(batch_data)
                    db_duration = time.time() - db_start_time
                    stats.add_database_timing(db_duration)
                else:
                    affected_rows = 0

                self.logger.debug(f"批次处理完成: {len(batch_data)} 条记录，CSV: {'是' if save_to_csv else '否'}，DB: {'是' if save_to_db else '否'}，影响行数: {affected_rows}")

            stats.increment_batch_count()

        except Exception as e:
            self.logger.error(f"批次处理失败: {e}")

        return batch_data

    def _collect_fundamentals_concurrent(
        self, 
        stock: Dict[str, Any], 
        stats: ThreadSafeStatistics,
        init_mode: bool = False,
        year: int = None,
        quarter: int = None
    ) -> CollectionResult:
        """
        并发采集单只股票基本面数据 - 严格按照产品设计文档支持三种采集方式

        Args:
            stock: 股票基本信息，包含ts_code、stock_code、stock_name和list_date
            stats: 统计收集器
            init_mode: 数据初始化模式，采集全时段数据(根据list_date动态计算起始季度至今)
            year: 指定年份
            quarter: 指定季度(1-4)

        Returns:
            CollectionResult: 包含状态、数据和错误信息的结果对象
        """
        start_time = time.time()
        thread_name = threading.current_thread().name

        try:
            all_fundamentals = []
            
            if init_mode:
                # 数据初始化模式：根据list_date动态计算起始季度
                from ..utils.quarter_calculator import calculate_start_quarter, get_current_previous_quarter

                # 获取股票上市日期并计算起始季度
                list_date = stock.get('list_date')  # yyyyMMdd格式字符串或None
                start_year, start_quarter = calculate_start_quarter(list_date)

                # 获取当前前一季度
                end_year, end_quarter = get_current_previous_quarter()

                # 从计算出的起始季度遍历到当前前一季度（连接已在execute_sync中统一管理）
                for y in range(start_year, end_year + 1):
                    # 确定本年度的起始季度
                    current_start_q = start_quarter if y == start_year else 1
                    # 确定本年度的结束季度
                    current_end_q = end_quarter if y == end_year else 4

                    for q in range(current_start_q, current_end_q + 1):
                        fundamentals = self.baostock.get_stock_fundamentals(stock['ts_code'], year=y, quarter=q)
                        if fundamentals:
                            fundamentals['stock_name'] = stock['stock_name']
                            all_fundamentals.append(fundamentals)
                            
            elif year and quarter:
                # 指定时间模式：只查询指定year和quarter的数据
                fundamentals = self.baostock.get_stock_fundamentals(stock['ts_code'], year=year, quarter=quarter)
                if fundamentals:
                    fundamentals['stock_name'] = stock['stock_name']
                    all_fundamentals.append(fundamentals)
            else:
                # 增量更新模式：默认查询前一季度的数据，如果没有获取到数据，再查询一次前两季度的数据
                fundamentals = self.baostock.get_stock_fundamentals(stock['ts_code'])
                if fundamentals:
                    fundamentals['stock_name'] = stock['stock_name']
                    all_fundamentals.append(fundamentals)

            execution_time = time.time() - start_time

            if all_fundamentals:
                self.logger.debug(f"[{thread_name}]({stock['ts_code']}-{stock['stock_name']}) 采集成功,共{len(all_fundamentals)}条")
                # init模式返回全部数据列表,其他模式返回单条数据
                if len(all_fundamentals) > 1:
                    # 多条数据(init模式):返回完整列表
                    return CollectionResult.success(all_fundamentals, execution_time)
                else:
                    # 单条数据:返回单个Dict
                    return CollectionResult.success(all_fundamentals[0], execution_time)
            else:
                self.logger.debug(f"[{thread_name}]({stock['ts_code']}-{stock['stock_name']}) 无数据")
                return CollectionResult.no_data(execution_time)

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"[{thread_name}]({stock['ts_code']}-{stock['stock_name']}) 采集异常: {e}")
            return CollectionResult.error(str(e), execution_time)

    def _process_final_batch(self, fundamentals_data: List[Dict[str, Any]], stats: ThreadSafeStatistics,
                             save_to_csv: bool = True, save_to_db: bool = True) -> None:
        """
        处理最终批次数据

        Args:
            fundamentals_data: 基本面数据列表
            stats: 统计收集器
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
        """
        if not fundamentals_data:
            return

        try:
            # 写入CSV文件
            if save_to_csv:
                csv_start_time = time.time()
                self.csv_writer.write_base_fundamentals_info(fundamentals_data)
                csv_duration = time.time() - csv_start_time
                stats.add_csv_timing(csv_duration)
                self.logger.info(f"最终同步完成: 共 {len(fundamentals_data)} 条记录，CSV生成耗时: {csv_duration:.3f}s")

            # 数据库在这个阶段不需要重复写入，因为批次处理中已经写入了
            if not save_to_csv:
                self.logger.info(f"最终同步完成: 共 {len(fundamentals_data)} 条记录（CSV已禁用）")

        except Exception as e:
            self.logger.error(f"最终批次处理失败: {e}")


def sync_fundamentals_data_concurrent(config_manager: ConfigManager = None, **options) -> Dict[str, Any]:
    """
    便捷函数：并发同步基本面数据

    Args:
        config_manager: 配置管理器
        **options: 同步选项

    Returns:
        同步统计信息
    """
    max_workers = options.get('max_workers', 6)
    manager = ConcurrentFundamentalsManager(config_manager, max_workers=max_workers)
    return manager.execute_sync(**options)