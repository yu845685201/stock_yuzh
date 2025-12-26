"""
并发基本面数据采集管理器
实现多线程并发处理，显著提升采集性能
"""

import time
import logging
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..config import ConfigManager
from ..database.connection import DatabaseConnection
from ..data_sources.thread_safe_baostock import ThreadSafeBaostockSource
from ..sync.csv_writer import CsvWriter
from ..utils.thread_safe_statistics import ThreadSafeStatistics
from ..models.collection_result import CollectionResult

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

    def execute_sync(self, **options) -> Dict[str, Any]:
        """
        执行并发基本面数据同步

        Args:
            **options: 配置选项
                - batch_size: 批次大小，默认150
                - dry_run: 是否试运行，默认False
                - list_status: 股票上市状态过滤，默认'L'
                - max_workers: 最大工作线程数，覆盖默认值

        Returns:
            同步统计信息
        """
        batch_size = options.get('batch_size', 150)
        dry_run = options.get('dry_run', False)
        list_status = options.get('list_status', 'L')
        max_workers = options.get('max_workers', self.max_workers)

        # 线程安全统计收集器
        stats = ThreadSafeStatistics()

        try:
            # 获取股票列表
            stocks = self._get_stock_list(list_status)
            stats.total_stocks = len(stocks)

            if not stocks:
                self.logger.warning(f"没有找到上市状态为'{list_status}'的股票")
                stats.finish()
                return stats.get_stats()

            self.logger.info(f"开始并发同步基本面数据: 总计{stats.total_stocks}只股票，批次大小{batch_size}，最大线程数{max_workers}")

            # 分批处理
            fundamentals_data = []
            batch_count = 0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                # 提交批次任务
                for i in range(0, len(stocks), batch_size):
                    batch = stocks[i:i + batch_size]
                    future = executor.submit(self._process_batch_concurrent, batch, dry_run, stats)
                    futures.append(future)

                # 收集批次结果
                for future in as_completed(futures):
                    try:
                        batch_data = future.result()
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
                self._process_final_batch(fundamentals_data, stats)

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

    def _get_stock_list(self, list_status: str = 'L') -> List[Dict[str, Any]]:
        """
        获取未退市股票列表

        Args:
            list_status: 上市状态过滤，'L'=上市，' D'=退市，'P'=暂停上市

        Returns:
            股票列表
        """
        query = """
            SELECT ts_code, stock_code, stock_name
            FROM base_stock_info
            WHERE list_status = %s
            ORDER BY ts_code
        """
        return self.db.execute_query(query, (list_status,))

    def _process_batch_concurrent(self, stock_batch: List[Dict[str, Any]], dry_run: bool, stats: ThreadSafeStatistics) -> List[Dict[str, Any]]:
        """
        并发处理股票批次

        Args:
            stock_batch: 股票批次数据
            dry_run: 是否试运行
            stats: 统计收集器

        Returns:
            处理后的基本面数据列表
        """
        batch_data = []
        batch_start_time = time.time()

        try:
            # 为批次中的每只股票创建独立任务
            with ThreadPoolExecutor(max_workers=min(10, len(stock_batch))) as batch_executor:
                futures = []

                for stock in stock_batch:
                    future = batch_executor.submit(self._collect_fundamentals_concurrent, stock, stats)
                    futures.append(future)

                # 收集结果
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result and result.is_success:
                            batch_data.append(result.data)
                    except Exception as e:
                        self.logger.error(f"股票处理异常: {e}")

            # 批次处理
            if batch_data and not dry_run:
                csv_start_time = time.time()
                self.csv_writer.write_base_fundamentals_info(batch_data)
                csv_duration = time.time() - csv_start_time
                stats.add_csv_timing(csv_duration)

                db_start_time = time.time()
                affected_rows = self.db.upsert_fundamentals_data(batch_data)
                db_duration = time.time() - db_start_time
                stats.add_database_timing(db_duration)

                self.logger.debug(f"批次处理完成: {len(batch_data)} 条记录，CSV: {csv_duration:.3f}s，DB: {db_duration:.3f}s，影响行数: {affected_rows}")

            stats.increment_batch_count()

        except Exception as e:
            self.logger.error(f"批次处理失败: {e}")

        return batch_data

    def _collect_fundamentals_concurrent(self, stock: Dict[str, Any], stats: ThreadSafeStatistics) -> CollectionResult:
        """
        并发采集单只股票基本面数据

        Args:
            stock: 股票基本信息
            stats: 统计收集器

        Returns:
            CollectionResult: 包含状态、数据和错误信息的结果对象
        """
        start_time = time.time()
        thread_name = threading.current_thread().name

        try:
            # 使用线程安全的baostock方法获取基本面数据
            with self.baostock:
                fundamentals = self.baostock.get_stock_fundamentals(stock['ts_code'])

            execution_time = time.time() - start_time

            if fundamentals:
                # 确保包含股票名称
                fundamentals['stock_name'] = stock['stock_name']
                self.logger.debug(f"[{thread_name}]({stock['ts_code']}-{stock['stock_name']}) 采集成功")
                return CollectionResult.success(fundamentals, execution_time)
            else:
                self.logger.debug(f"[{thread_name}]({stock['ts_code']}-{stock['stock_name']}) 无数据")
                return CollectionResult.no_data(execution_time)

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"[{thread_name}]({stock['ts_code']}-{stock['stock_name']}) 采集异常: {e}")
            return CollectionResult.error(str(e), execution_time)

    def _process_final_batch(self, fundamentals_data: List[Dict[str, Any]], stats: ThreadSafeStatistics) -> None:
        """
        处理最终批次数据

        Args:
            fundamentals_data: 基本面数据列表
            stats: 统计收集器
        """
        if not fundamentals_data:
            return

        try:
            # 写入CSV文件
            csv_start_time = time.time()
            self.csv_writer.write_base_fundamentals_info(fundamentals_data)
            csv_duration = time.time() - csv_start_time
            stats.add_csv_timing(csv_duration)

            # 数据库upsert
            db_start_time = time.time()
            affected_rows = self.db.upsert_fundamentals_data(fundamentals_data)
            db_duration = time.time() - db_start_time
            stats.add_database_timing(db_duration)

            self.logger.info(f"最终批次处理完成: {len(fundamentals_data)} 条记录，CSV: {csv_duration:.3f}s，DB: {db_duration:.3f}s，影响行数: {affected_rows}")

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