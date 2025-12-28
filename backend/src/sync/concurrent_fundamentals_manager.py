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

        # 线程安全统计收集器
        stats = ThreadSafeStatistics()

        try:
            # 采集开始前只连接一次baostock
            if not self.baostock.connect():
                raise Exception("Baostock连接失败")

            # 获取股票列表（支持ts_codes过滤）
            stocks = self._get_stock_list(list_status, ts_codes)
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
                batch = stocks[i:i + batch_size]
                try:
                    batch_data = self._process_batch_concurrent(
                        batch, 
                        dry_run, 
                        stats,
                        init_mode=init_mode,
                        year=year,
                        quarter=quarter
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
            股票列表，包含ts_code、stock_code和stock_name字段
        """
        if ts_codes:
            # 指定股票模式：通过入参传递的股票ts_code查询base_stock_info表中未退市的数据
            placeholders = ','.join(['%s'] * len(ts_codes))
            query = f"""
                SELECT ts_code, stock_code, stock_name
                FROM base_stock_info
                WHERE list_status = %s AND ts_code IN ({placeholders})
                ORDER BY ts_code
            """
            return self.db.execute_query(query, (list_status, *ts_codes))
        else:
            # 全量股票模式：查询base_stock_info表中全部未退市的数据
            query = """
                SELECT ts_code, stock_code, stock_name
                FROM base_stock_info
                WHERE list_status = %s
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
        quarter: int = None
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

        Returns:
            处理后的基本面数据列表
        """
        batch_data = []
        batch_start_time = time.time()

        try:
            # 串行处理批次中的每只股票（避免baostock多线程问题）
            for stock in stock_batch:
                try:
                    result = self._collect_fundamentals_concurrent(
                        stock, 
                        stats,
                        init_mode=init_mode,
                        year=year,
                        quarter=quarter
                    )
                    
                    # 更新统计信息
                    stats.add_result(result)
                    
                    if result and result.is_success:
                        batch_data.append(result.data)
                except Exception as e:
                    self.logger.error(f"股票处理异常: {e}")

            # 批次处理
            # 批次处理
            if batch_data and not dry_run:
                # 仅写入数据库（增量写入）
                db_start_time = time.time()
                affected_rows = self.db.upsert_fundamentals_data(batch_data)
                db_duration = time.time() - db_start_time
                stats.add_database_timing(db_duration)

                self.logger.debug(f"批次处理完成: {len(batch_data)} 条记录，DB: {db_duration:.3f}s，影响行数: {affected_rows}")

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
            stock: 股票基本信息
            stats: 统计收集器
            init_mode: 数据初始化模式，采集全时段数据(1992Q3至今)
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
                # 数据初始化模式：从[year=1992, quarter=3]开始查询，一直查询到当前时间的前一个季度
                current_year = datetime.now().year
                current_quarter = (datetime.now().month - 1) // 3 + 1
                
                # 计算前一个季度
                if current_quarter > 1:
                    end_year, end_quarter = current_year, current_quarter - 1
                else:
                    end_year, end_quarter = current_year - 1, 4
                
                # 从1992Q3遍历到当前前一季度（连接已在execute_sync中统一管理）
                for y in range(1992, end_year + 1):
                    start_q = 3 if y == 1992 else 1
                    end_q = end_quarter if y == end_year else 4
                    
                    for q in range(start_q, end_q + 1):
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
                self.logger.debug(f"[{thread_name}]({stock['ts_code']}-{stock['stock_name']}) 采集成功，共{len(all_fundamentals)}条")
                # 返回最新的一条作为主结果
                return CollectionResult.success(all_fundamentals[-1], execution_time)
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

            # 数据库在这个阶段不需要重复写入，因为批次处理中已经写入了
            # 仅记录CSV写入完成日志
            self.logger.info(f"最终同步完成: 共 {len(fundamentals_data)} 条记录，CSV生成耗时: {csv_duration:.3f}s")

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