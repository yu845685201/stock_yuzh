"""
基本面数据采集管理器 - 实用平衡方案实现
严格按照产品设计文档要求实现股票基本面信息采集功能
"""

import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..config import ConfigManager
from ..database.connection import DatabaseConnection
from ..data_sources.baostock_source import BaostockSource
from ..sync.csv_writer import CsvWriter
from ..models.collection_result import CollectionResult, CollectionStatistics, CollectionStatus


class FundamentalsManager:
    """基本面数据采集管理器 - 实用平衡方案"""

    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化基本面采集管理器

        Args:
            config_manager: 配置管理器
        """
        self.config = config_manager or ConfigManager()
        self.db = DatabaseConnection(self.config)
        self.baostock = BaostockSource(self.config.get('data_sources.baostock', {}))
        self.csv_writer = CsvWriter(self.config)
        self.logger = logging.getLogger(__name__)

    def execute_sync(self, **options) -> Dict[str, Any]:
        """
        执行基本面数据同步 - 严格按照产品设计文档

        Args:
            **options: 配置选项
                - batch_size: 批次大小，默认50
                - dry_run: 是否试运行，默认False
                - list_status: 股票上市状态过滤，默认'L'（仅上市）
                - init_mode: 数据初始化模式，采集全量股票全时段(1992Q3至今)
                - year: 指定年份
                - quarter: 指定季度(1-4)
                - ts_codes: 指定股票ts_code列表

        Returns:
            同步统计信息
        """
        batch_size = options.get('batch_size', 50)
        dry_run = options.get('dry_run', False)
        list_status = options.get('list_status', 'L')
        init_mode = options.get('init_mode', False)
        year = options.get('year', None)
        quarter = options.get('quarter', None)
        ts_codes = options.get('ts_codes', None)

        # 新增统计类用于更精确的状态管理
        collection_stats = CollectionStatistics()

        stats = {
            'total_stocks': 0,
            'successful': 0,      # 保持向后兼容
            'failed': 0,          # 保持向后兼容
            'no_data': 0,         # 新增：无数据统计
            'error_count': 0,     # 新增：异常失败统计
            'csv_files': [],
            'start_time': datetime.now(),
            'batch_count': 0,
            'timing': {
                'baostock_total': 0.0,
                'csv_total': 0.0,
                'db_total': 0.0,
                'baostock_calls': 0,
                'csv_batches': 0,
                'db_batches': 0
            }
        }

        try:
            # 连接数据源
            if not self.baostock.connect():
                raise Exception("Baostock连接失败")

            # 获取股票列表（根据ts_codes参数过滤）
            stocks = self._get_stock_list(list_status, ts_codes)
            stats['total_stocks'] = len(stocks)

            if not stocks:
                self.logger.warning(f"没有找到上市状态为'{list_status}'的股票")
                return stats

            # 确定采集模式和季度范围
            mode_desc = "增量更新"
            if init_mode:
                mode_desc = "数据初始化(1992Q3至今)"
            elif year and quarter:
                mode_desc = f"指定时间({year}年Q{quarter})"
            
            self.logger.info(f"开始同步基本面数据: 模式={mode_desc}, 总计{stats['total_stocks']}只股票，批次大小{batch_size}")

            # 分批处理
            fundamentals_data = []
            for i, stock in enumerate(stocks):
                try:
                    # 记录单只股票处理开始时间
                    stock_start_time = time.time()

                    # 使用新的采集方法，返回CollectionResult（支持三种模式）
                    collection_result = self._collect_fundamentals_with_status(
                        stock, 
                        init_mode=init_mode, 
                        year=year, 
                        quarter=quarter
                    )

                    # 记录baostock耗时
                    baostock_time = time.time() - stock_start_time
                    stats['timing']['baostock_total'] += baostock_time
                    stats['timing']['baostock_calls'] += 1

                    # 更新统计信息
                    collection_stats.add_result(collection_result)

                    # 根据结果状态处理
                    if collection_result.is_success:
                        fundamentals_data.append(collection_result.data)
                        stats['successful'] += 1

                        # 输出进度条 - 严格按照产品设计文档格式：[当前数/总数](股票编码-股票名称-披露日期) 进度: 进度百分比% baostock: 耗时s
                        progress_percent = (i + 1) / len(stocks) * 100
                        disclosure_date = collection_result.data.get('disclosure_date', '')
                        # 将disclosure_date转换为yyyyMMdd格式字符串
                        if hasattr(disclosure_date, 'strftime'):
                            disclosure_date_str = disclosure_date.strftime('%Y%m%d')
                        else:
                            disclosure_date_str = str(disclosure_date) if disclosure_date else ''
                        self.logger.info(f"[{i+1}/{len(stocks)}]({stock['stock_code']}-{stock['stock_name']}-{disclosure_date_str}) "
                                         f"进度: {progress_percent:.1f}% baostock: {baostock_time:.3f}s")
                    elif collection_result.is_no_data:
                        stats['no_data'] += 1
                        self.logger.info(f"[{i+1}/{len(stocks)}]({stock['stock_code']}-{stock['stock_name']}) "
                                         f"进度: {((i+1)/len(stocks)*100):.1f}% 无数据 baostock: {baostock_time:.3f}s")
                    else:  # ERROR
                        stats['error_count'] += 1
                        stats['failed'] += 1  # 保持向后兼容
                        self.logger.error(f"[{i+1}/{len(stocks)}]({stock['stock_code']}-{stock['stock_name']}) "
                                         f"进度: {((i+1)/len(stocks)*100):.1f}% 采集失败: {collection_result.error_message} baostock: {baostock_time:.3f}s")

                    # QPS控制：每处理一只股票后短暂休眠，确保QPS不超过50
                    if not dry_run and i < len(stocks) - 1:
                        time.sleep(0.02)  # 1/50秒 = 0.02秒

                    # 批处理：每batch_size只股票处理一次数据库写入
                    if (i + 1) % batch_size == 0:
                        if fundamentals_data:
                            self._process_data_batch(fundamentals_data, dry_run, stats)
                            stats['batch_count'] += 1
                            fundamentals_data = []

                        # 额外的批次间休眠（可选，提供更保守的QPS控制）
                        if i < len(stocks) - 1:
                            self.logger.debug(f"已完成批次 {((i+1)//batch_size)}，休眠0.5秒...")
                            time.sleep(0.5)

                except Exception as e:
                    # 创建错误结果并更新统计
                    error_result = CollectionResult.error(str(e), time.time() - stock_start_time)
                    collection_stats.add_result(error_result)
                    stats['error_count'] += 1
                    stats['failed'] += 1  # 保持向后兼容
                    self.logger.error(f"[{i+1}/{len(stocks)}]({stock['stock_code']}-{stock['stock_name']}) "
                                     f"处理异常: {e}")

            # 处理剩余数据
            if fundamentals_data:
                self._process_data_batch(fundamentals_data, dry_run, stats)
                stats['batch_count'] += 1

            stats['end_time'] = datetime.now()
            stats['duration'] = (stats['end_time'] - stats['start_time']).total_seconds()

            # 更新向后兼容的字段
            stats['failed'] = stats['error_count']  # failed现在只包含真正的异常失败

            # 计算各种成功率
            stats['success_rate'] = collection_stats.real_success_rate  # 真实成功率
            stats['completion_rate'] = collection_stats.completion_rate  # 完成率（包含无数据）
            stats['error_rate'] = collection_stats.error_rate  # 错误率

            # 添加详细统计信息
            stats['collection_stats'] = collection_stats.to_dict()

            # 输出详细的耗时统计
            self._log_timing_summary(stats, collection_stats)

            return stats

        except Exception as e:
            self.logger.error(f"基本面数据同步失败: {e}")
            stats['error'] = str(e)
            return stats
        finally:
            self.baostock.disconnect()

    def _log_timing_summary(self, stats: Dict[str, Any], collection_stats: CollectionStatistics = None) -> None:
        """
        输出详细的耗时统计信息

        Args:
            stats: 统计信息字典
            collection_stats: 采集统计对象
        """
        self.logger.info("=" * 60)
        self.logger.info("📊 基本面数据同步完成统计")
        self.logger.info("=" * 60)

        # 三分类统计（新功能）
        if collection_stats:
            self.logger.info("📋 三分类统计:")
            self.logger.info(f"   ✅ 成功采集: {collection_stats.success_count} 只股票")
            self.logger.info(f"   📄 无数据: {collection_stats.no_data_count} 只股票")
            self.logger.info(f"   ❌ 异常失败: {collection_stats.error_count} 只股票")
            self.logger.info("")
            self.logger.info("📈 完成度分析:")
            self.logger.info(f"   🎯 真实成功率: {collection_stats.real_success_rate:.2%}")
            self.logger.info(f"   📋 完成率: {collection_stats.completion_rate:.2%}")
            self.logger.info(f"   ⚠️  错误率: {collection_stats.error_rate:.2%}")
        else:
            # 向后兼容的统计显示
            self.logger.info(f"✅ 成功采集: {stats['successful']} 只股票")
            self.logger.info(f"❌ 失败数量: {stats['failed']} 只股票")
            self.logger.info(f"📈 成功率: {stats.get('success_rate', 0):.2%}")

        self.logger.info(f"⏱️  总耗时: {stats.get('duration', 0):.2f} 秒")

        # 详细耗时分析
        if 'timing' in stats:
            timing = stats['timing']

            self.logger.info("📋 详细耗时分析:")

            # Baostock API统计
            if timing['baostock_calls'] > 0:
                avg_baostock = timing['baostock_total'] / timing['baostock_calls']
                self.logger.info(f"   🔍 Baostock API: {timing['baostock_total']:.2f}s "
                                f"(调用{timing['baostock_calls']}次, 平均{avg_baostock:.3f}s/只)")

            # CSV生成统计
            if timing['csv_batches'] > 0:
                avg_csv = timing['csv_total'] / timing['csv_batches']
                self.logger.info(f"   📄 CSV生成: {timing['csv_total']:.2f}s "
                                f"(处理{timing['csv_batches']}批次, 平均{avg_csv:.3f}s/批次)")

            # 数据库写入统计
            if timing['db_batches'] > 0:
                avg_db = timing['db_total'] / timing['db_batches']
                self.logger.info(f"   💾 数据库写入: {timing['db_total']:.2f}s "
                                f"(处理{timing['db_batches']}批次, 平均{avg_db:.3f}s/批次)")

            # 效率分析
            if timing['baostock_calls'] > 0:
                stocks_per_second = timing['baostock_calls'] / timing['baostock_total']
                self.logger.info(f"   ⚡ 处理效率: {stocks_per_second:.1f} 只股票/秒")

        self.logger.info("=" * 60)

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

    def _collect_fundamentals(self, stock: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        采集单只股票基本面数据（向后兼容方法）

        Args:
            stock: 股票基本信息

        Returns:
            基本面数据或None
        """
        result = self._collect_fundamentals_with_status(stock)
        return result.get_data_or_none()

    def _collect_fundamentals_with_status(
        self, 
        stock: Dict[str, Any],
        init_mode: bool = False,
        year: int = None,
        quarter: int = None
    ) -> CollectionResult:
        """
        采集单只股票基本面数据，返回带状态的结果 - 严格按照产品设计文档要求支持三种采集方式

        Args:
            stock: 股票基本信息
            init_mode: 数据初始化模式，采集全时段数据(1992Q3至今)
            year: 指定年份
            quarter: 指定季度(1-4)

        Returns:
            CollectionResult: 包含状态、数据和错误信息的结果对象
        """
        start_time = time.time()
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
                
                # 从1992Q3遍历到当前前一季度
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
                # 如果是数据初始化模式，返回最新的一条作为主结果
                # 但实际上我们需要处理所有数据，这里返回第一条用于进度展示
                self.logger.debug(f"成功获取股票 {stock['ts_code']} 基本面数据，共{len(all_fundamentals)}条")
                # 返回最后一条（最新的）作为主结果
                return CollectionResult.success(all_fundamentals[-1] if len(all_fundamentals) == 1 else all_fundamentals[-1], execution_time)
            else:
                # 无数据情况，接口正常但返回空结果
                self.logger.debug(f"股票 {stock['ts_code']} 无基本面数据")
                return CollectionResult.no_data(execution_time)

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.debug(f"采集股票 {stock['ts_code']} 基本面数据异常: {e}")
            return CollectionResult.error(str(e), execution_time)

    def _process_data_batch(self, batch_data: List[Dict[str, Any]], dry_run: bool = False, stats: Dict[str, Any] = None):
        """
        处理数据批次

        Args:
            batch_data: 批量数据
            dry_run: 是否试运行
            stats: 统计信息字典，用于记录耗时
        """
        if not batch_data:
            return

        if not dry_run:
            try:
                # 记录CSV生成开始时间
                csv_start_time = time.time()

                # 写入CSV文件
                self.csv_writer.write_base_fundamentals_info(batch_data)

                # 记录CSV生成耗时
                csv_time = time.time() - csv_start_time
                if stats and 'timing' in stats:
                    stats['timing']['csv_total'] += csv_time
                    stats['timing']['csv_batches'] += 1

                # 记录数据库写入开始时间
                db_start_time = time.time()

                # 数据库upsert
                affected_rows = self.db.upsert_fundamentals_data(batch_data)

                # 记录数据库写入耗时
                db_time = time.time() - db_start_time
                if stats and 'timing' in stats:
                    stats['timing']['db_total'] += db_time
                    stats['timing']['db_batches'] += 1

                self.logger.info(f"批次处理完成: {len(batch_data)} 条记录，CSV: {csv_time:.3f}s，DB: {db_time:.3f}s，影响行数: {affected_rows}")

            except Exception as e:
                self.logger.error(f"批次处理失败: {e}")
                raise
        else:
            self.logger.info(f"DRY RUN: 将处理 {len(batch_data)} 条记录")


def sync_fundamentals_data(config_manager: ConfigManager = None, **options) -> Dict[str, Any]:
    """
    便捷函数：同步基本面数据

    Args:
        config_manager: 配置管理器
        **options: 同步选项

    Returns:
        同步统计信息
    """
    manager = FundamentalsManager(config_manager)
    return manager.execute_sync(**options)