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
from ..utils.progress_formatter import ProgressFormatter


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
        self.progress_formatter = ProgressFormatter()  # 初始化进度格式化器

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
                - batch: 指定批次编号（仅在init模式下有效，从1开始）

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
        save_to_csv = options.get('save_to_csv', True)
        save_to_db = options.get('save_to_db', True)
        batch = options.get('batch', None)

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
                    stats['error'] = error_msg
                    return stats

                # 计算批次的起始和结束索引
                start_idx = (batch - 1) * batch_size
                end_idx = min(batch * batch_size, len(stocks))

                # 切片获取该批次的股票
                stocks = stocks[start_idx:end_idx]
                self.logger.info(f"批次过滤: 批次{batch}/{total_batches}，处理股票索引{start_idx}-{end_idx-1}，共{len(stocks)}只股票")

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

                    # 计算批次信息
                    batch_num, total_batches = self.progress_formatter.calculate_batch_info(
                        current_index=i,
                        total_stocks=len(stocks),
                        batch_size=batch_size,
                        specified_batch=batch,
                        original_total_stocks=original_total_stocks if batch is not None else None
                    )

                    # 根据结果状态处理并输出进度
                    if collection_result.is_success:
                        # 支持列表数据(init模式)和单条数据
                        if isinstance(collection_result.data, list):
                            fundamentals_data.extend(collection_result.data)
                        else:
                            fundamentals_data.append(collection_result.data)
                        stats['successful'] += 1

                        # 提取披露日期(从第一条或唯一的数据中)
                        first_data = collection_result.data[0] if isinstance(collection_result.data, list) else collection_result.data
                        disclosure_date = first_data.get('disclosure_date', '')
                        if hasattr(disclosure_date, 'strftime'):
                            disclosure_date_str = disclosure_date.strftime('%Y%m%d')
                        else:
                            disclosure_date_str = str(disclosure_date) if disclosure_date else ''

                        # 使用formatter格式化进度信息
                        progress_msg = self.progress_formatter.format_progress(
                            current=i + 1,
                            total=len(stocks),
                            ts_code=stock['ts_code'],
                            stock_name=stock['stock_name'],
                            status=CollectionStatus.SUCCESS,
                            elapsed_time=baostock_time,
                            batch_num=batch_num,
                            total_batches=total_batches,
                            disclosure_date=disclosure_date_str
                        )
                        self.logger.info(progress_msg)

                    elif collection_result.is_no_data:
                        stats['no_data'] += 1

                        # 无数据状态进度
                        progress_msg = self.progress_formatter.format_progress(
                            current=i + 1,
                            total=len(stocks),
                            ts_code=stock['ts_code'],
                            stock_name=stock['stock_name'],
                            status=CollectionStatus.NO_DATA,
                            elapsed_time=baostock_time,
                            batch_num=batch_num,
                            total_batches=total_batches
                        )
                        self.logger.info(progress_msg)

                    else:  # ERROR
                        stats['error_count'] += 1
                        stats['failed'] += 1  # 保持向后兼容

                        # 错误状态进度
                        progress_msg = self.progress_formatter.format_progress(
                            current=i + 1,
                            total=len(stocks),
                            ts_code=stock['ts_code'],
                            stock_name=stock['stock_name'],
                            status=CollectionStatus.ERROR,
                            elapsed_time=baostock_time,
                            batch_num=batch_num,
                            total_batches=total_batches,
                            error_message=collection_result.error_message
                        )
                        self.logger.error(progress_msg)

                    # QPS控制：每处理一只股票后短暂休眠，确保QPS不超过50
                    if not dry_run and i < len(stocks) - 1:
                        time.sleep(0.02)  # 1/50秒 = 0.02秒

                    # 批处理：每batch_size只股票处理一次数据库写入
                    if (i + 1) % batch_size == 0:
                        if fundamentals_data:
                            self._process_data_batch(fundamentals_data, dry_run, stats, save_to_csv, save_to_db)
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
                self._process_data_batch(fundamentals_data, dry_run, stats, save_to_csv, save_to_db)
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
            stock: 股票基本信息，包含ts_code、stock_code、stock_name和list_date
            init_mode: 数据初始化模式，采集全时段数据(根据list_date动态计算起始季度至今)
            year: 指定年份
            quarter: 指定季度(1-4)

        Returns:
            CollectionResult: 包含状态、数据和错误信息的结果对象
        """
        start_time = time.time()
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

                # 从计算出的起始季度遍历到当前前一季度
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
                # init模式返回全部数据列表,其他模式返回单条数据
                self.logger.debug(f"成功获取股票 {stock['ts_code']} 基本面数据,共{len(all_fundamentals)}条")
                if len(all_fundamentals) > 1:
                    # 多条数据(init模式):返回完整列表
                    return CollectionResult.success(all_fundamentals, execution_time)
                else:
                    # 单条数据:返回单个Dict
                    return CollectionResult.success(all_fundamentals[0], execution_time)
            else:
                # 无数据情况，接口正常但返回空结果
                self.logger.debug(f"股票 {stock['ts_code']} 无基本面数据")
                return CollectionResult.no_data(execution_time)

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.debug(f"采集股票 {stock['ts_code']} 基本面数据异常: {e}")
            return CollectionResult.error(str(e), execution_time)

    def _process_data_batch(self, batch_data: List[Dict[str, Any]], dry_run: bool = False, stats: Dict[str, Any] = None,
                            save_to_csv: bool = True, save_to_db: bool = True):
        """
        处理数据批次

        Args:
            batch_data: 批量数据
            dry_run: 是否试运行
            stats: 统计信息字典，用于记录耗时
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
        """
        if not batch_data:
            return

        if not dry_run:
            try:
                # 记录CSV生成开始时间
                csv_start_time = time.time()

                # 写入CSV文件
                if save_to_csv:
                    self.csv_writer.write_base_fundamentals_info(batch_data)

                # 记录CSV生成耗时
                csv_time = time.time() - csv_start_time
                if stats and 'timing' in stats:
                    stats['timing']['csv_total'] += csv_time
                    if save_to_csv:
                        stats['timing']['csv_batches'] += 1

                # 记录数据库写入开始时间
                db_start_time = time.time()

                # 数据库upsert
                if save_to_db:
                    affected_rows = self.db.upsert_fundamentals_data(batch_data)
                else:
                    affected_rows = 0

                # 记录数据库写入耗时
                db_time = time.time() - db_start_time
                if stats and 'timing' in stats:
                    stats['timing']['db_total'] += db_time
                    if save_to_db:
                        stats['timing']['db_batches'] += 1

                self.logger.info(f"批次处理完成: {len(batch_data)} 条记录，CSV: {'是' if save_to_csv else '否'}，DB: {'是' if save_to_db else '否'}，影响行数: {affected_rows}")

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