"""
数据同步管理器
"""

import time
import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
from ..config import ConfigManager
from ..data_sources import PytdxSource, BaostockSource
from ..database import DatabaseConnection, Stock, DailyData
from .csv_writer import CsvWriter
from .fundamentals_manager import FundamentalsManager
from ..utils.progress_tracker import MultiStageProgressTracker
from ..utils.performance_tracker import DailyKLinePerformanceTracker
from ..utils.log_aggregator import LogAggregator
from ..utils.daily_kline_anomaly_detector import DailyKlineAnomalyDetector


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

        # 初始化异常检测器
        self.anomaly_detector = DailyKlineAnomalyDetector(self.config_manager)

        # 初始化数据源
        self.pytdx_source = None
        self.baostock_source = None

        self._init_data_sources()

    def _init_data_sources(self) -> None:
        """初始化数据源"""
        # 初始化Pytdx数据源
        if self.config_manager.get('data_sources.pytdx.enabled', True):
            pytdx_config = {
                'vipdoc_path': self.config_manager.get('data_sources.pytdx.vipdoc_path'),
                'data_path': self.config_manager.get_data_paths().get('csv')
            }
            self.pytdx_source = PytdxSource(pytdx_config)

        # 初始化Baostock数据源
        if self.config_manager.get('data_sources.baostock.enabled', True):
            baostock_config = {
                'data_path': self.config_manager.get_data_paths().get('csv')
            }
            self.baostock_source = BaostockSource(baostock_config)

        # 初始化基本面数据管理器
        self.fundamentals_manager = FundamentalsManager(self.config_manager)

    def _get_optimal_batch_size(self, data_type: str, record_count: int) -> int:
        """
        根据数据类型和记录数量计算最优批次大小

        Args:
            data_type: 数据类型 ('daily_kline', 'min5_kline', 'min1_kline', 'stock_info')
            record_count: 总记录数量

        Returns:
            最优批次大小
        """
        # 从配置文件读取批次大小
        batch_sizes = self.config_manager.get('sync.batch_sizes', {})
        base_size = batch_sizes.get(data_type, 5000)

        # 根据总记录数量动态调整
        if record_count < 1000:
            # 小数据量使用较小的批次
            return min(1000, record_count)
        elif record_count > 50000:
            # 大数据量使用较大的批次
            return min(10000, base_size * 2)
        else:
            # 中等数据量使用基础批次大小
            return base_size

    def sync_all(self, save_to_csv: bool = True, save_to_db: bool = True) -> Dict[str, Any]:
        """
        同步所有数据 - 包含5分钟K线数据

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
            'daily_data_count': 0,
            'min5_data_count': 0,
            'errors': []
        }

        try:
            # 1. 同步股票列表
            stocks = self.sync_stocks(save_to_csv, save_to_db)
            result['stocks_count'] = len(stocks)

            # 2. 同步日K线数据
            daily_count = self.sync_daily_data(save_to_csv, save_to_db)
            result['daily_data_count'] = daily_count

            # 3. 同步5分钟K线数据
            min5_count = self.sync_5min_data(save_to_csv, save_to_db)
            result['min5_data_count'] = min5_count

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

    def sync_daily_data(
        self,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        start_date: date = None,
        end_date: date = None,
        codes: List[str] = None,
        silent_mode: bool = False,
        generate_anomaly_report: bool = True
    ) -> int:
        """
        同步日K线数据 - 严格按照功能要求直接加载通达信日K线数据

        实现逻辑：
        1. 加载{通达信数据根目录}/vipdoc/{market}/lday/*.day文件，文件名是股票的不包含"."的ts编码
        2. 过滤出时间范围内的日k线数据
        3. 数据组装成表格结构，换手率设为NULL
        4. 基于组装后的数据生成csv文件，每个交易日生成一个csv文件
        5. 将组装后的数据写入数据表his_kline_day，使用ts_code+trade_date判断insert/update
        6. 进行异常检测并生成报告（可选）

        Args:
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
            start_date: 开始日期，默认为2020-01-01
            end_date: 结束日期，默认为今天
            codes: 股票代码列表，为None则处理所有.day文件
            silent_mode: 静默模式，隐藏实时进度日志
            generate_anomaly_report: 是否生成异常报告

        Returns:
            同步的数据条数
        """
        # 初始化性能跟踪器
        perf_tracker = DailyKLinePerformanceTracker()

        # 不指定日期范围时，采集.day文件中的所有数据
        if not start_date:
            start_date = None
        if not end_date:
            end_date = None  # 不限制结束日期

        if not silent_mode:
            if start_date and end_date:
                print(f"🔄 开始采集日K线数据: {start_date} 至 {end_date}")
            elif start_date:
                print(f"🔄 开始采集日K线数据: 从 {start_date} 起")
            elif end_date:
                print(f"🔄 开始采集日K线数据: 至 {end_date} 止")
            else:
                print("🔄 开始采集日K线数据: 所有数据")

        # 1. 批量扫描所有.day文件，获取股票列表和日K线数据
        all_daily_data = []
        processed_files = 0

        try:
            if self.pytdx_source and self.pytdx_source.connect():
                # 开始文件扫描性能跟踪
                perf_tracker.start_file_scanning()

                # 批量扫描所有市场的.day文件（带进度显示），返回 (all_data, all_raw_data)
                all_daily_data, all_raw_data = self._scan_all_day_files_with_progress(
                    start_date, end_date, codes, perf_tracker, silent_mode
                )

                # 结束文件扫描性能跟踪
                processed_files = len(set(data['ts_code'] for data in all_daily_data))
                perf_tracker.end_file_scanning(processed_files, len(all_daily_data))

            else:
                print("❌ 无法连接到Pytdx数据源")
                return 0

        except Exception as e:
            print(f"❌ 扫描.day文件失败: {e}")
            return 0

        if not all_daily_data:
            print("⚠️  未找到符合条件的日K线数据")
            perf_tracker.finish()
            perf_tracker.print_daily_summary()
            return 0

        if not silent_mode:
            print(f"✅ 日K线数据采集完成，共收集 {len(all_daily_data)} 条记录")

        # 3. 数据组装 - 基本面数据已删除，换手率设为NULL
        perf_tracker.start_data_assembly()
        enriched_data = self._assemble_daily_data_with_fundamentals(all_daily_data)
        perf_tracker.end_data_assembly(len(enriched_data))

        # 4. 异常检测
        if not silent_mode:
            print(f"🔄 开始异常检测...")

        anomaly_records = self.anomaly_detector.detect_anomalies_batch(enriched_data)

        # 生成异常报告
        if generate_anomaly_report and anomaly_records:
            try:
                from ..reports import AnomalyReportGenerator
                report_generator = AnomalyReportGenerator(self.config_manager)

                # 构建原始数据映射
                raw_data_map = {}
                for record in enriched_data:
                    key = f"{record['ts_code']}_{record['trade_date']}"
                    raw_data_map[key] = record

                # 生成报告
                report_path = report_generator.generate_report(
                    anomaly_records, raw_data_map, start_date
                )
                if report_path and not silent_mode:
                    print(f"📄 异常报告已生成: {report_path}")
            except Exception as e:
                self.logger.error(f"生成异常报告失败: {e}")

        if anomaly_records:
            # 设置异常汇总信息到日志汇总器
            anomaly_summary = self.anomaly_detector.get_anomaly_summary()
            self._db_log_aggregator.set_anomaly_summary(anomaly_summary)
            if not silent_mode:
                print(f"⚠️  检测到 {len(anomaly_records)} 个异常")
        else:
            if not silent_mode:
                print(f"✅ 未检测到异常")

        # 5. 数据持久化
        total_count = 0
        csv_files_count = 0
        db_batches_count = 0

        if enriched_data:
            # 使用动态批次大小优化数据库写入性能
            batch_size = self._get_optimal_batch_size('daily_kline', len(enriched_data))
            total_batches = (len(enriched_data) + batch_size - 1) // batch_size
            if not silent_mode:
                print(f"📊 使用动态批次大小: {batch_size} 条/批次，共 {total_batches} 个批次")

            # CSV生成 - 使用原始.day文件数据，不包含基本面关联字段
            if save_to_csv:
                if not silent_mode:
                    print(f"🔄 开始生成CSV文件...")
                perf_tracker.start_csv_generation()
                # 启动静默模式
                self.csv_writer.start_silent_mode()
                # CSV使用原始.day文件数据（all_raw_data）
                self.csv_writer.write_his_kline_day(all_raw_data)
                # 结束静默模式并获取汇总信息
                csv_summary = self.csv_writer.end_silent_mode()
                if not silent_mode:
                    print(f"✅ CSV文件生成完成")

                # 计算生成的CSV文件数（按交易日分组）
                trade_dates = set(data.get('trade_date') for data in enriched_data)
                csv_files_count = len(trade_dates)
                perf_tracker.end_csv_generation(csv_files_count, len(enriched_data))

            # 数据库写入
            if save_to_db:
                if not silent_mode:
                    print(f"🔄 开始写入数据库...")
                perf_tracker.start_database_write()
                # 启动数据库静默模式统计
                self._db_log_aggregator.start_operation('database')

                for i in range(0, len(enriched_data), batch_size):
                    batch = enriched_data[i:i+batch_size]

                    try:
                        self._save_daily_data_to_db(batch)
                        total_count += len(batch)
                        db_batches_count += 1
                        # 添加批次统计
                        self._db_log_aggregator.add_batch_summary(1, len(batch), 'database')

                    except Exception as e:
                        print(f"❌ 处理批次数据失败: {e}")
                        self._db_log_aggregator.add_error('database', str(e))

                # 结束数据库静默模式并显示汇总
                self._db_log_aggregator.finish_operation('database')
                self._db_log_aggregator.print_summary('database')
                print(f"✅ 数据库写入完成")

                perf_tracker.end_database_write(total_count, db_batches_count)
            else:
                total_count = len(enriched_data)

        # 完成性能跟踪并显示统计
        perf_tracker.finish()
        if not silent_mode:
            perf_tracker.print_daily_summary()

        # 显示异常汇总
        if not silent_mode:
            self._db_log_aggregator.print_anomaly_summary()

        if not silent_mode:
            print(f"✅ 日K线数据同步完成，共处理 {total_count} 条数据，涉及 {processed_files} 只股票")
        return total_count

    def _process_daily_data_according_to_requirements(self, daily_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        按照产品要求处理日K线数据 - 严格按照6步骤20字段要求

        Args:
            daily_data: 原始日K线数据列表

        Returns:
            处理后的日K线数据列表
        """
        if not daily_data:
            return []

        processed_data = []

        # 按日期排序以计算昨日收盘价
        daily_data.sort(key=lambda x: x.get('trade_date'))

        for i, data in enumerate(daily_data):
            try:
                # 步骤3：数据补充
                # 计算preclose（昨日收盘价）
                preclose = None
                if i > 0:
                    preclose = daily_data[i-1].get('close')

                # 判断is_st（是否ST股）
                stock_name = data.get('stock_name', '')
                is_st = 'ST' in stock_name.upper()

                # pe_ttm, pb_rate, ps_ttm, pcf_ttm 留空（按照要求）
                processed_record = {
                    'ts_code': data.get('ts_code'),
                    'stock_code': data.get('stock_code'),
                    'stock_name': stock_name,
                    'trade_date': data.get('trade_date'),
                    'open': data.get('open'),
                    'high': data.get('high'),
                    'low': data.get('low'),
                    'close': data.get('close'),
                    'preclose': preclose,  # 计算所得
                    'volume': data.get('volume'),
                    'amount': data.get('amount'),
                    'trade_status': data.get('trade_status', 1),  # 默认正常交易
                    'is_st': is_st,  # 判断所得
                    'adjust_flag': data.get('adjust_flag', 3),  # 默认不复权
                    'change_rate': data.get('change_rate'),
                    'turnover_rate': data.get('turnover_rate'),
                    'pe_ttm': None,  # 按要求留空
                    'pb_rate': None,  # 按要求留空
                    'ps_ttm': None,  # 按要求留空
                    'pcf_ttm': None  # 按要求留空
                }

                processed_data.append(processed_record)

            except Exception as e:
                print(f"处理日K线数据失败: {data}, 错误: {e}")
                continue

        return processed_data

    
    def _scan_all_day_files(self, start_date: date, end_date: date, codes: List[str] = None,
                            silent_mode: bool = False) -> List[Dict[str, Any]]:
        """
        批量扫描所有.day文件，获取日K线数据 - 使用通用扫描方法

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定的股票代码列表，为None则处理所有
            silent_mode: 静默模式，隐藏实时进度日志

        Returns:
            日K线数据列表
        """
        return self._scan_all_files(start_date, end_date, codes, 'day', silent_mode)

    def _scan_all_day_files_with_progress(self, start_date: date, end_date: date,
                                          codes: List[str] = None,
                                          perf_tracker: 'DailyKLinePerformanceTracker' = None,
                                          silent_mode: bool = False) -> tuple:
        """
        批量扫描所有.day文件，获取日K线数据 - 带进度显示版本

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定的股票代码列表，为None则处理所有
            perf_tracker: 性能跟踪器

        Returns:
            元组 (all_data, all_raw_data): all_data用于数据库写入，all_raw_data用于CSV输出
        """
        import os
        from ..utils.data_transformer import DataTransformer

        # 文件类型参数
        subdir = 'lday'
        ext = '.day'
        parse_func = DataTransformer.parse_day_file_data
        record_size = 32

        all_data = []
        all_raw_data = []  # 保存原始.day文件数据用于CSV输出
        vipdoc_path = self.pytdx_source.vipdoc_path
        markets = ['bj', 'sh', 'sz']

        # 收集所有需要处理的文件
        all_files = []
        for market in markets:
            market_path = os.path.join(vipdoc_path, market, subdir)
            if not os.path.exists(market_path):
                print(f"⚠️  市场目录不存在: {market_path}")
                continue

            try:
                files = [f for f in os.listdir(market_path) if f.endswith(ext)]
                if not silent_mode:
                    print(f"📁 扫描 {market} 市场: 找到 {len(files)} 个{ext}文件")

                for filename in files:
                    if not filename.startswith(market):
                        continue

                    stock_code = filename[2:-len(ext)]  # 去掉market前缀和扩展名
                    ts_code = f"{market}.{stock_code}"

                    # 1. 优先应用新定义的股票代码过滤规则
                    if not self._is_valid_stock_file(market, stock_code):
                        continue

                    # 2. 如果指定了codes，再进行具体代码过滤
                    # 如果指定了codes，只处理指定的股票
                    # codes可以是完整ts_code（如sh.600000）或纯股票代码（如600000）
                    if codes:
                        match = False
                        for code in codes:
                            # 支持完整ts_code或纯股票代码
                            if code == ts_code or code == stock_code:
                                match = True
                                break
                        if not match:
                            continue

                    all_files.append((market, filename, stock_code))

            except Exception as e:
                print(f"❌ 扫描市场目录 {market_path} 失败: {e}")
                continue

        # 如果没有文件需要处理
        if not all_files:
            return all_data

        # 初始化进度跟踪器
        progress_tracker = MultiStageProgressTracker()
        progress_tracker.start_stage("文件扫描", len(all_files), "扫描.day文件")

        # 预先获取股票名称映射
        stock_names = {}
        try:
            ts_codes = [f"{market}.{stock_code}" for market, _, stock_code in all_files]
            if ts_codes:
                # 批量查询股票名称
                fundamentals_data = self._batch_query_fundamentals_data(ts_codes)
                missing_stock_names = self._query_missing_stock_names(ts_codes, fundamentals_data)

                # 合并股票名称
                stock_names.update({ts_code: data.get('stock_name')
                                  for ts_code, data in fundamentals_data.items()
                                  if data.get('stock_name')})
                stock_names.update(missing_stock_names)

                progress_tracker.set_stock_names(stock_names)
        except Exception as e:
            self.logger.warning(f"获取股票名称失败: {e}")

        # 处理每个文件
        for i, (market, filename, stock_code) in enumerate(all_files):
            filepath = os.path.join(vipdoc_path, market, subdir, filename)
            ts_code = f"{market}.{stock_code}"

            # 更新进度
            progress_tracker.update_stage("文件扫描", ts_code)

            try:
                # 读取文件数据
                with open(filepath, 'rb') as f:
                    file_data = []
                    # 每条记录的字节数
                    while True:
                        data = f.read(record_size)
                        if not data:
                            break

                        # 解析文件数据
                        parsed_data = parse_func(data, stock_code, market)
                        if parsed_data is None:
                            continue

                        trade_date = parsed_data['trade_date']

                        # 过滤日期范围
                        if start_date and trade_date < start_date:
                            continue
                        if end_date and trade_date > end_date:
                            continue

                        # 构建K线记录（带元数据，用于数据库写入）
                        record = {
                            'ts_code': ts_code,
                            'stock_code': stock_code,
                            'stock_name': stock_names.get(ts_code),  # 从预查询获取
                            'trade_date': trade_date,
                            'open': parsed_data['open'],
                            'high': parsed_data['high'],
                            'low': parsed_data['low'],
                            'close': parsed_data['close'],
                            'preclose': parsed_data.get('preclose'),  # 从文件解析
                            'volume': parsed_data['volume'],
                            'amount': parsed_data['amount'],
                            'trade_status': None,
                            'is_st': None,
                            'adjust_flag': 3,  # 默认不复权
                            'change_rate': None,  # 后续计算
                            'turnover_rate': None,  # 后续计算
                            'pe_ttm': None,
                            'pb_rate': None,
                            'ps_ttm': None,
                            'pcf_ttm': None
                        }

                        file_data.append(record)

                    # 后处理：计算涨跌幅
                    file_data = self._post_process_daily_data(file_data)

                    # 保存原始.day文件数据（仅添加ts_code，保持.day文件解析的原始字段名）
                    raw_file_data = []
                    for r in file_data:
                        # 使用.day文件解析的原始数据，仅增加ts_code字段
                        raw_record = {
                            'ts_code': r['ts_code'],  # 唯一新增字段
                            'trade_date': r['trade_date'],
                            'open': r['open'],
                            'high': r['high'],
                            'low': r['low'],
                            'close': r['close'],
                            'preclose': r.get('preclose'),  # 保持原始解析值
                            'amount': r['amount'],
                            'volume': r['volume']
                        }
                        raw_file_data.append(raw_record)

                    all_data.extend(file_data)
                    all_raw_data.extend(raw_file_data)

            except Exception as e:
                # 统一的文件读取错误处理
                if not silent_mode:
                    print(f"❌ 读取文件 {filepath} 失败: {e}")
                continue

        # 完成进度跟踪
        if not silent_mode:
            progress_tracker.finish_stage("文件扫描")
            progress_tracker.finish_all()

        file_type_name = "日K线"
        if not silent_mode:
            print(f"✅ 成功扫描 {len(set(data['ts_code'] for data in all_data))} 只股票的{file_type_name}数据，共 {len(all_data)} 条记录")

        return all_data, all_raw_data

    
    def _assemble_daily_data_with_fundamentals(self, daily_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        数据组装 - 查询基本面数据并计算换手率
        按交易日动态查询最近的披露日，添加fundamentals_disclosure_date和total_share字段

        Args:
            daily_data: 日K线数据列表

        Returns:
            组装后的日K线数据列表
        """
        if not daily_data:
            return []

        # 1. 提取所有唯一的ts_code
        ts_codes = list(set(record['ts_code'] for record in daily_data))

        # 2. 批量查询全量基本面数据（用于按交易日动态查找最近披露日）
        all_fundamentals_data = self._batch_query_all_fundamentals_data(ts_codes)

        # 3. 查询缺失的股票名称
        latest_fundamentals_data = self._batch_query_fundamentals_data(ts_codes)
        missing_stock_names = self._query_missing_stock_names(ts_codes, latest_fundamentals_data)

        # 4. 组装数据并计算换手率
        enriched_data = []
        for record in daily_data:
            enriched_record = record.copy()
            ts_code = record['ts_code']
            trade_date = record.get('trade_date')

            # 设置股票名称（优先使用基本面数据中的名称）
            if ts_code in latest_fundamentals_data and latest_fundamentals_data[ts_code].get('stock_name'):
                enriched_record['stock_name'] = latest_fundamentals_data[ts_code]['stock_name']
            elif ts_code in missing_stock_names:
                enriched_record['stock_name'] = missing_stock_names[ts_code]
            else:
                enriched_record['stock_name'] = None

            # 按交易日动态查找最近的披露日及相关字段
            if ts_code in all_fundamentals_data and trade_date:
                # 获取该交易日之前最近的基本面信息
                nearest_fundamentals = self._get_nearest_fundamentals(
                    ts_code, trade_date, all_fundamentals_data[ts_code]
                )

                if nearest_fundamentals:
                    # 添加新字段：关联的基本面信息披露日期、总股本、流通股本
                    enriched_record['fundamentals_disclosure_date'] = nearest_fundamentals.get('disclosure_date')
                    enriched_record['total_share'] = nearest_fundamentals.get('total_share')
                    enriched_record['float_share'] = nearest_fundamentals.get('float_share')

                    # 计算换手率（使用动态查找到的float_share）
                    float_share = nearest_fundamentals.get('float_share')
                    if float_share and record.get('volume'):
                        try:
                            from ..utils.data_transformer import DataTransformer
                            enriched_record['turnover_rate'] = DataTransformer.calculate_turnover_rate(
                                record['volume'], float_share
                            )
                        except Exception as e:
                            self.logger.warning(f"计算换手率失败 {ts_code}: {e}")
                            enriched_record['turnover_rate'] = None
                    else:
                        enriched_record['turnover_rate'] = None
                else:
                    # 没有找到满足条件的基本面数据
                    enriched_record['fundamentals_disclosure_date'] = None
                    enriched_record['total_share'] = None
                    enriched_record['float_share'] = None
                    enriched_record['turnover_rate'] = None
            else:
                # 没有基本面数据
                enriched_record['fundamentals_disclosure_date'] = None
                enriched_record['total_share'] = None
                enriched_record['float_share'] = None
                enriched_record['turnover_rate'] = None

            enriched_data.append(enriched_record)

        # 5. 基于ts_code和trade_date去重，保留最后一条记录
        import pandas as pd
        df = pd.DataFrame(enriched_data)
        if not df.empty:
            df_deduped = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
            enriched_data = df_deduped.to_dict('records')

        return enriched_data

    def _batch_query_fundamentals_data(self, ts_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        批量查询基本面数据 - 查询全部6个字段

        Args:
            ts_codes: ts代码列表

        Returns:
            基本面数据字典 {ts_code: {stock_code, stock_name, disclosure_date, total_share, float_share}}
            注意：只保留每个ts_code的最新一条记录（按disclosure_date降序）
        """
        if not ts_codes:
            return {}

        try:
            # 构建批量查询SQL - 查询全部6个字段
            query = '''
                SELECT ts_code, stock_code, stock_name, disclosure_date, total_share, float_share
                FROM base_fundamentals_info
                WHERE ts_code = ANY(%s)
                ORDER BY disclosure_date DESC
            '''

            results = self.db.execute_query(query, (ts_codes,))

            # 处理结果：保留每个ts_code的最新记录
            fundamentals_data = {}
            for row in results:
                ts_code = row['ts_code']
                # 如果还没有记录，则保存（按disclosure_date降序，第一条就是最新的）
                if ts_code not in fundamentals_data:
                    fundamentals_data[ts_code] = {
                        'stock_code': row['stock_code'],
                        'stock_name': row['stock_name'],
                        'disclosure_date': row['disclosure_date'],
                        'total_share': row['total_share'],
                        'float_share': row['float_share']
                    }

            self.logger.info(f"批量查询基本面数据完成: {len(fundamentals_data)}/{len(ts_codes)} 只股票")
            return fundamentals_data

        except Exception as e:
            self.logger.error(f"批量查询基本面数据失败: {e}")
            return {}

    def _get_nearest_fundamentals(self, ts_code: str, trade_date: Any,
                                   fundamentals_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        获取指定交易日之前最近的基本面信息

        Args:
            ts_code: 股票ts_code
            trade_date: 交易日期 (date对象 或 yyyyMMdd字符串)
            fundamentals_records: 该股票的所有基本面记录（已按disclosure_date降序排列）

        Returns:
            最近的基本面信息，如果找不到则返回空字典
        """
        if not fundamentals_records:
            return {}

        # 统一将trade_date转为date对象
        trade_date_obj = None
        if isinstance(trade_date, date):
            trade_date_obj = trade_date
        elif isinstance(trade_date, str):
            try:
                if len(trade_date) == 8:
                    trade_date_obj = datetime.strptime(trade_date, '%Y%m%d').date()
                elif len(trade_date) == 10:
                    trade_date_obj = datetime.strptime(trade_date, '%Y-%m-%d').date()
            except ValueError:
                pass

        if not trade_date_obj:
            return {}

        # 找到trade_date之前最近的disclosure_date
        # 假设fundamentals_records已按disclosure_date降序排列
        # 我们需要找到第一个 disclosure_date <= trade_date 的记录
        nearest_record = {}
        for record in fundamentals_records:
            disclosure_date = record.get('disclosure_date')
            if disclosure_date:
                # 将disclosure_date字符串转换为date对象进行比较
                try:
                    disclosure_date_obj = datetime.strptime(str(disclosure_date), '%Y%m%d').date()
                    if disclosure_date_obj <= trade_date_obj:
                        # 找到第一个满足条件的（因为已按日期降序，这是最近的）
                        nearest_record = record.copy()
                        nearest_record['disclosure_date'] = disclosure_date  # 保持字符串格式
                        break
                except (ValueError, TypeError):
                    continue

        return nearest_record

    def _batch_query_all_fundamentals_data(self, ts_codes: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        批量查询所有基本面数据 - 用于按交易日动态查找最近的披露日

        Args:
            ts_codes: ts代码列表

        Returns:
            基本面数据字典 {ts_code: [{stock_code, stock_name, disclosure_date, total_share, float_share}, ...]}
            每个ts_code对应多条记录，按disclosure_date升序排列
        """
        if not ts_codes:
            return {}

        try:
            # 构建批量查询SQL - 查询全部字段
            query = '''
                SELECT ts_code, stock_code, stock_name, disclosure_date, total_share, float_share
                FROM base_fundamentals_info
                WHERE ts_code = ANY(%s)
                ORDER BY ts_code, disclosure_date DESC
            '''

            results = self.db.execute_query(query, (ts_codes,))

            # 处理结果：按ts_code分组，所有记录按disclosure_date降序
            all_fundamentals_data = {}
            for row in results:
                ts_code = row['ts_code']
                if ts_code not in all_fundamentals_data:
                    all_fundamentals_data[ts_code] = []

                all_fundamentals_data[ts_code].append({
                    'stock_code': row['stock_code'],
                    'stock_name': row['stock_name'],
                    'disclosure_date': row['disclosure_date'],
                    'total_share': row['total_share'],
                    'float_share': row['float_share']
                })

            self.logger.info(f"批量查询全量基本面数据完成: {len(all_fundamentals_data)}/{len(ts_codes)} 只股票")
            return all_fundamentals_data

        except Exception as e:
            self.logger.error(f"批量查询全量基本面数据失败: {e}")
            return {}

    def _query_missing_stock_names(self, ts_codes: List[str], fundamentals_data: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """
        查询缺失的股票名称 - 从base_stock_info表降级获取

        Args:
            ts_codes: ts代码列表
            fundamentals_data: 已查询的基本面数据

        Returns:
            股票名称字典 {ts_code: stock_name}
        """
        # 找出没有股票名称的ts_code
        missing_ts_codes = [
            ts_code for ts_code in ts_codes
            if ts_code not in fundamentals_data or not fundamentals_data[ts_code].get('stock_name')
        ]

        if not missing_ts_codes:
            return {}

        try:
            # 从base_stock_info表查询股票名称
            query = '''
                SELECT ts_code, stock_name
                FROM base_stock_info
                WHERE ts_code = ANY(%s)
            '''

            results = self.db.execute_query(query, (missing_ts_codes,))

            stock_names = {}
            for row in results:
                stock_names[row['ts_code']] = row['stock_name']

            self.logger.info(f"从base_stock_info查询股票名称完成: {len(stock_names)}/{len(missing_ts_codes)} 只股票")
            return stock_names

        except Exception as e:
            self.logger.error(f"查询股票名称失败: {e}")
            return {}

    def _post_process_daily_data(self, daily_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        后处理日K线数据 - 计算涨跌幅

        Args:
            daily_data: 日K线数据列表

        Returns:
            处理后的日K线数据列表
        """
        if len(daily_data) <= 1:
            return daily_data

        # 按日期排序
        daily_data.sort(key=lambda x: x['trade_date'])

        # 计算涨跌幅和设置昨日收盘价
        for i in range(len(daily_data)):
            if i > 0:
                # 设置昨日收盘价
                daily_data[i]['preclose'] = daily_data[i-1]['close']
                # 计算涨跌幅
                if daily_data[i]['preclose'] and daily_data[i]['close']:
                    try:
                        from ..utils.data_transformer import DataTransformer
                        daily_data[i]['change_rate'] = DataTransformer.calculate_change_rate(
                            daily_data[i]['close'], daily_data[i]['preclose']
                        )
                    except Exception as e:
                        print(f"⚠️  计算涨跌幅失败 {daily_data[i]['ts_code']}: {e}")
                        daily_data[i]['change_rate'] = None

        return daily_data

    def _is_valid_stock_file(self, market: str, stock_code: str) -> bool:
        """
        判断是否为有效的股票文件

        规则：
        1. 上海证券交易所 (sh):
           - sh60开头
           - sh688开头
        2. 深圳证券交易所 (sz):
           - sz0开头
           - sz300开头
           - sz301开头
        3. 忽略其他市场 (如bj)

        Args:
            market: 市场代码 (sh, sz, bj)
            stock_code: 股票代码 (不含市场前缀)

        Returns:
            bool: 是否有效
        """
        if market == 'sh':
            return stock_code.startswith('60') or stock_code.startswith('688')
        elif market == 'sz':
            return stock_code.startswith('0') or stock_code.startswith('300') or stock_code.startswith('301')
        return False

    def _is_valid_ts_code(self, ts_code: str) -> bool:
        """
        判断是否为有效的TS代码

        Args:
            ts_code: TS代码 (如 sh.600000)

        Returns:
            bool: 是否有效
        """
        if not ts_code or '.' not in ts_code:
            return False
        market, code = ts_code.split('.')
        return self._is_valid_stock_file(market, code)

    def _scan_all_files(self, start_date: date, end_date: date,
                       codes: List[str] = None,
                       file_type: str = 'day',
                       silent_mode: bool = False) -> List[Dict[str, Any]]:
        """
        通用文件扫描方法 - 支持日K线和5分钟K线文件

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定的股票代码列表
            file_type: 文件类型 ('day' 或 '5min')
            silent_mode: 静默模式，隐藏实时进度日志

        Returns:
            K线数据列表
        """
        import os
        from ..utils.data_transformer import DataTransformer

        # 根据文件类型设置参数
        if file_type == 'day':
            subdir = 'lday'
            ext = '.day'
            parse_func = DataTransformer.parse_day_file_data
            record_size = 32
            time_field = None  # 日K线没有时间字段
        elif file_type == '5min':
            subdir = 'fzline'
            ext = '.lc5'
            parse_func = DataTransformer.parse_minute_file_data
            record_size = 32
            time_field = 'trade_time'
        else:
            raise ValueError(f"不支持的文件类型: {file_type}")

        all_data = []
        vipdoc_path = self.pytdx_source.vipdoc_path
        markets = ['bj', 'sh', 'sz']
        processed_files = 0

        for market in markets:
            market_path = os.path.join(vipdoc_path, market, subdir)
            if not os.path.exists(market_path):
                print(f"⚠️  市场目录不存在: {market_path}")
                continue

            try:
                files = [f for f in os.listdir(market_path) if f.endswith(ext)]
                if not silent_mode:
                    print(f"📁 扫描 {market} 市场: 找到 {len(files)} 个{ext}文件")

                for filename in files:
                    if not filename.startswith(market):
                        continue

                    stock_code = filename[2:-len(ext)]  # 去掉market前缀和扩展名

                    # 1. 优先应用新定义的股票代码过滤规则
                    if not self._is_valid_stock_file(market, stock_code):
                        continue

                    # 2. 如果指定了codes，再进行具体代码过滤
                    # 如果指定了codes，只处理指定的股票
                    if codes and stock_code not in codes:
                        continue

                    filepath = os.path.join(market_path, filename)
                    ts_code = f"{market}.{stock_code}"

                    try:
                        # 读取文件数据
                        with open(filepath, 'rb') as f:
                            file_data = []
                            # 每条记录的字节数
                            while True:
                                data = f.read(record_size)
                                if not data:
                                    break

                                # 解析文件数据
                                parsed_data = parse_func(data, stock_code, market)
                                if parsed_data is None:
                                    continue

                                trade_date = parsed_data['trade_date']

                                # 过滤日期范围
                                if start_date and trade_date < start_date:
                                    continue
                                if end_date and trade_date > end_date:
                                    continue

                                # 构建K线记录
                                if file_type == 'day':
                                    record = {
                                        'ts_code': ts_code,
                                        'stock_code': stock_code,
                                        'stock_name': None,  # 后续从数据库查询
                                        'trade_date': trade_date,
                                        'open': parsed_data['open'],
                                        'high': parsed_data['high'],
                                        'low': parsed_data['low'],
                                        'close': parsed_data['close'],
                                        'preclose': parsed_data.get('preclose'),  # 从文件解析
                                        'volume': parsed_data['volume'],
                                        'amount': parsed_data['amount'],
                                        'trade_status': None,
                                        'is_st': None,
                                        'adjust_flag': 3,  # 默认不复权
                                        'change_rate': None,  # 后续计算
                                        'turnover_rate': None,  # 后续计算
                                        'pe_ttm': None,
                                        'pb_rate': None,
                                        'ps_ttm': None,
                                        'pcf_ttm': None
                                    }
                                else:  # 5min
                                    # 保持为对象，以便后续处理（如异常检测）
                                    # 在保存到数据库时再进行格式化
                                    trade_date_val = trade_date # 这是一个date对象
                                    trade_time_val = parsed_data['trade_time'] # 这是一个time对象

                                    record = {
                                        'ts_code': ts_code,
                                        'stock_code': stock_code,
                                        'stock_name': None,  # 后续从数据库查询
                                        'trade_date': trade_date_val,
                                        'trade_time': trade_time_val,
                                        'trade_datetime': parsed_data.get('trade_datetime'), # 字符串
                                        'open': parsed_data['open'],
                                        'high': parsed_data['high'],
                                        'low': parsed_data['low'],
                                        'close': parsed_data['close'],
                                        'preclose': None,  # 后续计算
                                        'volume': parsed_data['volume'],
                                        'amount': parsed_data['amount'],
                                        'adjust_flag': 3,
                                        'change_rate': None,  # 后续计算
                                        'turnover_rate': None  # 后续计算
                                    }

                                file_data.append(record)

                            # 后处理：计算涨跌幅（仅对日K线）
                            if file_type == 'day':
                                file_data = self._post_process_daily_data(file_data)
                            else:  # 5min
                                file_data = self._post_process_5min_data(file_data)

                            all_data.extend(file_data)
                            processed_files += 1

                    except Exception as e:
                        # 统一的文件读取错误处理
                        print(f"❌ 读取文件 {filepath} 失败: {e}")
                        continue

            except Exception as e:
                # 统一的目录扫描错误处理
                print(f"❌ 扫描市场目录 {market_path} 失败: {e}")
                continue

        file_type_name = "日K线" if file_type == 'day' else "5分钟K线"
        print(f"✅ 成功扫描 {processed_files} 只股票的{file_type_name}数据，共 {len(all_data)} 条记录")
        return all_data

    def _scan_all_5min_files(self, start_date: date, end_date: date,
                             codes: List[str] = None,
                             silent_mode: bool = False) -> List[Dict[str, Any]]:
        """
        批量扫描所有.lc5文件，获取5分钟K线数据 - 使用通用扫描方法

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定的股票代码列表
            silent_mode: 静默模式，隐藏实时进度日志

        Returns:
            5分钟K线数据列表
        """
        return self._scan_all_files(start_date, end_date, codes, '5min', silent_mode)

    def _post_process_5min_data(self, min5_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        后处理5分钟K线数据 - 计算涨跌幅

        修正逻辑：使用上一根K线的收盘价作为preclose（Bar-to-Bar），而不是昨日收盘价
        """
        if not min5_data:
            return min5_data

        # 按股票和日期时间排序
        min5_data.sort(key=lambda x: (x['ts_code'], x['trade_date'], x['trade_time']))

        # 按股票分组计算涨跌幅
        current_ts_code = None

        for i in range(len(min5_data)):
            data = min5_data[i]
            ts_code = data['ts_code']

            # 如果是新的一只股票，第一条数据特殊处理
            if ts_code != current_ts_code:
                current_ts_code = ts_code
                # 第一条记录：如果没有获取到前一5分钟数据，就使用本条数据open字段值
                # 注意：PytdxSource可能已经尝试获取过preclose，如果data['preclose']为0或空，则使用open
                if not data.get('preclose'):
                    data['preclose'] = data['open']
                data['change_rate'] = None
            else:
                # 使用上一根K线的收盘价作为preclose
                preclose = min5_data[i-1]['close']
                data['preclose'] = preclose

                # 计算涨跌幅
                if preclose and data['close']:
                    try:
                        from ..utils.data_transformer import DataTransformer
                        data['change_rate'] = DataTransformer.calculate_change_rate(
                            data['close'], preclose
                        )
                    except Exception as e:
                        # print(f"⚠️  计算涨跌幅失败 {ts_code}: {e}")
                        data['change_rate'] = None
                else:
                    data['change_rate'] = None

        return min5_data

    def _get_yesterday_preclose_map(self, ts_codes: List[str]) -> Dict[str, Dict[date, float]]:
        """
        获取股票的昨日收盘价映射表

        Args:
            ts_codes: 股票代码列表

        Returns:
            Dict[ts_code, Dict[trade_date, preclose]]: 股票代码->交易日期->昨日收盘价
        """
        if not ts_codes:
            return {}

        preclose_map = {}

        for ts_code in ts_codes:
            try:
                # 查询该股票的日K线数据，获取昨日收盘价
                daily_data = self.db_conn.fetch_all("""
                    SELECT trade_date, close
                    FROM his_kline_day
                    WHERE ts_code = %s
                    ORDER BY trade_date DESC
                    LIMIT 100
                """, (ts_code,))

                if daily_data:
                    # 构建日期->昨日收盘价的映射
                    date_to_preclose = {}
                    for i, record in enumerate(daily_data):
                        current_date = record['trade_date']
                        current_close = record['close']

                        # 昨日收盘价是下一天的preclose
                        if i > 0:
                            previous_date = daily_data[i-1]['trade_date']
                            date_to_preclose[previous_date] = current_close

                    preclose_map[ts_code] = date_to_preclose

            except Exception as e:
                print(f"⚠️  获取股票 {ts_code} 的昨日收盘价失败: {e}")
                preclose_map[ts_code] = {}

        return preclose_map

    def _assemble_5min_data_with_fundamentals(self, min5_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        为5分钟K线数据添加基础信息 - 严格按照产品设计要求：
        1. 优先从base_fundamentals_info表获取stock_name
        2. 从base_stock_info表降级获取缺失的stock_name
        3. 按交易日动态查找最近的disclosure_date获取total_share和float_share
        4. 计算换手率（volume/float_share*100）

        Args:
            min5_data: 5分钟K线数据列表

        Returns:
            丰富后的5分钟K线数据列表，包含stock_name、fundamentals_disclosure_date、total_share、float_share、turnover_rate
        """
        if not min5_data:
            return []

        try:
            # 1. 提取所有唯一的ts_code
            ts_codes = list(set(record['ts_code'] for record in min5_data))

            # 2. 批量查询全量基本面数据（用于按交易日动态查找最近披露日）
            all_fundamentals_data = self._batch_query_all_fundamentals_data(ts_codes)

            # 3. 查询最新基本面数据（用于获取stock_name）
            latest_fundamentals_data = self._batch_query_fundamentals_data(ts_codes)

            # 4. 查询缺失的股票名称（从base_stock_info表降级获取）
            missing_stock_names = self._query_missing_stock_names(ts_codes, latest_fundamentals_data)

            # 5. 丰富每条5分钟K线数据
            enriched_data = []
            for record in min5_data:
                enriched_record = record.copy()
                ts_code = record['ts_code']
                trade_date = record.get('trade_date')

                # 5.1 设置stock_name（优先base_fundamentals_info表）
                if ts_code in latest_fundamentals_data and latest_fundamentals_data[ts_code].get('stock_name'):
                    enriched_record['stock_name'] = latest_fundamentals_data[ts_code]['stock_name']
                elif ts_code in missing_stock_names:
                    enriched_record['stock_name'] = missing_stock_names[ts_code]
                else:
                    enriched_record['stock_name'] = None

                # 设置is_st标记 (用于异常检测)
                if enriched_record.get('stock_name'):
                    from ..utils.data_transformer import DataTransformer
                    enriched_record['is_st'] = DataTransformer.check_is_st(enriched_record['stock_name'])
                else:
                    enriched_record['is_st'] = False

                # 5.2 按交易日动态查找最近的披露日及相关字段
                if ts_code in all_fundamentals_data and trade_date:
                    # 获取该交易日之前最近的基本面信息
                    nearest_fundamentals = self._get_nearest_fundamentals(
                        ts_code, trade_date, all_fundamentals_data[ts_code]
                    )

                    if nearest_fundamentals:
                        enriched_record['fundamentals_disclosure_date'] = nearest_fundamentals.get('disclosure_date')
                        enriched_record['total_share'] = nearest_fundamentals.get('total_share')
                        enriched_record['float_share'] = nearest_fundamentals.get('float_share')

                        # 5.3 计算换手率
                        float_share = nearest_fundamentals.get('float_share')
                        volume = record.get('volume')
                        if float_share and volume:
                            try:
                                from ..utils.data_transformer import DataTransformer
                                enriched_record['turnover_rate'] = DataTransformer.calculate_turnover_rate(
                                    volume, float_share
                                )
                            except Exception as e:
                                self.logger.warning(f"计算5分钟换手率失败 {ts_code}: {e}")
                                enriched_record['turnover_rate'] = None
                        else:
                            enriched_record['turnover_rate'] = None
                    else:
                        # 没有找到满足条件的基本面数据
                        enriched_record['fundamentals_disclosure_date'] = None
                        enriched_record['total_share'] = None
                        enriched_record['float_share'] = None
                        enriched_record['turnover_rate'] = None
                else:
                    # 没有基本面数据
                    enriched_record['fundamentals_disclosure_date'] = None
                    enriched_record['total_share'] = None
                    enriched_record['float_share'] = None
                    enriched_record['turnover_rate'] = None

                enriched_data.append(enriched_record)

            return enriched_data

        except Exception as e:
            self.logger.error(f"丰富5分钟K线数据基础信息失败: {e}")
            # 返回原始数据，换手率设为NULL
            for record in min5_data:
                record['turnover_rate'] = None
            return min5_data

    
    def _get_all_stock_info(self, codes: List[str] = None, filter_delisted: bool = True) -> List[Dict[str, Any]]:
        """
        从base_stock_info表获取股票信息

        Args:
            codes: 股票代码列表（可选）
            filter_delisted: 是否过滤退市股票，默认True

        Returns:
            股票信息列表
        """
        try:
            if codes:
                # 如果指定了股票代码，添加过滤条件
                code_list = "', '".join(codes)
                list_status_filter = "AND list_status = 'L'" if filter_delisted else ""
                query = f"""
                SELECT ts_code, stock_code, stock_name, list_status
                FROM base_stock_info
                WHERE stock_code IN ('{code_list}') {list_status_filter}
                ORDER BY ts_code
                """
                print(f"查询指定股票代码: {codes} {'(仅上市股票)' if filter_delisted else '(包含退市股票)'}")
            else:
                # 查询全量数据
                list_status_filter = "WHERE list_status = 'L'" if filter_delisted else ""
                query = f"""
                SELECT ts_code, stock_code, stock_name, list_status
                FROM base_stock_info
                {list_status_filter}
                ORDER BY ts_code
                """
                print(f"查询全量股票信息 {'(仅上市股票)' if filter_delisted else '(包含退市股票)'}")

            stocks = self.db_conn.fetch_all(query)

            # 输出统计信息
            if filter_delisted:
                print(f"✅ 过滤退市股票，获取到 {len(stocks)} 只上市股票")
            else:
                listed_count = len([s for s in stocks if s.get('list_status') == 'L'])
                delisted_count = len([s for s in stocks if s.get('list_status') != 'L'])
                print(f"📊 股票分布: 上市 {listed_count} 只, 退市 {delisted_count} 只, 总计 {len(stocks)} 只")

            return stocks
        except Exception as e:
            print(f"查询base_stock_info表失败: {e}")
            return []

    def sync_1min_data(
        self,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        mode: str = 'incremental',
        start_date: date = None,
        end_date: date = None,
        codes: List[str] = None,
        generate_anomaly_report: bool = True
    ) -> int:
        """
        同步1分钟K线数据 - 严格按照产品设计文档要求

        采集模式：
        - init: 数据初始化，加载全部.lc1文件，不过滤日期范围
        - incremental: 增量更新，加载全部.lc1文件，过滤指定日期范围数据

        Args:
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
            mode: 采集模式 ('init' 或 'incremental')
            start_date: 开始日期，增量模式下默认为7天前
            end_date: 结束日期，默认为今天
            codes: 股票代码列表，为None则同步所有股票
            generate_anomaly_report: 是否生成异常报告

        Returns:
            同步的数据条数
        """
        import time
        from datetime import datetime

        # 记录开始时间
        total_start_time = time.time()
        lc1_read_time = 0.0
        csv_generation_time = 0.0
        db_write_time = 0.0

        # 根据采集模式确定日期范围
        if mode == 'init':
            # 数据初始化：不需要过滤日期
            if start_date is None:
                start_date = date(2000, 1, 1)  # 足够早的日期
            if end_date is None:
                end_date = date.today()
            print(f"开始同步1分钟K线数据（数据初始化模式）: 无日期限制")
        else:
            # 增量更新：默认今天
            if not start_date:
                start_date = date.today()
            if not end_date:
                end_date = date.today()
            print(f"开始同步1分钟K线数据（增量更新模式）: {start_date} 至 {end_date}")

        # 获取股票列表
        if not codes:
            stocks = self.sync_stocks(False, False)
            # 过滤股票列表
            codes = []
            for stock in stocks:
                ts_code = stock.get('ts_code')
                if self._is_valid_ts_code(ts_code):
                    codes.append(stock.get('stock_code'))
            print(f"  - 过滤后股票数量: {len(codes)}")

        total_count = 0
        batch_size = self.config_manager.get('sync.batch_size', 1000)  # 1分钟数据量更大
        all_min1_data = []

        # 缓冲区
        csv_buffer = []
        db_buffer = []

        # 初始化异常检测器
        if generate_anomaly_report:
            if not hasattr(self, 'minute_anomaly_detector') or self.minute_anomaly_detector is None:
                self.minute_anomaly_detector = DailyKlineAnomalyDetector(self.config_manager)

        anomaly_records = []

        for i, code in enumerate(codes):
            try:
                # 使用Pytdx获取1分钟K线数据（记录耗时）
                data = None
                lc1_start = time.time()
                if self.pytdx_source:
                    if self.pytdx_source.connect():
                        data = self.pytdx_source.get_minute_data(code, '1min', start_date if mode == 'incremental' else None, end_date)
                        self.pytdx_source.disconnect()
                lc1_end = time.time()
                lc1_read_time += (lc1_end - lc1_start)

                if data:
                    # 1. 收集原始数据用于CSV生成
                    if save_to_csv:
                        csv_buffer.extend(data)

                    # 2. 收集丰富后的数据用于数据库写入
                    ts_code = data[0].get('ts_code') if data else None
                    if ts_code:
                        enriched_data = self._enrich_minute_data_with_fundamentals(data, ts_code)
                        db_buffer.extend(enriched_data)

                # 批量处理：达到批次大小 或 最后一个股票
                # 注意：这里判断的是累积的记录数，或者简单的按股票数量处理也可以
                # 为了防止内存溢出，按记录数判断更安全
                current_records_count = len(db_buffer)
                is_last_stock = (i == len(codes) - 1)

                if current_records_count >= batch_size or is_last_stock:
                    # 处理CSV生成
                    if save_to_csv and csv_buffer:
                        csv_start = time.time()
                        self.csv_writer.write_his_kline_1min(csv_buffer)
                        csv_end = time.time()
                        csv_generation_time += (csv_end - csv_start)
                        csv_buffer = [] # 清空缓冲区

                    # 处理数据库写入
                    if db_buffer:
                        # 异常检测
                        if generate_anomaly_report:
                            try:
                                batch_anomalies = self.minute_anomaly_detector.detect_anomalies_batch(db_buffer)
                                anomaly_records.extend(batch_anomalies)
                            except Exception as e:
                                self.logger.error(f"1分钟K线异常检测失败: {e}")

                        # 写入数据库
                        if save_to_db:
                            db_start = time.time()
                            self._save_1min_data_to_db(db_buffer)
                            db_end = time.time()
                            db_write_time += (db_end - db_start)

                        total_count += len(db_buffer)
                        print(f"已处理 {i+1}/{len(codes)} 只股票，同步1分钟数据 {total_count} 条")
                        db_buffer = [] # 清空缓冲区

            except Exception as e:
                print(f"同步股票 {code} 的1分钟K线数据失败: {e}")
                # 发生异常时也要清理缓冲区，防止坏数据影响后续
                csv_buffer = []
                db_buffer = []

        # 生成异常报告

        # 生成异常报告
        if generate_anomaly_report and anomaly_records:
            try:
                from ..reports import AnomalyReportGenerator
                report_generator = AnomalyReportGenerator(self.config_manager)
                report_path = report_generator.generate_report(anomaly_records, {}, start_date)
                if report_path:
                    print(f"📄 异常报告已生成: {report_path}")
            except Exception as e:
                self.logger.error(f"生成1分钟K线异常报告失败: {e}")

        # 输出性能信息和异常汇总
        total_time = time.time() - total_start_time
        print(f"\n{'='*50}")
        print(f"1分钟K线数据采集报告")
        print(f"{'='*50}")
        print(f"性能信息：")
        print(f"  - 读取lc1文件总耗时: {lc1_read_time:.2f}秒")
        print(f"  - 生成csv文件总耗时: {csv_generation_time:.2f}秒")
        print(f"  - 写库总耗时: {db_write_time:.2f}秒")
        print(f"  - 总耗时: {total_time:.2f}秒")
        print(f"  - 处理数据: {total_count} 条")
        if anomaly_records:
            print(f"  - 异常数据: {len(anomaly_records)} 条")
        print(f"{'='*50}")

        print(f"同步1分钟K线数据完成，共 {total_count} 条数据")
        return total_count

    def _enrich_minute_data_with_fundamentals(self, minute_data: List[Dict[str, Any]], ts_code: str) -> List[Dict[str, Any]]:
        """
        为分钟K线数据添加基础信息 - 严格按照产品设计要求：
        1. 优先从base_fundamentals_info表获取stock_name
        2. 从base_stock_info表降级获取缺失的stock_name
        3. 按交易日动态查找最近的disclosure_date获取total_share和float_share
        4. 计算换手率（volume/float_share*100）

        Args:
            minute_data: 分钟K线数据列表
            ts_code: ts代码

        Returns:
            丰富后的分钟K线数据列表，包含stock_name、fundamentals_disclosure_date、total_share、float_share、turnover_rate
        """
        try:
            # 1. 批量查询全量基本面数据（用于按交易日动态查找最近披露日）
            all_fundamentals_data = self._batch_query_all_fundamentals_data([ts_code])

            # 2. 查询最新基本面数据（用于获取stock_name）
            latest_fundamentals_data = self._batch_query_fundamentals_data([ts_code])

            # 3. 查询缺失的股票名称（从base_stock_info表降级获取）
            missing_stock_names = self._query_missing_stock_names([ts_code], latest_fundamentals_data)

            # 4. 丰富每条分钟K线数据
            enriched_data = []
            for record in minute_data:
                enriched_record = record.copy()
                trade_date = record.get('trade_date')

                # 4.1 设置stock_name（优先base_fundamentals_info表）
                if ts_code in latest_fundamentals_data and latest_fundamentals_data[ts_code].get('stock_name'):
                    enriched_record['stock_name'] = latest_fundamentals_data[ts_code]['stock_name']
                elif ts_code in missing_stock_names:
                    enriched_record['stock_name'] = missing_stock_names[ts_code]
                else:
                    enriched_record['stock_name'] = None

                # 设置is_st标记 (用于异常检测)
                if enriched_record.get('stock_name'):
                    from ..utils.data_transformer import DataTransformer
                    enriched_record['is_st'] = DataTransformer.check_is_st(enriched_record['stock_name'])
                else:
                    enriched_record['is_st'] = False

                # 4.2 按交易日动态查找最近的披露日及相关字段
                if ts_code in all_fundamentals_data and trade_date:
                    # 获取该交易日之前最近的基本面信息
                    nearest_fundamentals = self._get_nearest_fundamentals(
                        ts_code, trade_date, all_fundamentals_data[ts_code]
                    )

                    if nearest_fundamentals:
                        enriched_record['fundamentals_disclosure_date'] = nearest_fundamentals.get('disclosure_date')
                        enriched_record['total_share'] = nearest_fundamentals.get('total_share')
                        enriched_record['float_share'] = nearest_fundamentals.get('float_share')

                        # 4.3 计算换手率
                        float_share = nearest_fundamentals.get('float_share')
                        volume = record.get('volume')
                        if float_share and volume:
                            try:
                                from ..utils.data_transformer import DataTransformer
                                enriched_record['turnover_rate'] = DataTransformer.calculate_turnover_rate(
                                    volume, float_share
                                )
                            except Exception as e:
                                self.logger.warning(f"计算换手率失败 {ts_code}: {e}")
                                enriched_record['turnover_rate'] = None
                        else:
                            enriched_record['turnover_rate'] = None
                    else:
                        # 没有找到满足条件的基本面数据
                        enriched_record['fundamentals_disclosure_date'] = None
                        enriched_record['total_share'] = None
                        enriched_record['float_share'] = None
                        enriched_record['turnover_rate'] = None
                else:
                    # 没有基本面数据
                    enriched_record['fundamentals_disclosure_date'] = None
                    enriched_record['total_share'] = None
                    enriched_record['float_share'] = None
                    enriched_record['turnover_rate'] = None

                enriched_data.append(enriched_record)

            return enriched_data

        except Exception as e:
            print(f"丰富分钟K线数据基础信息失败: {e}")
            return minute_data

    def _save_1min_data_to_db(self, min1_data: List[Dict[str, Any]]) -> None:
        """保存1分钟K线数据到数据库 - 严格按照文档要求使用ts_code+trade_date+trade_time判断

        包含字段：ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime, open, high, low,
                 close, preclose, volume, amount, adjust_flag, change_rate, turnover_rate,
                 fundamentals_disclosure_date, total_share, float_share
        """
        batch_size = self.config_manager.get('sync.batch_size', 2000)
        for i in range(0, len(min1_data), batch_size):
            batch = min1_data[i:i+batch_size]
            values = []
            for data in batch:
                # 确保trade_datetime存在
                trade_date = data.get('trade_date')
                trade_time = data.get('trade_time')
                trade_datetime = data.get('trade_datetime')
                if not trade_datetime and trade_date and trade_time:
                    trade_datetime = f"{trade_date}{trade_time}"

                values.append((
                    data.get('ts_code'),
                    data.get('stock_code'),
                    data.get('stock_name'),
                    data.get('trade_date'),
                    data.get('trade_time'),
                    trade_datetime,
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('preclose'),
                    data.get('volume'),
                    data.get('amount'),
                    data.get('adjust_flag', 3),
                    data.get('change_rate'),
                    data.get('turnover_rate'),
                    data.get('fundamentals_disclosure_date'),
                    data.get('total_share'),
                    data.get('float_share')
                ))

            query = """
                INSERT INTO his_kline_1min (
                    ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
                    open, high, low, close, preclose, volume, amount,
                    adjust_flag, change_rate, turnover_rate,
                    fundamentals_disclosure_date, total_share, float_share
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
                    stock_code = EXCLUDED.stock_code,
                    stock_name = EXCLUDED.stock_name,
                    trade_datetime = EXCLUDED.trade_datetime,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    preclose = EXCLUDED.preclose,
                    volume = EXCLUDED.volume,
                    amount = EXCLUDED.amount,
                    adjust_flag = EXCLUDED.adjust_flag,
                    change_rate = EXCLUDED.change_rate,
                    turnover_rate = EXCLUDED.turnover_rate,
                    fundamentals_disclosure_date = EXCLUDED.fundamentals_disclosure_date,
                    total_share = EXCLUDED.total_share,
                    float_share = EXCLUDED.float_share,
                    update_time = CURRENT_TIMESTAMP
            """
            self.db_conn.execute_batch(query, values)

    def sync_5min_data(
        self,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        mode: str = 'incremental',
        start_date: date = None,
        end_date: date = None,
        codes: List[str] = None,
        generate_anomaly_report: bool = True
    ) -> int:
        """
        同步5分钟K线数据 - 严格按照产品设计文档要求

        采集模式：
        - init: 数据初始化，加载全部.lc5文件，不过滤日期范围
        - incremental: 增量更新，加载全部.lc5文件，过滤指定日期范围数据

        Args:
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
            mode: 采集模式 ('init' 或 'incremental')
            start_date: 开始日期，增量模式下默认为7天前
            end_date: 结束日期，默认为今天
            codes: 股票代码列表，为None则同步所有股票
            generate_anomaly_report: 是否生成异常报告

        Returns:
            同步的数据条数
        """
        import time

        # 记录开始时间
        total_start_time = time.time()
        lc5_read_time = 0.0
        csv_generation_time = 0.0
        db_write_time = 0.0

        # 根据采集模式确定日期范围
        if mode == 'init':
            # 数据初始化：不需要过滤日期
            if start_date is None:
                start_date = date(2000, 1, 1)  # 足够早的日期
            if end_date is None:
                end_date = date.today()
            print(f"开始同步5分钟K线数据（数据初始化模式）: 无日期限制")
        else:
            # 增量更新：默认7天前到今天
            if not start_date:
                default_start_str = self.config_manager.get('sync.5min_data.default_start_date', '2025-12-01')
                try:
                    start_date = datetime.strptime(default_start_str, '%Y-%m-%d').date()
                except ValueError:
                    start_date = date(2025, 12, 1)
            if not end_date:
                end_date = date.today()
            print(f"开始同步5分钟K线数据（增量更新模式）: {start_date} 至 {end_date}")

        # 智能选择处理模式
        if not codes:
            # 批量扫描模式：高性能处理所有数据
            return self._sync_5min_batch_mode(save_to_csv, save_to_db, start_date, end_date,
                                               generate_anomaly_report, total_start_time)
        else:
            # 兼容模式：处理指定股票
            return self._sync_5min_compatibility_mode(save_to_csv, save_to_db,
                                                      start_date, end_date, codes,
                                                      generate_anomaly_report, total_start_time)

    def _sync_5min_batch_mode(self, save_to_csv: bool, save_to_db: bool,
                             start_date: date, end_date: date,
                             generate_anomaly_report: bool = True,
                             total_start_time: float = None) -> int:
        """批量扫描模式 - 参考日K线架构"""
        import time

        if total_start_time is None:
            total_start_time = time.time()

        print("🚀 使用批量扫描模式")

        # 1. 批量扫描所有.lc5文件
        all_5min_data = []
        lc5_read_start = time.time()
        try:
            if self.pytdx_source and self.pytdx_source.connect():
                all_5min_data = self._scan_all_5min_files(start_date, end_date)
                self.pytdx_source.disconnect()
        except Exception as e:
            print(f"❌ 批量扫描失败: {e}")
            return 0
        lc5_read_time = time.time() - lc5_read_start

        if not all_5min_data:
            print("⚠️  未找到5分钟K线数据")
            return 0

        print(f"✅ 找到 {len(set(data['ts_code'] for data in all_5min_data))} 支股票的5分钟K线数据")

        # 2. 批量处理数据（处理所有找到的数据）
        csv_gen_start = time.time()
        result = self._process_5min_data_batch(all_5min_data, save_to_csv, save_to_db, generate_anomaly_report)
        csv_gen_time = time.time() - csv_gen_start

        # 3. 输出报告
        total_time = time.time() - total_start_time
        print(f"\n{'='*50}")
        print(f"5分钟K线数据采集报告（批量扫描模式）")
        print(f"{'='*50}")
        print(f"性能信息：")
        print(f"  - 读取lc5文件总耗时: {lc5_read_time:.2f}秒")
        print(f"  - 生成csv文件总耗时: {csv_gen_time:.2f}秒")
        print(f"  - 总耗时: {total_time:.2f}秒")
        print(f"  - 处理数据: {result} 条")
        print(f"{'='*50}")

        return result

    def _sync_5min_compatibility_mode(self, save_to_csv: bool, save_to_db: bool,
                                     start_date: date, end_date: date,
                                     codes: List[str],
                                     generate_anomaly_report: bool = True,
                                     total_start_time: float = None) -> int:
        """兼容模式 - 处理指定股票"""
        import time

        if total_start_time is None:
            total_start_time = time.time()

        print(f"🔧 使用兼容模式处理 {len(codes)} 支指定股票")

        total_count = 0
        batch_size = self.config_manager.get('sync.batch_size', 10000)
        all_5min_data = []
        lc5_read_time = 0.0
        csv_generation_time = 0.0
        db_write_time = 0.0

        for i, code in enumerate(codes):
            try:
                # 使用Pytdx获取5分钟K线数据（记录耗时）
                lc5_start = time.time()
                data = None
                if self.pytdx_source and self.pytdx_source.connect():
                    data = self.pytdx_source.get_minute_data(code, '5min', start_date, end_date)
                    self.pytdx_source.disconnect()
                lc5_end = time.time()
                lc5_read_time += (lc5_end - lc5_start)

                if data:
                    # 批量积累数据
                    all_5min_data.extend(data)

                    # 批量处理
                    if len(all_5min_data) >= batch_size or i == len(codes) - 1:
                        csv_start = time.time()
                        processed_count = self._process_5min_data_batch(
                            all_5min_data, save_to_csv, save_to_db, generate_anomaly_report
                        )
                        csv_end = time.time()
                        csv_generation_time += (csv_end - csv_start)
                        db_write_time += (csv_end - csv_start)

                        total_count += processed_count
                        all_5min_data = []
                        print(f"已处理 {i+1}/{len(codes)} 只股票，同步数据 {total_count} 条")

            except Exception as e:
                print(f"同步股票 {code} 的5分钟K线数据失败: {e}")

        # 输出报告
        total_time = time.time() - total_start_time
        print(f"\n{'='*50}")
        print(f"5分钟K线数据采集报告（兼容模式）")
        print(f"{'='*50}")
        print(f"性能信息：")
        print(f"  - 读取lc5文件总耗时: {lc5_read_time:.2f}秒")
        print(f"  - 生成csv文件总耗时: {csv_generation_time:.2f}秒")
        print(f"  - 写库总耗时: {db_write_time:.2f}秒")
        print(f"  - 总耗时: {total_time:.2f}秒")
        print(f"  - 处理数据: {total_count} 条")
        print(f"{'='*50}")

        return total_count

    def _process_5min_data_batch(self, min5_data: List[Dict[str, Any]],
                               save_to_csv: bool, save_to_db: bool,
                               generate_anomaly_report: bool = True) -> int:
        """批量处理5分钟数据 - 统一的数据处理逻辑"""
        if not min5_data:
            return 0

        # 1. 保存原始数据到CSV (严格按照文档要求：使用.lc5文件解析的原始数据)
        if save_to_csv:
            try:
                # 注意：传入的是未经加工的min5_data
                self.csv_writer.write_his_kline_5min(min5_data)
            except Exception as e:
                self.logger.error(f"保存5分钟K线CSV失败: {e}")

        # 2. 数据组装 - 基本面数据关联，换手率计算 (这些操作会修改/丰富数据，不影响CSV生成)
        enriched_data = self._assemble_5min_data_with_fundamentals(min5_data)
        enriched_data = self._post_process_5min_data(enriched_data)

        # 3. 异常检测
        anomaly_records = []
        if generate_anomaly_report:
            try:
                if not hasattr(self, 'minute_anomaly_detector') or self.minute_anomaly_detector is None:
                    self.minute_anomaly_detector = DailyKlineAnomalyDetector(self.config_manager)
                anomaly_records = self.minute_anomaly_detector.detect_anomalies_batch(enriched_data)

                # 生成异常报告
                if anomaly_records:
                    from ..reports import AnomalyReportGenerator
                    report_generator = AnomalyReportGenerator(self.config_manager)
                    # 获取第一条数据的trade_date作为报告日期
                    report_date = enriched_data[0].get('trade_date') if enriched_data else None

                    # 确保report_date是date对象
                    if report_date and isinstance(report_date, str):
                        try:
                            # 尝试解析字符串日期 (yyyyMMdd)
                            if len(report_date) == 8:
                                report_date = datetime.strptime(report_date, '%Y%m%d').date()
                            else:
                                # 其他格式尝试
                                report_date = datetime.strptime(report_date, '%Y-%m-%d').date()
                        except ValueError:
                            # 解析失败，使用当天
                            report_date = date.today()

                    report_path = report_generator.generate_report(anomaly_records, {}, report_date)
                    if report_path:
                        print(f"📄 5分钟K线异常报告已生成: {report_path}")
                    print(f"⚠️  检测到 {len(anomaly_records)} 个5分钟K线异常")
            except Exception as e:
                self.logger.error(f"5分钟K线异常检测失败: {e}")

        # 4. 数据持久化 (数据库)
        try:
            if save_to_db:
                self._save_5min_data_to_db(enriched_data)

            print(f"✅ 批量处理完成: {len(enriched_data)} 条记录")
            return len(enriched_data)

        except Exception as e:
            print(f"❌ 批量处理失败: {e}")
            return 0

    
    def _save_5min_data_to_db(self, min5_data: List[Dict[str, Any]]) -> None:
        """
        保存5分钟K线数据到数据库 - 严格按照文档要求使用ts_code+trade_date+trade_time判断

        包含字段：ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime, open, high, low,
                 close, preclose, volume, amount, adjust_flag, change_rate, turnover_rate,
                 fundamentals_disclosure_date, total_share, float_share

        Args:
            min5_data: 5分钟K线数据列表
        """
        if not min5_data:
            return

        # 使用合理的批次大小，平衡内存使用和性能
        batch_size = self.config_manager.get('sync.batch_size', 5000)

        for i in range(0, len(min5_data), batch_size):
            batch = min5_data[i:i+batch_size]
            values = []

            for data in batch:
                # 格式化日期和时间（如果它们还不是字符串）
                trade_date = data.get('trade_date')
                if hasattr(trade_date, 'strftime'):
                    trade_date = trade_date.strftime('%Y%m%d')

                trade_time = data.get('trade_time')
                if hasattr(trade_time, 'strftime'):
                    trade_time = trade_time.strftime('%H%M')

                # 确保trade_datetime存在
                trade_datetime = data.get('trade_datetime')
                if not trade_datetime and trade_date and trade_time:
                    trade_datetime = f"{trade_date}{trade_time}"

                # 增加字段兼容性处理，支持不同的字段名
                values.append((
                    data.get('ts_code'),
                    data.get('stock_code') or data.get('code'),
                    data.get('stock_name') or data.get('name'),
                    trade_date,
                    trade_time,
                    trade_datetime,
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('preclose'),
                    data.get('volume'),
                    data.get('amount'),
                    data.get('adjust_flag', 3),  # 默认不复权
                    data.get('change_rate'),
                    data.get('turnover_rate'),
                    data.get('fundamentals_disclosure_date'),
                    data.get('total_share'),
                    data.get('float_share')
                ))

            # 严格按照文档要求使用ts_code+trade_date+trade_time作为冲突键
            query = """
                INSERT INTO his_kline_5min (
                    ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
                    open, high, low, close, preclose, volume, amount, adjust_flag,
                    change_rate, turnover_rate,
                    fundamentals_disclosure_date, total_share, float_share
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
                    stock_code = EXCLUDED.stock_code,
                    stock_name = EXCLUDED.stock_name,
                    trade_datetime = EXCLUDED.trade_datetime,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    preclose = EXCLUDED.preclose,
                    volume = EXCLUDED.volume,
                    amount = EXCLUDED.amount,
                    adjust_flag = EXCLUDED.adjust_flag,
                    change_rate = EXCLUDED.change_rate,
                    turnover_rate = EXCLUDED.turnover_rate,
                    fundamentals_disclosure_date = EXCLUDED.fundamentals_disclosure_date,
                    total_share = EXCLUDED.total_share,
                    float_share = EXCLUDED.float_share,
                    update_time = CURRENT_TIMESTAMP
            """
            self.db_conn.execute_batch(query, values)

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

    def _save_daily_data_to_db(self, daily_data: List[Dict[str, Any]]) -> None:
        """保存日K线数据到数据库"""
        batch_size = self.config_manager.get('sync.batch_size', 5000)  # 优化批次大小
        for i in range(0, len(daily_data), batch_size):
            batch = daily_data[i:i+batch_size]
            values = []
            for data in batch:
                # 直接从字典获取数据，避免模型转换问题
                # 确保trade_date转换为字符串格式
                trade_date_val = data.get('trade_date') or data.get('date')
                if hasattr(trade_date_val, 'strftime'):
                    trade_date_str = trade_date_val.strftime('%Y%m%d')
                else:
                    trade_date_str = str(trade_date_val)

                value_tuple = (
                    data.get('ts_code'),
                    data.get('stock_code') or data.get('code'),
                    data.get('stock_name') or data.get('name'),
                    trade_date_str,
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('preclose'),
                    data.get('volume'),
                    data.get('amount'),
                    data.get('trade_status', 1),
                    data.get('is_st', False),
                    data.get('change_rate') or data.get('pct_chg'),
                    data.get('turnover_rate') or data.get('turn'),
                    data.get('fundamentals_disclosure_date'),  # 新增字段
                    data.get('total_share'),  # 新增字段
                    data.get('float_share')  # 新增字段（与基本面数据关联）
                )

                values.append(value_tuple)

            query = """
                INSERT INTO his_kline_day (
                    ts_code, stock_code, stock_name, trade_date, open, high, low, close,
                    preclose, volume, amount, trade_status, is_st,
                    change_rate, turnover_rate, fundamentals_disclosure_date, total_share, float_share
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    ts_code = EXCLUDED.ts_code,
                    stock_name = EXCLUDED.stock_name,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    preclose = EXCLUDED.preclose,
                    volume = EXCLUDED.volume,
                    amount = EXCLUDED.amount,
                    trade_status = EXCLUDED.trade_status,
                    is_st = EXCLUDED.is_st,
                    change_rate = EXCLUDED.change_rate,
                    turnover_rate = EXCLUDED.turnover_rate,
                    fundamentals_disclosure_date = EXCLUDED.fundamentals_disclosure_date,
                    total_share = EXCLUDED.total_share,
                    float_share = EXCLUDED.float_share,
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
