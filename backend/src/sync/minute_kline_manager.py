"""
并发5分钟K线数据采集管理器
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

logger = logging.getLogger(__name__)


class MinuteKlineSyncManager:
    """并发5分钟K线数据采集管理器"""

    def __init__(self, config_manager: ConfigManager = None, max_workers: int = 6):
        """
        初始化并发5分钟K线采集管理器

        Args:
            config_manager: 配置管理器
            max_workers: 最大工作线程数，默认6
        """
        self.config = config_manager or ConfigManager()
        self.db = DatabaseConnection(self.config)

        # 使用线程安全的baostock源
        baostock_config = {
            'data_path': self.config.get_data_paths().get('csv', 'uat/data'),
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
        执行并发5分钟K线数据同步

        Args:
            **options: 配置选项
                - batch_size: 批次大小，默认20 (5分钟数据量更大，批次减小)
                - dry_run: 是否试运行，默认False
                - list_status: 股票上市状态过滤，默认'L'
                - max_workers: 最大工作线程数，覆盖默认值
                - init_mode: 数据初始化模式，采集2019-01-05至今
                - start_date: 指定开始日期 (YYYY-MM-DD)
                - end_date: 指定结束日期 (YYYY-MM-DD)
                - ts_codes: 指定股票ts_code列表
                - batch: 指定批次编号（仅在init模式下有效，从1开始）

        Returns:
            同步统计信息
        """
        batch_size = options.get('batch_size', 20)
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

            self.logger.info(f"开始并发同步5分钟K线: 模式={mode_desc}, 总计{stats.total_stocks}只股票，批次大小{batch_size}，最大线程数{max_workers}")

            # 分批处理
            batch_count = 0

            for i in range(0, len(stocks), batch_size):
                batch_stocks = stocks[i:i + batch_size]

                current_batch_num = batch if batch is not None else (i // batch_size) + 1
                calc_total_batches = (len(stocks) + batch_size - 1) // batch_size

                try:
                    # 处理批次
                    self._process_batch_concurrent(
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
            self.logger.error(f"并发5分钟K线同步失败: {e}")
            stats.finish()
            error_stats = stats.get_stats()
            error_stats['error'] = str(e)
            return error_stats
        finally:
            self.baostock.disconnect()

    def _get_stock_list(self, list_status: str = 'L', ts_codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        获取股票列表 - 从 base_fundamentals_info 获取
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
            # 初始化模式：从2019-01-05开始
            return '2019-01-05', today

        if start_date and end_date:
            # 指定范围模式
            return start_date, end_date

        # 默认模式（日增量更新）：仅更新当天
        return today, today

    def _generate_sync_report(self, stats: ThreadSafeStatistics, batch_count: int, db_duration: float):
        """生成同步报告"""
        report_path = f"doc/reports/kline_5min_sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

        try:
            s = stats.get_stats()
            timing = s.get('timing', {})

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("# 5分钟K线数据采集报告\n\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                f.write("## 1. 性能信息\n\n")
                f.write("| 环节 | 总耗时(秒) | 说明 |\n")
                f.write("| --- | --- | --- |\n")
                f.write(f"| Baostock接口 | {timing.get('baostock_total', 0):.3f} | 调用接口获取数据耗时 |\n")
                f.write(f"| CSV生成 | {timing.get('csv_total', 0):.3f} | 写入CSV文件耗时 |\n")
                f.write(f"| 数据库写入 | {s.get('db_total_time', 0):.3f} | 写库总耗时 |\n")
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
                    f.write("| 代码 | 名称 | 日期 | 时间 | 涨跌幅(%) | 阈值(%) | ST |\n")
                    f.write("| --- | --- | --- | --- | --- | --- | --- |\n")
                    for a in self.anomalies:
                        f.write(f"| {a['ts_code']} | {a['stock_name']} | {a['trade_date']} | {a['trade_time']} | {a['pct_chg']} | {a['limit']} | {a['is_st']} |\n")
                else:
                    f.write("本次采集未发现异常数据。\n")

            self.logger.info(f"已生成采集报告: {report_path}")
        except Exception as e:
            self.logger.error(f"生成采集报告失败: {e}")

    def _enrich_and_transform(self, raw_data: List[Dict[str, str]], stock_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        丰富和转换数据 - 适配 his_kline_5min 表结构
        计算: turnover_rate, preclose, change_rate
        """
        if not raw_data:
            return []

        ts_code = stock_info.get('ts_code')
        stock_code = stock_info.get('stock_code')
        stock_name = stock_info.get('stock_name')

        # 1. 加载基本面数据
        query = """
            SELECT disclosure_date, total_share, float_share
            FROM base_fundamentals_info
            WHERE ts_code = %s
            ORDER BY disclosure_date ASC
        """
        fundamentals = self.db.execute_query(query, (ts_code,))
        fund_dates = [f['disclosure_date'] for f in fundamentals]

        enriched_data = []

        # 记录上一条的收盘价作为下一条的前收盘价
        # 对于第一条数据，如果没有前收盘价，使用开盘价代替
        previous_close = None

        for row in raw_data:
            try:
                # 原始格式
                # date: YYYY-MM-DD
                # time: YYYYMMDDHHMMSSsss (例如: 20240105093500000)

                raw_date = row['date']
                raw_time = row['time']

                # 转换 date -> YYYYMMDD
                trade_date_str = raw_date.replace('-', '')

                # 转换 time -> HHMM (trade_time) 和 YYYYMMDDHHMM (trade_datetime)
                # raw_time 是 20240105093500000
                if len(raw_time) >= 12:
                    trade_time_str = raw_time[8:12] # HHMM
                    trade_datetime_str = raw_time[0:12] # YYYYMMDDHHMM
                else:
                    # 容错处理
                    trade_time_str = "0000"
                    trade_datetime_str = trade_date_str + "0000"

                # 查找基本面数据
                idx = bisect.bisect_right(fund_dates, trade_date_str)

                total_share = None
                float_share = None
                disclosure_date = None

                if idx > 0:
                    fund_rec = fundamentals[idx - 1]
                    total_share = float(fund_rec['total_share']) if fund_rec['total_share'] is not None else None
                    float_share = float(fund_rec['float_share']) if fund_rec['float_share'] is not None else None
                    disclosure_date = fund_rec['disclosure_date']

                # 类型转换
                def to_float(val):
                    return float(val) if val and val != '' else None

                def to_int(val):
                    return int(val) if val and val != '' else None

                open_val = to_float(row['open'])
                close_val = to_float(row['close'])
                high_val = to_float(row['high'])
                low_val = to_float(row['low'])
                volume_val = to_float(row['volume'])
                amount_val = to_float(row['amount'])

                # 计算 preclose
                # 规则：如果有上一条收盘价，则使用上一条收盘价；否则（第一条），使用当前开盘价
                # 注意：这里我们是在一个批次内处理，如果跨批次，第一条可能会有问题，
                # 但由于我们主要关注的是计算涨跌幅，且5分钟级别的preclose通常定义为上一周期的close
                # 严格来说应该去库里查上一条，但为了性能和批量写入，这里简化处理
                # 如果是当天的第一根K线，preclose应该是昨收。
                # Baostock 5分钟数据不返回 preclose 字段。
                # 暂定策略：第一条使用 open 代替 preclose，后续使用 prev_close
                if previous_close is not None:
                    current_preclose = previous_close
                else:
                    current_preclose = open_val

                # 更新 previous_close
                previous_close = close_val

                # 计算 change_rate = (close - preclose) / preclose * 100
                change_rate = None
                if close_val is not None and current_preclose is not None and current_preclose != 0:
                    change_rate = (close_val - current_preclose) / current_preclose * 100
                    change_rate = round(change_rate, 4)

                # 计算 turnover_rate = volume / float_share * 100
                turnover_rate = None
                if volume_val is not None and float_share is not None and float_share != 0:
                    turnover_rate = (volume_val / float_share) * 100
                    turnover_rate = round(turnover_rate, 4)

                item = {
                    'ts_code': ts_code,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'trade_date': trade_date_str,
                    'trade_time': trade_time_str,
                    'trade_datetime': trade_datetime_str,
                    'open': open_val,
                    'high': high_val,
                    'low': low_val,
                    'close': close_val,
                    'preclose': current_preclose,
                    'volume': volume_val,
                    'amount': amount_val,
                    'change_rate': change_rate,
                    'turnover_rate': turnover_rate,
                    'fundamentals_disclosure_date': disclosure_date,
                    'total_share': total_share,
                    'float_share': float_share,
                    'adjust_flag': to_int(row.get('adjustflag')),
                    'source': 'BAOSTOCK',
                    'create_time': datetime.now(),
                    'update_time': datetime.now()
                }
                enriched_data.append(item)

            except Exception as e:
                self.logger.error(f"数据转换失败: {e} Row: {row}")
                continue

        return enriched_data

    def _collect_minute_kline_raw(
        self,
        stock: Dict[str, Any],
        start_date: str,
        end_date: str
    ) -> CollectionResult:
        """采集单只股票原始5分钟数据"""
        start_time = time.time()
        try:
            # 使用 fetch_5min_k_data_raw
            k_data = self.baostock.fetch_5min_k_data_raw(stock['ts_code'], start_date, end_date)
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
        batch_size: int = 20,
        original_total_stocks: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """处理股票批次"""
        batch_data = []

        for stock in stock_batch:
            try:
                stock_start_time = time.time()

                # 检查上市日期
                actual_start_date = start_date
                if stock.get('list_date'):
                    list_date_val = stock['list_date']
                    if hasattr(list_date_val, 'strftime'):
                        list_date_str = list_date_val.strftime('%Y-%m-%d')
                    else:
                        s = str(list_date_val)
                        if len(s) == 8 and s.isdigit():
                            list_date_str = f"{s[:4]}-{s[4:6]}-{s[6:]}"
                        else:
                            list_date_str = s

                    if list_date_str > start_date:
                        actual_start_date = list_date_str

                if end_date and actual_start_date > end_date:
                    self.logger.debug(f"{stock['ts_code']} 上市日期{actual_start_date}晚于结束日期{end_date}，跳过")
                    stats.add_result(CollectionResult.no_data(0))
                    continue

                # 采集数据
                result = self._collect_minute_kline_raw(
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
                        # CSV保存
                        if save_to_csv and not dry_run:
                            csv_start_time = time.time()
                            self.csv_writer.write_5min_kline_data(raw_kline_data, stock['ts_code'])
                            csv_duration = time.time() - csv_start_time
                            stats.add_csv_timing(csv_duration)

                        # 转换和丰富数据
                        enriched_data = self._enrich_and_transform(raw_kline_data, stock)

                        if enriched_data:
                            batch_data.extend(enriched_data)

                            # 验证数据
                            self._validate_data(enriched_data, stock)

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
                self.db.upsert_kline_5min(batch_data)
                db_duration = time.time() - db_start_time
                stats.add_database_timing(db_duration)
                self.logger.debug(f"批次入库完成: {len(batch_data)}条记录，耗时{db_duration:.3f}s")
            except Exception as e:
                self.logger.error(f"批次入库失败: {e}")

        return batch_data

    def _validate_data(self, data: List[Dict[str, Any]], stock: Dict[str, Any]) -> None:
        """
        验证数据，检查异常波动
        由于5分钟波动通常较小，我们可以设置一个相对宽松的阈值，例如5%
        """
        ts_code = stock.get('ts_code', '')

        limit = 5.0 # 5分钟内波动超过5%视为异常

        for item in data:
            pct_chg = item.get('change_rate')
            if pct_chg is not None and abs(pct_chg) > limit:
                anomaly = {
                    'ts_code': ts_code,
                    'stock_name': stock.get('stock_name'),
                    'trade_date': item.get('trade_date'),
                    'trade_time': item.get('trade_time'),
                    'pct_chg': pct_chg,
                    'limit': limit,
                    'is_st': 'Unknown'
                }
                self.anomalies.append(anomaly)

def sync_minute_kline_concurrent(config_manager: ConfigManager = None, **options) -> Dict[str, Any]:
    """便捷函数"""
    max_workers = options.get('max_workers', 6)
    manager = MinuteKlineSyncManager(config_manager, max_workers=max_workers)
    return manager.execute_sync(**options)
