
import logging
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal
from ..database.connection import DatabaseConnection
from ..config import ConfigManager

logger = logging.getLogger(__name__)

class ThreeDimensionKlineGenerator:
    """
    3D K线生成器
    基于累计涨幅阈值(2.5%)聚合K线，只响应上涨
    """

    def __init__(self, config_manager: ConfigManager = None):
        self.config = config_manager or ConfigManager()
        self.db = DatabaseConnection(self.config)
        self.threshold = 0.025  # 2.5%

    def generate(self, ts_code: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, source_type: str = '5min') -> Dict[str, Any]:
        """
        生成3D K线数据
        """
        logger.info(f"开始生成3D K线数据。数据源: {source_type}, 代码: {ts_code}, 范围: {start_date}-{end_date}")

        # 性能统计
        perf_stats = {
            'query_time': 0.0,
            'calc_time': 0.0,
            'db_write_time': 0.0,
            'total_time': 0.0
        }
        total_start_time = time.time()

        # 1. 获取待处理股票列表
        if ts_code:
            stocks = [{'ts_code': ts_code}]
        else:
            stocks = self._get_stock_list()

        total_stocks = len(stocks)
        processed_count = 0
        total_records = 0

        # 默认只支持5分钟，若未来支持1分钟可扩展
        source_table = "his_kline_5min" if source_type == '5min' else "his_kline_1min"
        target_table = "anal_kline_rise_25pre" if source_type == '5min' else "anal_kline_rise_25pre_1min"

        for stock in stocks:
            current_ts_code = stock['ts_code']
            try:
                # 2. 确定查询起点
                query_start_dt = None

                # 如果明确指定了start_date，优先使用
                if start_date:
                    # 格式化为YYYYMMDD0000以便与trade_datetime比较
                    query_start_dt = f"{start_date.replace('-', '')}0000"
                else:
                    # 自动增量更新模式：查询上次最后一条3D K线的时间
                    last_kline_time = self._get_last_kline_time(current_ts_code, target_table)
                    if last_kline_time:
                        query_start_dt = last_kline_time
                        # 注意：查询时应该是 > last_kline_time

                # 3. 加载源数据
                t0 = time.time()
                source_data = self._load_source_data(
                    current_ts_code,
                    source_table,
                    start_datetime=query_start_dt,
                    end_date=end_date
                )
                perf_stats['query_time'] += (time.time() - t0)

                if not source_data:
                    logger.debug(f"{current_ts_code} 无新数据需要处理")
                    continue

                # 4. 计算3D K线
                t1 = time.time()
                anal_data = self._calculate_3d_klines(source_data, current_ts_code)
                perf_stats['calc_time'] += (time.time() - t1)

                # 5. 保存数据
                if anal_data:
                    t2 = time.time()
                    self._save_data(anal_data, target_table)
                    perf_stats['db_write_time'] += (time.time() - t2)

                    count = len(anal_data)
                    total_records += count
                    logger.debug(f"{current_ts_code} 生成了 {count} 条3D K线数据")

                processed_count += 1
                if processed_count % 10 == 0:
                    logger.info(f"已处理 {processed_count}/{total_stocks} 只股票")

            except Exception as e:
                logger.error(f"处理 {current_ts_code} 失败: {e}")

        perf_stats['total_time'] = time.time() - total_start_time

        # 生成报告
        self._print_report(processed_count, total_records, perf_stats)

        return {
            "status": "success",
            "processed_stocks": processed_count,
            "total_records": total_records,
            "performance": perf_stats
        }

    def _get_stock_list(self) -> List[Dict[str, Any]]:
        # 优先从 base_stock_info 获取，如果没有则尝试从 his_kline_5min 获取 distinct ts_code
        # 这里假设 base_stock_info 是主表
        query = "SELECT DISTINCT ts_code FROM base_stock_info ORDER BY ts_code"
        res = self.db.execute_query(query)
        if not res:
             query = "SELECT DISTINCT ts_code FROM his_kline_5min ORDER BY ts_code"
             res = self.db.execute_query(query)
        return res

    def _get_last_kline_time(self, ts_code: str, table_name: str = 'anal_kline_rise_25pre') -> Optional[str]:
        """获取某只股票最后一条3D K线的结束时间(trade_datetime)"""
        # 简单验证表名防止SQL注入
        if table_name not in ['anal_kline_rise_25pre', 'anal_kline_rise_25pre_1min']:
             return None

        query = f"SELECT max(trade_datetime) as last_dt FROM {table_name} WHERE ts_code = %s"
        res = self.db.fetch_one(query, (ts_code,))
        if res and res['last_dt']:
            return res['last_dt']
        return None

    def _load_source_data(self, ts_code: str, table_name: str, start_datetime: Optional[str], end_date: Optional[str]) -> List[Dict[str, Any]]:
        """
        加载源K线数据
        注意：start_datetime 是 YYYYMMDDHHMM 格式
        """
        query = f"SELECT * FROM {table_name} WHERE ts_code = %s"
        params = [ts_code]

        if start_datetime:
            # 增量更新：查询大于上次结束时间的数据
            query += " AND trade_datetime > %s"
            params.append(start_datetime)

        if end_date:
            # end_date 是 YYYY-MM-DD 或 YYYYMMDD
            # 转换为 YYYYMMDD
            fmt_end_date = end_date.replace('-', '')
            query += " AND trade_date <= %s"
            params.append(fmt_end_date)

        # 严格按时间排序
        query += " ORDER BY trade_datetime ASC"

        return self.db.execute_query(query, tuple(params))

    def _calculate_3d_klines(self, source_data: List[Dict[str, Any]], ts_code: str) -> List[Dict[str, Any]]:
        result = []
        if not source_data:
            return result

        # 数据预处理 helper
        def to_float(val):
            return float(val) if val is not None else 0.0

        # 当前聚合状态
        current_bar = None

        # 缓存静态信息
        first_rec = source_data[0]
        stock_code = first_rec.get('stock_code', '')
        stock_name = first_rec.get('stock_name', '')

        for kline in source_data:
            # 提取字段
            open_p = to_float(kline['open'])
            high_p = to_float(kline['high'])
            low_p = to_float(kline['low'])
            close_p = to_float(kline['close'])
            volume = to_float(kline['volume'])
            amount = to_float(kline['amount'])
            turnover = to_float(kline.get('turnover_rate', 0))

            trade_date = kline['trade_date']
            trade_time = kline['trade_time']
            trade_datetime = kline.get('trade_datetime') # YYYYMMDDHHMM

            if current_bar is None:
                # 初始化新的一根3D K线
                current_bar = {
                    'ts_code': ts_code,
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    # 区间起始信息
                    'trade_begin_date': trade_date,
                    'trade_begin_time': trade_time,
                    'trade_begin_datetime': trade_datetime,
                    # 区间结束信息 (初始同起始)
                    'trade_date': trade_date,
                    'trade_time': trade_time,
                    'trade_datetime': trade_datetime,
                    # 价格信息
                    'open': open_p, # 3D K线的Open = 区间第一根的Open
                    'high': high_p,
                    'low': low_p,
                    'close': close_p,
                    # 累积信息
                    'volume': volume,
                    'amount': amount,
                    'turnover_rate': turnover,
                    # 涨跌幅 (初始为0)
                    'change_rate': 0.0,
                    'adjust_flag': 3 # 默认
                }
            else:
                # 累积到当前K线
                current_bar['high'] = max(current_bar['high'], high_p)
                current_bar['low'] = min(current_bar['low'], low_p)
                current_bar['volume'] += volume
                current_bar['amount'] += amount
                current_bar['turnover_rate'] += turnover

                # 更新收盘状态
                current_bar['close'] = close_p
                current_bar['trade_date'] = trade_date
                current_bar['trade_time'] = trade_time
                current_bar['trade_datetime'] = trade_datetime

            # 检查触发条件：相对于当前3D K线开盘价的累计涨幅 >= 2.5%
            # 注意：只响应上涨
            ref_open = current_bar['open']
            curr_close = current_bar['close']

            if ref_open <= 0:
                # 异常数据处理，避免除零
                current_bar = None
                continue

            # 计算涨幅
            change_pct = (curr_close - ref_open) / ref_open

            # 触发条件：涨幅 >= 2.5%
            if change_pct >= self.threshold:
                # 记录最终涨幅 (%)
                current_bar['change_rate'] = round(change_pct * 100, 4)

                # 完成当前K线
                result.append(current_bar)
                logger.debug(f"生成3D K线: {ts_code} {current_bar['trade_begin_time']}->{current_bar['trade_time']} 涨幅:{current_bar['change_rate']}%")

                # 重置，下一根K线将从source_data的下一条记录开始初始化
                current_bar = None
            elif change_pct > 0.015:
                # 记录接近阈值的中间状态 (大于1.5%)
                 logger.debug(f"3D K线累积中: {ts_code} 当前涨幅:{change_pct*100:.2f}% (开始:{current_bar['trade_begin_time']} 当前:{current_bar['trade_time']})")

        # 循环结束
        # 遗留的未完成 current_bar 丢弃，因为不满足3D K线定义

        return result

    def _save_data(self, data: List[Dict[str, Any]], table_name: str = 'anal_kline_rise_25pre'):
        self.db.upsert_anal_kline(data, table_name)

    def _print_report(self, processed_count: int, total_records: int, stats: Dict[str, float]):
        report = f"""
==================================================
3D K线生成报告 (阈值 2.5% - 仅上涨)
==================================================
处理股票数: {processed_count}
生成记录数: {total_records}
--------------------------------------------------
性能统计:
查询耗时: {stats['query_time']:.3f} s
计算耗时: {stats['calc_time']:.3f} s
写库耗时: {stats['db_write_time']:.3f} s
总耗时  : {stats['total_time']:.3f} s
==================================================
"""
        logger.info(report)
