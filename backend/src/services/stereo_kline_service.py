import logging
from typing import List, Dict, Any, Optional, Literal
from datetime import datetime, date
from ..database.connection import DatabaseConnection
from ..database.models import HisKline5Min, HisKline1Min, AnalKlineRise25Pre

class StereoKlineService:
    """立体K线生成服务"""

    # 涨幅阈值常量
    RISE_THRESHOLD = 0.025  # 2.5% 涨幅阈值

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.db_conn = DatabaseConnection(config_manager)
        self.logger = logging.getLogger(__name__)

    def generate_stereo_klines(
        self,
        ts_code: str,
        start_date: date,
        end_date: date,
        source_type: Literal['1min', '5min'] = '5min'
    ) -> Dict[str, Any]:
        """
        生成立体K线数据

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            source_type: 数据源类型，'1min' 或 '5min'，默认为 '5min'

        Returns:
            Dict: 生成结果统计
        """
        self.logger.info(f"开始生成立体K线: {ts_code}, {start_date} - {end_date}, 数据源: {source_type}")

        # 1. 根据数据源类型获取K线数据
        if source_type == '1min':
            kline_data = self._fetch_1min_data(ts_code, start_date, end_date)
        elif source_type == '5min':
            kline_data = self._fetch_5min_data(ts_code, start_date, end_date)
        else:
            raise ValueError(f"不支持的数据源类型: {source_type}")

        if not kline_data:
            self.logger.warning(f"未找到{source_type}K线数据: {ts_code}")
            return {'count': 0, 'success': True}

        # 2. 生成立体K线
        stereo_klines = self._calculate_stereo_klines(kline_data)

        # 3. 保存结果
        saved_count = self._save_stereo_klines(stereo_klines)

        return {
            'count': saved_count,
            'success': True
        }

    def _fetch_kline_data(
        self,
        ts_code: str,
        start_date: date,
        end_date: date,
        table_name: str,
        model_class: type
    ) -> List:
        """
        通用的K线数据获取方法

        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            table_name: 数据库表名
            model_class: 数据模型类

        Returns:
            K线数据列表
        """
        start_date_str = start_date.strftime('%Y%m%d')
        end_date_str = end_date.strftime('%Y%m%d')

        # 注意：trade_date 字段格式为 varchar(8)，格式 yyyyMMdd
        query = f"""
        SELECT * FROM {table_name}
        WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date ASC, trade_time ASC
        """

        with self.db_conn.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (ts_code, start_date_str, end_date_str))
                columns = [desc[0] for desc in cursor.description]
                results = []
                for row in cursor.fetchall():
                    data = dict(zip(columns, row))
                    results.append(model_class.from_dict(data))
                return results

    def _fetch_1min_data(self, ts_code: str, start_date: date, end_date: date) -> List[HisKline1Min]:
        """获取1分钟K线数据"""
        return self._fetch_kline_data(ts_code, start_date, end_date, 'his_kline_1min', HisKline1Min)

    def _fetch_5min_data(self, ts_code: str, start_date: date, end_date: date) -> List[HisKline5Min]:
        """获取5分钟K线数据"""
        return self._fetch_kline_data(ts_code, start_date, end_date, 'his_kline_5min', HisKline5Min)

    def _calculate_stereo_klines(self, klines: List) -> List[AnalKlineRise25Pre]:
        """
        计算立体K线

        规则：
        1. 触发条件：股价相对于当前立体K线的开盘价累计涨幅达到2.5%
        2. K线生成逻辑：
           - 每根立体K线代表一次累计涨幅2.5%的价格变动区间
           - 包含区间内的：开盘价、最高价、最低价、收盘价
           - 成交量、成交额、换手率为区间内累计值
        3. 时间标记：记录区间开始时间和结束时间，以区间结束时间作为K线时间戳

        算法：
        - 初始化：参考价 = 区间开始K线的开盘价
        - 遍历：
          - 如果 (当前收盘价 - 参考价)/参考价 >= 2.5%：
            - 生成立体K线
            - 更新参考价 = 当前收盘价
            - 重置区间累积
        """
        if not klines:
            return []

        # 数据验证：过滤无效K线
        valid_klines = []
        for kline in klines:
            if (kline.open is None or kline.open <= 0 or
                kline.close is None or kline.close <= 0 or
                kline.high is None or kline.low is None):
                self.logger.warning(
                    f"跳过无效K线数据: {kline.stock_code} "
                    f"{kline.trade_date} {kline.trade_time}"
                )
                continue
            valid_klines.append(kline)

        if not valid_klines:
            self.logger.warning("没有有效的K线数据")
            return []

        klines = valid_klines
        results = []

        # 初始参考价（确保为float）
        ref_price = float(klines[0].open or 0)

        # 当前区间状态
        interval_start_kline = klines[0]
        acc_volume = 0.0
        acc_amount = 0.0
        acc_turnover = 0.0
        acc_high = -float('inf')
        acc_low = float('inf')

        for i in range(len(klines)):
            kline = klines[i]

            # 转换当前K线数据为float，避免与Decimal混用导致的错误
            k_open = float(kline.open or 0)
            k_high = float(kline.high or 0)
            k_low = float(kline.low or 0)
            k_close = float(kline.close or 0)
            k_volume = float(kline.volume or 0)
            k_amount = float(kline.amount or 0)
            k_turnover = float(kline.turnover_rate or 0)

            # 如果是新区间的开始（即上一次循环刚生成了K线）
            if interval_start_kline is None:
                interval_start_kline = kline
                # 新区间的参考价 = 新区间开始K线的开盘价
                ref_price = k_open
                # 新区间的各项累积从当前kline开始
                acc_volume = 0.0
                acc_amount = 0.0
                acc_turnover = 0.0
                acc_high = -float('inf')
                acc_low = float('inf')

            # 累积数据
            acc_volume += k_volume
            acc_amount += k_amount
            acc_turnover += k_turnover
            acc_high = max(acc_high, k_high)
            if k_low > 0:
                acc_low = min(acc_low, k_low)
            elif acc_low == float('inf'):
                acc_low = k_low

            # 计算涨幅
            if ref_price <= 0:
                ref_price = k_close
                continue

            # 计算相对于参考价的涨跌幅
            change_rate = (k_close - ref_price) / ref_price

            # 触发条件：涨幅 >= 2.5%（只考虑上涨）
            if change_rate >= self.RISE_THRESHOLD:
                # 生成立体K线
                # 提取区间起始时间信息
                interval_start_date = str(interval_start_kline.trade_date) if hasattr(interval_start_kline.trade_date, '__str__') else interval_start_kline.trade_date
                interval_start_time = str(interval_start_kline.trade_time) if hasattr(interval_start_kline.trade_time, '__str__') else interval_start_kline.trade_time

                # 提取区间结束时间信息
                interval_end_date = str(kline.trade_date) if hasattr(kline.trade_date, '__str__') else kline.trade_date
                interval_end_time = str(kline.trade_time) if hasattr(kline.trade_time, '__str__') else kline.trade_time

                # 格式化时间字段
                trade_begin_date = self._format_date(interval_start_date)
                trade_begin_time = self._format_time(interval_start_time)
                trade_begin_datetime = f"{trade_begin_date}{trade_begin_time}"

                trade_date = self._format_date(interval_end_date)
                trade_time = self._format_time(interval_end_time)
                trade_datetime = f"{trade_date}{trade_time}"

                stereo_kline = AnalKlineRise25Pre(
                    ts_code=kline.ts_code,
                    stock_code=kline.stock_code,
                    stock_name=kline.stock_name,
                    trade_begin_date=trade_begin_date,
                    trade_begin_time=trade_begin_time,
                    trade_begin_datetime=trade_begin_datetime,
                    trade_date=trade_date,
                    trade_time=trade_time,
                    trade_datetime=trade_datetime,
                    open=float(interval_start_kline.open or 0),  # 区间开始时间的Open
                    high=acc_high,
                    low=acc_low,
                    close=k_close,  # 区间结束时间的Close
                    volume=int(acc_volume),
                    amount=acc_amount,
                    adjust_flag=kline.adjust_flag,
                    change_rate=change_rate * 100,  # 存百分比
                    turnover_rate=acc_turnover,
                    create_time=datetime.now(),
                    update_time=datetime.now()
                )
                results.append(stereo_kline)

                # 标记区间结束，等待下一个kline作为新起点
                # 注意：不在这里更新ref_price，而是在下一次循环开始新区间时更新
                interval_start_kline = None

        return results

    def _format_date(self, date_value) -> str:
        """格式化日期为 yyyyMMdd"""
        if isinstance(date_value, str):
            # 移除所有分隔符
            clean_date = date_value.replace('-', '').replace('/', '').replace(' ', '')
            # 取前8位
            return clean_date[:8]
        elif isinstance(date_value, (date, datetime)):
            return date_value.strftime('%Y%m%d')
        else:
            return str(date_value)

    def _format_time(self, time_value) -> str:
        """格式化时间为 hhmm"""
        if isinstance(time_value, str):
            # 移除所有分隔符
            clean_time = time_value.replace(':', '').replace(' ', '')
            # 取前4位
            return clean_time[:4]
        elif hasattr(time_value, 'hour'):  # datetime.time 对象
            return f"{time_value.hour:02d}{time_value.minute:02d}"
        else:
            return str(time_value)

    def _save_stereo_klines(self, klines: List[AnalKlineRise25Pre]) -> int:
        """保存立体K线数据"""
        if not klines:
            return 0

        # 转换为字典列表
        data_list = [k.to_dict() for k in klines]

        # 批量插入 SQL
        insert_sql = """
        INSERT INTO anal_kline_rise_25pre
        (ts_code, stock_code, stock_name,
         trade_begin_date, trade_begin_time, trade_begin_datetime,
         trade_date, trade_time, trade_datetime,
         open, high, low, close, volume, amount,
         adjust_flag, change_rate, turnover_rate, create_time, update_time)
        VALUES
        (%(ts_code)s, %(stock_code)s, %(stock_name)s,
         %(trade_begin_date)s, %(trade_begin_time)s, %(trade_begin_datetime)s,
         %(trade_date)s, %(trade_time)s, %(trade_datetime)s,
         %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(amount)s,
         %(adjust_flag)s, %(change_rate)s, %(turnover_rate)s, %(create_time)s, %(update_time)s)
        """

        # 为了幂等性，先删除该股票该时间段的数据
        if klines:
            ts_code = klines[0].ts_code
            min_begin_date = min(k.trade_begin_date for k in klines if k.trade_begin_date)
            max_end_date = max(k.trade_date for k in klines if k.trade_date)

            delete_sql = """
            DELETE FROM anal_kline_rise_25pre
            WHERE ts_code = %s
              AND trade_begin_date >= %s
              AND trade_date <= %s
            """
            self.db_conn.execute_update(delete_sql, (ts_code, min_begin_date, max_end_date))

        return self.db_conn.execute_batch(insert_sql, data_list)
