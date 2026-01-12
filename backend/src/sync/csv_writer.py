"""
CSV文件写入器 - 严格按照产品设计文档要求，支持智能删除+Append模式
"""

import os
import csv
import pandas as pd
import logging
import uuid
from typing import List, Dict, Any, Optional, Set
from datetime import date, datetime
from ..config import ConfigManager
from ..utils.csv_file_manager import CsvFileManager
from ..utils.log_aggregator import LogAggregator


class CsvWriter:
    """CSV文件写入器，严格按照产品设计文档要求生成CSV文件，支持智能删除+Append模式"""

    # .day文件解析的原始字段集合（用于验证）
    DAY_FILE_RAW_FIELDS = {'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'preclose', 'amount', 'volume'}

    # CSV文件字段顺序（与.day文件解析一致）
    CSV_FIELD_ORDER = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'preclose', 'amount', 'volume']

    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化CSV写入器

        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager or ConfigManager()
        self.csv_path = self.config_manager.get_data_paths().get('csv', 'uat/data')
        self.logger = logging.getLogger(__name__)

        # 初始化文件管理器
        csv_config = self.config_manager.load_config().get('csv', {})
        self.file_manager = CsvFileManager(csv_config)

        # 写入会话管理
        self._write_sessions: Dict[str, Set[str]] = {}  # session_id -> set of files written
        self._session_files: Dict[str, str] = {}  # session_id -> session description

        # 静默模式和日志汇总
        self._silent_mode = False
        self._log_aggregator = LogAggregator()

    def start_write_session(self, description: str = None) -> str:
        """
        开始一个新的写入会话

        Args:
            description: 会话描述，用于日志记录

        Returns:
            会话ID
        """
        session_id = str(uuid.uuid4())
        self._write_sessions[session_id] = set()
        self._session_files[session_id] = description or f"Session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"开始新的写入会话: {session_id} - {self._session_files[session_id]}")
        return session_id

    def end_write_session(self, session_id: str) -> Dict[str, Any]:
        """
        结束写入会话并返回统计信息

        Args:
            session_id: 会话ID

        Returns:
            会话统计信息
        """
        if session_id not in self._write_sessions:
            self.logger.warning(f"会话 {session_id} 不存在")
            return {'error': 'Session not found'}

        files_written = list(self._write_sessions[session_id])
        description = self._session_files[session_id]

        # 清理会话数据
        del self._write_sessions[session_id]
        del self._session_files[session_id]

        stats = {
            'session_id': session_id,
            'description': description,
            'files_written': files_written,
            'total_files': len(files_written)
        }

        self.logger.info(f"写入会话结束: {session_id}, 写入文件数: {len(files_written)}")
        return stats

    def start_silent_mode(self):
        """启动静默模式，隐藏单个文件的日志输出"""
        self._silent_mode = True
        self._log_aggregator.start_operation('csv')

    def end_silent_mode(self):
        """结束静默模式并显示汇总信息"""
        if self._silent_mode:
            self._log_aggregator.finish_operation('csv')
            self._log_aggregator.print_summary('csv')
            self._silent_mode = False

        return self._log_aggregator.get_summary('csv')

    def _should_delete_file(self, filepath: str, data_type: str) -> bool:
        """
        判断是否应该删除文件（智能删除策略）

        Args:
            filepath: 文件路径
            data_type: 数据类型

        Returns:
            是否应该删除文件
        """
        # 检查是否有活跃的写入会话
        for session_id, written_files in self._write_sessions.items():
            if filepath in written_files:
                # 文件已在当前会话中写入过，不需要删除
                return False

        # 文件未被当前会话写入过，应该删除
        return os.path.exists(filepath)

    def _mark_file_written(self, filepath: str, session_id: str = None):
        """
        标记文件已写入

        Args:
            filepath: 文件路径
            session_id: 会话ID，如果为None则使用最新会话
        """
        if session_id is None:
            # 使用最新的会话
            if self._write_sessions:
                session_id = list(self._write_sessions.keys())[-1]
            else:
                # 没有活跃会话，创建一个新会话
                session_id = self.start_write_session("Auto session")

        if session_id in self._write_sessions:
            self._write_sessions[session_id].add(filepath)

    def _generate_filename(self, data_type: str, include_time: bool = False) -> str:
        """
        生成符合产品设计文档要求的CSV文件名

        Args:
            data_type: 数据类型 (base_stock_info, his_kline_day等)
            include_time: 是否包含时分信息，默认False
                         False: yyyyMMdd格式 (如: 20241219)
                         True: yyyyMMddhhmm格式 (如: 202412191430)

        Returns:
            符合格式的文件名
            - 不含时分: base_stock_info_20241219.csv
            - 含时分: base_fundamentals_info_202501051430.csv
        """
        if include_time:
            date_str = datetime.now().strftime('%Y%m%d%H%M')
        else:
            date_str = datetime.now().strftime('%Y%m%d')
        return f"{data_type}_{date_str}.csv"

    def _write_csv_file(self, filepath: str, data: List[Dict[str, Any]],
                       unique_keys: List[str] = None, data_type: str = None) -> None:
        """
        写入CSV文件的通用方法 - 按需求要求：存在同名文件则先删除再重建

        Args:
            filepath: 文件路径
            data: 数据列表
            unique_keys: 用于去重的字段列表（在此模式下不使用）
            data_type: 数据类型，用于文件管理决策
        """
        if not data:
            return

        # 创建目录
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # 按需求要求：如果存在同名文件，先删除已存在文件
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                self.logger.info(f"已删除旧文件: {filepath}")
            except Exception as e:
                self.logger.error(f"删除旧文件失败: {e}")

        # 准备数据
        df = pd.DataFrame(data)

        # 写入模式：总是创建新文件（删除重建模式）
        try:
            df.to_csv(filepath, mode='w', index=False, encoding='utf-8-sig', header=True)

            # 标记文件已写入
            self._mark_file_written(filepath)

            # 移除CSV保存明细日志，避免无意义的输出

        except Exception as e:
            self.logger.error(f"保存CSV文件失败: {e}")
            raise

    
    def write_base_stock_info(self, stocks: List[Dict[str, Any]]) -> None:
        """
        写入股票基本信息到CSV - 严格按照产品设计文档要求

        Args:
            stocks: 股票基本信息列表
        """
        filename = self._generate_filename('base_stock_info')
        # 按照文档要求：csv文件输出目录为{csv文件根目录}/base_stock_info
        subdir = 'base_stock_info'
        dirpath = os.path.join(self.csv_path, subdir)

        # 确保子目录存在
        os.makedirs(dirpath, exist_ok=True)
        filepath = os.path.join(dirpath, filename)

        # 确保字段名符合产品设计文档
        mapped_data = []
        for stock in stocks:
            mapped_stock = {
                'ts_code': stock.get('ts_code'),
                'stock_code': stock.get('stock_code') or stock.get('code'),
                'stock_name': stock.get('stock_name') or stock.get('name'),
                'cnspell': stock.get('cnspell'),
                'market_code': stock.get('market_code') or stock.get('market'),
                'market_name': stock.get('market_name'),
                'exchange_code': stock.get('exchange_code'),
                'sector_code': stock.get('sector_code'),  # 无法获取则留空
                'sector_name': stock.get('sector_name'),  # 无法获取则留空
                'industry_code': stock.get('industry_code'),
                'industry_name': stock.get('industry_name') or stock.get('industry'),
                'list_status': stock.get('list_status') or stock.get('status'),
                'list_date': stock.get('list_date'),
                'delist_date': stock.get('delist_date'),
                'type': stock.get('type')
            }
            mapped_data.append(mapped_stock)

        self._write_csv_file(filepath, mapped_data, unique_keys=['stock_code'], data_type='base_stock_info')

    
    def write_his_kline_day(self, daily_data: List[Dict[str, Any]]) -> None:
        """
        写入日K线数据到CSV - 按股票分组，每只股票生成一个独立文件
        CSV数据来源：.day文件解析的原始数据（未经任何加工）
        字段规则：仅增加ts_code字段，其他字段使用.day文件解析的原始字段名

        文件命名：his_kline_day_{ts_code}_{timestamp}.csv
        时间戳格式：yyyyMMddhhmmss

        Args:
            daily_data: 日K线数据列表（.day文件原始数据 + ts_code）
        """
        if not daily_data:
            return

        # 按照文档要求：csv文件输出目录为{csv文件根目录}/his_kline_day
        subdir = 'his_kline_day'
        dirpath = os.path.join(self.csv_path, subdir)
        os.makedirs(dirpath, exist_ok=True)

        # 直接使用.day文件解析的原始数据，不做任何字段映射和加工
        self._write_kline_data_by_stock(daily_data, dirpath)

    def _write_kline_data_by_stock(self, raw_data: List[Dict[str, Any]], dirpath: str) -> None:
        """
        按股票分组写入K线数据到独立CSV文件
        严格保持.day文件解析的原始字段，仅增加ts_code字段

        文件命名：his_kline_day_{ts_code}_{timestamp}.csv
        时间戳格式：yyyyMMddhhmmss
        每只股票生成一个CSV文件，包含该股票所有日期的数据

        Args:
            raw_data: .day文件解析数据列表（仅增加ts_code字段）
            dirpath: CSV文件目录路径
        """
        if not raw_data:
            return

        try:
            # 生成时间戳
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

            # 转换为DataFrame以便分组处理
            df = pd.DataFrame(raw_data)

            # 确保ts_code字段存在
            if 'ts_code' not in df.columns:
                self.logger.error("数据中缺少ts_code字段，无法按股票分组")
                return

            # 按ts_code分组
            grouped = df.groupby('ts_code')

            # 为每只股票生成独立CSV文件
            for ts_code, group_df in grouped:
                # 生成文件名：his_kline_day_{ts_code}_{timestamp}.csv
                filename = f"his_kline_day_{ts_code}_{timestamp}.csv"
                filepath = os.path.join(dirpath, filename)

                # 如果存在同名文件，先删除
                if os.path.exists(filepath):
                    try:
                        os.remove(filepath)
                        self.logger.debug(f"已删除旧文件: {filename}")
                    except Exception as e:
                        self.logger.warning(f"删除旧文件失败: {e}")

                # 将DataFrame转换为字典列表
                group_data = group_df.to_dict('records')

                # 验证数据原始性：确保只有.day文件解析字段 + ts_code
                actual_fields = set(group_df.columns)

                if not actual_fields.issubset(self.DAY_FILE_RAW_FIELDS):
                    unexpected_fields = actual_fields - self.DAY_FILE_RAW_FIELDS
                    self.logger.warning(f"CSV数据包含非预期字段: {unexpected_fields}")
                    # 移除非预期字段，确保数据原始性
                    group_df = group_df[list(self.DAY_FILE_RAW_FIELDS.intersection(actual_fields))]

                # 确保字段顺序与.day文件解析一致，ts_code在前
                # 只保留存在的字段
                available_fields = [field for field in self.CSV_FIELD_ORDER if field in group_df.columns]

                try:
                    # 按指定字段顺序写入CSV，保持原始字段名
                    group_df[available_fields].to_csv(filepath, index=False, encoding='utf-8-sig')
                    # 标记文件已写入，用于会话管理
                    self._mark_file_written(filepath)
                except Exception as e:
                    self.logger.error(f"写入CSV文件失败: {filepath}, 错误: {e}")
                    raise

                # 记录日志，强调数据原始性
                if self._silent_mode:
                    self._log_aggregator.add_file_summary(filename, len(group_data), 'csv')
                else:
                    field_list = ', '.join(available_fields)
                    self.logger.info(f"已生成CSV文件: {filename} ({len(group_data)} 条记录)")
                    self.logger.debug(f"CSV字段: {field_list} (仅.day文件解析字段 + ts_code)")

        except Exception as e:
            self.logger.error(f"按股票分组写入CSV失败: {e}")
            raise

    def _write_kline_data_by_trade_date(self, mapped_data: List[Dict[str, Any]], dirpath: str) -> None:
        """
        按交易日期分组写入K线数据到独立CSV文件（修复重复数据问题）

        Args:
            mapped_data: 已映射的K线数据列表
            dirpath: CSV文件目录路径
        """
        if not mapped_data:
            return

        try:
            # 转换为DataFrame以便分组处理
            df = pd.DataFrame(mapped_data)

            # 获取采集日期（当前日期）
            collection_date = datetime.now().strftime('%Y%m%d')

            # 确保trade_date是datetime类型
            if 'trade_date' in df.columns:
                # 修复：统一日期格式并标准化，避免因格式不同导致的重复数据
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()

                # 修复：在分组前先去重，确保同一股票每天只有一条记录
                df_deduped = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')

                # 记录去重统计
                original_count = len(df)
                deduped_count = len(df_deduped)
                if original_count > deduped_count:
                    self.logger.info(f"数据去重：{original_count} -> {deduped_count} 条记录，去除 {original_count - deduped_count} 条重复")

                # 按标准化的交易日期分组
                grouped = df_deduped.groupby('trade_date')

                # 为每个交易日生成独立CSV文件
                for trade_date, group_df in grouped:
                    # 生成文件名：使用交易日期和采集日期格式 YYYYMMDD
                    # 产品设计要求：his_kline_day_交易日_采集日.csv
                    date_str = trade_date.strftime('%Y%m%d')
                    filename = f"his_kline_day_{date_str}_{collection_date}.csv"
                    filepath = os.path.join(dirpath, filename)

                    # 转换回字典列表格式
                    group_data = group_df.to_dict('records')

                    # 写入该交易日的数据（通过文件管理器统一处理，确保代码一致性）
                    self._write_csv_file(filepath, group_data, unique_keys=['ts_code', 'trade_date'], data_type='his_kline_day')

                    # 静默模式下不输出单个文件日志，而是收集到汇总器中
                    if self._silent_mode:
                        self._log_aggregator.add_file_summary(filename, len(group_data), 'csv')
                    else:
                        self.logger.info(f"已生成CSV文件: {filename} ({len(group_data)} 条记录)")

            else:
                self.logger.error("数据中缺少trade_date字段，无法按日期分组")
                # 回退到原始方式
                filename = self._generate_filename('his_kline_day')
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date'], data_type='his_kline_day')

        except Exception as e:
            self.logger.error(f"按交易日期分组写入CSV失败: {e}")
            # 回退到原始方式
            try:
                filename = self._generate_filename('his_kline_day')
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date'], data_type='his_kline_day')
                print(f"⚠️  已回退到原始方式写入: {filename}")
            except Exception as fallback_error:
                print(f"❌ 回退写入也失败: {fallback_error}")

    def write_his_kline_1min(self, min1_data: List[Dict[str, Any]]) -> None:
        """
        写入1分钟K线数据到CSV - 严格按照产品设计文档要求
        每只股票生成一个CSV文件，命名规则为his_kline_1min_{ts_code}_{当前时间}.csv
        数据源：ts_code加上.lc1文件中解析出来的未经任何加工的数据

        包含字段（原始数据）：
        - 基本信息：ts_code, stock_code
        - 交易时间：trade_date, trade_time
        - 价格数据：open, high, low, close
        - 成交数据：volume, amount

        Args:
            min1_data: 1分钟K线数据列表
        """
        if not min1_data:
            print("❌ 没有数据需要写入")
            return

        # 按照文档要求：csv文件输出目录为{csv文件根目录}/his_kline_1min
        subdir = 'his_kline_1min'
        dirpath = os.path.join(self.csv_path, subdir)

        # 确保子目录存在
        os.makedirs(dirpath, exist_ok=True)

        # 确保字段名符合产品设计文档，只提取原始字段
        mapped_data = []
        for data in min1_data:
            # 格式化日期和时间
            trade_date_val = data.get('trade_date')
            if hasattr(trade_date_val, 'strftime'):
                trade_date_val = trade_date_val.strftime('%Y%m%d')
            elif hasattr(data.get('trade_date_str'), '__len__'):
                 trade_date_val = data.get('trade_date_str')

            trade_time_val = data.get('trade_time')
            if hasattr(trade_time_val, 'strftime'):
                trade_time_val = trade_time_val.strftime('%H%M')
            elif hasattr(data.get('trade_time_str'), '__len__'):
                 trade_time_val = data.get('trade_time_str')

            mapped_min1 = {
                'ts_code': data.get('ts_code'),
                'stock_code': data.get('stock_code'),
                'trade_date': trade_date_val,
                'trade_time': trade_time_val,
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'volume': data.get('volume'),
                'amount': data.get('amount')
            }
            mapped_data.append(mapped_min1)

        # 按股票分组并写入独立文件（符合文档要求）
        self._write_1min_data_by_stock(mapped_data, dirpath)

    def write_his_kline_5min(self, min5_data: List[Dict[str, Any]]) -> None:
        """
        写入5分钟K线数据到CSV - 严格按照产品设计文档要求
        每只股票生成一个CSV文件，命名规则为his_kline_5min_{ts_code}_{当前时间}.csv
        数据源：ts_code加上.lc5文件中解析出来的未经任何加工的数据

        包含字段（原始数据）：
        - 基本信息：ts_code, stock_code
        - 交易时间：trade_date, trade_time
        - 价格数据：open, high, low, close
        - 成交数据：volume, amount

        Args:
            min5_data: 5分钟K线数据列表（原始解析数据）
        """
        if not min5_data:
            print("❌ 没有数据需要写入")
            return

        # 按照文档要求：csv文件输出目录为{csv文件根目录}/his_kline_5min
        subdir = 'his_kline_5min'
        dirpath = os.path.join(self.csv_path, subdir)

        # 确保子目录存在
        os.makedirs(dirpath, exist_ok=True)

        # 确保字段名符合产品设计文档，只提取原始字段
        mapped_data = []
        for data in min5_data:
            # 格式化日期和时间，确保是字符串以便写入
            trade_date_val = data.get('trade_date')
            if hasattr(trade_date_val, 'strftime'):
                trade_date_val = trade_date_val.strftime('%Y%m%d')
            elif hasattr(data.get('trade_date_str'), '__len__'): # 兼容处理
                 trade_date_val = data.get('trade_date_str')

            trade_time_val = data.get('trade_time')
            if hasattr(trade_time_val, 'strftime'):
                trade_time_val = trade_time_val.strftime('%H%M')
            elif hasattr(data.get('trade_time_str'), '__len__'): # 兼容处理
                 trade_time_val = data.get('trade_time_str')

            mapped_min5 = {
                'ts_code': data.get('ts_code'),
                'stock_code': data.get('stock_code') or data.get('code'),
                'trade_date': trade_date_val,
                'trade_time': trade_time_val,
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'volume': data.get('volume'),
                'amount': data.get('amount')
            }
            mapped_data.append(mapped_min5)

        # 按股票分组并写入独立文件（符合文档要求）
        self._write_5min_data_by_stock(mapped_data, dirpath)

    def _write_5min_data_by_stock(self, mapped_data: List[Dict[str, Any]], dirpath: str) -> None:
        """
        按股票分组写入5分钟K线数据到独立CSV文件
        命名规则：his_kline_5min_{ts_code}_{当前时间}.csv
        时间格式：yyyyMMddhhmmss

        Args:
            mapped_data: 已映射的5分钟K线数据列表
            dirpath: CSV文件目录路径
        """
        if not mapped_data:
            return

        try:
            # 转换为DataFrame以便分组处理
            df = pd.DataFrame(mapped_data)

            # 在分组前先去重，确保同一股票同一时间只有一条记录
            if 'ts_code' in df.columns and 'trade_date' in df.columns and 'trade_time' in df.columns:
                # 确保按时间排序
                df.sort_values(by=['ts_code', 'trade_date', 'trade_time'], inplace=True)

                df_deduped = df.drop_duplicates(subset=['ts_code', 'trade_date', 'trade_time'], keep='last')

                # 记录去重统计
                original_count = len(df)
                deduped_count = len(df_deduped)
                if original_count > deduped_count:
                    self.logger.info(f"5分钟K线数据去重：{original_count} -> {deduped_count} 条记录")

                # 生成时间戳：yyyyMMddhhmmss
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

                # 按股票分组
                grouped = df_deduped.groupby('ts_code')

                # 为每只股票生成独立CSV文件（严格按照文档要求）
                for ts_code, group_df in grouped:
                    # 生成文件名：his_kline_5min_{ts_code}_{timestamp}.csv
                    filename = f"his_kline_5min_{ts_code}_{timestamp}.csv"
                    filepath = os.path.join(dirpath, filename)

                    # 转换回字典列表格式
                    group_data = group_df.to_dict('records')

                    # 写入该股票的数据（通过文件管理器统一处理，确保代码一致性）
                    self._write_csv_file(filepath, group_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_5min')

                    print(f"✅ 已生成5分钟K线CSV文件: {filename} ({len(group_data)} 条记录)")
            else:
                print("❌ 5分钟K线数据中缺少必要字段(ts_code/trade_date/trade_time)，无法按股票分组")

        except Exception as e:
            print(f"❌ 按股票分组写入5分钟K线数据失败: {e}")
            import traceback
            traceback.print_exc()

    # 保持向后兼容的方法名
    def write_stocks(self, stocks: List[Dict[str, Any]]) -> None:
        """向后兼容：调用新的股票基本信息写入方法"""
        self.write_base_stock_info(stocks)

    def write_daily_data(self, daily_data: List[Dict[str, Any]]) -> None:
        """向后兼容：调用新的日K线数据写入方法"""
        self.write_his_kline_day(daily_data)

    def write_financial_data(self, financial_data: List[Dict[str, Any]]) -> None:
        """向后兼容：调用新的基本面信息写入方法"""
        self.write_base_fundamentals_info(financial_data)

    def write_base_fundamentals_info(self, fundamentals_data: List[Dict[str, Any]]) -> None:
        """
        写入基本面信息到CSV - 严格按照产品设计文档要求

        Args:
            fundamentals_data: 基本面信息列表

        注意:
            文件名格式为 base_fundamentals_info_{yyyyMMddhhmm}.csv
            包含时分信息以支持同一天多次采集
        """
        if not fundamentals_data:
            self.logger.info("没有基本面数据需要写入")
            return

        # 基本面信息使用包含时分的文件名格式
        filename = self._generate_filename('base_fundamentals_info', include_time=True)
        # 按照文档要求：csv文件输出目录为{csv文件根目录}/base_fundamentals_info
        subdir = 'base_fundamentals_info'
        dirpath = os.path.join(self.csv_path, subdir)

        # 确保子目录存在
        os.makedirs(dirpath, exist_ok=True)
        filepath = os.path.join(dirpath, filename)

        # 确保字段名符合实际表结构
        mapped_data = []
        for data in fundamentals_data:
            mapped_fundamental = {
                'ts_code': data.get('ts_code'),
                'stock_code': data.get('stock_code'),
                'stock_name': data.get('stock_name'),
                'disclosure_date': data.get('disclosure_date'),
                'total_share': data.get('total_share'),
                'float_share': data.get('float_share')
            }
            mapped_data.append(mapped_fundamental)

        self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code'], data_type='base_fundamentals_info')
        self.logger.info(f"基本面信息CSV文件已生成: {filename} ({len(mapped_data)} 条记录)")

    def write_5min_data(self, min5_data: List[Dict[str, Any]]) -> None:
        """新增：5分钟K线数据写入方法"""
        self.write_his_kline_5min(min5_data)

    def validate_csv_data_purity(self, csv_filepath: str) -> Dict[str, Any]:
        """
        验证CSV文件数据原始性 - 确保只包含.day文件解析字段 + ts_code

        Args:
            csv_filepath: CSV文件路径

        Returns:
            验证结果字典
        """
        try:
            import pandas as pd

            # 读取CSV文件
            df = pd.read_csv(csv_filepath)

            # 期望的字段集合（.day文件解析字段 + ts_code）
            expected_fields = self.DAY_FILE_RAW_FIELDS
            actual_fields = set(df.columns)

            # 验证结果
            validation_result = {
                'is_valid': True,
                'file_path': csv_filepath,
                'record_count': len(df),
                'expected_fields': expected_fields,
                'actual_fields': actual_fields,
                'missing_fields': expected_fields - actual_fields,
                'unexpected_fields': actual_fields - expected_fields,
                'validation_message': ''
            }

            # 检查是否有缺失字段
            if validation_result['missing_fields']:
                validation_result['is_valid'] = False
                validation_result['validation_message'] += f"缺失字段: {validation_result['missing_fields']}; "

            # 检查是否有非预期字段
            if validation_result['unexpected_fields']:
                validation_result['is_valid'] = False
                validation_result['validation_message'] += f"非预期字段: {validation_result['unexpected_fields']}; "

            if validation_result['is_valid']:
                validation_result['validation_message'] = "✅ 数据原始性验证通过，仅包含.day文件解析字段 + ts_code"

            return validation_result

        except Exception as e:
            return {
                'is_valid': False,
                'file_path': csv_filepath,
                'validation_message': f"❌ 验证失败: {str(e)}"
            }

    def get_backup_info(self) -> Dict[str, Any]:
        """
        获取备份信息

        Returns:
            备份信息字典
        """
        return self.file_manager.get_backup_info()

    def set_file_mode(self, data_type: str, mode: str) -> None:
        """
        设置指定数据类型的文件管理模式（临时设置，不修改配置文件）

        Args:
            data_type: 数据类型
            mode: 模式 ('append', 'overwrite', 'backup_overwrite')
        """
        if mode not in ['append', 'overwrite', 'backup_overwrite']:
            raise ValueError(f"无效的文件管理模式: {mode}")

        if 'per_type_settings' not in self.file_manager.config:
            self.file_manager.config['per_type_settings'] = {}

        self.file_manager.config['per_type_settings'][data_type] = {'mode': mode}

    def _write_1min_data_by_stock(self, mapped_data: List[Dict[str, Any]], dirpath: str) -> None:
        """
        按股票分组写入1分钟K线数据到独立CSV文件 - 严格按照产品设计文档要求
        命名规则：his_kline_1min_{ts_code}_{当前时间}.csv
        时间格式：yyyyMMddhhmmss

        Args:
            mapped_data: 已映射的1分钟K线数据列表
            dirpath: CSV文件目录路径
        """
        if not mapped_data:
            return

        try:
            # 转换为DataFrame以便分组处理
            df = pd.DataFrame(mapped_data)

            # 在分组前先去重，确保同一股票同一时间只有一条记录
            if 'ts_code' in df.columns and 'trade_date' in df.columns and 'trade_time' in df.columns:
                df_deduped = df.drop_duplicates(subset=['ts_code', 'trade_date', 'trade_time'], keep='last')

                # 记录去重统计
                original_count = len(df)
                deduped_count = len(df_deduped)
                if original_count > deduped_count:
                    self.logger.info(f"1分钟K线数据去重：{original_count} -> {deduped_count} 条记录，去除 {original_count - deduped_count} 条重复")

                # 生成时间戳：yyyyMMddhhmmss
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

                # 按股票分组
                grouped = df_deduped.groupby('ts_code')

                # 为每只股票生成独立CSV文件（严格按照文档要求）
                for ts_code, group_df in grouped:
                    # 生成文件名：his_kline_1min_{ts_code}_{timestamp}.csv
                    filename = f"his_kline_1min_{ts_code}_{timestamp}.csv"
                    filepath = os.path.join(dirpath, filename)

                    # 转换回字典列表格式
                    group_data = group_df.to_dict('records')

                    # 写入该股票的数据（通过文件管理器统一处理，确保代码一致性）
                    self._write_csv_file(filepath, group_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_1min')

                    print(f"✅ 已生成1分钟K线CSV文件: {filename} ({len(group_data)} 条记录)")
            else:
                print("❌ 1分钟K线数据中缺少必要字段(ts_code/trade_date/trade_time)，无法按股票分组")
                # 回退到原始方式
                filename = self._generate_1min_filename()
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_1min')
                print(f"⚠️  已回退到原始方式写入: {filename}")

        except Exception as e:
            print(f"❌ 按股票分组写入1分钟K线数据失败: {e}")
            # 回退到原始方式
            try:
                filename = self._generate_1min_filename()
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_1min')
                print(f"⚠️  已回退到原始方式写入: {filename}")
            except Exception as fallback_error:
                print(f"❌ 回退写入也失败: {fallback_error}")

    def _generate_1min_filename(self) -> str:
        """
        生成符合产品设计文档要求的1分钟K线CSV文件名

        Returns:
            文件名，如: his_kline_1min_{ts_code}_20241219123045.csv
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"his_kline_1min_{timestamp}.csv"
