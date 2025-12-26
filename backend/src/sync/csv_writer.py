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


class CsvWriter:
    """CSV文件写入器，严格按照产品设计文档要求生成CSV文件，支持智能删除+Append模式"""

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

    def _generate_filename(self, data_type: str) -> str:
        """
        生成符合产品设计文档要求的CSV文件名

        Args:
            data_type: 数据类型 (base_stock_info, his_kline_day等)

        Returns:
            符合格式的文件名，如: base_stock_info_20241219.csv
        """
        date_str = datetime.now().strftime('%Y%m%d')
        return f"{data_type}_{date_str}.csv"

    def _write_csv_file(self, filepath: str, data: List[Dict[str, Any]],
                       unique_keys: List[str] = None, data_type: str = None) -> None:
        """
        写入CSV文件的通用方法 - 支持智能删除+Append模式

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

        # 智能删除策略：检查是否需要删除现有文件
        should_delete = self._should_delete_file(filepath, data_type)
        if should_delete:
            try:
                os.remove(filepath)
                self.logger.info(f"已删除旧文件: {filepath}")
            except Exception as e:
                self.logger.error(f"删除旧文件失败: {e}")

        # 准备数据
        df = pd.DataFrame(data)

        # 追加模式写入
        try:
            # 如果文件不存在，写入header；如果存在，追加数据
            file_exists = os.path.exists(filepath)
            mode = 'a' if file_exists else 'w'
            header = not file_exists

            df.to_csv(filepath, mode=mode, index=False, encoding='utf-8-sig', header=header)

            # 标记文件已写入
            self._mark_file_written(filepath)

            # 避免重新读取大文件，使用累计计数
            existing_count = 0
            if file_exists:
                try:
                    # 只获取行数，不读取数据
                    existing_count = sum(1 for _ in open(filepath, 'r', encoding='utf-8-sig')) - 1  # 减去header行
                except Exception:
                    # 如果获取行数失败，估算一个值
                    existing_count = 0

            total_records = existing_count + len(df)
            self.logger.info(f"CSV文件已保存: {filepath}，本批记录数: {len(df)}，总记录数: {total_records}")

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
                'delist_date': stock.get('delist_date')
            }
            mapped_data.append(mapped_stock)

        self._write_csv_file(filepath, mapped_data, unique_keys=['stock_code'], data_type='base_stock_info')

    
    def write_his_kline_day(self, daily_data: List[Dict[str, Any]]) -> None:
        """
        写入日K线数据到CSV - 按交易日期分组，每个交易日生成独立文件

        Args:
            daily_data: 日K线数据列表
        """
        if not daily_data:
            return

        # 按照文档要求：csv文件输出目录为{csv文件根目录}/his_kline_day
        subdir = 'his_kline_day'
        dirpath = os.path.join(self.csv_path, subdir)
        os.makedirs(dirpath, exist_ok=True)

        # 确保字段名符合产品设计文档
        mapped_data = []
        for data in daily_data:
            mapped_daily = {
                'ts_code': data.get('ts_code'),
                'stock_code': data.get('stock_code') or data.get('code'),
                'stock_name': data.get('stock_name') or data.get('name'),
                'trade_date': data.get('trade_date') or data.get('date'),
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'preclose': data.get('preclose'),
                'volume': data.get('volume'),
                'amount': data.get('amount'),
                'trade_status': data.get('trade_status', 1),  # 默认正常交易
                'is_st': data.get('is_st', False),  # 默认非ST
                'adjust_flag': data.get('adjust_flag', 3),  # 默认不复权
                'change_rate': data.get('change_rate') or data.get('pct_chg'),
                'turnover_rate': data.get('turnover_rate') or data.get('turn'),
                'pe_ttm': data.get('pe_ttm'),  # 无法获取则留空
                'pb_rate': data.get('pb_rate'),  # 无法获取则留空
                'ps_ttm': data.get('ps_ttm'),  # 无法获取则留空
                'pcf_ttm': data.get('pcf_ttm')  # 无法获取则留空
            }
            mapped_data.append(mapped_daily)

        # 按交易日期分组并写入独立文件
        self._write_kline_data_by_trade_date(mapped_data, dirpath)

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
                    # 生成文件名：使用交易日期格式 YYYYMMDD
                    date_str = trade_date.strftime('%Y%m%d')
                    filename = f"his_kline_day_{date_str}.csv"
                    filepath = os.path.join(dirpath, filename)

                    # 转换回字典列表格式
                    group_data = group_df.to_dict('records')

                    # 写入该交易日的数据（通过文件管理器统一处理，确保代码一致性）
                    self._write_csv_file(filepath, group_data, unique_keys=['ts_code', 'trade_date'], data_type='his_kline_day')

                    print(f"✅ 已生成CSV文件: {filename} ({len(group_data)} 条记录)")

            else:
                print("❌ 数据中缺少trade_date字段，无法按日期分组")
                # 回退到原始方式
                filename = self._generate_filename('his_kline_day')
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date'], data_type='his_kline_day')

        except Exception as e:
            print(f"❌ 按交易日期分组写入CSV失败: {e}")
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
        每个交易日生成一个CSV文件

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

        # 确保字段名符合产品设计文档
        mapped_data = []
        for data in min1_data:
            mapped_min1 = {
                'ts_code': data.get('ts_code'),
                'stock_code': data.get('stock_code'),
                'stock_name': data.get('stock_name'),
                'trade_date': data.get('trade_date'),
                'trade_time': data.get('trade_time'),
                'trade_datetime': data.get('trade_datetime'),
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'preclose': data.get('preclose'),
                'volume': data.get('volume'),
                'amount': data.get('amount'),
                'adjust_flag': data.get('adjust_flag', 3),
                'change_rate': data.get('change_rate'),
                'turnover_rate': data.get('turnover_rate')
            }
            mapped_data.append(mapped_min1)

        # 按交易日期分组并写入独立文件（符合文档要求）
        self._write_1min_data_by_trade_date(mapped_data, dirpath)

    def write_his_kline_5min(self, min5_data: List[Dict[str, Any]]) -> None:
        """
        写入5分钟K线数据到CSV - 严格按照产品设计文档要求
        每个交易日生成一个CSV文件

        Args:
            min5_data: 5分钟K线数据列表
        """
        if not min5_data:
            print("❌ 没有数据需要写入")
            return

        # 按照文档要求：csv文件输出目录为{csv文件根目录}/his_kline_5min
        subdir = 'his_kline_5min'
        dirpath = os.path.join(self.csv_path, subdir)

        # 确保子目录存在
        os.makedirs(dirpath, exist_ok=True)

        # 确保字段名符合产品设计文档
        mapped_data = []
        for data in min5_data:
            mapped_min5 = {
                'ts_code': data.get('ts_code'),
                'stock_code': data.get('stock_code'),
                'stock_name': data.get('stock_name'),
                'trade_date': data.get('trade_date'),
                'trade_time': data.get('trade_time'),
                'trade_datetime': data.get('trade_datetime'),
                'open': data.get('open'),
                'high': data.get('high'),
                'low': data.get('low'),
                'close': data.get('close'),
                'preclose': data.get('preclose'),
                'volume': data.get('volume'),
                'amount': data.get('amount'),
                'adjust_flag': data.get('adjust_flag', 3),
                'change_rate': data.get('change_rate'),
                'turnover_rate': data.get('turnover_rate')
            }
            mapped_data.append(mapped_min5)

        # 按交易日期分组并写入独立文件（符合文档要求）
        self._write_5min_data_by_trade_date(mapped_data, dirpath)

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
        """
        if not fundamentals_data:
            self.logger.info("没有基本面数据需要写入")
            return

        filename = self._generate_filename('base_fundamentals_info')
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

    def _write_1min_data_by_trade_date(self, mapped_data: List[Dict[str, Any]], dirpath: str) -> None:
        """
        按交易日期分组写入1分钟K线数据到独立CSV文件（符合产品设计文档要求）

        Args:
            mapped_data: 已映射的1分钟K线数据列表
            dirpath: CSV文件目录路径
        """
        if not mapped_data:
            return

        try:
            # 转换为DataFrame以便分组处理
            df = pd.DataFrame(mapped_data)

            # 确保trade_date字段存在
            if 'trade_date' in df.columns:
                # 确保trade_date是datetime类型
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()

                # 在分组前先去重，确保同一股票同一时间只有一条记录
                df_deduped = df.drop_duplicates(subset=['ts_code', 'trade_date', 'trade_time'], keep='last')

                # 记录去重统计
                original_count = len(df)
                deduped_count = len(df_deduped)
                if original_count > deduped_count:
                    self.logger.info(f"1分钟K线数据去重：{original_count} -> {deduped_count} 条记录，去除 {original_count - deduped_count} 条重复")

                # 按交易日期分组
                grouped = df_deduped.groupby('trade_date')

                # 为每个交易日生成独立CSV文件（严格按照文档要求）
                for trade_date, group_df in grouped:
                    # 生成文件名：使用交易日期格式 YYYYMMDD
                    date_str = trade_date.strftime('%Y%m%d')
                    filename = f"his_kline_1min_{date_str}.csv"  # 符合文档要求的命名规则
                    filepath = os.path.join(dirpath, filename)

                    # 转换回字典列表格式
                    group_data = group_df.to_dict('records')

                    # 写入该交易日的数据（通过文件管理器统一处理，确保代码一致性）
                    self._write_csv_file(filepath, group_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_1min')

                    print(f"✅ 已生成1分钟K线CSV文件: {filename} ({len(group_data)} 条记录)")

            else:
                print("❌ 1分钟K线数据中缺少trade_date字段，无法按日期分组")
                # 回退到原始方式
                filename = self._generate_filename('his_kline_1min')
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_1min')
                print(f"⚠️  已回退到原始方式写入: {filename}")

        except Exception as e:
            print(f"❌ 按交易日期分组写入1分钟K线数据失败: {e}")
            # 回退到原始方式
            try:
                filename = self._generate_filename('his_kline_1min')
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_1min')
                print(f"⚠️  已回退到原始方式写入: {filename}")
            except Exception as fallback_error:
                print(f"❌ 回退写入也失败: {fallback_error}")
    def _write_5min_data_by_trade_date(self, mapped_data: List[Dict[str, Any]], dirpath: str) -> None:
        """
        按交易日期分组写入5分钟K线数据到独立CSV文件（符合产品设计文档要求）

        Args:
            mapped_data: 已映射的5分钟K线数据列表
            dirpath: CSV文件目录路径
        """
        if not mapped_data:
            return

        try:
            # 转换为DataFrame以便分组处理
            df = pd.DataFrame(mapped_data)

            # 确保trade_date字段存在
            if 'trade_date' in df.columns:
                # 确保trade_date是datetime类型
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()

                # 在分组前先去重，确保同一股票同一时间只有一条记录
                df_deduped = df.drop_duplicates(subset=['ts_code', 'trade_date', 'trade_time'], keep='last')

                # 记录去重统计
                original_count = len(df)
                deduped_count = len(df_deduped)
                if original_count > deduped_count:
                    self.logger.info(f"5分钟K线数据去重：{original_count} -> {deduped_count} 条记录，去除 {original_count - deduped_count} 条重复")

                # 按交易日期分组
                grouped = df_deduped.groupby('trade_date')

                # 为每个交易日生成独立CSV文件（严格按照文档要求）
                for trade_date, group_df in grouped:
                    # 生成文件名：使用交易日期格式 YYYYMM（5分钟K线文档要求）
                    date_str = trade_date.strftime('%Y%m')
                    filename = f"his_kline_5min_{date_str}.csv"  # 符合文档要求的命名规则
                    filepath = os.path.join(dirpath, filename)

                    # 转换回字典列表格式
                    group_data = group_df.to_dict('records')

                    # 写入该交易日的数据（通过文件管理器统一处理，确保代码一致性）
                    self._write_csv_file(filepath, group_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_5min')

                    print(f"✅ 已生成5分钟K线CSV文件: {filename} ({len(group_data)} 条记录)")

            else:
                print("❌ 5分钟K线数据中缺少trade_date字段，无法按日期分组")
                # 回退到原始方式
                filename = self._generate_filename('his_kline_5min')
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_5min')
                print(f"⚠️  已回退到原始方式写入: {filename}")

        except Exception as e:
            print(f"❌ 按交易日期分组写入5分钟K线数据失败: {e}")
            # 回退到原始方式
            try:
                filename = self._generate_filename('his_kline_5min')
                filepath = os.path.join(dirpath, filename)
                self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code', 'trade_date', 'trade_time'], data_type='his_kline_5min')
                print(f"⚠️  已回退到原始方式写入: {filename}")
            except Exception as fallback_error:
                print(f"❌ 回退写入也失败: {fallback_error}")
