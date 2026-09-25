"""
CSV文件写入器 - 严格按照产品设计文档要求，支持智能删除+Append模式
"""

import os
import csv
import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from ..config import ConfigManager

class CsvWriter:
    """CSV文件写入器，严格按照产品设计文档要求生成CSV文件，支持智能删除+Append模式"""

    # CSV文件字段顺序

    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化CSV写入器

        Args:
            config_manager: 配置管理器
        """
        self.config_manager = config_manager or ConfigManager()
        self.csv_path = self.config_manager.get_data_paths().get('csv', 'data')
        self.logger = logging.getLogger(__name__)

    def _generate_filename(self, data_type: str, include_time: bool = False, include_seconds: bool = False) -> str:
        """
        生成符合产品设计文档要求的CSV文件名

        Args:
            data_type: 数据类型 (base_stock_info, base_fundamentals_info等)
            include_time: 是否包含时分信息，默认False
                         False: yyyyMMdd格式 (如: 20241219)
                         True: yyyyMMddhhmm格式 (如: 202412191430)
            include_seconds: 是否包含秒，默认False
                            True: yyyyMMddhhmmss格式 (如: 20241219143059)

        Returns:
            符合格式的文件名
            - 不含时分: base_stock_info_20241219.csv
            - 含时分: base_fundamentals_info_202501051430.csv
            - 含秒: his_kline_1min_sz.000001_20250105143059.csv
        """
        if include_seconds:
            date_str = datetime.now().strftime('%Y%m%d%H%M%S')
        elif include_time:
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

        try:
            # 使用标准库流式写入，避免pandas开销
            fieldnames = list(data[0].keys())
            with open(filepath, mode='w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(data)

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

    
    def write_stocks(self, stocks: List[Dict[str, Any]]) -> None:
        """向后兼容：调用新的股票基本信息写入方法"""
        self.write_base_stock_info(stocks)

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
                'stat_date': data.get('stat_date'),
                'disclosure_date': data.get('disclosure_date'),
                'total_share': data.get('total_share'),
                'float_share': data.get('float_share')
            }
            mapped_data.append(mapped_fundamental)

        self._write_csv_file(filepath, mapped_data, unique_keys=['ts_code'], data_type='base_fundamentals_info')
        self.logger.info(f"基本面信息CSV文件已生成: {filename} ({len(mapped_data)} 条记录)")

    def write_his_kline_1min(self, ts_code: str, kline_data: List[Dict[str, Any]]) -> None:
        """
        写入1分钟K线数据到CSV - 严格按照产品设计文档要求

        Args:
            ts_code: 股票ts_code
            kline_data: 1分钟K线数据列表
        """
        if not kline_data:
            return

        filename = self._generate_filename(f"his_kline_1min_{ts_code}", include_seconds=True)
        subdir = 'his_kline_1min'
        dirpath = os.path.join(self.csv_path, subdir)

        os.makedirs(dirpath, exist_ok=True)
        filepath = os.path.join(dirpath, filename)

        self._write_csv_file(filepath, kline_data, data_type='his_kline_1min')

    def write_his_kline_1min_raw(self, ts_code: str, raw_data: List[Dict[str, Any]]) -> None:
        """
        写入1分钟K线原始数据到CSV - ts_code + 原始字段

        Args:
            ts_code: 股票ts_code
            raw_data: 原始数据列表（直接来自tdx-api）
        """
        if not raw_data:
            return

        filename = self._generate_filename(f"his_kline_1min_{ts_code}", include_seconds=True)
        subdir = 'his_kline_1min'
        dirpath = os.path.join(self.csv_path, subdir)

        os.makedirs(dirpath, exist_ok=True)
        filepath = os.path.join(dirpath, filename)

        self._write_csv_file(filepath, raw_data, data_type='his_kline_1min')

    def write_his_kline_day_raw(self, ts_code: str, raw_data: List[Dict[str, Any]]) -> None:
        """
        写入日K线原始数据到CSV - ts_code + 原始字段

        Args:
            ts_code: 股票ts_code
            raw_data: 原始数据列表（直接来自tdx-api）
        """
        if not raw_data:
            return

        filename = self._generate_filename(f"his_kline_day_{ts_code}", include_seconds=True)
        subdir = 'his_kline_day'
        dirpath = os.path.join(self.csv_path, subdir)

        os.makedirs(dirpath, exist_ok=True)
        filepath = os.path.join(dirpath, filename)

        self._write_csv_file(filepath, raw_data, data_type='his_kline_day')
