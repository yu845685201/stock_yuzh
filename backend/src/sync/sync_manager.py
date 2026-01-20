"""
数据同步管理器
"""

import time
import logging
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
from ..config import ConfigManager
from ..data_sources import BaostockSource
from ..database import DatabaseConnection, Stock
from .csv_writer import CsvWriter
from .fundamentals_manager import FundamentalsManager
from ..utils.log_aggregator import LogAggregator

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

        # 初始化数据源
        self.baostock_source = None

        self._init_data_sources()

    def _init_data_sources(self) -> None:
        """初始化数据源"""
        # 初始化Baostock数据源
        if self.config_manager.get('data_sources.baostock.enabled', True):
            baostock_config = {
                'data_path': self.config_manager.get_data_paths().get('csv')
            }
            self.baostock_source = BaostockSource(baostock_config)

        # 初始化基本面数据管理器
        self.fundamentals_manager = FundamentalsManager(self.config_manager)

    def sync_all(self, save_to_csv: bool = True, save_to_db: bool = True) -> Dict[str, Any]:
        """
        同步所有数据

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
            'errors': []
        }

        try:
            # 1. 同步股票列表
            stocks = self.sync_stocks(save_to_csv, save_to_db)
            result['stocks_count'] = len(stocks)

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
