"""
数据同步管理器
"""

import time
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
from ..config import ConfigManager
from ..data_sources import PytdxSource, BaostockSource
from ..database import DatabaseConnection, Stock, DailyData
from .csv_writer import CsvWriter
from .fundamentals_manager import FundamentalsManager


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
        """同步股票列表 - 严格按照要求使用纯baostock方案"""
        all_stocks = []

        # 从Baostock获取股票列表
        if self.baostock_source:
            if self.baostock_source.connect():
                stocks = self.baostock_source.get_stock_list()
                all_stocks.extend(stocks)
                self.baostock_source.disconnect()

        # 去重 - 修复字段引用错误，使用更严格的去重逻辑
        unique_stocks = {}
        for stock in all_stocks:
            code = stock.get('stock_code')
            ts_code = stock.get('ts_code')
            # 优先使用ts_code作为唯一标识，stock_code作为备选
            unique_key = ts_code if ts_code else code
            if unique_key and unique_key not in unique_stocks:
                unique_stocks[unique_key] = stock
            else:
                print(f"⚠️  跳过重复股票: {unique_key}")

        stocks_list = list(unique_stocks.values())

        # 保存数据
        if save_to_csv:
            self.csv_writer.write_stocks(stocks_list)

        if save_to_db:
            self._save_stocks_to_db(stocks_list)

        print(f"同步股票列表完成，共 {len(stocks_list)} 只股票")
        return stocks_list

    def sync_daily_data(
        self,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        start_date: date = None,
        end_date: date = None,
        codes: List[str] = None
    ) -> int:
        """
        同步日K线数据 - 严格按照功能要求直接加载通达信日K线数据

        实现逻辑：
        1. 加载{通达信数据根目录}/vipdoc/{market}/lday/*.day文件，文件名是股票的不包含"."的ts编码
        2. 过滤出时间范围内的日k线数据
        3. 数据组装成表格结构，换手率设为NULL
        4. 基于组装后的数据生成csv文件，每个交易日生成一个csv文件
        5. 将组装后的数据写入数据表his_kline_day，使用ts_code+trade_date判断insert/update

        Args:
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
            start_date: 开始日期，默认为2020-01-01
            end_date: 结束日期，默认为今天
            codes: 股票代码列表，为None则处理所有.day文件

        Returns:
            同步的数据条数
        """
        # 严格按照要求：日期范围从2020-01-01开始
        if not start_date:
            start_date = date(2020, 1, 1)
        if not end_date:
            end_date = date.today()

        print(f"开始同步日K线数据: {start_date} 至 {end_date}")

        # 1. 批量扫描所有.day文件，获取股票列表和日K线数据
        all_daily_data = []
        processed_files = 0

        try:
            if self.pytdx_source and self.pytdx_source.connect():
                # 批量扫描所有市场的.day文件
                all_daily_data = self._scan_all_day_files(start_date, end_date, codes)
                processed_files = len(set(data['ts_code'] for data in all_daily_data))
                print(f"成功扫描 {processed_files} 只股票的日K线数据，共 {len(all_daily_data)} 条记录")
            else:
                print("❌ 无法连接到Pytdx数据源")
                return 0

        except Exception as e:
            print(f"❌ 扫描.day文件失败: {e}")
            return 0

        if not all_daily_data:
            print("⚠️  未找到符合条件的日K线数据")
            return 0

        # 3. 数据组装 - 基本面数据已删除，换手率设为NULL
        enriched_data = self._assemble_daily_data_with_fundamentals(all_daily_data)
        print(f"数据组装完成，共 {len(enriched_data)} 条记录")

        # 5. 数据持久化
        total_count = 0
        if enriched_data:
            batch_size = self.config_manager.get('sync.batch_size', 5000)

            for i in range(0, len(enriched_data), batch_size):
                batch = enriched_data[i:i+batch_size]

                try:
                    if save_to_csv:
                        self.csv_writer.write_his_kline_day(batch)
                    if save_to_db:
                        self._save_daily_data_to_db(batch)

                    total_count += len(batch)
                    print(f"已处理批次 {i//batch_size + 1}: {len(batch)} 条记录")

                except Exception as e:
                    print(f"❌ 处理批次数据失败: {e}")

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

    
    def _scan_all_day_files(self, start_date: date, end_date: date, codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        批量扫描所有.day文件，获取日K线数据 - 使用通用扫描方法

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定的股票代码列表，为None则处理所有

        Returns:
            日K线数据列表
        """
        return self._scan_all_files(start_date, end_date, codes, 'day')

    
    def _assemble_daily_data_with_fundamentals(self, daily_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        数据组装 - 基本面数据已删除，换手率设为NULL

        Args:
            daily_data: 日K线数据列表

        Returns:
            组装后的日K线数据列表
        """
        enriched_data = []

        for record in daily_data:
            enriched_record = record.copy()
            # 基本面数据已删除，换手率设为NULL
            enriched_record['turnover_rate'] = None
            enriched_data.append(enriched_record)

        # 修复：基于ts_code和trade_date去重，保留最后一条记录
        import pandas as pd
        df = pd.DataFrame(enriched_data)
        if not df.empty:
            df_deduped = df.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
            enriched_data = df_deduped.to_dict('records')

        return enriched_data

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

    def _scan_all_files(self, start_date: date, end_date: date,
                       codes: List[str] = None,
                       file_type: str = 'day') -> List[Dict[str, Any]]:
        """
        通用文件扫描方法 - 支持日K线和5分钟K线文件

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定的股票代码列表
            file_type: 文件类型 ('day' 或 '5min')

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
            record_size = 28
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
                print(f"📁 扫描 {market} 市场: 找到 {len(files)} 个{ext}文件")

                for filename in files:
                    if not filename.startswith(market):
                        continue

                    stock_code = filename[2:-len(ext)]  # 去掉market前缀和扩展名

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
                                    record = {
                                        'ts_code': ts_code,
                                        'stock_code': stock_code,
                                        'stock_name': None,  # 后续从数据库查询
                                        'trade_date': trade_date,
                                        'trade_time': parsed_data['trade_time'],
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
                             codes: List[str] = None) -> List[Dict[str, Any]]:
        """
        批量扫描所有.lc5文件，获取5分钟K线数据 - 使用通用扫描方法

        Args:
            start_date: 开始日期
            end_date: 结束日期
            codes: 指定的股票代码列表

        Returns:
            5分钟K线数据列表
        """
        return self._scan_all_files(start_date, end_date, codes, '5min')

    def _post_process_5min_data(self, min5_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """后处理5分钟K线数据 - 计算涨跌幅（基于昨日收盘价）"""
        if not min5_data:
            return min5_data

        # 按股票和日期时间排序
        min5_data.sort(key=lambda x: (x['ts_code'], x['trade_date'], x['trade_time']))

        # 获取所有股票的昨日收盘价映射
        stock_codes = list(set(data['ts_code'] for data in min5_data))
        preclose_map = self._get_yesterday_preclose_map(stock_codes)

        # 按股票分组计算涨跌幅
        for data in min5_data:
            ts_code = data['ts_code']
            trade_date = data['trade_date']

            # 获取昨日收盘价
            yesterday_preclose = preclose_map.get(ts_code, {}).get(trade_date)

            if yesterday_preclose and data['close']:
                try:
                    from ..utils.data_transformer import DataTransformer
                    data['change_rate'] = DataTransformer.calculate_change_rate(
                        data['close'], yesterday_preclose
                    )
                    data['preclose'] = yesterday_preclose
                except Exception as e:
                    print(f"⚠️  计算涨跌幅失败 {ts_code}: {e}")
                    data['change_rate'] = None
                    data['preclose'] = yesterday_preclose
            else:
                data['change_rate'] = None
                data['preclose'] = yesterday_preclose

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
        """数据组装 - 5分钟版本，基本面数据已删除，换手率设为NULL"""
        enriched_data = []

        for record in min5_data:
            enriched_record = record.copy()
            # 基本面数据已删除，换手率设为NULL
            enriched_record['turnover_rate'] = None
            enriched_data.append(enriched_record)

        return enriched_data

    
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
        start_date: date = None,
        end_date: date = None,
        codes: List[str] = None
    ) -> int:
        """
        同步1分钟K线数据 - 严格按照产品设计文档要求

        Args:
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
            start_date: 开始日期，默认为7天前
            end_date: 结束日期，默认为今天
            codes: 股票代码列表，为None则同步所有股票

        Returns:
            同步的数据条数
        """
        if not start_date:
            start_date = date.today() - timedelta(days=7)
        if not end_date:
            end_date = date.today()

        # 获取股票列表
        if not codes:
            stocks = self.sync_stocks(False, False)
            codes = [stock['stock_code'] for stock in stocks]

        total_count = 0
        batch_size = self.config_manager.get('sync.batch_size', 1000)  # 1分钟数据量更大
        all_min1_data = []

        for i, code in enumerate(codes):
            try:
                # 使用Pytdx获取1分钟K线数据
                data = None
                if self.pytdx_source:
                    if self.pytdx_source.connect():
                        data = self.pytdx_source.get_minute_data(code, '1min', start_date, end_date)
                        self.pytdx_source.disconnect()

                if data:
                    # 提取股票ts_code集合，查询base_fundamentals_info表
                    # 从第一条记录获取ts_code
                    ts_code = data[0].get('ts_code') if data else None
                    if ts_code:
                        enriched_data = self._enrich_minute_data_with_fundamentals(data, ts_code)
                        all_min1_data.extend(enriched_data)

                    # 批量处理
                    if len(all_min1_data) >= batch_size or i == len(codes) - 1:
                        if save_to_csv:
                            self.csv_writer.write_his_kline_1min(all_min1_data)
                        if save_to_db:
                            self._save_1min_data_to_db(all_min1_data)
                        total_count += len(all_min1_data)
                        print(f"已处理 {i+1}/{len(codes)} 只股票，同步1分钟数据 {total_count} 条")
                        all_min1_data = []

            except Exception as e:
                print(f"同步股票 {code} 的1分钟K线数据失败: {e}")

        print(f"同步1分钟K线数据完成，共 {total_count} 条数据")
        return total_count

    def _enrich_minute_data_with_fundamentals(self, minute_data: List[Dict[str, Any]], ts_code: str) -> List[Dict[str, Any]]:
        """
        为分钟K线数据添加基础信息 - 基本面数据已删除，换手率设为NULL

        Args:
            minute_data: 分钟K线数据列表
            ts_code: ts代码

        Returns:
            丰富后的分钟K线数据列表
        """
        try:
            # 从base_stock_info表获取stock_name
            stock_query = """
            SELECT ts_code, stock_code, stock_name
            FROM base_stock_info
            WHERE ts_code = %s
            """
            stock_info = self.db_conn.fetch_one(stock_query, (ts_code,))

            # 丰富每条分钟K线数据
            enriched_data = []
            for record in minute_data:
                enriched_record = record.copy()
                if stock_info:
                    enriched_record['stock_name'] = stock_info['stock_name']
                else:
                    enriched_record['stock_name'] = None

                # 基本面数据已删除，换手率设为NULL
                enriched_record['turnover_rate'] = None
                enriched_data.append(enriched_record)

            return enriched_data

        except Exception as e:
            print(f"丰富分钟K线数据基础信息失败: {e}")
            return minute_data

    def _save_1min_data_to_db(self, min1_data: List[Dict[str, Any]]) -> None:
        """保存1分钟K线数据到数据库 - 严格按照文档要求使用ts_code+trade_date+trade_time判断"""
        batch_size = self.config_manager.get('sync.batch_size', 2000)
        for i in range(0, len(min1_data), batch_size):
            batch = min1_data[i:i+batch_size]
            values = []
            for data in batch:
                values.append((
                    data.get('ts_code'),
                    data.get('stock_code'),
                    data.get('stock_name'),
                    data.get('trade_date'),
                    data.get('trade_time'),
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('preclose'),
                    data.get('volume'),
                    data.get('amount'),
                    data.get('adjust_flag', 3),
                    data.get('change_rate'),
                    data.get('turnover_rate')
                ))

            query = """
                INSERT INTO his_kline_1min (
                    ts_code, stock_code, stock_name, trade_date, trade_time,
                    open, high, low, close, preclose, volume, amount,
                    adjust_flag, change_rate, turnover_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
                    stock_code = EXCLUDED.stock_code,
                    stock_name = EXCLUDED.stock_name,
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
                    update_time = CURRENT_TIMESTAMP
            """
            self.db_conn.execute_batch(query, values)

    def sync_5min_data(
        self,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        start_date: date = None,
        end_date: date = None,
        codes: List[str] = None
    ) -> int:
        """
        同步5分钟K线数据 - 实用平衡版本，完全无Baostock依赖

        保持现有接口不变，内部实现采用混合模式：
        - 如果未指定codes，使用批量扫描模式（高性能）
        - 如果指定codes，使用逐股票处理模式（兼容性）
        - 完全无Baostock依赖
        - 支持随机选择10支股票进行测试

        Args:
            save_to_csv: 是否保存到CSV文件
            save_to_db: 是否保存到数据库
            start_date: 开始日期，默认为2025-12-01
            end_date: 结束日期，默认为今天
            codes: 股票代码列表，为None则处理所有.lc5文件

        Returns:
            同步的数据条数
        """
        # 使用配置化的默认日期范围
        if not start_date:
            default_start_str = self.config_manager.get('sync.5min_data.default_start_date', '2025-12-01')
            try:
                start_date = datetime.strptime(default_start_str, '%Y-%m-%d').date()
            except ValueError:
                start_date = date(2025, 12, 1)
        if not end_date:
            end_date = date.today()

        print(f"开始同步5分钟K线数据: {start_date} 至 {end_date}")

        # 智能选择处理模式
        if not codes:
            # 批量扫描模式：高性能处理所有数据
            return self._sync_5min_batch_mode(save_to_csv, save_to_db, start_date, end_date)
        else:
            # 兼容模式：处理指定股票
            return self._sync_5min_compatibility_mode(save_to_csv, save_to_db,
                                                     start_date, end_date, codes)

    def _sync_5min_batch_mode(self, save_to_csv: bool, save_to_db: bool,
                             start_date: date, end_date: date) -> int:
        """批量扫描模式 - 参考日K线架构"""
        print("🚀 使用批量扫描模式")

        # 1. 批量扫描所有.lc5文件
        all_5min_data = []
        try:
            if self.pytdx_source and self.pytdx_source.connect():
                all_5min_data = self._scan_all_5min_files(start_date, end_date)
                self.pytdx_source.disconnect()
        except Exception as e:
            print(f"❌ 批量扫描失败: {e}")
            return 0

        if not all_5min_data:
            print("⚠️  未找到5分钟K线数据")
            return 0

        # 2. 批量处理数据（处理所有找到的数据）
        print(f"✅ 找到 {len(set(data['ts_code'] for data in all_5min_data))} 支股票的5分钟K线数据")
        return self._process_5min_data_batch(all_5min_data, save_to_csv, save_to_db)

    def _sync_5min_compatibility_mode(self, save_to_csv: bool, save_to_db: bool,
                                     start_date: date, end_date: date,
                                     codes: List[str]) -> int:
        """兼容模式 - 处理指定股票"""
        print(f"🔧 使用兼容模式处理 {len(codes)} 支指定股票")

        # 使用现有的逐股票处理逻辑，但股票获取不依赖Baostock
        total_count = 0
        batch_size = self.config_manager.get('sync.batch_size', 10000)
        all_5min_data = []

        for i, code in enumerate(codes):
            try:
                # 使用Pytdx获取5分钟K线数据
                data = None
                if self.pytdx_source and self.pytdx_source.connect():
                    data = self.pytdx_source.get_minute_data(code, '5min', start_date, end_date)
                    self.pytdx_source.disconnect()

                if data:
                    # 批量积累数据
                    all_5min_data.extend(data)

                    # 批量处理
                    if len(all_5min_data) >= batch_size or i == len(codes) - 1:
                        processed_count = self._process_5min_data_batch(
                            all_5min_data, save_to_csv, save_to_db
                        )
                        total_count += processed_count
                        all_5min_data = []
                        print(f"已处理 {i+1}/{len(codes)} 只股票，同步数据 {total_count} 条")

            except Exception as e:
                print(f"同步股票 {code} 的5分钟K线数据失败: {e}")

        return total_count

    def _process_5min_data_batch(self, min5_data: List[Dict[str, Any]],
                               save_to_csv: bool, save_to_db: bool) -> int:
        """批量处理5分钟数据 - 统一的数据处理逻辑"""
        if not min5_data:
            return 0

        # 2. 数据组装 - 基本面数据已删除，换手率设为NULL
        enriched_data = self._assemble_5min_data_with_fundamentals(min5_data)
        enriched_data = self._post_process_5min_data(enriched_data)

        # 4. 数据持久化
        try:
            if save_to_csv:
                self.csv_writer.write_his_kline_5min(enriched_data)
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
                # 增加字段兼容性处理，支持不同的字段名
                values.append((
                    data.get('ts_code'),
                    data.get('stock_code') or data.get('code'),
                    data.get('stock_name') or data.get('name'),
                    data.get('trade_date'),
                    data.get('trade_time'),
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('preclose'),
                    data.get('volume'),
                    data.get('amount'),
                    data.get('adjust_flag', 3),  # 默认不复权
                    data.get('change_rate'),
                    data.get('turnover_rate')
                ))

            # 严格按照文档要求使用ts_code+trade_date+trade_time作为冲突键
            query = """
                INSERT INTO his_kline_5min (
                    ts_code, stock_code, stock_name, trade_date, trade_time,
                    open, high, low, close, preclose, volume, amount, adjust_flag,
                    change_rate, turnover_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
                    stock_code = EXCLUDED.stock_code,
                    stock_name = EXCLUDED.stock_name,
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
                    stock.get('list_date'),
                    stock.get('delist_date')
                ))

            query = """
                INSERT INTO base_stock_info (
                    ts_code, stock_code, stock_name, cnspell, market_code, market_name,
                    exchange_code, sector_code, sector_name, industry_code, industry_name,
                    list_status, list_date, delist_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    update_time = CURRENT_TIMESTAMP
            """
            self.db_conn.execute_batch(query, values)

    def _save_daily_data_to_db(self, daily_data: List[Dict[str, Any]]) -> None:
        """保存日K线数据到数据库 - 修复表名和字段映射"""
        batch_size = self.config_manager.get('sync.batch_size', 5000)  # 优化批次大小
        for i in range(0, len(daily_data), batch_size):
            batch = daily_data[i:i+batch_size]
            values = []
            for data in batch:
                # 直接从字典获取数据，避免模型转换问题
                value_tuple = (
                    data.get('ts_code'),
                    data.get('stock_code') or data.get('code'),
                    data.get('stock_name') or data.get('name'),
                    data.get('trade_date') or data.get('date'),
                    data.get('open'),
                    data.get('high'),
                    data.get('low'),
                    data.get('close'),
                    data.get('preclose'),
                    data.get('volume'),
                    data.get('amount'),
                    data.get('trade_status', 1),
                    data.get('is_st', False),
                    data.get('adjust_flag', 3),
                    data.get('change_rate') or data.get('pct_chg'),
                    data.get('turnover_rate') or data.get('turn')
                )

                # 调试：检查数值字段是否超出数据库精度限制
                change_rate = value_tuple[14]
                turnover_rate = value_tuple[15]

                if change_rate is not None and abs(change_rate) >= 10000:
                    print(f"⚠️  change_rate 超出精度限制: {change_rate}")
                if turnover_rate is not None and abs(turnover_rate) >= 10000:
                    print(f"⚠️  turnover_rate 超出精度限制: {turnover_rate}")

                values.append(value_tuple)

            query = """
                INSERT INTO his_kline_day (
                    ts_code, stock_code, stock_name, trade_date, open, high, low, close,
                    preclose, volume, amount, trade_status, is_st, adjust_flag,
                    change_rate, turnover_rate
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    adjust_flag = EXCLUDED.adjust_flag,
                    change_rate = EXCLUDED.change_rate,
                    turnover_rate = EXCLUDED.turnover_rate,
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
