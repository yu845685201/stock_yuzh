"""
数据库连接管理 - 支持连接池优化
"""

import psycopg2
import psycopg2.extras
import psycopg2.pool
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from datetime import datetime
from ..config import ConfigManager
import logging
import threading

class DatabaseConnectionPool:
    """数据库连接池管理类 - 提供高性能的连接复用"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, config_manager: ConfigManager = None):
        """单例模式确保全局只有一个连接池"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config_manager: ConfigManager = None):
        """
        初始化连接池

        Args:
            config_manager: 配置管理器
        """
        if hasattr(self, '_initialized'):
            return

        self.config_manager = config_manager or ConfigManager()
        self.db_config = self.config_manager.get_database_config()
        self.logger = logging.getLogger(__name__)

        # 连接池配置
        pool_config = self.config_manager.get('database.pool', {})
        self.min_conn = pool_config.get('min_connections', 2)
        self.max_conn = pool_config.get('max_connections', 10)

        # 创建连接池
        self._create_pool()
        self._initialized = True

        self.logger.info(f"数据库连接池已初始化: min={self.min_conn}, max={self.max_conn}")

    def _create_pool(self):
        """创建连接池"""
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.min_conn,
                maxconn=self.max_conn,
                **self.db_config
            )
            self.logger.info("连接池创建成功")
        except Exception as e:
            self.logger.error(f"连接池创建失败: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """从连接池获取连接的上下文管理器"""
        conn = None
        try:
            conn = self.pool.getconn()
            conn.autocommit = False
            yield conn
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"数据库操作失败: {e}")
            raise e
        finally:
            if conn:
                self.pool.putconn(conn)

    def get_pool_status(self) -> Dict[str, Any]:
        """获取连接池状态信息"""
        try:
            return {
                'min_connections': self.min_conn,
                'max_connections': self.max_conn,
                'current_connections': getattr(self.pool, '_used', 0) if hasattr(self.pool, '_used') else 'unknown'
            }
        except Exception as e:
            self.logger.error(f"获取连接池状态失败: {e}")
            return {'error': str(e)}

    def close_pool(self):
        """关闭连接池"""
        try:
            if hasattr(self, 'pool'):
                self.pool.closeall()
                self.logger.info("连接池已关闭")
        except Exception as e:
            self.logger.error(f"关闭连接池失败: {e}")

class DatabaseConnection:
    """数据库连接管理类 - 向后兼容，支持连接池优化"""

    def __init__(self, config_manager: ConfigManager = None, use_pool: bool = True):
        """
        初始化数据库连接

        Args:
            config_manager: 配置管理器
            use_pool: 是否使用连接池，默认True
        """
        self.config_manager = config_manager or ConfigManager()
        self.db_config = self.config_manager.get_database_config()
        self.use_pool = use_pool and self.config_manager.get('database.pool.enabled', True)

        if self.use_pool:
            self.pool_manager = DatabaseConnectionPool(config_manager)
            self.logger = logging.getLogger(__name__)
            self.logger.info("数据库连接已配置为使用连接池模式")
        else:
            self.logger = logging.getLogger(__name__)
            self.logger.info("数据库连接已配置为使用传统模式")

    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器 - 支持连接池和传统模式"""
        if self.use_pool:
            # 使用连接池模式
            with self.pool_manager.get_connection() as conn:
                yield conn
        else:
            # 使用传统模式
            conn = None
            try:
                conn = psycopg2.connect(**self.db_config)
                conn.autocommit = False
                yield conn
            except psycopg2.Error as e:
                if conn:
                    conn.rollback()
                raise e
            finally:
                if conn:
                    conn.close()

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        执行查询语句

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果列表
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        执行查询并返回所有结果 - 兼容方法

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果列表
        """
        return self.execute_query(query, params)

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        """
        执行查询并返回第一条结果

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果字典或None
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return dict(result) if result else None

    def execute_update(self, query: str, params: tuple = None) -> int:
        """
        执行更新语句

        Args:
            query: SQL更新语句
            params: 更新参数

        Returns:
            影响的行数
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount

    def execute_batch(self, query: str, params_list: List[tuple]) -> int:
        """
        批量执行语句

        Args:
            query: SQL语句
            params_list: 参数列表

        Returns:
            影响的行数
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_batch(cursor, query, params_list)
                conn.commit()
                return cursor.rowcount

    def initialize_tables(self) -> None:
        """初始化数据库表 - 严格按照文档要求"""
        create_tables_sql = """
        -- 股票基本信息表
        CREATE TABLE IF NOT EXISTS base_stock_info (
            id BIGSERIAL PRIMARY KEY,
            ts_code VARCHAR(20),
            stock_code VARCHAR(20),
            stock_name VARCHAR(50),
            cnspell VARCHAR(10),
            market_code VARCHAR(5),
            market_name VARCHAR(20),
            exchange_code VARCHAR(10),
            sector_code VARCHAR(20),
            sector_name VARCHAR(20),
            industry_code VARCHAR(20),
            industry_name VARCHAR(20),
            list_status VARCHAR(2),
            list_date DATE,
            delist_date DATE,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        

        -- 基本面信息数据表
        CREATE TABLE IF NOT EXISTS base_fundamentals_info (
            id BIGSERIAL PRIMARY KEY,
            ts_code VARCHAR(20),
            stock_code VARCHAR(20),
            stock_name VARCHAR(50),
            disclosure_date TIMESTAMP,
            total_share NUMERIC(20, 4),
            float_share NUMERIC(20, 4),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 交易日历表
        CREATE TABLE IF NOT EXISTS base_trade_calendar (
            id BIGSERIAL PRIMARY KEY,
            calendar_date VARCHAR(10),
            is_trading_day SMALLINT
        );

        -- 历史1分钟K线数据表
        CREATE TABLE IF NOT EXISTS his_kline_1min (
            id BIGSERIAL PRIMARY KEY,
            ts_code VARCHAR(20),
            stock_code VARCHAR(20),
            stock_name VARCHAR(50),
            trade_date VARCHAR(8),
            trade_time VARCHAR(4),
            trade_datetime VARCHAR(12),
            open NUMERIC(30, 8),
            high NUMERIC(30, 8),
            low NUMERIC(30, 8),
            close NUMERIC(30, 8),
            preclose NUMERIC(30, 8),
            volume NUMERIC(30, 8),
            amount NUMERIC(30, 8),
            change_rate NUMERIC(30, 8),
            turnover_rate NUMERIC(30, 8),
            fundamentals_disclosure_date VARCHAR(8),
            total_share NUMERIC(30, 8),
            float_share NUMERIC(30, 8),
            source VARCHAR(20),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 历史日K线数据表
        CREATE TABLE IF NOT EXISTS his_kline_day (
            id BIGSERIAL PRIMARY KEY,
            ts_code VARCHAR(20),
            stock_code VARCHAR(20),
            stock_name VARCHAR(50),
            trade_date VARCHAR(8),
            open NUMERIC(30, 8),
            high NUMERIC(30, 8),
            low NUMERIC(30, 8),
            close NUMERIC(30, 8),
            preclose NUMERIC(30, 8),
            volume NUMERIC(30, 8),
            amount NUMERIC(30, 8),
            change_rate NUMERIC(30, 8),
            turnover_rate NUMERIC(30, 8),
            fundamentals_disclosure_date VARCHAR(8),
            total_share NUMERIC(30, 8),
            float_share NUMERIC(30, 8),
            source VARCHAR(20),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- 创建唯一约束 (PostgreSQL不支持IF NOT EXISTS，需要先检查约束是否存在)
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uk_base_stock_info_code' AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ) THEN
                ALTER TABLE base_stock_info ADD CONSTRAINT uk_base_stock_info_code UNIQUE (ts_code);
            END IF;

            
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uk_base_fundamentals_info_code_date' AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ) THEN
                -- 按照产品设计文档要求，使用ts_code+disclosure_date组合作为唯一约束
                ALTER TABLE base_fundamentals_info ADD CONSTRAINT uk_base_fundamentals_info_code_date UNIQUE (ts_code, disclosure_date);
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uk_base_trade_calendar_date' AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ) THEN
                ALTER TABLE base_trade_calendar ADD CONSTRAINT uk_base_trade_calendar_date UNIQUE (calendar_date);
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uk_his_kline_1min_code_date_time' AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ) THEN
                ALTER TABLE his_kline_1min ADD CONSTRAINT uk_his_kline_1min_code_date_time UNIQUE (ts_code, trade_date, trade_time);
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uk_his_kline_day_code_date' AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            ) THEN
                ALTER TABLE his_kline_day ADD CONSTRAINT uk_his_kline_day_code_date UNIQUE (ts_code, trade_date);
            END IF;
        END $$;

        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_base_stock_info_code ON base_stock_info(stock_code);
        CREATE INDEX IF NOT EXISTS idx_base_fundamentals_info_code ON base_fundamentals_info(ts_code);
        CREATE INDEX IF NOT EXISTS idx_base_trade_calendar_date ON base_trade_calendar(calendar_date);
        CREATE INDEX IF NOT EXISTS idx_his_kline_1min_ts_code ON his_kline_1min(ts_code);
        CREATE INDEX IF NOT EXISTS idx_his_kline_1min_trade_date ON his_kline_1min(trade_date);
        CREATE INDEX IF NOT EXISTS idx_his_kline_1min_trade_time ON his_kline_1min(trade_time);
        CREATE INDEX IF NOT EXISTS idx_his_kline_day_ts_code ON his_kline_day(ts_code);
        CREATE INDEX IF NOT EXISTS idx_his_kline_day_trade_date ON his_kline_day(trade_date);

        """

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_tables_sql)
                conn.commit()

    def upsert_fundamentals_data(self, fundamentals_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert基本面数据

        Args:
            fundamentals_data: 基本面数据列表

        Returns:
            影响的行数
        """
        if not fundamentals_data:
            return 0

        upsert_sql = """
        INSERT INTO base_fundamentals_info
        (ts_code, stock_code, stock_name, disclosure_date, total_share, float_share, create_time, update_time)
        VALUES (%(ts_code)s, %(stock_code)s, %(stock_name)s, %(disclosure_date)s, %(total_share)s, %(float_share)s,
                %(create_time)s, NOW())
        ON CONFLICT (ts_code, disclosure_date)
        DO UPDATE SET
            stock_code = EXCLUDED.stock_code,
            stock_name = EXCLUDED.stock_name,
            total_share = EXCLUDED.total_share,
            float_share = EXCLUDED.float_share,
            update_time = NOW()
        """

        params_list = []
        for item in fundamentals_data:
            params = {
                'ts_code': item['ts_code'],
                'stock_code': item['stock_code'],
                'stock_name': item['stock_name'],
                'disclosure_date': item['disclosure_date'],
                'total_share': item['total_share'],
                'float_share': item['float_share'],
                'create_time': item.get('create_time', datetime.now())  # 确保始终有值
            }
            params_list.append(params)

        return self.execute_batch(upsert_sql, params_list)

    def upsert_trade_calendar(self, calendar_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert交易日历数据

        Args:
            calendar_data: 交易日历数据列表

        Returns:
            影响的行数
        """
        if not calendar_data:
            return 0

        upsert_sql = """
        INSERT INTO base_trade_calendar
        (calendar_date, is_trading_day)
        VALUES (%(calendar_date)s, %(is_trading_day)s)
        ON CONFLICT (calendar_date)
        DO UPDATE SET
            is_trading_day = EXCLUDED.is_trading_day
        """

        params_list = []
        for item in calendar_data:
            params_list.append({
                'calendar_date': item['calendar_date'],
                'is_trading_day': item['is_trading_day']
            })

        return self.execute_batch(upsert_sql, params_list)

    def upsert_his_kline_1min(self, kline_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert 1分钟K线数据

        Args:
            kline_data: 1分钟K线数据列表

        Returns:
            影响的行数
        """
        if not kline_data:
            return 0

        upsert_sql = """
        INSERT INTO his_kline_1min
        (ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
         open, high, low, close, preclose, volume, amount, change_rate, turnover_rate,
         fundamentals_disclosure_date, total_share, float_share, source, create_time, update_time)
        VALUES (
            %(ts_code)s, %(stock_code)s, %(stock_name)s, %(trade_date)s, %(trade_time)s, %(trade_datetime)s,
            %(open)s, %(high)s, %(low)s, %(close)s, %(preclose)s, %(volume)s, %(amount)s, %(change_rate)s, %(turnover_rate)s,
            %(fundamentals_disclosure_date)s, %(total_share)s, %(float_share)s, %(source)s, %(create_time)s, NOW()
        )
        ON CONFLICT (ts_code, trade_date, trade_time)
        DO UPDATE SET
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
            change_rate = EXCLUDED.change_rate,
            turnover_rate = EXCLUDED.turnover_rate,
            fundamentals_disclosure_date = EXCLUDED.fundamentals_disclosure_date,
            total_share = EXCLUDED.total_share,
            float_share = EXCLUDED.float_share,
            source = EXCLUDED.source,
            update_time = NOW()
        """

        params_list = []
        for item in kline_data:
            params_list.append({
                'ts_code': item['ts_code'],
                'stock_code': item['stock_code'],
                'stock_name': item['stock_name'],
                'trade_date': item['trade_date'],
                'trade_time': item['trade_time'],
                'trade_datetime': item['trade_datetime'],
                'open': item.get('open'),
                'high': item.get('high'),
                'low': item.get('low'),
                'close': item.get('close'),
                'preclose': item.get('preclose'),
                'volume': item.get('volume'),
                'amount': item.get('amount'),
                'change_rate': item.get('change_rate'),
                'turnover_rate': item.get('turnover_rate'),
                'fundamentals_disclosure_date': item.get('fundamentals_disclosure_date'),
                'total_share': item.get('total_share'),
                'float_share': item.get('float_share'),
                'source': item.get('source'),
                'create_time': item.get('create_time', datetime.now())
            })

        return self.execute_batch(upsert_sql, params_list)

    def upsert_his_kline_day(self, kline_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert 日K线数据

        Args:
            kline_data: 日K线数据列表

        Returns:
            影响的行数
        """
        if not kline_data:
            return 0

        upsert_sql = """
        INSERT INTO his_kline_day
        (ts_code, stock_code, stock_name, trade_date,
         open, high, low, close, preclose, volume, amount, change_rate, turnover_rate,
         fundamentals_disclosure_date, total_share, float_share, source, create_time, update_time)
        VALUES (
            %(ts_code)s, %(stock_code)s, %(stock_name)s, %(trade_date)s,
            %(open)s, %(high)s, %(low)s, %(close)s, %(preclose)s, %(volume)s, %(amount)s, %(change_rate)s, %(turnover_rate)s,
            %(fundamentals_disclosure_date)s, %(total_share)s, %(float_share)s, %(source)s, %(create_time)s, NOW()
        )
        ON CONFLICT (ts_code, trade_date)
        DO UPDATE SET
            stock_code = EXCLUDED.stock_code,
            stock_name = EXCLUDED.stock_name,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            preclose = EXCLUDED.preclose,
            volume = EXCLUDED.volume,
            amount = EXCLUDED.amount,
            change_rate = EXCLUDED.change_rate,
            turnover_rate = EXCLUDED.turnover_rate,
            fundamentals_disclosure_date = EXCLUDED.fundamentals_disclosure_date,
            total_share = EXCLUDED.total_share,
            float_share = EXCLUDED.float_share,
            source = EXCLUDED.source,
            update_time = NOW()
        """

        params_list = []
        for item in kline_data:
            params_list.append({
                'ts_code': item['ts_code'],
                'stock_code': item['stock_code'],
                'stock_name': item['stock_name'],
                'trade_date': item['trade_date'],
                'open': item.get('open'),
                'high': item.get('high'),
                'low': item.get('low'),
                'close': item.get('close'),
                'preclose': item.get('preclose'),
                'volume': item.get('volume'),
                'amount': item.get('amount'),
                'change_rate': item.get('change_rate'),
                'turnover_rate': item.get('turnover_rate'),
                'fundamentals_disclosure_date': item.get('fundamentals_disclosure_date'),
                'total_share': item.get('total_share'),
                'float_share': item.get('float_share'),
                'source': item.get('source'),
                'create_time': item.get('create_time', datetime.now())
            })

        return self.execute_batch(upsert_sql, params_list)

    def fetch_trade_calendar(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取交易日历区间数据

        Args:
            start_date: yyyy-mm-dd
            end_date: yyyy-mm-dd
        """
        query = """
        SELECT calendar_date
        FROM base_trade_calendar
        WHERE calendar_date >= %s AND calendar_date <= %s AND is_trading_day = 1
        ORDER BY calendar_date ASC
        """
        return self.execute_query(query, (start_date, end_date))

    def fetch_stock_basic(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        获取股票基本信息（优先按type=1过滤）
        """
        params: List[Any] = []
        base_query = "SELECT ts_code, stock_code, stock_name FROM base_stock_info"
        where_clause = []

        if ts_codes:
            where_clause.append("ts_code = ANY(%s)")
            params.append(ts_codes)

        # 尝试按type=1过滤，如果字段不存在则回退
        try:
            query = base_query
            if where_clause:
                query += " WHERE " + " AND ".join(where_clause) + " AND type = '1'"
            else:
                query += " WHERE type = '1'"
            return self.execute_query(query, tuple(params))
        except Exception:
            query = base_query
            if where_clause:
                query += " WHERE " + " AND ".join(where_clause)
            return self.execute_query(query, tuple(params))

    def fetch_fundamentals_all(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        获取所有基本面数据
        """
        params: List[Any] = []
        query = """
        SELECT ts_code, disclosure_date, total_share, float_share
        FROM base_fundamentals_info
        """
        if ts_codes:
            query += " WHERE ts_code = ANY(%s)"
            params.append(ts_codes)
        return self.execute_query(query, tuple(params))

    def fetch_fundamentals_latest(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        获取每只股票最新基本面数据
        """
        params: List[Any] = []
        query = """
        SELECT t.ts_code, t.disclosure_date, t.total_share, t.float_share
        FROM base_fundamentals_info t
        INNER JOIN (
            SELECT ts_code, MAX(disclosure_date) AS disclosure_date
            FROM base_fundamentals_info
            GROUP BY ts_code
        ) m ON t.ts_code = m.ts_code AND t.disclosure_date = m.disclosure_date
        """
        if ts_codes:
            query += " WHERE t.ts_code = ANY(%s)"
            params.append(ts_codes)
        return self.execute_query(query, tuple(params))

    def fetch_fundamentals_in_range(self, ts_codes: Optional[List[str]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        获取指定日期范围内基本面数据
        """
        params: List[Any] = [start_date, end_date]
        query = """
        SELECT ts_code, disclosure_date, total_share, float_share
        FROM base_fundamentals_info
        WHERE disclosure_date >= %s AND disclosure_date <= %s
        """
        if ts_codes:
            query += " AND ts_code = ANY(%s)"
            params.append(ts_codes)
        return self.execute_query(query, tuple(params))

    def fetch_fundamentals_up_to(self, ts_codes: Optional[List[str]], end_date: str) -> List[Dict[str, Any]]:
        """
        获取截止到指定日期的基本面数据（用于匹配交易日前最近披露）
        """
        params: List[Any] = [end_date]
        query = """
        SELECT ts_code, disclosure_date, total_share, float_share
        FROM base_fundamentals_info
        WHERE disclosure_date <= %s
        """
        if ts_codes:
            query += " AND ts_code = ANY(%s)"
            params.append(ts_codes)
        return self.execute_query(query, tuple(params))

    def fetch_fundamentals_range_with_prev(
        self,
        ts_codes: Optional[List[str]],
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        获取指定日期范围内的基本面数据，并补充每只股票在开始日期之前最近一条数据
        """
        params: List[Any] = [start_date, end_date]
        ts_filter = ""
        if ts_codes:
            ts_filter = " AND ts_code = ANY(%s)"
            params.append(ts_codes)

        query = f"""
        WITH in_range AS (
            SELECT ts_code, disclosure_date, total_share, float_share
            FROM base_fundamentals_info
            WHERE disclosure_date >= %s AND disclosure_date <= %s
            {ts_filter}
        ),
        prev_one AS (
            SELECT DISTINCT ON (ts_code)
                ts_code, disclosure_date, total_share, float_share
            FROM base_fundamentals_info
            WHERE disclosure_date < %s
            {ts_filter}
            ORDER BY ts_code, disclosure_date DESC
        )
        SELECT * FROM in_range
        UNION ALL
        SELECT * FROM prev_one
        """

        # params for prev_one: start_date + optional ts_codes
        params_prev: List[Any] = [start_date]
        if ts_codes:
            params_prev.append(ts_codes)

        return self.execute_query(query, tuple(params + params_prev))

    def fetch_last_his_kline_1min_preclose(self, ts_code: str) -> Optional[float]:
        """
        获取指定股票最后一条1分钟K线的preclose
        """
        query = """
        SELECT preclose
        FROM his_kline_1min
        WHERE ts_code = %s
        ORDER BY trade_date DESC, trade_time DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code,))
        if result:
            return result.get('preclose')
        return None

    def fetch_last_his_kline_1min_close(self, ts_code: str) -> Optional[float]:
        """
        获取指定股票最后一条1分钟K线的close
        """
        query = """
        SELECT close
        FROM his_kline_1min
        WHERE ts_code = %s
        ORDER BY trade_date DESC, trade_time DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code,))
        if result:
            return result.get('close')
        return None

    def fetch_prev_his_kline_1min_close(self, ts_code: str, trade_date: str, trade_time: str) -> Optional[float]:
        """
        获取指定股票在当前交易时间之前最近一条1分钟K线的close
        """
        query = """
        SELECT close
        FROM his_kline_1min
        WHERE ts_code = %s
          AND (trade_date < %s OR (trade_date = %s AND trade_time < %s))
        ORDER BY trade_date DESC, trade_time DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code, trade_date, trade_date, trade_time))
        if result:
            return result.get('close')
        return None

    def fetch_prev_his_kline_day_close(self, ts_code: str, trade_date: str) -> Optional[float]:
        """
        获取指定股票在当前交易日之前最近一条日K线的close
        """
        query = """
        SELECT close
        FROM his_kline_day
        WHERE ts_code = %s
          AND trade_date < %s
        ORDER BY trade_date DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code, trade_date))
        if result:
            return result.get('close')
        return None

    def test_connection(self) -> bool:
        """
        测试数据库连接

        Returns:
            连接是否成功
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            print(f"数据库连接测试失败: {e}")
            return False
