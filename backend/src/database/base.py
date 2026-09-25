"""
DatabaseConnection 基础层：初始化、连接上下文与通用执行方法（自 connection.py 拆分，R-06 纯物理移动）
"""

import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

from ..config import ConfigManager
from .pool import DatabaseConnectionPool
import logging


class ConnectionMixin:
    """连接上下文与通用执行（原 DatabaseConnection 主体方法）"""

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

    def execute_values(self, query: str, params_list: List[tuple], page_size: int = 1000) -> int:
        """
        批量执行语句（使用execute_values，适合大批量插入）

        Args:
            query: SQL语句（包含VALUES %s）
            params_list: 参数列表
            page_size: 每批次大小

        Returns:
            影响的行数
        """
        if not params_list:
            return 0
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, query, params_list, page_size=page_size)
                conn.commit()
                return cursor.rowcount
