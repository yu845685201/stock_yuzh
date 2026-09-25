"""
数据库连接池管理（自 connection.py 拆分，R-06 纯物理移动）
"""

import psycopg2
import psycopg2.pool
from contextlib import contextmanager

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
