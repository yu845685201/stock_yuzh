"""
股票名称查询服务 - 平衡优化方案实现
提供高效的股票名称查询和缓存机制
"""

import logging
from typing import Dict, Any, List, Optional
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)


class StockNameService:
    """
    股票名称查询服务

    设计原则：
    1. 分层数据获取：内存缓存 -> 数据库 -> API
    2. 预加载策略：批量加载减少查询次数
    3. 优雅降级：确保数据完整性
    4. 高性能：支持批量查询
    """

    def __init__(self, db_conn: DatabaseConnection):
        """
        初始化股票名称服务

        Args:
            db_conn: 数据库连接实例
        """
        self.db_conn = db_conn
        self.cache: Dict[str, Optional[str]] = {}  # 内存缓存
        self.cache_loaded = False
        self.baostock_source = None

    def preload_cache(self) -> bool:
        """
        预加载股票名称缓存

        Returns:
            bool: 加载是否成功
        """
        if self.cache_loaded:
            return True

        try:
            logger.info("开始预加载股票名称缓存...")

            # 从数据库批量加载所有股票名称
            query = """
                SELECT stock_code, stock_name
                FROM base_stock_info
                WHERE stock_name IS NOT NULL
            """

            results = self.db_conn.execute_query(query)

            if results:
                for row in results:
                    stock_code = row[0]
                    stock_name = row[1]
                    self.cache[stock_code] = stock_name

                logger.info(f"成功加载 {len(results)} 条股票名称到缓存")
                self.cache_loaded = True
                return True
            else:
                logger.warning("数据库中未找到股票名称数据")
                return False

        except Exception as e:
            logger.error(f"预加载股票名称缓存失败: {e}")
            return False

    def get_stock_name(self, stock_code: str) -> Optional[str]:
        """
        获取股票名称 - 优先从缓存，其次数据库，最后API

        Args:
            stock_code: 股票代码（6位）

        Returns:
            str: 股票名称，获取失败时返回None
        """
        # 1. 检查内存缓存
        if stock_code in self.cache:
            return self.cache[stock_code]

        # 2. 从数据库查询
        try:
            query = """
                SELECT stock_name
                FROM base_stock_info
                WHERE stock_code = %s
            """
            results = self.db_conn.execute_query(query, (stock_code,))

            if results and results[0][0]:
                stock_name = results[0][0]
                self.cache[stock_code] = stock_name
                return stock_name
        except Exception as e:
            logger.error(f"从数据库查询股票名称失败 {stock_code}: {e}")

        # 3. 从API查询并更新数据库
        try:
            # 动态导入避免循环依赖
            from ..data_sources.baostock_source import BaostockSource

            if not self.baostock_source:
                self.baostock_source = BaostockSource({})
                if not self.baostock_source.connect():
                    logger.error("无法连接Baostock API")
                    self.cache[stock_code] = None
                    return None

            stock_name = self._get_stock_name_from_api(stock_code)

            if stock_name:
                # 更新缓存
                self.cache[stock_code] = stock_name

                # 更新数据库
                self._update_stock_name_in_db(stock_code, stock_name)

                return stock_name
            else:
                self.cache[stock_code] = None
                return None

        except Exception as e:
            logger.error(f"从API获取股票名称失败 {stock_code}: {e}")
            self.cache[stock_code] = None
            return None

    def batch_get_stock_names(self, stock_codes: List[str]) -> Dict[str, Optional[str]]:
        """
        批量获取股票名称

        Args:
            stock_codes: 股票代码列表

        Returns:
            Dict: 股票代码到股票名称的映射
        """
        result = {}

        # 1. 从缓存中获取已有的
        missing_codes = []
        for code in stock_codes:
            if code in self.cache:
                result[code] = self.cache[code]
            else:
                missing_codes.append(code)

        # 2. 批量查询数据库中缺失的
        if missing_codes:
            try:
                placeholders = ','.join(['%s'] * len(missing_codes))
                query = f"""
                    SELECT stock_code, stock_name
                    FROM base_stock_info
                    WHERE stock_code IN ({placeholders})
                """

                results = self.db_conn.execute_query(query, tuple(missing_codes))

                if results:
                    db_result = {row[0]: row[1] for row in results}
                    for code in missing_codes:
                        if code in db_result:
                            result[code] = db_result[code]
                            self.cache[code] = db_result[code]
                        else:
                            missing_codes.remove(code)

            except Exception as e:
                logger.error(f"批量查询股票名称失败: {e}")

        # 3. 对于数据库中没有的，从API获取
        if missing_codes:
            for code in missing_codes:
                stock_name = self.get_stock_name(code)  # 单个处理
                result[code] = stock_name

        return result

    def _get_stock_name_from_api(self, stock_code: str) -> Optional[str]:
        """
        从Baostock API获取股票名称

        Args:
            stock_code: 股票代码

        Returns:
            str: 股票名称
        """
        try:
            # 尝试不同市场前缀
            for market in ['sz', 'sh', 'bj']:
                baostock_code = f"{market}.{stock_code}"
                stock_name = self.baostock_source._get_stock_name(baostock_code)
                if stock_name:
                    return stock_name

            return None

        except Exception as e:
            logger.error(f"API获取股票名称失败 {stock_code}: {e}")
            return None

    def _update_stock_name_in_db(self, stock_code: str, stock_name: str) -> bool:
        """
        更新数据库中的股票名称

        Args:
            stock_code: 股票代码
            stock_name: 股票名称

        Returns:
            bool: 更新是否成功
        """
        try:
            # 先查询是否存在记录
            query = """
                SELECT id FROM base_stock_info WHERE stock_code = %s
            """
            results = self.db_conn.execute_query(query, (stock_code,))

            if results:
                # 更新现有记录
                update_query = """
                    UPDATE base_stock_info
                    SET stock_name = %s, update_time = CURRENT_TIMESTAMP
                    WHERE stock_code = %s
                """
                self.db_conn.execute_update(update_query, (stock_name, stock_code))
            else:
                # 插入新记录
                insert_query = """
                    INSERT INTO base_stock_info (stock_code, stock_name, create_time, update_time)
                    VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                self.db_conn.execute_update(insert_query, (stock_code, stock_name))

            logger.info(f"成功更新股票名称: {stock_code} -> {stock_name}")
            return True

        except Exception as e:
            logger.error(f"更新股票名称到数据库失败 {stock_code}: {e}")
            return False

    def clear_cache(self):
        """清空缓存"""
        self.cache.clear()
        self.cache_loaded = False
        logger.info("股票名称缓存已清空")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        获取缓存统计信息

        Returns:
            Dict: 缓存统计信息
        """
        total_cached = len(self.cache)
        valid_names = sum(1 for name in self.cache.values() if name is not None)

        return {
            'total_cached': total_cached,
            'valid_names': valid_names,
            'null_names': total_cached - valid_names,
            'cache_loaded': self.cache_loaded
        }