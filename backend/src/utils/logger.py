"""
日志系统配置
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


class LoggerSetup:
    """日志系统设置"""

    @staticmethod
    def setup_logger(
        name: str,
        level: str = "INFO",
        log_file: Optional[str] = None,
        format_string: Optional[str] = None
    ) -> logging.Logger:
        """
        设置日志记录器

        Args:
            name: 记录器名称
            level: 日志级别
            log_file: 日志文件路径
            format_string: 日志格式字符串

        Returns:
            logging.Logger: 配置好的记录器
        """
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))

        # 清除现有处理器
        logger.handlers.clear()

        # 设置格式
        if format_string is None:
            format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(format_string)

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 文件处理器
        if log_file:
            # 确保日志目录存在
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    @staticmethod
    def get_sync_logger(env: str = "uat") -> logging.Logger:
        """获取同步模块专用日志记录器"""
        log_file = f"logs/sync_{env}_{datetime.now().strftime('%Y%m%d')}.log"
        return LoggerSetup.setup_logger(
            "sync",
            level="INFO",
            log_file=log_file
        )

    @staticmethod
    def get_database_logger(env: str = "uat") -> logging.Logger:
        """获取数据库模块专用日志记录器"""
        log_file = f"logs/database_{env}_{datetime.now().strftime('%Y%m%d')}.log"
        return LoggerSetup.setup_logger(
            "database",
            level="INFO",
            log_file=log_file
        )


class ProgressLogger:
    """进度日志记录器"""

    def __init__(self, logger: logging.Logger, total: int, description: str = "处理进度"):
        self.logger = logger
        self.total = total
        self.current = 0
        self.description = description
        self.last_log_time = 0

    def update(self, increment: int = 1):
        """更新进度"""
        self.current += increment
        progress = (self.current / self.total) * 100

        # 每10%记录一次日志
        if int(progress) % 10 == 0 and self.current != self.last_log_time:
            self.logger.info(f"{self.description}: {self.current}/{self.total} ({progress:.1f}%)")
            self.last_log_time = self.current

    def finish(self):
        """完成进度"""
        self.logger.info(f"{self.description}完成: {self.total}/{self.total} (100.0%)")


class ErrorLogger:
    """错误日志记录器"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.errors = []

    def log_error(self, error: Exception, context: str = ""):
        """
        记录错误

        Args:
            error: 异常对象
            context: 错误上下文
        """
        error_msg = f"{context}: {str(error)}" if context else str(error)
        self.logger.error(error_msg, exc_info=True)
        self.errors.append({
            'timestamp': datetime.now(),
            'message': error_msg,
            'context': context,
            'type': type(error).__name__
        })

    def get_errors(self) -> list:
        """获取所有错误"""
        return self.errors

    def has_errors(self) -> bool:
        """是否有错误"""
        return len(self.errors) > 0

    def clear_errors(self):
        """清除错误记录"""
        self.errors.clear()