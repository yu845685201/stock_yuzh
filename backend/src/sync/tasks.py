"""
数据同步任务抽象接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import date
from enum import Enum


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"


class SyncTask(ABC):
    """同步任务基类"""

    def __init__(self, task_id: str, config: Dict[str, Any]):
        self.task_id = task_id
        self.config = config
        self.status = TaskStatus.PENDING
        self.progress = 0.0
        self.error_message = None
        self.retry_count = 0
        self.max_retries = config.get('max_retries', 3)

    @abstractmethod
    def execute(self) -> Dict[str, Any]:
        """
        执行同步任务

        Returns:
            Dict: 执行结果
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """
        验证任务参数

        Returns:
            bool: 验证是否通过
        """
        pass

    def can_retry(self) -> bool:
        """判断是否可以重试"""
        return self.retry_count < self.max_retries

    def mark_retry(self, error: str):
        """标记重试"""
        self.retry_count += 1
        self.status = TaskStatus.RETRY
        self.error_message = error


class StockListTask(SyncTask):
    """股票列表同步任务"""

    def __init__(self, task_id: str, config: Dict[str, Any]):
        super().__init__(task_id, config)
        self.save_to_csv = config.get('save_to_csv', True)
        self.save_to_db = config.get('save_to_db', True)

    def validate(self) -> bool:
        return True

    def execute(self) -> Dict[str, Any]:
        # 实现细节在具体类中
        pass


class DailyDataTask(SyncTask):
    """日K线数据同步任务"""

    def __init__(self, task_id: str, config: Dict[str, Any]):
        super().__init__(task_id, config)
        self.start_date = config.get('start_date')
        self.end_date = config.get('end_date')
        self.codes = config.get('codes')
        self.save_to_csv = config.get('save_to_csv', True)
        self.save_to_db = config.get('save_to_db', True)

    def validate(self) -> bool:
        if self.start_date and self.end_date:
            return self.start_date <= self.end_date
        return True

    def execute(self) -> Dict[str, Any]:
        # 实现细节在具体类中
        pass


