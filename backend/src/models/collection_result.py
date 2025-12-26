"""
数据采集结果封装类 - 实用平衡方案
用于区分采集成功、无数据和异常失败三种状态
"""

from typing import Optional, Dict, Any, Union
from enum import Enum
from datetime import datetime


class CollectionStatus(Enum):
    """采集状态枚举"""
    SUCCESS = "success"      # 成功获取数据
    NO_DATA = "no_data"      # 接口正常但无数据
    ERROR = "error"          # 接口调用异常


class CollectionResult:
    """
    数据采集结果封装类

    用于封装单只股票的基本面数据采集结果，清晰区分成功、无数据和异常失败状态
    """

    def __init__(
        self,
        status: CollectionStatus,
        data: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        execution_time: Optional[float] = None
    ):
        """
        初始化采集结果

        Args:
            status: 采集状态
            data: 采集到的数据（成功时有效）
            error_message: 错误信息（失败时有效）
            execution_time: 执行耗时（秒）
        """
        self.status = status
        self.data = data
        self.error_message = error_message
        self.execution_time = execution_time
        self.timestamp = datetime.now()

    @property
    def is_success(self) -> bool:
        """是否成功获取到数据"""
        return self.status == CollectionStatus.SUCCESS

    @property
    def is_no_data(self) -> bool:
        """是否为无数据状态"""
        return self.status == CollectionStatus.NO_DATA

    @property
    def is_error(self) -> bool:
        """是否为异常失败"""
        return self.status == CollectionStatus.ERROR

    @property
    def has_data(self) -> bool:
        """是否包含有效数据"""
        return self.is_success and self.data is not None

    def get_data_or_none(self) -> Optional[Dict[str, Any]]:
        """获取数据，如果没有数据则返回None"""
        return self.data if self.has_data else None

    @classmethod
    def success(cls, data: Dict[str, Any], execution_time: Optional[float] = None) -> 'CollectionResult':
        """创建成功结果"""
        return cls(
            status=CollectionStatus.SUCCESS,
            data=data,
            execution_time=execution_time
        )

    @classmethod
    def no_data(cls, execution_time: Optional[float] = None) -> 'CollectionResult':
        """创建无数据结果"""
        return cls(
            status=CollectionStatus.NO_DATA,
            execution_time=execution_time
        )

    @classmethod
    def error(cls, error_message: str, execution_time: Optional[float] = None) -> 'CollectionResult':
        """创建错误结果"""
        return cls(
            status=CollectionStatus.ERROR,
            error_message=error_message,
            execution_time=execution_time
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'status': self.status.value,
            'data': self.data,
            'error_message': self.error_message,
            'execution_time': self.execution_time,
            'timestamp': self.timestamp.isoformat()
        }

    def __str__(self) -> str:
        """字符串表示"""
        if self.is_success:
            return f"CollectionResult(SUCCESS, data={len(self.data) if self.data else 0} fields, {self.execution_time:.3f}s)"
        elif self.is_no_data:
            return f"CollectionResult(NO_DATA, {self.execution_time:.3f}s)"
        else:
            return f"CollectionResult(ERROR, {self.error_message}, {self.execution_time:.3f}s)"

    def __repr__(self) -> str:
        return self.__str__()


class CollectionStatistics:
    """
    采集统计类

    用于统计采集过程中的各种状态数量和计算相关指标
    """

    def __init__(self):
        """初始化统计"""
        self.total = 0
        self.success_count = 0
        self.no_data_count = 0
        self.error_count = 0
        self.total_execution_time = 0.0

    def add_result(self, result: CollectionResult) -> None:
        """添加一个采集结果到统计"""
        self.total += 1
        if result.execution_time:
            self.total_execution_time += result.execution_time

        if result.is_success:
            self.success_count += 1
        elif result.is_no_data:
            self.no_data_count += 1
        else:
            self.error_count += 1

    @property
    def completion_rate(self) -> float:
        """完成率（成功+无数据）/总数"""
        if self.total == 0:
            return 0.0
        return (self.success_count + self.no_data_count) / self.total

    @property
    def real_success_rate(self) -> float:
        """真实成功率（仅成功）/总数"""
        if self.total == 0:
            return 0.0
        return self.success_count / self.total

    @property
    def error_rate(self) -> float:
        """错误率（仅异常）/总数"""
        if self.total == 0:
            return 0.0
        return self.error_count / self.total

    @property
    def average_execution_time(self) -> float:
        """平均执行时间"""
        if self.total == 0:
            return 0.0
        return self.total_execution_time / self.total

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'total': self.total,
            'success_count': self.success_count,
            'no_data_count': self.no_data_count,
            'error_count': self.error_count,
            'completion_rate': self.completion_rate,
            'real_success_rate': self.real_success_rate,
            'error_rate': self.error_rate,
            'total_execution_time': self.total_execution_time,
            'average_execution_time': self.average_execution_time
        }

    def __str__(self) -> str:
        """字符串表示"""
        return (f"CollectionStatistics(total={self.total}, success={self.success_count}, "
                f"no_data={self.no_data_count}, error={self.error_count}, "
                f"completion_rate={self.completion_rate:.2%}, real_success_rate={self.real_success_rate:.2%})")