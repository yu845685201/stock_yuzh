"""
数据处理管道 - 实现数据的提取、转换、加载
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Iterator, Optional
from datetime import date


class PipelineStage(ABC):
    """管道处理阶段基类"""

    @abstractmethod
    def process(self, data: Any) -> Any:
        """
        处理数据

        Args:
            data: 输入数据

        Returns:
            Any: 处理后的数据
        """
        pass


class ExtractStage(PipelineStage):
    """数据提取阶段"""

    def __init__(self, data_source):
        self.data_source = data_source

    def process(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从数据源提取数据

        Args:
            params: 提取参数

        Returns:
            List[Dict]: 提取的数据
        """
        data_type = params.get('type')

        if data_type == 'stock_list':
            return self.data_source.get_stock_list()
        elif data_type == 'daily_data':
            return self.data_source.get_daily_data(
                params['code'],
                params.get('start_date'),
                params.get('end_date')
            )
        elif data_type == 'financial_data':
            return self.data_source.get_financial_data(
                params['code'],
                params['year'],
                params['quarter']
            )
        else:
            raise ValueError(f"不支持的数据类型: {data_type}")


class TransformStage(PipelineStage):
    """数据转换阶段"""

    def __init__(self, transformers: List[PipelineStage] = None):
        self.transformers = transformers or []

    def process(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        转换数据

        Args:
            data: 原始数据

        Returns:
            List[Dict]: 转换后的数据
        """
        result = data
        for transformer in self.transformers:
            result = transformer.process(result)
        return result


class ValidateStage(PipelineStage):
    """数据验证阶段"""

    def __init__(self, required_fields: List[str]):
        self.required_fields = required_fields

    def process(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        验证数据完整性

        Args:
            data: 待验证数据

        Returns:
            List[Dict]: 验证通过的数据
        """
        validated_data = []
        for item in data:
            # 检查必需字段
            if all(item.get(field) is not None for field in self.required_fields):
                validated_data.append(item)
            else:
                # 记录验证失败的数据
                pass
        return validated_data


class LoadStage(PipelineStage):
    """数据加载阶段"""

    def __init__(self, loaders: List[PipelineStage]):
        self.loaders = loaders

    def process(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        加载数据到存储

        Args:
            data: 待加载数据

        Returns:
            Dict: 加载结果
        """
        results = {}
        for loader in self.loaders:
            result = loader.process(data)
            results.update(result)
        return results


class DataPipeline:
    """数据处理管道"""

    def __init__(self, stages: List[PipelineStage]):
        self.stages = stages

    def execute(self, initial_data: Any) -> Any:
        """
        执行管道

        Args:
            initial_data: 初始数据

        Returns:
            Any: 最终结果
        """
        data = initial_data
        for stage in self.stages:
            data = stage.process(data)
        return data

    def add_stage(self, stage: PipelineStage, position: int = -1):
        """
        添加处理阶段

        Args:
            stage: 处理阶段
            position: 插入位置，-1表示末尾
        """
        if position == -1:
            self.stages.append(stage)
        else:
            self.stages.insert(position, stage)

    def remove_stage(self, position: int):
        """
        移除处理阶段

        Args:
            position: 移除位置
        """
        if 0 <= position < len(self.stages):
            self.stages.pop(position)


class BatchPipeline:
    """批量数据处理管道"""

    def __init__(self, pipeline: DataPipeline, batch_size: int = 1000):
        self.pipeline = pipeline
        self.batch_size = batch_size

    def execute_batch(self, data_iterator: Iterator[Any]) -> List[Any]:
        """
        批量执行管道

        Args:
            data_iterator: 数据迭代器

        Returns:
            List[Any]: 所有批次的处理结果
        """
        results = []
        batch = []

        for item in data_iterator:
            batch.append(item)

            if len(batch) >= self.batch_size:
                result = self.pipeline.execute(batch)
                results.append(result)
                batch = []

        # 处理最后一批
        if batch:
            result = self.pipeline.execute(batch)
            results.append(result)

        return results