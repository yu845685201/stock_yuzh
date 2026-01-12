"""
进度格式化器 - 提供统一的进度显示格式化
用于基本面数据采集的进度条显示
"""

from typing import Optional
from ..models.collection_result import CollectionStatus


class ProgressFormatter:
    """
    基本面数据进度格式化器

    统一管理进度条格式,消除代码重复
    格式:[批次X/Y][当前数/总数](ts_code-股票名称-披露日期) 进度: X.X%,耗时:X.XXXs
    """

    def format_progress(
        self,
        current: int,
        total: int,
        ts_code: str,
        stock_name: str,
        status: CollectionStatus,
        elapsed_time: float,
        batch_num: Optional[int] = None,
        total_batches: Optional[int] = None,
        disclosure_date: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> str:
        """
        格式化基本面数据采集进度

        Args:
            current: 当前处理数量
            total: 总数量
            ts_code: 股票代码
            stock_name: 股票名称
            status: 处理状态(SUCCESS/NO_DATA/ERROR)
            elapsed_time: 耗时(秒)
            batch_num: 当前批次号(可选)
            total_batches: 总批次数(可选)
            disclosure_date: 披露日期(可选,格式:yyyyMMdd)
            error_message: 错误信息(可选)

        Returns:
            格式化后的进度字符串

        Examples:
            >>> formatter = ProgressFormatter()
            >>> formatter.format_progress(
            ...     current=150, total=5000,
            ...     ts_code="sz.000001", stock_name="平安银行",
            ...     status=CollectionStatus.SUCCESS,
            ...     elapsed_time=0.125,
            ...     batch_num=1, total_batches=34,
            ...     disclosure_date="20240930"
            ... )
            '[批次1/34][150/5000](sz.000001-平安银行-20240930) 进度: 3.0%,耗时:0.125s'
        """
        # 计算进度百分比
        progress_percentage = (current / total * 100) if total > 0 else 0.0

        # 批次信息(如果提供)
        batch_info = ""
        if batch_num is not None and total_batches is not None:
            batch_info = f"[批次{batch_num}/{total_batches}]"

        # 进度信息
        progress_info = f"[{current}/{total}]"

        # 股票信息
        stock_info = f"({ts_code}-{stock_name}"
        if disclosure_date:
            stock_info += f"-{disclosure_date}"
        stock_info += ")"

        # 状态信息
        if status == CollectionStatus.SUCCESS:
            status_info = f"进度: {progress_percentage:.1f}%,耗时:{elapsed_time:.3f}s"
        elif status == CollectionStatus.NO_DATA:
            status_info = f"进度: {progress_percentage:.1f}% 无数据,耗时:{elapsed_time:.3f}s"
        elif status == CollectionStatus.ERROR:
            error_msg = error_message or "未知错误"
            status_info = f"进度: {progress_percentage:.1f}% 采集失败: {error_msg},耗时:{elapsed_time:.3f}s"
        else:
            status_info = f"进度: {progress_percentage:.1f}%,耗时:{elapsed_time:.3f}s"

        # 拼接完整信息
        parts = []
        if batch_info:
            parts.append(batch_info)
        parts.append(progress_info)
        parts.append(stock_info)
        parts.append(status_info)

        return "".join(parts)

    def calculate_batch_info(
        self,
        current_index: int,
        total_stocks: int,
        batch_size: int,
        specified_batch: Optional[int] = None,
        original_total_stocks: Optional[int] = None
    ) -> tuple[Optional[int], Optional[int]]:
        """
        计算批次号信息

        Args:
            current_index: 当前索引(从0开始)
            total_stocks: 总股票数(当前批次的股票数)
            batch_size: 批次大小
            specified_batch: 用户指定的批次号(可选)
            original_total_stocks: 原始总股票数(用于指定批次模式,可选)

        Returns:
            (当前批次号, 总批次数) 或 (None, None)

        Examples:
            >>> formatter = ProgressFormatter()
            >>> formatter.calculate_batch_info(0, 5000, 150, None)
            (1, 34)
            >>> formatter.calculate_batch_info(150, 5000, 150, None)
            (2, 34)
            >>> formatter.calculate_batch_info(0, 150, 150, 5, 5000)
            (5, 34)  # 指定批次模式,显示用户指定的批次号,总批次数基于原始总数
        """
        if total_stocks == 0:
            return (None, None)

        # 计算总批次数 - 优先使用original_total_stocks
        calc_total = original_total_stocks if original_total_stocks is not None else total_stocks
        total_batches = (calc_total + batch_size - 1) // batch_size

        # 确定当前批次号
        if specified_batch is not None:
            # 用户指定批次模式:显示用户指定的批次号
            current_batch = specified_batch
        else:
            # 自动计算批次模式:根据当前索引计算
            current_batch = (current_index // batch_size) + 1

        return (current_batch, total_batches)

    @staticmethod
    def format_disclosure_date(disclosure_date) -> str:
        """
        格式化披露日期为yyyyMMdd格式

        Args:
            disclosure_date: 日期对象、字符串或None

        Returns:
            格式化后的日期字符串(yyyyMMdd)或空字符串

        Examples:
            >>> ProgressFormatter.format_disclosure_date(datetime(2024, 9, 30))
            '20240930'
            >>> ProgressFormatter.format_disclosure_date('2024-09-30')
            '20240930'
            >>> ProgressFormatter.format_disclosure_date(None)
            ''
        """
        if disclosure_date is None:
            return ''
        # 处理datetime对象
        if hasattr(disclosure_date, 'strftime'):
            return disclosure_date.strftime('%Y%m%d')
        # 处理字符串(移除可能的分隔符)
        if isinstance(disclosure_date, str):
            return disclosure_date.replace('-', '').replace('/', '').replace(' ', '')[:8]
        # 其他类型尝试转字符串
        return str(disclosure_date) if disclosure_date else ''
