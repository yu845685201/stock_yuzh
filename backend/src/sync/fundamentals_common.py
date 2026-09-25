"""
基本面采集公共逻辑（R-13 提取：FundamentalsManager 与 ConcurrentFundamentalsManager 之间
已验证逐字重复的块收敛于此；FundamentalsRebuildManager 的股票池/季度遍历为 RK-5 变体，
按方案不统一。三个类不合并，编排差异保留。）
"""

import time
from typing import Any, Dict, List, Optional, Tuple

from ..models.collection_result import CollectionStatus


def expand_quarters(list_date) -> List[Tuple[int, int]]:
    """数据初始化模式：根据上市日期展开 (year, quarter) 序列至当前前一季度。

    原 FM/CFM init 分支的嵌套季度循环（逐字重复块），季度计算委托 quarter_calculator。
    """
    from ..utils.quarter_calculator import calculate_start_quarter, get_current_previous_quarter

    start_year, start_quarter = calculate_start_quarter(list_date)
    end_year, end_quarter = get_current_previous_quarter()

    quarters: List[Tuple[int, int]] = []
    for y in range(start_year, end_year + 1):
        current_start_q = start_quarter if y == start_year else 1
        current_end_q = end_quarter if y == end_year else 4
        for q in range(current_start_q, current_end_q + 1):
            quarters.append((y, q))
    return quarters


def slice_batch(
    stocks: List[Dict[str, Any]],
    batch: int,
    batch_size: int,
    logger,
) -> Tuple[Optional[str], int, List[Dict[str, Any]]]:
    """批次过滤（原 FM/CFM 逐字重复块）。

    Returns:
        (error_msg, total_batches, sliced_stocks)；error_msg 非 None 表示批次越界，
        sliced_stocks 为原列表，由调用方决定错误返回形态。
    """
    total_batches = (len(stocks) + batch_size - 1) // batch_size

    if batch > total_batches:
        error_msg = f"批次编号超出范围: 请求批次{batch}，但总共只有{total_batches}个批次（共{len(stocks)}只股票，批次大小{batch_size}）"
        return error_msg, total_batches, stocks

    start_idx = (batch - 1) * batch_size
    end_idx = min(batch * batch_size, len(stocks))

    sliced = stocks[start_idx:end_idx]
    logger.info(f"批次过滤: 批次{batch}/{total_batches}，处理股票索引{start_idx}-{end_idx-1}，共{len(sliced)}只股票")
    return None, total_batches, sliced


def extract_disclosure_date_str(first_data: Dict[str, Any]) -> str:
    """提取披露日期字符串（原 FM/CFM 逐字重复块）。"""
    disclosure_date = first_data.get('disclosure_date', '')
    if hasattr(disclosure_date, 'strftime'):
        return disclosure_date.strftime('%Y%m%d')
    return str(disclosure_date) if disclosure_date else ''


def render_stock_progress(
    progress_formatter,
    status: CollectionStatus,
    *,
    current: int,
    total: int,
    stock: Dict[str, Any],
    elapsed_time: float,
    batch_num,
    total_batches,
    disclosure_date_str: Optional[str] = None,
    error_message: Optional[str] = None,
) -> str:
    """单股进度三态渲染（原 FM/CFM 成功/无数据/错误三分支逐字重复块）。"""
    if status == CollectionStatus.SUCCESS:
        return progress_formatter.format_progress(
            current=current,
            total=total,
            ts_code=stock['ts_code'],
            stock_name=stock['stock_name'],
            status=status,
            elapsed_time=elapsed_time,
            batch_num=batch_num,
            total_batches=total_batches,
            disclosure_date=disclosure_date_str
        )
    if status == CollectionStatus.NO_DATA:
        return progress_formatter.format_progress(
            current=current,
            total=total,
            ts_code=stock['ts_code'],
            stock_name=stock['stock_name'],
            status=status,
            elapsed_time=elapsed_time,
            batch_num=batch_num,
            total_batches=total_batches
        )
    return progress_formatter.format_progress(
        current=current,
        total=total,
        ts_code=stock['ts_code'],
        stock_name=stock['stock_name'],
        status=status,
        elapsed_time=elapsed_time,
        batch_num=batch_num,
        total_batches=total_batches,
        error_message=error_message
    )


def persist_batch(
    batch_data: List[Dict[str, Any]],
    *,
    csv_writer,
    db,
    save_to_csv: bool = True,
    save_to_db: bool = True,
) -> Dict[str, Any]:
    """CSV + DB 双写尾段（原 FM/CFM 逐字重复块；统计口径差异由调用方记录）。

    Returns:
        {'csv_time': float, 'db_time': float, 'affected_rows': int}
    """
    csv_start_time = time.time()
    if save_to_csv:
        csv_writer.write_base_fundamentals_info(batch_data)
    csv_time = time.time() - csv_start_time

    db_start_time = time.time()
    if save_to_db:
        affected_rows = db.upsert_fundamentals_data(batch_data)
    else:
        affected_rows = 0
    db_time = time.time() - db_start_time

    return {'csv_time': csv_time, 'db_time': db_time, 'affected_rows': affected_rows}
