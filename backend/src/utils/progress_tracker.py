"""
进度跟踪器 - 用于显示日K线数据采集的实时进度
"""

import time
from typing import Dict, Any, Optional
from datetime import datetime


class ProgressTracker:
    """进度跟踪器，显示格式：[当前数/总数](股票编码-股票名称) 进度: 进度百分比% 耗时"""

    def __init__(self, total_items: int, description: str = "处理进度"):
        """
        初始化进度跟踪器

        Args:
            total_items: 总项目数
            description: 进度描述
        """
        self.total_items = total_items
        self.current_item = 0
        self.description = description
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.stock_names: Dict[str, str] = {}

    def set_stock_names(self, stock_names: Dict[str, str]):
        """
        设置股票名称映射

        Args:
            stock_names: {ts_code: stock_name} 映射字典
        """
        self.stock_names = stock_names

    def update(self, ts_code: str, force_update: bool = False):
        """
        更新进度

        Args:
            ts_code: 当前处理的股票代码
            force_update: 是否强制更新显示
        """
        self.current_item += 1
        current_time = time.time()

        # 控制更新频率，避免刷新过于频繁
        if force_update or (current_time - self.last_update_time) >= 0.1:  # 每0.1秒更新一次
            self._display_progress(ts_code, current_time)
            self.last_update_time = current_time

    def _display_progress(self, ts_code: str, current_time: float):
        """显示进度信息"""
        # 计算进度百分比
        progress_percentage = (self.current_item / self.total_items) * 100

        # 获取股票名称
        stock_name = self.stock_names.get(ts_code, "")
        if stock_name:
            stock_display = f"{ts_code}-{stock_name}"
        else:
            stock_display = ts_code

        # 计算耗时
        elapsed_time = current_time - self.start_time

        # 格式化显示 - 使用标准输出而非回车，确保在所有环境下都能看到
        if self.current_item % 5 == 0 or self.current_item == self.total_items:
            # 每5个或最后一个显示一次进度
            print(f"[{self.current_item}/{self.total_items}]({stock_display}) 进度: {progress_percentage:.1f}% 耗时: {elapsed_time:.1f}s")
        else:
            # 其他时候使用回车（在支持的终端中）
            try:
                print(f"\r[{self.current_item}/{self.total_items}]({stock_display}) 进度: {progress_percentage:.1f}% 耗时: {elapsed_time:.1f}s", end="", flush=True)
            except:
                # 如果回车不支持，就静默处理
                pass

    def finish(self):
        """完成进度显示"""
        elapsed_time = time.time() - self.start_time
        print(f"\n✅ {self.description}完成，共处理 {self.total_items} 项，总耗时: {elapsed_time:.2f}s")

    def get_elapsed_time(self) -> float:
        """获取已耗时"""
        return time.time() - self.start_time


class MultiStageProgressTracker:
    """多阶段进度跟踪器，用于跟踪不同阶段的进度"""

    def __init__(self):
        """初始化多阶段进度跟踪器"""
        self.stages: Dict[str, Dict[str, Any]] = {}
        self.start_time = time.time()

    def start_stage(self, stage_name: str, total_items: int, description: str):
        """
        开始一个新阶段

        Args:
            stage_name: 阶段名称
            total_items: 该阶段的总项目数
            description: 阶段描述
        """
        self.stages[stage_name] = {
            'total_items': total_items,
            'current_item': 0,
            'description': description,
            'start_time': time.time(),
            'end_time': None,
            'elapsed_time': None,
            'tracker': ProgressTracker(total_items, description)
        }
        print(f"\n🚀 开始{description}...")

    def update_stage(self, stage_name: str, ts_code: str, force_update: bool = False):
        """
        更新阶段进度

        Args:
            stage_name: 阶段名称
            ts_code: 当前处理的股票代码
            force_update: 是否强制更新显示
        """
        if stage_name in self.stages:
            self.stages[stage_name]['current_item'] += 1
            self.stages[stage_name]['tracker'].update(ts_code, force_update)

    def finish_stage(self, stage_name: str):
        """
        完成一个阶段

        Args:
            stage_name: 阶段名称
        """
        if stage_name in self.stages:
            stage = self.stages[stage_name]
            stage['end_time'] = time.time()
            stage['elapsed_time'] = stage['end_time'] - stage['start_time']
            stage['tracker'].finish()

            # 记录阶段耗时
            print(f"⏱️  {stage['description']}总耗时: {stage['elapsed_time']:.2f}s")

    def set_stock_names(self, stock_names: Dict[str, str]):
        """
        设置股票名称映射

        Args:
            stock_names: {ts_code: stock_name} 映射字典
        """
        for stage in self.stages.values():
            if 'tracker' in stage:
                stage['tracker'].set_stock_names(stock_names)

    def get_stage_time(self, stage_name: str) -> Optional[float]:
        """
        获取阶段耗时

        Args:
            stage_name: 阶段名称

        Returns:
            阶段耗时（秒），如果阶段未完成则返回None
        """
        if stage_name in self.stages:
            return self.stages[stage_name].get('elapsed_time')
        return None

    def finish_all(self):
        """完成所有阶段"""
        total_time = time.time() - self.start_time
        print(f"\n🎉 所有阶段完成，总耗时: {total_time:.2f}s")

        # 显示各阶段耗时统计
        print("\n📊 各阶段耗时统计:")
        for stage_name, stage in self.stages.items():
            elapsed = stage.get('elapsed_time')
            if elapsed is not None:
                print(f"  - {stage['description']}: {elapsed:.2f}s")
            else:
                print(f"  - {stage['description']}: 未完成")