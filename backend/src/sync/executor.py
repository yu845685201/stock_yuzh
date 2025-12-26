"""
任务执行器 - 负责任务的调度和执行
"""

import time
import logging
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from .tasks import SyncTask, TaskStatus
from ..config import ConfigManager


class TaskExecutor:
    """任务执行器"""

    def __init__(self, config_manager: ConfigManager, max_workers: int = 4):
        self.config_manager = config_manager
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
        self._running_tasks = {}

    def execute_task(self, task: SyncTask) -> Dict[str, Any]:
        """
        执行单个任务

        Args:
            task: 同步任务

        Returns:
            Dict: 执行结果
        """
        task.status = TaskStatus.RUNNING
        self._running_tasks[task.task_id] = task

        try:
            # 验证任务
            if not task.validate():
                task.status = TaskStatus.FAILED
                task.error_message = "任务参数验证失败"
                return {
                    'task_id': task.task_id,
                    'success': False,
                    'error': task.error_message
                }

            # 执行任务
            result = task.execute()
            task.status = TaskStatus.SUCCESS
            task.progress = 100.0

            return {
                'task_id': task.task_id,
                'success': True,
                'result': result
            }

        except Exception as e:
            self.logger.error(f"任务 {task.task_id} 执行失败: {e}")

            # 判断是否需要重试
            if task.can_retry():
                task.mark_retry(str(e))
                self.logger.info(f"任务 {task.task_id} 将进行第 {task.retry_count} 次重试")
                time.sleep(2 ** task.retry_count)  # 指数退避
                return self.execute_task(task)  # 递归重试
            else:
                task.status = TaskStatus.FAILED
                task.error_message = str(e)
                return {
                    'task_id': task.task_id,
                    'success': False,
                    'error': str(e),
                    'retry_count': task.retry_count
                }

        finally:
            self._running_tasks.pop(task.task_id, None)

    def execute_parallel(self, tasks: List[SyncTask]) -> List[Dict[str, Any]]:
        """
        并行执行多个任务

        Args:
            tasks: 任务列表

        Returns:
            List[Dict]: 执行结果列表
        """
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_task = {
                executor.submit(self.execute_task, task): task
                for task in tasks
            }

            # 收集结果
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    self.logger.error(f"任务 {task.task_id} 执行异常: {e}")
                    results.append({
                        'task_id': task.task_id,
                        'success': False,
                        'error': str(e)
                    })

        return results

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        获取任务状态

        Args:
            task_id: 任务ID

        Returns:
            Optional[Dict]: 任务状态信息
        """
        task = self._running_tasks.get(task_id)
        if task:
            return {
                'task_id': task.task_id,
                'status': task.status.value,
                'progress': task.progress,
                'error_message': task.error_message,
                'retry_count': task.retry_count
            }
        return None

    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务（暂未实现）

        Args:
            task_id: 任务ID

        Returns:
            bool: 是否成功取消
        """
        # TODO: 实现任务取消逻辑
        return False