"""
CSV文件管理器 - 处理CSV文件的删除、备份等操作
"""

import os
import shutil
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import tempfile
import fcntl


class CsvFileManager:
    """
    CSV文件管理器，负责在写入前准备文件（删除、备份等操作）
    """

    def __init__(self, config: Dict[str, Any]):
        """
        初始化CSV文件管理器

        Args:
            config: 文件管理配置
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 获取文件管理配置，支持多种配置格式
        file_management_config = config.get('file_management', {})

        # 验证配置
        self._validate_config(file_management_config)

        # 默认配置
        self.default_mode = file_management_config.get('mode', 'append')
        self.backup_config = file_management_config.get('backup', {})
        self.per_type_settings = file_management_config.get('per_type_settings', {}) or {}

        # 初始化备份目录
        self.backup_dir = self.backup_config.get('directory', '.backup')
        self.max_backup_files = self.backup_config.get('max_files', 10)
        self.backup_enabled = self.backup_config.get('enabled', True)

        # 验证备份目录路径
        self._validate_backup_path()

    def prepare_for_write(self, filepath: str, data_type: str) -> None:
        """
        根据配置准备文件供写入

        Args:
            filepath: 文件路径
            data_type: 数据类型（base_stock_info, his_kline_day等）
        """
        if not os.path.exists(filepath):
            # 文件不存在，无需处理
            return

        # 获取该数据类型对应的模式
        mode = self._get_mode_for_type(data_type)

        self.logger.debug(f"准备文件 {filepath}，数据类型 {data_type}，模式 {mode}")

        if mode == 'overwrite':
            self._delete_file_if_exists(filepath)
        elif mode == 'backup_overwrite':
            self._backup_and_delete_file(filepath)
        elif mode == 'append':
            # 保持原有行为，不做任何操作
            pass
        else:
            self.logger.warning(f"未知的文件管理模式: {mode}，将使用追加模式")

    def _get_mode_for_type(self, data_type: str) -> str:
        """
        获取指定数据类型的文件管理模式

        Args:
            data_type: 数据类型

        Returns:
            文件管理模式
        """
        # 首先检查是否有特定类型的设置
        if data_type in self.per_type_settings:
            type_config = self.per_type_settings[data_type]
            if 'mode' in type_config:
                return type_config['mode']

        # 返回默认模式
        return self.default_mode

    def _delete_file_if_exists(self, filepath: str) -> bool:
        """
        安全删除文件（带文件锁）

        Args:
            filepath: 文件路径

        Returns:
            是否成功删除
        """
        try:
            if os.path.exists(filepath):
                # 使用文件锁防止并发访问
                lock_file = f"{filepath}.lock"
                with open(lock_file, 'w') as lock_fd:
                    try:
                        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        os.remove(filepath)
                        self.logger.info(f"已删除文件: {filepath}")
                        return True
                    except IOError:
                        self.logger.warning(f"文件被其他进程锁定，跳过删除: {filepath}")
                        return False
                    finally:
                        if os.path.exists(lock_file):
                            os.remove(lock_file)
            return False
        except OSError as e:
            self.logger.error(f"删除文件失败 {filepath}: {e}")
            return False

    def _backup_and_delete_file(self, filepath: str) -> Optional[str]:
        """
        备份文件后删除

        Args:
            filepath: 文件路径

        Returns:
            备份文件路径，失败时返回None
        """
        if not self.backup_enabled:
            self.logger.debug("备份功能已禁用，直接删除文件")
            self._delete_file_if_exists(filepath)
            return None

        try:
            # 确保备份目录存在
            backup_path = Path(self.backup_dir)
            backup_path.mkdir(parents=True, exist_ok=True)

            # 生成备份文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            original_name = os.path.basename(filepath)
            backup_filename = f"{timestamp}_{original_name}"
            backup_filepath = backup_path / backup_filename

            # 复制文件到备份目录
            shutil.copy2(filepath, backup_filepath)
            self.logger.info(f"已备份文件到: {backup_filepath}")

            # 清理旧备份文件
            self._cleanup_old_backups()

            # 删除原文件
            if self._delete_file_if_exists(filepath):
                return str(backup_filepath)
            else:
                # 删除失败，保留备份文件
                self.logger.warning(f"原文件删除失败，但备份文件已保留: {backup_filepath}")
                return str(backup_filepath)

        except Exception as e:
            self.logger.error(f"备份文件失败 {filepath}: {e}")
            return None

    def _cleanup_old_backups(self) -> None:
        """
        清理旧的备份文件，保留最新的N个文件
        """
        try:
            backup_path = Path(self.backup_dir)
            if not backup_path.exists():
                return

            # 获取所有备份文件并按修改时间排序
            backup_files = list(backup_path.glob("*.csv"))
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

            # 保留最新的N个文件，删除其余的
            if len(backup_files) > self.max_backup_files:
                files_to_delete = backup_files[self.max_backup_files:]
                for old_file in files_to_delete:
                    try:
                        old_file.unlink()
                        self.logger.debug(f"已删除旧备份文件: {old_file}")
                    except OSError as e:
                        self.logger.warning(f"删除旧备份文件失败 {old_file}: {e}")

        except Exception as e:
            self.logger.error(f"清理旧备份文件失败: {e}")

    def get_backup_info(self) -> Dict[str, Any]:
        """
        获取备份信息

        Returns:
            备份信息字典
        """
        try:
            backup_path = Path(self.backup_dir)
            if not backup_path.exists():
                return {
                    'backup_enabled': self.backup_enabled,
                    'backup_directory': str(backup_path),
                    'backup_count': 0,
                    'total_size': 0
                }

            backup_files = list(backup_path.glob("*.csv"))
            total_size = sum(f.stat().st_size for f in backup_files)

            return {
                'backup_enabled': self.backup_enabled,
                'backup_directory': str(backup_path),
                'backup_count': len(backup_files),
                'total_size': total_size,
                'max_backup_files': self.max_backup_files
            }

        except Exception as e:
            self.logger.error(f"获取备份信息失败: {e}")
            return {
                'backup_enabled': self.backup_enabled,
                'backup_directory': str(backup_path),
                'backup_count': 0,
                'total_size': 0,
                'error': str(e)
            }

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        验证配置参数

        Args:
            config: 文件管理配置

        Raises:
            ValueError: 配置参数无效时
        """
        valid_modes = ['append', 'overwrite', 'backup_overwrite']
        mode = config.get('mode', 'append')

        if mode not in valid_modes:
            raise ValueError(f"无效的文件管理模式: {mode}，有效值为: {valid_modes}")

        # 验证备份配置
        backup_config = config.get('backup', {})
        if backup_config.get('max_files', 10) <= 0:
            raise ValueError("备份文件数量必须大于0")

    def _validate_backup_path(self) -> None:
        """
        验证备份目录路径的安全性

        Raises:
            ValueError: 路径不安全时
        """
        # 将相对路径转换为绝对路径
        backup_path = Path(self.backup_dir).resolve()

        # 确保备份路径不会超出当前工作目录
        try:
            backup_path.relative_to(Path.cwd())
        except ValueError:
            # 如果是绝对路径，检查是否在安全范围内
            safe_paths = ['/tmp', '/var/tmp', tempfile.gettempdir()]
            if not any(str(backup_path).startswith(safe_path) for safe_path in safe_paths):
                raise ValueError(f"备份目录路径不安全: {self.backup_dir}")

        # 确保备份目录存在
        try:
            backup_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            self.logger.error(f"无法创建备份目录 {backup_path}: {e}")
            raise