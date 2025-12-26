"""
配置验证器
"""

from typing import Dict, Any, List, Optional
from ..utils.exceptions import ConfigurationError


class ConfigValidator:
    """配置验证器"""

    @staticmethod
    def validate_database_config(config: Dict[str, Any]) -> None:
        """
        验证数据库配置

        Args:
            config: 数据库配置字典

        Raises:
            ConfigurationError: 配置验证失败
        """
        required_fields = ['host', 'port', 'user', 'password', 'database']
        for field in required_fields:
            if field not in config:
                raise ConfigurationError(f"数据库配置缺少必需字段: {field}")

        # 验证端口号
        if not isinstance(config['port'], int) or config['port'] <= 0:
            raise ConfigurationError("数据库端口号必须是正整数")

    @staticmethod
    def validate_data_paths_config(config: Dict[str, Any]) -> None:
        """
        验证数据路径配置

        Args:
            config: 数据路径配置字典

        Raises:
            ConfigurationError: 配置验证失败
        """
        required_paths = ['csv', 'vipdoc']
        for path in required_paths:
            if path not in config:
                raise ConfigurationError(f"数据路径配置缺少必需路径: {path}")

    @staticmethod
    def validate_data_sources_config(config: Dict[str, Any]) -> None:
        """
        验证数据源配置

        Args:
            config: 数据源配置字典

        Raises:
            ConfigurationError: 配置验证失败
        """
        if not isinstance(config, dict):
            raise ConfigurationError("数据源配置必须是字典格式")

        # 验证pytdx配置
        if 'pytdx' in config:
            pytdx_config = config['pytdx']
            if pytdx_config.get('enabled', True):
                if 'vipdoc_path' not in pytdx_config:
                    raise ConfigurationError("pytdx配置缺少vipdoc_path")

        # 验证baostock配置
        if 'baostock' in config:
            baostock_config = config['baostock']
            if baostock_config.get('enabled', True):
                # baostock暂时不需要额外配置
                pass

    @staticmethod
    def validate_sync_config(config: Dict[str, Any]) -> None:
        """
        验证同步配置

        Args:
            config: 同步配置字典

        Raises:
            ConfigurationError: 配置验证失败
        """
        # 验证batch_size
        batch_size = config.get('batch_size', 1000)
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ConfigurationError("batch_size必须是正整数")

        # 验证max_retries
        max_retries = config.get('max_retries', 3)
        if not isinstance(max_retries, int) or max_retries < 0:
            raise ConfigurationError("max_retries必须是非负整数")

        # 验证timeout
        timeout = config.get('timeout', 30)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ConfigurationError("timeout必须是正数")

    @staticmethod
    def validate_full_config(config: Dict[str, Any]) -> None:
        """
        验证完整配置

        Args:
            config: 完整配置字典

        Raises:
            ConfigurationError: 配置验证失败
        """
        # 验证环境配置
        env = config.get('env')
        if env not in ['uat', 'prod']:
            raise ConfigurationError("env配置必须是'uat'或'prod'")

        # 验证各部分配置
        if 'database' in config:
            env_config = config['database'].get(env)
            if env_config:
                ConfigValidator.validate_database_config(env_config)

        if 'data_paths' in config:
            env_config = config['data_paths'].get(env)
            if env_config:
                ConfigValidator.validate_data_paths_config(env_config)

        if 'data_sources' in config:
            ConfigValidator.validate_data_sources_config(config['data_sources'])

        if 'sync' in config:
            ConfigValidator.validate_sync_config(config['sync'])