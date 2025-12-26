"""
配置管理器
"""

import os
import yaml
from typing import Dict, Any, Optional


class ConfigManager:
    """配置管理器，支持YAML配置文件和环境变量"""

    def __init__(self, config_path: str = None, env: str = 'uat'):
        """
        初始化配置管理器

        Args:
            config_path: 配置文件路径，默认为 backend/config/config.yaml
            env: 环境名称，默认为 uat
        """
        self.env = env
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), '..', '..', 'config', 'config.yaml'
        )
        self._config = None

    def load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if self._config is None:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    self._config = yaml.safe_load(f)
            else:
                # 如果配置文件不存在，使用默认配置
                self._config = self._get_default_config()
        return self._config

    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'env': self.env,
            'database': {
                'uat': {
                    'host': '127.0.0.1',
                    'port': 5432,
                    'user': 'postgres',
                    'password': os.getenv('DB_PASSWORD', 'yuzh1234'),
                    'database': 'stock_analysis_uat'
                },
                'prod': {
                    'host': '127.0.0.1',
                    'port': 5432,
                    'user': 'postgres',
                    'password': os.getenv('DB_PASSWORD', 'yuzh1234'),
                    'database': 'stock_analysis'
                }
            },
            'data_paths': {
                'uat': {
                    'csv': 'uat/data',
                    'vipdoc': 'uat/vipdoc'
                },
                'prod': {
                    'csv': 'prod/data',
                    'vipdoc': 'prod/vipdoc'
                }
            },
            'data_sources': {
                'pytdx': {
                    'enabled': True,
                    'vipdoc_path': 'uat/vipdoc'
                },
                'baostock': {
                    'enabled': True
                }
            },
            'sync': {
                'batch_size': 1000,
                'max_retries': 3,
                'timeout': 30
            },
            'rate_limit': {
                'enabled': True,
                'calls_per_period': 50,
                'sleep_duration': 1.0,
                'retry': {
                    'max_attempts': 3,
                    'retry_delay': 0.1
                }
            }
        }

    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值，支持点号分隔的嵌套键

        Args:
            key: 配置键，支持 'database.host' 格式
            default: 默认值

        Returns:
            配置值
        """
        config = self.load_config()
        keys = key.split('.')
        value = config

        try:
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default
            return value
        except (KeyError, TypeError):
            return default

    def get_database_config(self) -> Dict[str, Any]:
        """获取当前环境的数据库配置"""
        return self.get(f'database.{self.env}', {})

    def get_data_paths(self) -> Dict[str, str]:
        """获取当前环境的数据路径配置"""
        return self.get(f'data_paths.{self.env}', {})

    def save_config(self) -> None:
        """保存配置到文件"""
        os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self._config, f, default_flow_style=False, allow_unicode=True)