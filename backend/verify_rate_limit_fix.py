#!/usr/bin/env python3
"""
快速验证限流移除效果的脚本
"""

import sys
import time
from src.config import ConfigManager
from src.data_sources.baostock_source import BaostockSource

def main():
    print("🔍 验证基本面数据采集限流移除效果")
    print("=" * 50)

    # 加载配置
    config_manager = ConfigManager(env='uat')
    config_manager.load_config()

    # 检查配置
    financial_rate_limit = config_manager.get('financial_data_rate_limit', {})
    print(f"限流配置状态: {'启用' if financial_rate_limit.get('enabled', True) else '禁用'}")

    # 初始化baostock数据源
    baostock_source = BaostockSource(config_manager._config)

    # 检查限流器
    if hasattr(baostock_source, 'rate_limiter') and baostock_source.rate_limiter:
        limiter = baostock_source.rate_limiter
        print(f"限流器状态: 启用={limiter.enabled}")

        if not limiter.enabled:
            print("✅ 限流已成功禁用")
            print("🚀 预期性能提升: 省去约311秒休眠时间（5.2分钟）")
            print("💡 5178只股票的采集时间将从19.6分钟缩短至约14.4分钟")
        else:
            print("❌ 限流仍然启用")
    else:
        print("❌ 限流器未正确初始化")

if __name__ == "__main__":
    main()