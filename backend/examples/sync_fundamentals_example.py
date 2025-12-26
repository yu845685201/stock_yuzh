"""
股票基本面数据采集示例
演示如何使用基本面数据采集功能
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import ConfigManager
from src.sync.fundamentals_manager import sync_fundamentals_data
from src.sync.sync_manager import SyncManager


def example_basic_usage():
    """基础使用示例"""
    print("=== 基础使用示例 ===")

    # 方式1: 直接使用便捷函数
    print("1. 使用便捷函数同步基本面数据...")
    result = sync_fundamentals_data()

    print(f"采集结果: 成功 {result['successful']}/{result['total_stocks']} 只股票")
    print()


def example_with_sync_manager():
    """通过SyncManager使用示例"""
    print("=== SyncManager使用示例 ===")

    # 初始化同步管理器
    config = ConfigManager()
    sync_manager = SyncManager(config)

    # 同步基本面数据
    print("2. 通过SyncManager同步基本面数据...")
    result = sync_manager.sync_fundamentals_data(
        batch_size=30,  # 自定义批次大小
        list_status='L'  # 只采集上市股票
    )

    print(f"采集结果: 成功 {result['successful']}/{result['total_stocks']} 只股票")
    print(f"处理批次: {result['batch_count']} 个")
    print()


def example_dry_run():
    """试运行示例"""
    print("=== 试运行示例 ===")

    print("3. 试运行模式（不实际写入数据）...")
    result = sync_fundamentals_data(
        dry_run=True,
        batch_size=10
    )

    print(f"试运行结果: 将处理 {result['total_stocks']} 只股票")
    print()


def example_custom_options():
    """自定义选项示例"""
    print("=== 自定义选项示例 ===")

    print("4. 自定义配置选项...")
    result = sync_fundamentals_data(
        batch_size=20,        # 批次大小20
        list_status='L',      # 只采集上市股票
        dry_run=False         # 实际写入数据
    )

    print(f"采集统计:")
    print(f"  - 总股票数: {result['total_stocks']}")
    print(f"  - 成功采集: {result['successful']}")
    print(f"  - 失败数量: {result['failed']}")
    print(f"  - 成功率: {result.get('success_rate', 0):.2%}")
    print(f"  - 耗时: {result.get('duration', 0):.2f}秒")
    print()


def main():
    """主函数 - 运行所有示例"""
    print("🚀 股票基本面数据采集功能演示")
    print("=" * 50)

    try:
        # 运行各种示例
        example_basic_usage()
        example_with_sync_manager()
        example_dry_run()
        example_custom_options()

        print("✅ 所有示例运行完成！")

    except Exception as e:
        print(f"❌ 示例运行失败: {e}")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())