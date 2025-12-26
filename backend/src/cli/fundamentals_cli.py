"""
基本面数据采集CLI命令行接口
提供简单的命令行调用方式
"""

import sys
import argparse
from datetime import datetime
from ..config import ConfigManager
from ..sync.fundamentals_manager import sync_fundamentals_data


def main():
    """CLI主函数"""
    parser = argparse.ArgumentParser(description='股票基本面数据采集工具')

    parser.add_argument('--dry-run', action='store_true',
                       help='试运行模式，不实际写入数据')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='批次大小，默认50（符合baostock QPS限制）')
    parser.add_argument('--list-status', type=str, default='L',
                       choices=['L', 'D', 'P'],
                       help='股票上市状态过滤: L=上市, D=退市, P=暂停上市 (默认: L)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='详细输出模式')

    args = parser.parse_args()

    # 设置日志级别
    import logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    print("🚀 开始股票基本面数据采集...")
    print(f"📊 配置参数: 批次大小={args.batch_size}, 上市状态={args.list_status}, 试运行={args.dry_run}")
    print()

    try:
        # 执行同步
        config = ConfigManager()
        result = sync_fundamentals_data(
            config_manager=config,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            list_status=args.list_status
        )

        # 显示结果
        print("\n" + "="*60)
        print("📈 采集结果统计")
        print("="*60)
        print(f"📊 总股票数量: {result.get('total_stocks', 0)}")
        print(f"✅ 成功采集: {result.get('successful', 0)}")
        print(f"❌ 失败数量: {result.get('failed', 0)}")
        print(f"📦 处理批次数: {result.get('batch_count', 0)}")

        if 'duration' in result:
            print(f"⏱️  总耗时: {result['duration']:.2f}秒")

        if 'success_rate' in result:
            print(f"📊 成功率: {result['success_rate']:.2%}")

        if 'error' in result:
            print(f"🚨 错误信息: {result['error']}")

        print("="*60)

        # 判断执行结果
        if result.get('error'):
            print("❌ 采集失败")
            sys.exit(1)
        elif result.get('successful', 0) > 0:
            print("✅ 采集完成")
            sys.exit(0)
        else:
            print("⚠️  无数据采集")
            sys.exit(0)

    except KeyboardInterrupt:
        print("\n⏹️  用户中断操作")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 程序异常: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()