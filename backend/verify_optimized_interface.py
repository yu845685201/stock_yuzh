#!/usr/bin/env python3
"""
验证优化接口的正确性
使用少量股票测试优化版本是否正常工作
"""

import sys
import os
import time
from datetime import datetime

# 添加项目路径
sys.path.append('.')

from src.config import ConfigManager
from src.database.connection import DatabaseConnectionPool
from src.sync.csv_writer import CsvWriter
from src.data_sources.baostock_source import BaostockSource
from src.sync.optimized_sync_manager import OptimizedSyncManager

def verify_optimized_interface():
    """验证优化接口"""
    print("🔍 验证优化接口的正确性...")

    # 测试参数：只采集5只股票
    test_codes = ['000001', '000002', '000858', '002415', '600036']

    try:
        # 初始化组件
        print("📦 初始化组件...")
        config_manager = ConfigManager(env='uat')
        db_pool = DatabaseConnectionPool(config_manager)
        db_conn = db_pool.get_connection()
        csv_writer = CsvWriter(config_manager)
        baostock_source = BaostockSource(config_manager)

        # 创建优化管理器
        optimized_manager = OptimizedSyncManager(
            config_manager=config_manager,
            baostock_source=baostock_source,
            db_conn=db_conn,
            csv_writer=csv_writer
        )

        print("✅ 优化管理器初始化成功")

        # 执行测试采集
        print(f"\n🎯 开始测试采集 {len(test_codes)} 只股票...")
        start_time = time.time()

        result = optimized_manager.sync_financial_data_optimized(
            save_to_csv=True,
            save_to_db=True,
            codes=test_codes
        )

        end_time = time.time()
        duration = end_time - start_time

        # 验证结果
        print(f"\n📊 验证结果:")
        print(f"   ⏱️  执行时间: {duration:.1f}秒")
        print(f"   📈 目标股票: {result['total_stocks']} 只")
        print(f"   ✅ 成功获取: {result['success_count']} 只")
        print(f"   ⚪ 无数据: {result['no_data_count']} 只")
        print(f"   ❌ 技术错误: {result['error_count']} 只")
        print(f"   💾 实际保存: {result['records_count']} 条记录")
        print(f"   📊 技术成功率: {result['technical_success_rate']:.1f}%")

        # 验证关键指标
        if result['total_stocks'] == len(test_codes):
            print("✅ 股票数量匹配")
        else:
            print("❌ 股票数量不匹配")

        if result['technical_success_rate'] >= 80:
            print("✅ 技术成功率达标")
        else:
            print("⚠️ 技术成功率较低")

        if result['throughput_stocks_per_second'] > 0:
            print("✅ 吞吐量计算正常")
        else:
            print("❌ 吞吐量计算异常")

        # 性能对比
        avg_time_per_stock = duration / len(test_codes)
        print(f"\n⚡ 性能指标:")
        print(f"   - 平均耗时: {avg_time_per_stock:.2f}秒/股")
        print(f"   - 吞吐量: {result['throughput_stocks_per_second']:.2f}股/秒")

        # 结论
        print(f"\n🎯 验证结论:")
        if result['error_count'] == 0 and result['total_stocks'] > 0:
            print("✅ 优化接口工作正常，可以投入使用")
            return True
        else:
            print("⚠️ 优化接口存在问题，需要进一步调试")
            return False

    except Exception as e:
        print(f"❌ 验证失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        try:
            db_pool.return_connection(db_conn)
            print("✅ 资源清理完成")
        except:
            pass

if __name__ == '__main__':
    success = verify_optimized_interface()
    if success:
        print("\n🎉 验证通过！优化接口已准备就绪。")
        print("\n📖 使用方法:")
        print("   1. 完整优化版本: python run_optimized_full_sync_financial.py")
        print("   2. 快速优化版本: python run_financial_optimized.py")
    else:
        print("\n⚠️ 验证失败，请检查错误信息。")