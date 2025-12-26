#!/usr/bin/env python3
"""
原始数据导出工具 - 使用pytdx解析逻辑原封不动输出CSV
用于数据真实性核对
"""

import sys
import os
import csv
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config import ConfigManager
from src.data_sources.pytdx_source import PytdxSource
from datetime import date

def export_daily_data_to_csv():
    """导出日K线数据到CSV"""
    print("🔄 开始导出日K线数据...")

    # 初始化
    config_manager = ConfigManager()
    pytdx = PytdxSource(config_manager.get('data_sources.pytdx'))

    if not pytdx.connect():
        print("❌ 无法连接到Pytdx数据源")
        return False

    try:
        # 解析sh000001.day文件
        print("📁 解析文件: /Users/yuzh/develop/ai/claude/claude-code/workspace/stock_yuzh/uat/vipdoc/sh/lday/sh000001.day")

        # 获取所有数据（不设置日期限制）
        daily_data = pytdx.get_daily_data('000001', None, None)

        if not daily_data:
            print("❌ 无法获取日K线数据")
            return False

        print(f"✅ 获取到 {len(daily_data)} 条日K线数据")

        # 导出到CSV
        csv_file = 'raw_daily_data_sh000001.csv'
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            if daily_data:
                # 获取所有记录的所有字段名（避免字段不匹配）
                all_fieldnames = set()
                for record in daily_data:
                    all_fieldnames.update(record.keys())
                fieldnames = sorted(all_fieldnames)

                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                # 写入所有数据
                for record in daily_data:
                    writer.writerow(record)

        print(f"✅ 日K线数据已导出到: {csv_file}")
        print(f"   前3条记录:")
        for i, record in enumerate(daily_data[:3]):
            print(f"     {i+1}. {record['trade_date']}: 开{record['open']} 高{record['high']} 低{record['low']} 收{record['close']} 量{record['volume']}")

        return True

    except Exception as e:
        print(f"❌ 导出日K线数据失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        pytdx.disconnect()

def export_5min_data_to_csv():
    """导出5分钟K线数据到CSV"""
    print("\n🔄 开始导出5分钟K线数据...")

    # 初始化
    config_manager = ConfigManager()
    pytdx = PytdxSource(config_manager.get('data_sources.pytdx'))

    if not pytdx.connect():
        print("❌ 无法连接到Pytdx数据源")
        return False

    try:
        # 解析sh000001.lc5文件
        print("📁 解析文件: /Users/yuzh/develop/ai/claude/claude-code/workspace/stock_yuzh/uat/vipdoc/sh/fzline/sh000001.lc5")

        # 获取所有数据（不设置日期限制）
        min5_data = pytdx.get_minute_data('000001', '5min', None, None)

        if not min5_data:
            print("❌ 无法获取5分钟K线数据")
            return False

        print(f"✅ 获取到 {len(min5_data)} 条5分钟K线数据")

        # 导出到CSV
        csv_file = 'raw_5min_data_sh000001.csv'
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            if min5_data:
                # 获取所有记录的所有字段名（避免字段不匹配）
                all_fieldnames = set()
                for record in min5_data:
                    all_fieldnames.update(record.keys())
                fieldnames = sorted(all_fieldnames)

                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                # 写入所有数据
                for record in min5_data:
                    writer.writerow(record)

        print(f"✅ 5分钟K线数据已导出到: {csv_file}")
        print(f"   前5条记录:")
        for i, record in enumerate(min5_data[:5]):
            print(f"     {i+1}. {record['trade_date']} {record['trade_time']}: 开{record['open']} 高{record['high']} 低{record['low']} 收{record['close']} 量{record['volume']}")

        return True

    except Exception as e:
        print(f"❌ 导出5分钟K线数据失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        pytdx.disconnect()

def main():
    """主函数"""
    print("🎯 原始数据导出工具 - pytdx解析结果")
    print("=" * 60)
    print("📋 说明：使用现有pytdx解析逻辑，原封不动输出CSV")
    print("📋 用途：数据真实性核对")
    print("=" * 60)

    success_count = 0

    # 导出日K线数据
    if export_daily_data_to_csv():
        success_count += 1

    # 导出5分钟K线数据
    if export_5min_data_to_csv():
        success_count += 1

    print("\n" + "=" * 60)
    print(f"🎯 导出完成: {success_count}/2 个文件")

    if success_count == 2:
        print("✅ 所有数据导出成功，请核对CSV文件内容")
        print("📁 生成的文件:")
        print("   - raw_daily_data_sh000001.csv (日K线数据)")
        print("   - raw_5min_data_sh000001.csv (5分钟K线数据)")
    else:
        print("❌ 部分数据导出失败，请检查错误信息")

    return success_count == 2

if __name__ == "__main__":
    success = main()
    print(f"\n🏁 最终结果: {'成功' if success else '失败'}")
    sys.exit(0 if success else 1)