"""
pytdx本地DAT文件读取测试脚本
测试读取sh.600000（浦发银行）的全量日K线数据
"""

import time
import pandas as pd
from datetime import datetime
from pytdx.reader import TdxDailyBarReader

def read_local_kline(stock_code='600000', market='sh', vipdoc_path='uat/vipdoc'):
    """
    读取本地通达信DAT文件中的日K线数据

    Args:
        stock_code: 股票代码（如'600000'）
        market: 市场代码（'sh'=上海，'sz'=深圳）
        vipdoc_path: 通达信数据根目录

    Returns:
        DataFrame: 包含所有K线数据
    """
    # 构建DAT文件路径
    dat_file = f"{vipdoc_path}/{market}/lday/{market}{stock_code}.day"

    print(f"正在读取本地DAT文件: {dat_file}")
    print("-" * 60)

    start_time = time.time()

    try:
        # 使用TdxDailyBarReader读取日K线数据
        reader = TdxDailyBarReader()
        df = reader.get_df(dat_file)

        read_duration = time.time() - start_time

        if df is not None and not df.empty:
            print(f"✅ 读取成功")
            print(f"   耗时: {read_duration:.3f}秒")
            print(f"   记录数: {len(df)}")
            return df
        else:
            print(f"⚠️ 文件为空或读取失败")
            return pd.DataFrame()

    except FileNotFoundError:
        print(f"❌ 文件不存在: {dat_file}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ 读取失败: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def analyze_data_quality(df):
    """
    分析数据质量

    Args:
        df: K线数据DataFrame
    """
    print("\n" + "=" * 60)
    print("数据质量分析")
    print("=" * 60)

    if df.empty:
        print("⚠️ 数据为空，无法分析")
        return

    # 1. 基本信息
    print(f"\n1. 数据范围:")
    print(f"   - 总记录数: {len(df)}")
    if 'date' in df.columns:
        print(f"   - 起始日期: {df['date'].min()}")
        print(f"   - 结束日期: {df['date'].max()}")
        print(f"   - 时间跨度: {(pd.to_datetime(df['date'].max()) - pd.to_datetime(df['date'].min())).days} 天")

    # 2. 字段信息
    print(f"\n2. 数据字段:")
    print(f"   字段列表: {list(df.columns)}")
    print(f"   字段数量: {len(df.columns)}")

    # 3. 数据完整性
    print(f"\n3. 数据完整性:")
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = null_count / len(df) * 100
        zero_count = (df[col] == 0).sum() if col not in ['date'] else 0
        zero_pct = zero_count / len(df) * 100 if col not in ['date'] else 0
        print(f"   - {col}: 缺失 {null_count} 条 ({null_pct:.2f}%), 零值 {zero_count} 条 ({zero_pct:.2f}%)")

    # 4. 数据样本
    print(f"\n4. 最新10条数据样本:")
    print(df.head(10).to_string())

    # 5. 数据类型
    print(f"\n5. 数据类型:")
    print(df.dtypes)

    # 6. 数值统计
    print(f"\n6. 数值字段统计:")
    numeric_cols = df.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns
    if len(numeric_cols) > 0:
        print(df[numeric_cols].describe())

    # 7. 数据完整性检查（对比我们需要的字段）
    print(f"\n7. 字段对照分析:")
    required_fields = {
        'date': '日期',
        'open': '开盘价',
        'high': '最高价',
        'low': '最低价',
        'close': '收盘价',
        'amount': '成交额',
        'volume': '成交量',
    }

    print("\n   pytdx DAT文件提供的字段 vs 我们需要的字段:")
    print("   " + "-" * 56)
    print(f"   {'字段名':<15} {'说明':<10} {'是否提供':<10}")
    print("   " + "-" * 56)

    for field, desc in required_fields.items():
        is_available = "✅ 有" if field in df.columns else "❌ 无"
        print(f"   {field:<15} {desc:<10} {is_available:<10}")

    # 缺失的关键字段
    missing_fields = {
        'preclose': '昨收价',
        'change_rate': '涨跌幅(%)',
        'turnover_rate': '换手率(%)',
        'pe_ttm': '市盈率TTM',
        'pb_rate': '市净率',
        'ps_ttm': '市销率TTM',
        'pcf_ttm': '市现率TTM',
        'total_share': '总股本(万股)',
        'float_share': '流通股本(万股)',
        'is_st': 'ST标识',
        'trade_status': '交易状态',
    }

    print("\n   我们需要但pytdx不提供的字段:")
    print("   " + "-" * 56)
    print(f"   {'字段名':<20} {'说明':<15} {'状态':<10}")
    print("   " + "-" * 56)

    for field, desc in missing_fields.items():
        print(f"   {field:<20} {desc:<15} {'❌ 缺失':<10}")

    print(f"\n   ⚠️ 总结: pytdx提供 {len([f for f in required_fields if f in df.columns])} 个字段，")
    print(f"           缺失 {len(missing_fields)} 个关键字段（需要其他数据源补充）")


def save_to_csv(df, filename='pytdx_dat_sh600000_daily_kline.csv'):
    """
    保存数据到CSV文件

    Args:
        df: K线数据DataFrame
        filename: 输出文件名
    """
    if df.empty:
        print("\n⚠️ 数据为空，不保存文件")
        return

    try:
        # 按日期排序（降序，最新数据在前）
        if 'date' in df.columns:
            df_sorted = df.sort_values('date', ascending=False)
        else:
            df_sorted = df

        # 保存到CSV
        df_sorted.to_csv(filename, index=False, encoding='utf-8-sig')

        # 获取文件大小
        import os
        file_size = os.path.getsize(filename)

        print(f"\n✅ 数据已保存到: {filename}")
        print(f"   文件大小: {file_size / 1024:.2f} KB")
        print(f"   记录数: {len(df_sorted)}")
    except Exception as e:
        print(f"\n❌ 保存CSV失败: {e}")


def test_batch_read():
    """测试批量读取多只股票的性能"""
    print("\n" + "=" * 60)
    print("批量读取性能测试（100只股票）")
    print("=" * 60)

    # 测试股票列表（上海前100只）
    test_stocks = [f"{600000 + i:06d}" for i in range(100)]

    successful = 0
    failed = 0
    total_records = 0

    start_time = time.time()

    for idx, stock_code in enumerate(test_stocks, 1):
        try:
            reader = TdxDailyBarReader()
            dat_file = f"uat/vipdoc/sh/lday/sh{stock_code}.day"
            df = reader.get_df(dat_file)

            if df is not None and not df.empty:
                successful += 1
                total_records += len(df)
                if idx % 10 == 0:
                    print(f"进度: {idx}/100 ({idx}%), 成功: {successful}, 失败: {failed}")
            else:
                failed += 1
        except:
            failed += 1

    total_duration = time.time() - start_time

    print("\n测试结果:")
    print(f"  - 总耗时: {total_duration:.2f}秒")
    print(f"  - 平均每只: {total_duration / 100:.3f}秒")
    print(f"  - 成功: {successful}只")
    print(f"  - 失败: {failed}只")
    print(f"  - 总记录数: {total_records}条")

    # 性能推算
    print(f"\n性能推算:")
    avg_time_per_stock = total_duration / 100
    estimated_5000 = avg_time_per_stock * 5000
    print(f"  - 采集5000只股票预计耗时: {estimated_5000:.1f}秒 = {estimated_5000/60:.1f}分钟")


def main():
    """主函数"""
    print("=" * 60)
    print("pytdx本地DAT文件读取测试 - sh.600000")
    print("=" * 60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    start_time = time.time()

    try:
        # 1. 读取单只股票数据
        df = read_local_kline(stock_code='600000', market='sh', vipdoc_path='uat/vipdoc')

        # 2. 分析数据质量
        analyze_data_quality(df)

        # 3. 保存到CSV
        save_to_csv(df, 'pytdx_dat_sh600000_daily_kline.csv')

        # 4. 批量读取性能测试
        test_batch_read()

        # 5. 总耗时
        total_duration = time.time() - start_time
        print("\n" + "=" * 60)
        print(f"✅ 测试完成，总耗时: {total_duration:.2f}秒")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
