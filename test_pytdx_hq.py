"""
pytdx HQ接口测试脚本
测试采集sh.600000（浦发银行）的全量日K线数据
"""

import time
import socket
import pandas as pd
from datetime import datetime
from pytdx.hq import TdxHq_API

# 设置全局socket超时
socket.setdefaulttimeout(10)

# 通达信服务器列表（更新为最新可用服务器）
PYTDX_SERVERS = [
    ('119.147.212.81', 7709),   # 招商证券深圳
    ('106.14.95.149', 7709),    # 通用服务器1
    ('113.105.73.88', 7709),    # 通用服务器2
    ('221.231.141.60', 7709),   # 华泰证券南京
    ('101.227.73.20', 7709),    # 华泰证券上海电信
    ('101.227.77.254', 7709),   # 华泰证券上海电信2
    ('14.215.128.18', 7709),    # 华泰证券深圳电信
    ('59.173.18.140', 7709),    # 华泰证券武汉电信
    ('60.28.23.80', 7709),      # 华泰证券天津联通
    ('218.60.29.136', 7709),    # 华泰证券沈阳联通
    ('122.192.35.44', 7709),    # 华泰证券南京联通
]

def fetch_all_klines(stock_code='600000', market=1):
    """
    获取股票的全量日K线数据

    Args:
        stock_code: 股票代码（如'600000'）
        market: 市场代码（0=深圳，1=上海）

    Returns:
        DataFrame: 包含所有K线数据
    """
    # 使用auto_retry增强连接稳定性
    api = TdxHq_API(auto_retry=True, raise_exception=False)
    all_data = []

    # 尝试连接服务器
    connected = False
    successful_server = None

    print(f"\n开始尝试连接通达信服务器（共{len(PYTDX_SERVERS)}个）...")
    print("-" * 60)

    for idx, (server_host, server_port) in enumerate(PYTDX_SERVERS, 1):
        try:
            print(f"[{idx}/{len(PYTDX_SERVERS)}] 正在连接: {server_host}:{server_port} ...", end=' ')

            # 尝试连接（10秒超时）
            if api.connect(server_host, server_port):
                print("✅ 连接成功")
                connected = True
                successful_server = f"{server_host}:{server_port}"
                break
            else:
                print("❌ 连接失败（返回False）")
        except socket.timeout:
            print("❌ 连接超时（10秒）")
        except Exception as e:
            print(f"❌ 连接失败: {type(e).__name__}: {e}")
            continue

    if not connected:
        print("\n" + "=" * 60)
        print("❌ 所有服务器连接失败")
        print("=" * 60)
        print("\n可能原因：")
        print("1. 网络防火墙阻止了7709端口的访问")
        print("2. 代理设置问题（如果使用了HTTP/HTTPS代理）")
        print("3. 服务器IP地址已过期或不可用")
        print("\n建议：")
        print("1. 检查网络连接和防火墙设置")
        print("2. 如果在公司网络，可能需要配置代理或使用VPN")
        print("3. 尝试使用本地通达信DAT文件（如果有）")
        raise Exception("所有服务器连接失败")

    print(f"\n✅ 使用服务器: {successful_server}")
    print("-" * 60)

    try:
        # 分批获取数据（每次最多800条）
        start_pos = 0
        batch_size = 800
        batch_count = 0

        print(f"\n开始采集 sh.{stock_code} 的日K线数据...")
        print(f"批次大小: {batch_size}条/次")
        print("-" * 60)

        while True:
            batch_start_time = time.time()

            # 获取日K线数据
            # category=9: 日K线
            data = api.get_security_bars(
                category=9,
                market=market,
                code=stock_code,
                start=start_pos,
                count=batch_size
            )

            batch_duration = time.time() - batch_start_time

            if not data or len(data) == 0:
                print(f"\n✅ 数据采集完成，共{batch_count}批次")
                break

            batch_count += 1
            all_data.extend(data)

            print(f"批次 {batch_count}: 位置 {start_pos}-{start_pos+len(data)-1}, "
                  f"获取 {len(data)} 条, 耗时 {batch_duration:.3f}秒")

            # 如果返回数据少于batch_size，说明已经到末尾
            if len(data) < batch_size:
                print(f"\n✅ 数据采集完成（最后一批），共{batch_count}批次")
                break

            start_pos += batch_size
            time.sleep(0.1)  # 避免请求过快

        # 转换为DataFrame
        if all_data:
            df = api.to_df(all_data)
            print(f"\n总计获取: {len(df)} 条K线数据")
            return df
        else:
            print("\n⚠️ 未获取到任何数据")
            return pd.DataFrame()

    finally:
        api.disconnect()
        print("已断开服务器连接")


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
    print(f"   - 起始日期: {df['datetime'].min()}")
    print(f"   - 结束日期: {df['datetime'].max()}")
    print(f"   - 时间跨度: {(pd.to_datetime(df['datetime'].max()) - pd.to_datetime(df['datetime'].min())).days} 天")

    # 2. 字段信息
    print(f"\n2. 数据字段:")
    print(f"   字段列表: {list(df.columns)}")
    print(f"   字段数量: {len(df.columns)}")

    # 3. 数据完整性
    print(f"\n3. 数据完整性:")
    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_pct = null_count / len(df) * 100
        print(f"   - {col}: 缺失 {null_count} 条 ({null_pct:.2f}%)")

    # 4. 数据样本
    print(f"\n4. 最新5条数据样本:")
    print(df.head(5).to_string())

    # 5. 数据类型
    print(f"\n5. 数据类型:")
    print(df.dtypes)

    # 6. 数值统计
    print(f"\n6. 数值字段统计:")
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    if len(numeric_cols) > 0:
        print(df[numeric_cols].describe())


def save_to_csv(df, filename='pytdx_sh600000_daily_kline.csv'):
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
        # 按日期排序（升序）
        df_sorted = df.sort_values('datetime', ascending=True)

        # 保存到CSV
        df_sorted.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n✅ 数据已保存到: {filename}")
        print(f"   文件大小: {pd.io.common.file_size(filename) / 1024:.2f} KB")
    except Exception as e:
        print(f"\n❌ 保存CSV失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("pytdx HQ接口测试 - 采集sh.600000全量日K线")
    print("=" * 60)
    print(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    start_time = time.time()

    try:
        # 1. 采集数据
        df = fetch_all_klines(stock_code='600000', market=1)

        # 2. 分析数据质量
        analyze_data_quality(df)

        # 3. 保存到CSV
        save_to_csv(df, 'pytdx_sh600000_daily_kline.csv')

        # 4. 总耗时
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
