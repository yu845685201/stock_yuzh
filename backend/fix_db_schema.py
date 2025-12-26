#!/usr/bin/env python3
"""
修复数据库表结构 - 重新创建表以匹配代码定义
"""

import psycopg2
import sys

# 数据库连接配置
db_config = {
    'host': '127.0.0.1',
    'port': 5432,
    'user': 'postgres',
    'password': 'yuzh1234',
    'database': 'stock_analysis_uat'
}

def fix_table_schema():
    """修复base_stock_info表结构"""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        print("=== 删除旧表 ===")
        cursor.execute("DROP TABLE IF EXISTS base_stock_info CASCADE")
        print("旧表已删除")

        print("\n=== 重新创建表结构 ===")
        create_table_sql = """
        CREATE TABLE base_stock_info (
            id BIGSERIAL PRIMARY KEY,
            ts_code VARCHAR(20),
            stock_code VARCHAR(20),
            stock_name VARCHAR(20),
            cnspell VARCHAR(10),
            market_code VARCHAR(5),
            market_name VARCHAR(20),
            exchange_code VARCHAR(10),
            sector_code VARCHAR(20),
            sector_name VARCHAR(20),
            industry_code VARCHAR(20),
            industry_name VARCHAR(20),
            list_status VARCHAR(2),
            list_date DATE,
            delist_date DATE,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        cursor.execute(create_table_sql)
        print("表结构已重新创建")

        print("\n=== 创建唯一约束 ===")
        cursor.execute("ALTER TABLE base_stock_info ADD CONSTRAINT uk_base_stock_info_code UNIQUE (ts_code)")
        print("唯一约束已创建")

        print("\n=== 创建索引 ===")
        indexes = [
            "CREATE INDEX idx_base_stock_info_code ON base_stock_info(stock_code)",
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        print("索引已创建")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n=== 表结构修复完成 ===")

    except Exception as e:
        print(f"修复表结构失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    fix_table_schema()