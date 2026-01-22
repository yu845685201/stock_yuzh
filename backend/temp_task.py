import sys
import os
import logging
from datetime import datetime

# Add the backend directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from config.config_manager import ConfigManager
from database.connection import DatabaseConnection

def check_5min_data(db_conn):
    print("Checking 5min data for sh.600000 between 20230101 and 20230301...")
    sql = """
        SELECT count(*) as count
        FROM his_kline_5min
        WHERE ts_code = 'sh.600000'
        AND trade_date >= '20230101'
        AND trade_date <= '20230301'
    """
    try:
        result = db_conn.execute_query(sql)
        count = result[0]['count']
        print(f"Found {count} records.")
        return count
    except Exception as e:
        print(f"Error checking 5min data: {e}")
        # Try to check if table exists
        try:
            db_conn.execute_query("SELECT 1 FROM his_kline_5min LIMIT 1")
        except Exception as table_e:
            print(f"Table his_kline_5min might not exist: {table_e}")
        return 0

def find_volatile_stock(db_conn):
    print("Finding a stock with high volatility in 2023...")
    # Calculate average daily range (high-low)/preclose for 2023
    # limiting to stocks with enough data points (e.g. > 100 days)
    # Using a safer division
    sql = """
        SELECT ts_code,
               STDDEV(close) as std_close,
               AVG((high - low) / NULLIF(preclose, 0)) as avg_amplitude,
               COUNT(*) as days
        FROM his_kline_day
        WHERE trade_date >= '20230101' AND trade_date <= '20231231'
        AND preclose > 0
        GROUP BY ts_code
        HAVING COUNT(*) > 50
        ORDER BY avg_amplitude DESC
        LIMIT 1
    """
    try:
        result = db_conn.execute_query(sql)
        if result:
            stock = result[0]
            print(f"Volatile stock found: {stock['ts_code']} (Avg Amplitude: {stock['avg_amplitude']:.4f}, Days: {stock['days']})")
            return stock['ts_code']
        else:
            print("No suitable stock found in his_kline_day.")
            return None
    except Exception as e:
        print(f"Error finding volatile stock: {e}")
        return None

def main():
    config_manager = ConfigManager(env='uat')
    db_conn = DatabaseConnection(config_manager)

    check_5min_data(db_conn)
    volatile_stock = find_volatile_stock(db_conn)

    if volatile_stock:
        print(f"VOLATILE_STOCK={volatile_stock}")

if __name__ == "__main__":
    main()
