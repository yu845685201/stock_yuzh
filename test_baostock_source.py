
import sys
import os

# 添加项目根目录到python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'backend/src')))

from backend.src.data_sources.baostock_source import BaostockSource
from datetime import datetime, timedelta, date

def test_baostock_source():
    config = {
        'financial_data_rate_limit': {'enabled': False}
    }
    source = BaostockSource(config)

    if not source.connect():
        print("Failed to connect to Baostock")
        return

    end_date = date.today()
    start_date = end_date - timedelta(days=7)
    code = "sz.000001"

    print(f"Testing get_5min_data for {code} from {start_date} to {end_date}")

    data = source.get_5min_data(code, start_date, end_date)

    print(f"Collected {len(data)} records")
    if data:
        print("First record structure:")
        first = data[0]
        for k, v in first.items():
            print(f"  {k}: {v}")

        print("\nChecking key fields:")
        print(f"  change_rate: {first.get('change_rate')}")
        print(f"  turnover_rate: {first.get('turnover_rate')}")
        print(f"  preclose: {first.get('preclose')}")

    source.disconnect()

if __name__ == "__main__":
    test_baostock_source()
