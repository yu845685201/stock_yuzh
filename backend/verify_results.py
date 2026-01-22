
import sys
import os
import json
from decimal import Decimal

sys.path.append(os.getcwd())
from backend.src.config import ConfigManager
from backend.src.database import DatabaseConnection

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def verify_results():
    config = ConfigManager(env='uat')
    db = DatabaseConnection(config)

    ts_code = 'sh.000680'
    print(f"Verifying 3D K-line records for {ts_code}...")

    query = f"SELECT * FROM anal_kline_rise_25pre_1min WHERE ts_code = '{ts_code}' ORDER BY trade_datetime ASC"
    results = db.fetch_all(query)

    print(f"Found {len(results)} records:")
    for row in results:
        # Format for display
        print(f"[{row['trade_begin_datetime']} -> {row['trade_datetime']}] "
              f"Open: {row['open']}, Close: {row['close']}, "
              f"Change: {row['change_rate']}%, Volume: {row['volume']}")

if __name__ == "__main__":
    verify_results()
