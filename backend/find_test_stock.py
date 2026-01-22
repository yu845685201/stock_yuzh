
import sys
import os
from backend.src.config import ConfigManager
from backend.src.database import DatabaseConnection

def find_volatile_stock():
    # Initialize config and db connection
    config = ConfigManager(env='uat')
    db = DatabaseConnection(config)

    print("Searching for a stock with > 5% daily range in his_kline_1min...")

    # Get a list of stocks to check (limit to first 50 to save time)
    ts_codes_res = db.fetch_all("SELECT DISTINCT ts_code FROM his_kline_1min LIMIT 50")
    ts_codes = [x['ts_code'] for x in ts_codes_res]

    found_stock = None

    for code in ts_codes:
        # Get all 1min data for this stock
        # We process in memory to find a volatile day
        query = f"SELECT trade_date, open, close, high, low FROM his_kline_1min WHERE ts_code = '{code}' ORDER BY trade_datetime ASC"
        data = db.fetch_all(query)

        if not data:
            continue

        # Group by date
        daily_data = {}
        for row in data:
            d = row['trade_date']
            if d not in daily_data:
                daily_data[d] = {'high': -float('inf'), 'low': float('inf')}

            h = float(row['high'])
            l = float(row['low'])
            daily_data[d]['high'] = max(daily_data[d]['high'], h)
            daily_data[d]['low'] = min(daily_data[d]['low'], l)

        # Check volatility
        for date, values in daily_data.items():
            if values['low'] > 0:
                rise = (values['high'] - values['low']) / values['low']
                if rise > 0.05: # > 5% rise
                    print(f"FOUND: {code} on {date} had a range of {rise*100:.2f}%")
                    found_stock = {'ts_code': code, 'date': date}
                    break

        if found_stock:
            break

    if not found_stock:
        print("No stock with > 5% daily range found in the sample.")
        # Fallback to any stock just to test the mechanism
        if ts_codes:
            print(f"Fallback: Using {ts_codes[0]} for testing (even if volatility is low)")
            return ts_codes[0], None
        return None, None

    return found_stock['ts_code'], found_stock['date']

if __name__ == "__main__":
    find_volatile_stock()
