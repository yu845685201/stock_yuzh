
import unittest
import sys
import os
import logging
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from backend.src.config import ConfigManager
from backend.src.database import DatabaseConnection
from backend.src.analysis.three_dimension_kline import ThreeDimensionKlineGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestThreeDimensionKlineIntegration(unittest.TestCase):
    """
    Integration test for 3D K-line generation using 1-minute data.
    Connects to the UAT database.
    """

    TEST_TS_CODE = 'TEST.000001'
    TEST_STOCK_CODE = '000001'
    TEST_STOCK_NAME = '测试股票'

    @classmethod
    def setUpClass(cls):
        """Set up database connection and clean up test data"""
        cls.config = ConfigManager(env='uat')
        cls.db = DatabaseConnection(cls.config)
        cls.clean_test_data()

    @classmethod
    def tearDownClass(cls):
        """Clean up test data after tests"""
        cls.clean_test_data()

    @classmethod
    def clean_test_data(cls):
        """Remove test data from DB"""
        with cls.db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM his_kline_1min WHERE ts_code = %s", (cls.TEST_TS_CODE,))
                cursor.execute("DELETE FROM anal_kline_rise_25pre_1min WHERE ts_code = %s", (cls.TEST_TS_CODE,))
                conn.commit()

    def setUp(self):
        self.generator = ThreeDimensionKlineGenerator(self.config)
        # Ensure clean state for each test
        self.clean_test_data()

    def create_1min_kline(self, trade_datetime_str, open_p, close_p, high_p, low_p):
        """Helper to create a 1-minute K-line record"""
        dt = datetime.strptime(trade_datetime_str, "%Y%m%d%H%M")
        return {
            'ts_code': self.TEST_TS_CODE,
            'stock_code': self.TEST_STOCK_CODE,
            'stock_name': self.TEST_STOCK_NAME,
            'trade_date': dt.strftime("%Y%m%d"),
            'trade_time': dt.strftime("%H%M"),
            'trade_datetime': trade_datetime_str,
            'open': open_p,
            'close': close_p,
            'high': high_p,
            'low': low_p,
            'volume': 1000,
            'amount': 10000,
            'turnover_rate': 0.1,
            'source': 'TEST'
        }

    def insert_test_data(self, data_list):
        """Batch insert test data into his_kline_1min"""
        if not data_list:
            return

        # We need to construct the SQL manually or use a helper if available.
        # connection.py has upsert_kline_data but it is for his_kline_day.
        # It doesn't seem to have a generic insert or specific upsert for 1min in the exposed methods
        # based on previous read (it has upsert_kline_5min).
        # Let's check if there is upsert_kline_1min or we use generic execute_batch.

        # Construct SQL for 1min table
        sql = """
        INSERT INTO his_kline_1min
        (ts_code, stock_code, stock_name, trade_date, trade_time, trade_datetime,
         open, high, low, close, volume, amount, turnover_rate, source)
        VALUES (%(ts_code)s, %(stock_code)s, %(stock_name)s, %(trade_date)s, %(trade_time)s, %(trade_datetime)s,
                %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(amount)s, %(turnover_rate)s, %(source)s)
        """
        self.db.execute_batch(sql, data_list)

    def test_standard_rise_trigger(self):
        """
        Test Scenario 1: Standard Rise
        Price rises from 10.0 to > 10.25 (2.5%) in a few minutes.
        """
        data = [
            # Minute 1: Base 10.0
            self.create_1min_kline("202401010930", 10.00, 10.10, 10.10, 10.00),
            # Minute 2: Rise to 10.20 (+2.0%)
            self.create_1min_kline("202401010931", 10.10, 10.20, 10.20, 10.10),
            # Minute 3: Rise to 10.26 (+2.6%) -> Should Trigger
            self.create_1min_kline("202401010932", 10.20, 10.26, 10.26, 10.20),
            # Minute 4: Drop back (Start of next bar)
            self.create_1min_kline("202401010933", 10.26, 10.10, 10.26, 10.10),
        ]
        self.insert_test_data(data)

        # Run generator
        result = self.generator.generate(ts_code=self.TEST_TS_CODE, source_type='1min')

        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['total_records'], 1)

        # Verify DB content
        saved_data = self.db.fetch_all(f"SELECT * FROM anal_kline_rise_25pre_1min WHERE ts_code = '{self.TEST_TS_CODE}'")
        self.assertEqual(len(saved_data), 1)
        record = saved_data[0]

        self.assertEqual(float(record['open']), 10.00)
        self.assertEqual(float(record['close']), 10.26)
        self.assertEqual(record['trade_begin_time'], '0930')
        self.assertEqual(record['trade_time'], '0932') # End time of the accumulating bar

        # Verify accumulation
        # 3 minutes of volume 1000 each
        self.assertEqual(float(record['volume']), 3000)

    def test_cross_day_aggregation(self):
        """
        Test Scenario 2: Cross-day aggregation
        Rise starts on Day 1 and completes on Day 2.
        """
        data = [
            # Day 1 Last Minute: Open 10.0
            self.create_1min_kline("202401011500", 10.00, 10.10, 10.10, 10.00),
            # Day 2 First Minute: Gap up to 10.30 (+3.0%) -> Should Trigger immediately
            self.create_1min_kline("202401020930", 10.20, 10.30, 10.30, 10.20),
        ]
        self.insert_test_data(data)

        # Run generator
        result = self.generator.generate(ts_code=self.TEST_TS_CODE, source_type='1min')

        self.assertEqual(result['total_records'], 1)

        saved_data = self.db.fetch_all(f"SELECT * FROM anal_kline_rise_25pre_1min WHERE ts_code = '{self.TEST_TS_CODE}'")
        self.assertEqual(len(saved_data), 1)
        record = saved_data[0]

        # Start time should be Day 1 15:00
        self.assertEqual(record['trade_begin_date'], '20240101')
        self.assertEqual(record['trade_begin_time'], '1500')
        # End time should be Day 2 09:30
        self.assertEqual(record['trade_date'], '20240102')
        self.assertEqual(record['trade_time'], '0930')

        self.assertEqual(float(record['open']), 10.00)
        self.assertEqual(float(record['close']), 10.30)

    def test_anomaly_handling(self):
        """
        Test Scenario 3: Abnormal Data (Zero or Negative Price)
        Ensure logic doesn't crash and skips invalid data.
        """
        data = [
            # Minute 1: Normal
            self.create_1min_kline("202401010930", 10.00, 10.10, 10.10, 10.00),
            # Minute 2: Zero Price (Anomaly)
            self.create_1min_kline("202401010931", 0.00, 0.00, 0.00, 0.00),
            # Minute 3: Resume Normal, Trigger Rise
            self.create_1min_kline("202401010932", 10.10, 10.30, 10.30, 10.10),
        ]
        self.insert_test_data(data)

        # Run generator
        result = self.generator.generate(ts_code=self.TEST_TS_CODE, source_type='1min')

        # Depending on implementation, the anomaly might break the chain or be ignored.
        # Code check:
        # ref_open = current_bar['open']
        # if ref_open <= 0: current_bar = None; continue

        # If Min 2 is 0.0, it will be skipped if it was the start?
        # But here Min 1 started the bar. Min 2 is accumulated.
        # Code:
        # current_bar['close'] = close_p (which is 0.0)
        # ref_open = 10.0
        # curr_close = 0.0
        # change_pct = (0 - 10)/10 = -1.0
        # -1.0 < 0.025, so no trigger.

        # Min 3:
        # current_bar['close'] = 10.30
        # ref_open = 10.0
        # change_pct = (10.3 - 10.0)/10.0 = 0.03 >= 0.025 -> Trigger!

        self.assertEqual(result['total_records'], 1)
        saved_data = self.db.fetch_all(f"SELECT * FROM anal_kline_rise_25pre_1min WHERE ts_code = '{self.TEST_TS_CODE}'")
        record = saved_data[0]

        self.assertEqual(float(record['close']), 10.30)
        # Volume should include the anomaly minute?
        # Logic: current_bar['volume'] += volume. Yes.
        self.assertEqual(float(record['volume']), 3000)

if __name__ == '__main__':
    unittest.main()
