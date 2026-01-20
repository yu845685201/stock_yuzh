
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from backend.src.sync.daily_kline_manager import DailyKlineSyncManager
from backend.src.models.collection_result import CollectionResult

class TestDailyKlineSyncManager(unittest.TestCase):

    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.get.return_value = {}

        # Patching dependencies
        self.patcher_db = patch('backend.src.sync.daily_kline_manager.DatabaseConnection')
        self.patcher_baostock = patch('backend.src.sync.daily_kline_manager.ThreadSafeBaostockSource')
        self.patcher_csv = patch('backend.src.sync.daily_kline_manager.CsvWriter')

        self.MockDatabaseConnection = self.patcher_db.start()
        self.MockBaostockSource = self.patcher_baostock.start()
        self.MockCsvWriter = self.patcher_csv.start()

        self.mock_db_instance = self.MockDatabaseConnection.return_value
        self.mock_baostock_instance = self.MockBaostockSource.return_value
        self.mock_csv_instance = self.MockCsvWriter.return_value

        self.manager = DailyKlineSyncManager(self.mock_config)

    def tearDown(self):
        self.patcher_db.stop()
        self.patcher_baostock.stop()
        self.patcher_csv.stop()

    def test_get_date_range_init_mode(self):
        """Test date range calculation for init mode"""
        start, end = self.manager._get_date_range(init_mode=True, start_date=None, end_date=None)
        self.assertEqual(start, '1990-12-19')
        self.assertEqual(end, datetime.now().strftime('%Y-%m-%d'))

    def test_get_date_range_specified(self):
        """Test date range calculation for specified dates"""
        start, end = self.manager._get_date_range(init_mode=False, start_date='2023-01-01', end_date='2023-01-31')
        self.assertEqual(start, '2023-01-01')
        self.assertEqual(end, '2023-01-31')

    def test_validate_data_anomaly(self):
        """Test validation logic detecting anomalies"""
        stock = {'ts_code': 'sz.000001', 'stock_code': '000001', 'stock_name': '平安银行'}

        # Data with > 10.1% change for main board
        data = [
            {'trade_date': '20230101', 'pct_chg': 10.2, 'is_st': '0'},
            {'trade_date': '20230102', 'pct_chg': 5.0, 'is_st': '0'}
        ]

        self.manager._validate_data(data, stock)

        self.assertEqual(len(self.manager.anomalies), 1)
        self.assertEqual(self.manager.anomalies[0]['pct_chg'], 10.2)
        self.assertEqual(self.manager.anomalies[0]['limit'], 10.1)

    def test_validate_data_st_anomaly(self):
        """Test validation logic detecting anomalies for ST stocks"""
        stock = {'ts_code': 'sz.000001', 'stock_code': '000001', 'stock_name': 'ST平安'}

        # Data with > 5.1% change for ST stock
        data = [
            {'trade_date': '20230101', 'pct_chg': 5.2, 'is_st': '1'}
        ]

        self.manager._validate_data(data, stock)

        self.assertEqual(len(self.manager.anomalies), 1)
        self.assertEqual(self.manager.anomalies[0]['limit'], 5.1)

    def test_execute_sync_flow(self):
        """Test main execution flow"""
        # Setup mocks
        self.mock_baostock_instance.connect.return_value = True
        self.mock_db_instance.execute_query.return_value = [
            {'ts_code': 'sz.000001', 'stock_code': '000001', 'stock_name': 'Test', 'list_date': None}
        ]

        mock_k_data = [{'trade_date': '20230101', 'close': 10.0}]
        self.mock_baostock_instance.get_daily_k_data.return_value = mock_k_data

        # Execute
        result = self.manager.execute_sync(start_date='2023-01-01', end_date='2023-01-02', save_to_csv=True, save_to_db=True)

        # Assertions
        self.mock_baostock_instance.connect.assert_called_once()
        self.mock_baostock_instance.get_daily_k_data.assert_called()
        self.mock_csv_instance.write_daily_kline_data.assert_called()
        self.mock_db_instance.upsert_kline_data.assert_called()
        self.assertEqual(result['successful'], 1)

if __name__ == '__main__':
    unittest.main()
