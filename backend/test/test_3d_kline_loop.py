
import unittest
import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from backend.src.analysis.three_dimension_kline import ThreeDimensionKlineGenerator

class Test3DKlineLoop(unittest.TestCase):
    def setUp(self):
        self.generator = ThreeDimensionKlineGenerator()

    def test_continuous_generation_single_rise(self):
        """
        Test case where one huge rise should generate MULTIPLE 3D K-lines if applicable,
        OR just one if the logic is strictly "reset after trigger".

        If the requirement is "generate 1 3D k-line then reset baseline", then:
        10.0 -> 10.3 (Trigger 1) -> Reset Base to next bar's Open.

        The user says "Only generated 1 3D kline, didn't continue loop".

        Let's try a case where the loop might exit early or state is not reset correctly.
        """
        ts_code = "TEST.000001"

        # Data: Continuous rise for many minutes
        # 10.0 -> ... -> 11.0
        data = []
        price = 10.0
        for i in range(10):
            # Each minute rise 0.1
            # 10.0 -> 10.1 (1%)
            # 10.1 -> 10.2
            # 10.2 -> 10.3 (Trigger! 10.0->10.3 = 3% > 2.5%)
            # Next base should be 10.3 (next open)
            # 10.3 -> 10.4
            # ... -> 10.6 (Trigger! 10.3->10.6 = 2.9%)

            data.append({
                'ts_code': ts_code, 'stock_code': '000001', 'stock_name': 'Test',
                'trade_date': '20240101', 'trade_time': f"093{i}", 'trade_datetime': f"20240101093{i}",
                'open': price, 'high': price + 0.1, 'low': price, 'close': price + 0.1,
                'volume': 100, 'amount': 1000
            })
            price += 0.1

        result = self.generator._calculate_3d_klines(data, ts_code)

        print(f"\nGenerated {len(result)} records")
        for r in result:
            print(f"Time: {r['trade_begin_time']} -> {r['trade_time']}, Open: {r['open']}, Close: {r['close']}, Change: {r['change_rate']}%")

        # 0: 10.0 -> 10.1
        # 1: 10.1 -> 10.2
        # 2: 10.2 -> 10.3 (Trigger 1, Base 10.0, Close 10.3, Change 3.0%)
        # --- Reset ---
        # 3: 10.3 -> 10.4 (New Base 10.3)
        # 4: 10.4 -> 10.5
        # 5: 10.5 -> 10.6 (Trigger 2, Base 10.3, Close 10.6, Change 2.9%)
        # --- Reset ---
        # 6: 10.6 -> 10.7 (New Base 10.6)
        # 7: 10.7 -> 10.8
        # 8: 10.8 -> 10.9 (Trigger 3, Base 10.6, Close 10.9, Change 2.8%)

        self.assertTrue(len(result) >= 3, f"Expected at least 3 records, got {len(result)}")

    def test_gap_down_and_rise(self):
        """
        Test scenario: Rise -> Trigger -> Gap Down -> Rise -> Trigger
        """
        ts_code = "TEST.000002"
        data = [
            # Bar 1: 10.0 -> 10.3 (Trigger 1)
            {'ts_code': ts_code, 'stock_code': '000002', 'stock_name': 'GapTest',
             'trade_date': '20240101', 'trade_time': '0930', 'trade_datetime': '202401010930',
             'open': 10.0, 'high': 10.3, 'low': 10.0, 'close': 10.3, 'volume': 100, 'amount': 1000},

            # Bar 2: Gap Down to 9.0 (New Base), then rise to 9.3 (Trigger 2: (9.3-9.0)/9.0 = 3.3%)
            {'ts_code': ts_code, 'stock_code': '000002', 'stock_name': 'GapTest',
             'trade_date': '20240101', 'trade_time': '0931', 'trade_datetime': '202401010931',
             'open': 9.0, 'high': 9.3, 'low': 9.0, 'close': 9.3, 'volume': 100, 'amount': 1000},
        ]

        result = self.generator._calculate_3d_klines(data, ts_code)

        print(f"\nGap Down Test: Generated {len(result)} records")
        for r in result:
             print(f"Time: {r['trade_begin_time']} -> {r['trade_time']}, Open: {r['open']}, Close: {r['close']}, Change: {r['change_rate']}%")

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['change_rate'], 3.0)
        self.assertEqual(result[1]['change_rate'], 3.3333)

if __name__ == '__main__':
    unittest.main()
