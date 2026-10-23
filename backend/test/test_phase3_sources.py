"""第三阶段数据源单测：宏观（东财+金十）/ ETF 期权（SSE）/ 北向个股明细（东财）。

全部直连 HTTP 采集、**不依赖 akshare 库**（用户明确要求）。测试分两层：
- 纯函数：parse_* 行解析、_parse_contract_id、_merge_channels（无网络）；
- 契约：指标注册表完整性、样本取自 2026-10-23 实测响应。
"""

from src.sync.hkex_connect_manager import NorthFlowDetailManager


class TestMacroRegistry:
    """指标注册表契约（报告层只读渲染；新增指标必须登记）"""

    def test_indicator_meta_complete(self):
        from src.data_sources.macro_source import INDICATOR_META
        assert len(INDICATOR_META) == 17
        for code, meta in INDICATOR_META.items():
            name, unit, freq = meta
            assert name and freq in ('日', '月')

    def test_families_covered(self):
        from src.data_sources.macro_source import INDICATOR_META
        families = {c.split('_')[0] for c in INDICATOR_META}
        assert {'LPR', 'SHIBOR', 'PMI', 'M1', 'M2', 'SOCIAL', 'CPI',
                'PPI'} <= families

    def test_num_clean(self):
        from src.data_sources.macro_source import _num
        assert _num('1.2612') == 1.2612
        assert _num('574,056') == 574056.0
        assert _num(None) is None
        assert _num('--') is None
        assert _num('') is None


class TestSseOptionParse:
    """SSE 期权解析（纯函数；样本取自 2026-10-23 实测响应）"""

    def test_parse_risk_row(self):
        from src.data_sources.sse_option_source import parse_risk_row
        r = parse_risk_row({
            'SECURITY_ID': '10007437', 'TRADE_DATE': '2024-06-26',
            'CONTRACT_SYMBOL': '50ETF购7月2250',
            'CONTRACT_ID': '510050C2407M02250',
            'DELTA_VALUE': '0.954', 'THETA_VALUE': '-0.140',
            'GAMMA_VALUE': '0.783', 'VEGA_VALUE': '0.065',
            'RHO_VALUE': '0.163', 'IMPLC_VOLATLTY': '0.182'})
        assert r['contract_code'] == '10007437'
        assert r['trade_date'] == '20240626'
        assert r['underlying_code'] == '510050' and r['call_put'] == '购'
        assert r['iv'] == 0.182 and r['delta'] == 0.954
        assert r['theta'] == -0.140

    def test_parse_stats_row_thousands(self):
        from src.data_sources.sse_option_source import parse_stats_row
        s = parse_stats_row({
            'SECURITY_CODE': '510050', 'TRADE_DATE': '2026-10-22',
            'SECURITY_ABBR': '上证50ETF华夏', 'CONTRACT_VOLUME': '102',
            'TOTAL_MONEY': '36,793', 'TOTAL_VOLUME': '1,041,617',
            'CALL_VOLUME': '574,056', 'PUT_VOLUME': '467,561',
            'CP_RATE': '81.45', 'LEAVES_QTY': '1,504,817',
            'LEAVES_CALL_QTY': '968,875', 'LEAVES_PUT_QTY': '535,942'})
        assert s['underlying_code'] == '510050'
        assert s['contract_count'] == 102
        assert s['total_amount'] == 36793.0       # 千分位清洗
        assert s['call_volume'] == 574056.0
        assert s['put_call_ratio'] == 81.45

    def test_parse_contract_row(self):
        from src.data_sources.sse_option_source import parse_contract_row
        m = parse_contract_row({
            'SECURITY_ID': '10012535', 'CONTRACT_SYMBOL': '50ETF购10月2700',
            'CONTRACT_ID': '510050C2610M02700', 'CALL_OR_PUT': '认购',
            'EXERCISE_PRICE': '2.700', 'END_DATE': '20261028',
            'SECURITYNAMEBYID': '50ETF(510050)'})
        assert m['underlying_code'] == '510050'
        assert m['underlying_name'] == '50ETF'
        assert m['call_put'] == '购'
        assert m['strike'] == 2.7                  # EXERCISE_PRICE 权威
        assert m['expiry_date'] == '20261028'

    def test_parse_contract_id_encoding(self):
        from src.data_sources.sse_option_source import _parse_contract_id
        assert _parse_contract_id('510050C2610M02700') == ('510050', '购')
        assert _parse_contract_id('510300P2606M04000') == ('510300', '沽')
        u, cp, exp, strike = _parse_contract_id('510050C2610M02700',
                                                full=True)
        assert (u, cp, exp, strike) == ('510050', '购', '202610', 2.7)
        # 非法/短 ID → None 禁硬造
        assert _parse_contract_id('') == (None, None)
        assert _parse_contract_id('abc') == (None, None)

    def test_parse_rows_reject_garbage(self):
        from src.data_sources.sse_option_source import (parse_contract_row,
                                                        parse_risk_row,
                                                        parse_stats_row)
        assert parse_risk_row({}) is None
        assert parse_stats_row({}) is None
        assert parse_contract_row({}) is None


class TestNorthDetailParse:
    """北向个股明细解析 + 双通道聚合"""

    def test_parse_hold_row(self):
        from src.data_sources.hsgt_detail_source import parse_hold_row
        r = parse_hold_row({
            'TRADE_DATE': '2024-08-16 00:00:00', 'HOLD_SHARES': '82,354,427',
            'HOLD_MARKET_CAP': '117865655922.4', 'HOLD_SHARES_RATIO': '6.55',
            'ADD_SHARES_REPA': '39399', 'ADD_MARKET_CAP': '56264036',
            'MUTUAL_TYPE': '001', 'SECURITY_NAME': '贵州茅台'}, '600519')
        assert r['ts_code'] == 'sh.600519'
        assert r['trade_date'] == '20240816'
        assert r['hold_shares'] == 82354427.0
        assert r['hold_ratio'] == 6.55
        assert r['channel'] == '沪股通'

    def test_parse_hold_row_sz(self):
        from src.data_sources.hsgt_detail_source import parse_hold_row
        r = parse_hold_row({'TRADE_DATE': '2024-08-16', 'HOLD_SHARES': '1000',
                            'MUTUAL_TYPE': '003'}, '000858')
        assert r['ts_code'] == 'sz.000858'
        assert r['channel'] == '深股通'

    def test_merge_channels_sum_and_max(self):
        """沪/深双通道：同 (trade_date, ts_code) 持仓市值求和、占比取大"""
        mgr = NorthFlowDetailManager.__new__(NorthFlowDetailManager)
        rows = [
            {'trade_date': '20240816', 'ts_code': 'sh.600519',
             'hold_shares': 100.0, 'hold_market_value': 1000.0,
             'hold_ratio': 5.0, 'change_shares': 10.0, 'change_amount': 100.0},
            {'trade_date': '20240816', 'ts_code': 'sh.600519',
             'hold_shares': 50.0, 'hold_market_value': 500.0,
             'hold_ratio': 6.0, 'change_shares': 5.0, 'change_amount': 50.0},
        ]
        merged = mgr._merge_channels(rows)
        assert len(merged) == 1
        m = merged[0]
        assert m['hold_shares'] == 150.0          # 求和
        assert m['hold_market_value'] == 1500.0
        assert m['hold_ratio'] == 6.0             # 取大（不双计）
        assert m['change_shares'] == 15.0
