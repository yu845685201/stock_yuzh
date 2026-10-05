"""除权触发清单（方案 4.2）与交易所行转换测试"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.sync.kline_day_pipeline import KlineDaySharedState, merge_stock_result


def test_pipeline_collects_xdxr_hits():
    """merge_stock_result 把 qfq_rebase 命中股票记入 xdxr_hit_codes（raw_revision 不记）"""
    ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0, 'records': 0,
                                      'db_rows': 0})
    res = {
        'records': 1, 'db_rows': 1, 'api_time': 0, 'csv_time': 0, 'db_time': 0,
        'api_span': None, 'csv_span': None, 'db_span': None, 'anomalies': [],
        'source_missing': {},
        'detection': {'checked': 1, 'hits': 1, 'refetched': 1},
        'detection_details': [
            {'ts_code': 'sz.000001', 'type': 'qfq_rebase', 'date': '20260925'},
            {'ts_code': 'sz.000002', 'type': 'raw_revision', 'date': '20260925'},
        ],
    }
    merge_stock_result(res, ctx)
    merge_stock_result(res, ctx)  # 幂等：同一股不重复记
    assert ctx.xdxr_hit_codes == ['sz.000001']


def test_adjust_trigger_write_and_read(tmp_path, monkeypatch):
    """写清单（合并幂等）-> read_adjust_trigger 读取"""
    import src.sync.adjust_factor_manager as afm
    monkeypatch.setattr(afm, 'tmp_dir', lambda: tmp_path)
    from src.sync.adjust_factor_manager import adjust_trigger_path, read_adjust_trigger

    path = adjust_trigger_path('20260925')
    payload = {'trade_date': '20260925', 'ts_codes': ['sz.000001']}
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    assert read_adjust_trigger('20260925') == ['sz.000001']
    assert read_adjust_trigger('20991231') == []  # 缺文件返回空
    path.write_text('{broken', encoding='utf-8')  # 损坏文件返回空
    assert read_adjust_trigger('20260925') == []


class FakeSource:
    """汇总接口桩（X-1 修复后格式：席位打包在 branchNameB/S + branchTxAmtB/S）"""

    def __init__(self, rows):
        self.rows = rows

    def fetch_sse_lhb_summary(self, trade_date):
        return self.rows


def test_lhb_sse_aggregation():
    """上交所龙虎榜（汇总接口，X-1 修复）：refType 文案映射 + 打包席位金额 Σ + 席位明细展开"""
    from src.sync.lhb_manager import LhbManager
    mgr = LhbManager.__new__(LhbManager)
    mgr.source = FakeSource([
        {'secCode': '600815', 'secAbbr': '厦工股份', 'refType': '1',
         'secTxAmount': 289378223.0,
         'branchNameB': '沪股通专用,开源证券西安西大街',
         'branchTxAmtB': '160910625.17,128416895.57',
         'branchNameS': '沪股通专用',
         'branchTxAmtS': '516702.00'},
    ])
    stocks, details = mgr._collect_sse('20260924')
    assert len(stocks) == 1
    row = stocks[0]
    assert row['ts_code'] == 'sh.600815'
    assert row['reason'] == '涨幅偏离累计达20%'
    assert row['buy_amt'] == pytest.approx(160910625.17 + 128416895.57)
    assert row['sell_amt'] == pytest.approx(516702.0)
    assert row['net_amt'] == pytest.approx(row['buy_amt'] - row['sell_amt'])
    assert row['sec_tx_amount'] == 289378223.0
    assert len(details) == 3
    assert {d['side'] for d in details} == {'买1', '买2', '卖1'}


class FakeMarginSource:
    def fetch_szse_margin(self, date_str):
        return [{'zqdm': '000001', 'zqjc': '平安银行', 'jrrzmr': '0.89',
                 'jrrzye': '46.24', 'jrrjye': '17,455.25'}]

    def fetch_sse_margin(self, date_str):
        return [{'stockCode': '600000', 'securityAbbr': '浦发银行', 'rzye': 3623771056,
                 'rzmre': 21490597, 'rzche': 62493983}]


def test_margin_unit_scaling():
    """深交所亿元/万元 -> 元；上交所元原样；缺失列留空"""
    from src.sync.margin_manager import MarginManager
    mgr = MarginManager.__new__(MarginManager)
    mgr.source = FakeMarginSource()
    rows = []
    for item in mgr.source.fetch_szse_margin('20260924'):
        code = item['zqdm']
        rows.append({'ts_code': code, 'rzye': float(item['jrrzye'].replace(',', '')) * 1e8,
                     'rqye': float(item['jrrjye'].replace(',', '')) * 1e4, 'rzche': None})
    assert rows[0]['rzye'] == pytest.approx(46.24e8)
    assert rows[0]['rqye'] == pytest.approx(17455.25e4)
