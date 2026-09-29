"""分笔资金流计算测试（方案 4.10 近似口径：方向归并、tick rule、大单阈值）"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.config import ConfigManager
from src.sync.money_flow_manager import MoneyFlowManager


def make_manager(big_threshold=500000.0, pct_threshold=0.07):
    """不连数据库的构造：绕过 __init__，直接装配计算所需字段"""
    mgr = MoneyFlowManager.__new__(MoneyFlowManager)
    mgr.big_amount_threshold = big_threshold
    mgr.pct_threshold = pct_threshold
    mgr.concurrent_workers = 3
    mgr.request_interval = 0.0
    mgr.big_amount_threshold = big_threshold
    mgr.top_amount_n = 50
    mgr.gray_limit = 200
    mgr.throttle_on_timeout_pct = 0.05
    return mgr


def trade(price_li, volume_hand, status):
    return {'Price': price_li, 'Volume': volume_hand, 'Status': status, 'Time': 'x'}


def test_status_buy_sell_basic():
    mgr = make_manager()
    trades = [
        trade(10000, 6000, 0),   # 10元 × 60万股 = 600万 买
        trade(10000, 6000, 1),   # 600万 卖
        trade(10000, 300, 0),    # 30万 买（低于 50 万阈值不计大单）
    ]
    row = mgr.compute_money_flow(trades, total_amt=1500000.0)
    assert row['big_buy_amt'] == pytest.approx(6000000.0)
    assert row['big_sell_amt'] == pytest.approx(6000000.0)
    assert row['big_net_amt'] == pytest.approx(0.0)
    assert row['stock_count'] == 3
    assert row['big_net_pct'] == pytest.approx(0.0)


def test_neutral_tick_rule_up_down_flat():
    mgr = make_manager()
    trades = [
        trade(10000, 600, 0),    # 基准买盘，10元 60万 >= 50万 计大单
        trade(10100, 600, 2),    # 中性盘 价格上行 -> 归买 60.6万 >= 50万
        trade(10000, 600, 2),    # 中性盘 价格下行 -> 归卖 60万
        trade(10000, 600, 2),    # 平价 -> 继承前一笔方向（卖）
        trade(10000, 100, 2),    # 平价继承卖，30万 < 阈值不计
    ]
    row = mgr.compute_money_flow(trades, total_amt=None)
    assert row['big_buy_amt'] == pytest.approx(600000.0 + 606000.0)
    assert row['big_sell_amt'] == pytest.approx(1200000.0)
    assert row['big_net_pct'] is None  # 日K成交额缺失不硬造占比


def test_first_neutral_dropped():
    mgr = make_manager()
    trades = [trade(10000, 10000, 2)]  # 首笔中性盘剔除
    row = mgr.compute_money_flow(trades, total_amt=100.0)
    assert row['stock_count'] == 0
    assert row['big_buy_amt'] == 0.0


def test_status5_treated_neutral():
    mgr = make_manager()
    trades = [
        trade(10000, 600, 0),
        trade(10200, 600, 5),   # 未知状态按中性 tick rule：价升归买
    ]
    row = mgr.compute_money_flow(trades, total_amt=1000.0)
    assert row['stock_count'] == 2
    assert row['big_buy_amt'] == pytest.approx(600000.0 + 612000.0)


def test_big_threshold_configurable():
    mgr = make_manager(big_threshold=10**9)  # 阈值抬到 10 亿
    trades = [trade(10000, 6000, 0)]  # 600 万买
    row = mgr.compute_money_flow(trades, total_amt=100.0)
    assert row['big_buy_amt'] == 0.0
    assert row['big_net_pct'] == 0.0


def test_price_unit_scale():
    """分笔价格为厘：11350 -> 11.35 元；11.35 × 5000手 × 100 = 567.5 万"""
    mgr = make_manager()
    trades = [trade(11350, 5000, 0)]
    row = mgr.compute_money_flow(trades, total_amt=None)
    assert row['big_buy_amt'] == pytest.approx(5675000.0)
