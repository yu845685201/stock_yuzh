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
    mgr.full_market = False
    mgr.super_big_threshold = big_threshold * 2
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


# ---------- 开发项 F：分笔封板指标 ----------

def test_parse_tick_time_iso_and_plain():
    from src.sync.money_flow_manager import _parse_tick_time
    assert _parse_tick_time('2026-09-30T09:25:00+08:00') == '092500'   # T22 实测格式
    assert _parse_tick_time('2026-09-30T15:00:00+08:00') == '150000'
    assert _parse_tick_time('10:05:00') == '100500'
    assert _parse_tick_time('') is None and _parse_tick_time(None) is None
    assert _parse_tick_time('garbage') is None


def _tick(hhmmss, price_yuan):
    """构造分笔行（Price 厘；Time ISO8601 带时区）"""
    return {'Price': price_yuan * 1000.0, 'Volume': 10, 'Status': 0,
            'Time': f'2026-09-30T{hhmmss[:2]}:{hhmmss[2:4]}:{hhmmss[4:]}+08:00'}


def test_seal_stats_seal_open_reseal():
    """封板→开板→回封：first/last/open_count/sealed_ratio 全语义"""
    from src.sync.money_flow_manager import compute_seal_stats
    trades = [
        _tick('092500', 5.00),   # 未封
        _tick('093000', 5.34),   # 涨停价=分笔max
        _tick('093100', 5.34),
        _tick('093200', 5.30),   # 开板（sealed→非sealed 转换 1 次）
        _tick('093300', 5.32),
        _tick('100500', 5.34),   # 回封（最后一段连续 sealed 首笔）
        _tick('140000', 5.34),
        _tick('150001', 5.34),   # 15:00 后盘后笔——时段过滤剔除
    ]
    out = compute_seal_stats(trades, side='up')
    assert out['limit_price'] == 5.34
    assert out['first_seal_time'] == '093000'
    assert out['last_seal_time'] == '100500'
    assert out['open_count'] == 1
    # 8 笔中 7 笔在时段内，其中 4 笔 sealed（5.34×2 + 5.34×2）
    assert out['sealed_ratio'] == 0.5714            # round(4/7, 4)


def test_seal_stats_down_side_min():
    """跌停对称分支：limit_price = 分笔 min（T17 华升股份口径）"""
    from src.sync.money_flow_manager import compute_seal_stats
    trades = [_tick('092500', 7.35), _tick('095900', 7.35),
              _tick('100000', 7.40), _tick('100100', 7.35)]
    out = compute_seal_stats(trades, side='down')
    assert out['limit_price'] == 7.35
    assert out['first_seal_time'] == '092500'
    assert out['open_count'] == 1
    assert out['sealed_ratio'] == pytest.approx(0.75)


def test_seal_stats_empty_after_filter():
    from src.sync.money_flow_manager import compute_seal_stats
    assert compute_seal_stats([], side='up') is None
    assert compute_seal_stats([{'Price': None, 'Time': 'x'}], side='up') is None
    assert compute_seal_stats([_tick('151500', 5.34)], side='up') is None  # 全在时段外


def _make_mf_manager(source, db, config=None):
    from src.sync.money_flow_manager import MoneyFlowManager
    m = MoneyFlowManager.__new__(MoneyFlowManager)
    m.source = source
    m.db = db
    m.config = config or {}
    m.include_burst = False
    m.big_amount_threshold = 500000
    m.pct_threshold = 0.07
    m.top_amount_n = 50
    m.gray_limit = 200
    m._rate_lock = __import__('threading').Lock()
    m._last_request_ts = 0.0
    m._interval_holder = [0.0]
    return m


class FakeRetrySource:
    """X-4/X-9 重试桩：前 fail_times 次空响应/断连，之后返回全量"""

    def __init__(self, trades, fail_times=1, mode='empty'):
        self.trades = trades
        self.fail_times = fail_times
        self.mode = mode
        self.calls = 0

    def get_minute_trade_history(self, code6, trade_date=None):
        self.calls += 1
        if self.calls <= self.fail_times:
            if self.mode == 'empty':
                return []
            raise RuntimeError('use of closed network connection')
        return self.trades


class FakeMFDB:
    def fetch_money_flow_codes(self, d):
        return set()

    def upsert_money_flows(self, rows):
        return len(rows)

    def upsert_seal_stats(self, rows):
        return len(rows)

    def fetch_seal_stats_codes(self, d):
        return set()


def test_fetch_trades_retry_on_warmup_empty(monkeypatch):
    """X-4：历史日期首次空响应（预热），重试即得全量"""
    from src.sync.money_flow_manager import _SEAL_SESSION_START  # noqa: 确认模块可导入
    import src.sync.money_flow_manager as mm
    trades = [_tick('093000', 5.34)]
    source = FakeRetrySource(trades, fail_times=1, mode='empty')
    m = _make_mf_manager(source, FakeMFDB())
    got, timed_out = m._fetch_trades('600519', '20260929')
    assert source.calls == 2 and got == trades and timed_out is False


def test_fetch_trades_retry_on_closed_connection(monkeypatch):
    """X-9：上游断连（closed network connection）→ 等待后重试成功"""
    import src.sync.money_flow_manager as mm
    trades = [_tick('093000', 5.34)]
    source = FakeRetrySource(trades, fail_times=1, mode='closed')
    m = _make_mf_manager(source, FakeMFDB())
    monkeypatch.setattr(mm.time, 'sleep', lambda s: None)  # 跳过 30s 等待
    got, timed_out = m._fetch_trades('600519', '20260929')
    assert source.calls == 2 and got == trades and timed_out is False


def test_seal_row_side_detection():
    """_seal_row_for：close<=low → down（min）；其余 → up（max）"""
    import src.sync.money_flow_manager as mm
    trades = [_tick('092500', 7.35), _tick('100000', 7.40)]
    m = _make_mf_manager(None, FakeMFDB())
    stock_down = {'ts_code': 'sh.600000', 'stock_code': '600000', 'is_st': False,
                  'close': 7.35, 'low': 7.35, 'high': 8.20}
    row = m._seal_row_for(stock_down, trades, '20260929')
    assert row['limit_price'] == 7.35          # down → min
    stock_up = {'ts_code': 'sh.600000', 'stock_code': '600000', 'is_st': False,
                'close': 5.34, 'low': 5.00, 'high': 5.34}
    trades_up = [_tick('092500', 5.00), _tick('093000', 5.34)]
    row2 = m._seal_row_for(stock_up, trades_up, '20260929')
    assert row2['limit_price'] == 5.34         # up → max
    assert row2['first_seal_time'] == '093000'
