"""P0-4 北交所老号段防御单测（复盘报告层与 P0 修复技术方案 §四）

覆盖：白名单校验（43x/83x/87x/920x 通过，921+/其他形态/非 6 位数字抛错）
与僵尸码审计规则（连续 ≥5 日 volume=0 且 close=preclose）各一组用例。
"""
import pytest

from src.sync.review_common import detect_zombie_bj_codes, validate_bj_stock_code


class TestValidateBjStockCode:
    @pytest.mark.parametrize('code', [
        '920982', '920185', '920575',   # 920 新段
        '832982', '835185',             # 83 老段（在市未迁移形态）
        '873223',                       # 87 段
        '430047',                       # 43 段
    ])
    def test_allowed_prefixes(self, code):
        assert validate_bj_stock_code(code) == code

    @pytest.mark.parametrize('code', [
        '921001',                       # 921 未启用
        '925001',                       # 92 段其他形态
        '880001',                       # 通达信自编板块指数形态
        '600519',                       # 沪市码混入
        '92098',                        # 非 6 位
        '92098A',                       # 含字母
        '',
    ])
    def test_rejected(self, code):
        with pytest.raises(ValueError):
            validate_bj_stock_code(code)


def _row(code, date, volume, close, preclose):
    return {'stock_code': code, 'trade_date': date,
            'volume': volume, 'close': close, 'preclose': preclose}


class TestDetectZombieBjCodes:
    def test_consecutive_zero_volume_detected(self):
        rows = [_row('920001', f'2026090{d}', 0, 10.0, 10.0) for d in range(1, 6)]
        rows += [_row('920002', '20260901', 1000, 11.0, 10.0)]  # 正常股
        assert detect_zombie_bj_codes(rows) == ['920001']

    def test_short_run_or_trading_resumed_not_detected(self):
        # 仅 4 日定格（不足阈值 5）
        short = [_row('920003', f'2026090{d}', 0, 10.0, 10.0) for d in range(1, 5)]
        # 定格后恢复成交（连续性被打断；两段定格均不足 5 日）
        resumed = [_row('920004', '20260901', 0, 10.0, 10.0),
                   _row('920004', '20260902', 0, 10.0, 10.0),
                   _row('920004', '20260903', 0, 10.0, 10.0),
                   _row('920004', '20260904', 0, 10.0, 10.0),
                   _row('920004', '20260905', 500, 10.5, 10.0),
                   _row('920004', '20260908', 0, 10.5, 10.5),
                   _row('920004', '20260909', 0, 10.5, 10.5),
                   _row('920004', '20260910', 0, 10.5, 10.5),
                   _row('920004', '20260911', 0, 10.5, 10.5)]
        # 价格有变动（close != preclose）的零成交行（疑似停牌）不算定格
        halted = [_row('920005', f'2026090{d}', 0, 10.0, 9.5) for d in range(1, 7)]
        assert detect_zombie_bj_codes(short + resumed + halted) == []

    def test_run_breaks_across_long_gap(self):
        # 中间隔 >12 自然日的长断裂 → 连续性断裂，两段各 3 日不足阈值
        rows = ([_row('920006', f'2026092{d}', 0, 10.0, 10.0) for d in range(5, 8)]
                + [_row('920006', f'2026101{d}', 0, 10.0, 10.0) for d in range(2, 5)])
        assert detect_zombie_bj_codes(rows) == []

    def test_empty_and_missing_fields_safe(self):
        assert detect_zombie_bj_codes([]) == []
        assert detect_zombie_bj_codes([{'stock_code': '920007'},
                                       {'trade_date': '20260901', 'volume': None,
                                        'close': None, 'preclose': None}]) == []
