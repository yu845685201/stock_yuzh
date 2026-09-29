"""THS 源测试（方案 4.3：flash 结构解析、中文数字解析、风控特征识别）"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.ths_source import ThsSource, parse_ths_number


def make_flash():
    """构造与实测接口同构的 flash（title/report/report_yoy 行列对齐）

    注意：report_yoy[i] 与 title[i] 同序（每行是该指标的同比数值行），
    实测接口中每行都对齐到各自指标下标。
    """
    return {
        'title': [
            ['科目\\时间'],
            ['净利润', '元'], ['净利润同比增长率', ''],
            ['营业总收入', '元'], ['营业总收入同比增长率', ''],
            ['基本每股收益', '元'], ['每股净资产', '元'],
        ],
        'report': [
            ['2026-06-30', '2026-03-31', '2025-12-31'],
            ['256.96亿', '145.23亿', '426.33亿'],       # 净利润（累计）
            ['3.32%', '3.03%', '-4.21%'],               # 净利同比（report 里也有格式化行）
            ['706.17亿', '352.77亿', '1314.42亿'],      # 营收
            ['1.78%', '4.65%', '-10.40%'],
            ['1.2400', '0.6700', '2.0700'],             # EPS
            ['22.51', '22.30', '21.98'],                # 每股净资产
        ],
        'report_yoy': [
            ['2026-06-30', '2026-03-31', '2025-12-31'],
            [None, None, None],
            [3.32127061, 3.02922815, -4.2127258],       # 净利同比数值行（title 下标 2）
            [None, None, None],
            [1.78, 4.65, -10.40],                       # 营收同比数值行（title 下标 4）
            [None, None, None],
            [None, None, None],
        ],
    }


def test_parse_ths_number():
    assert parse_ths_number('256.96亿') == pytest.approx(2.5696e10)
    assert parse_ths_number('3.32%') == pytest.approx(3.32)
    assert parse_ths_number('1.2400') == pytest.approx(1.24)
    assert parse_ths_number('1200万') == pytest.approx(1.2e7)
    assert parse_ths_number('--') is None
    assert parse_ths_number(None) is None
    assert parse_ths_number('') is None


def test_parse_finance_flash_rows():
    source = ThsSource.__new__(ThsSource)  # 不发起请求
    rows = source.parse_finance_flash(make_flash(), total_share=None)
    assert len(rows) == 3
    latest = rows[0]
    assert latest['stat_date'] == '20260630'
    assert latest['net_profit'] == pytest.approx(2.5696e10)
    assert latest['revenue'] == pytest.approx(7.0617e10)
    assert latest['eps'] == pytest.approx(1.24)
    # 同比优先取 report_yoy 数值行
    assert latest['net_profit_yoy'] == pytest.approx(3.32127061)
    assert latest['revenue_yoy'] == pytest.approx(1.78)
    # 每股类指标无总股本不硬造绝对值
    assert latest['net_assets'] is None


def test_parse_finance_flash_with_share():
    source = ThsSource.__new__(ThsSource)
    rows = source.parse_finance_flash(make_flash(), total_share=2e9)
    # 每股净资产 22.51 × 20亿股 = 450.2 亿
    assert rows[0]['net_assets'] == pytest.approx(22.51 * 2e9)


def test_parse_finance_flash_backfill_years():
    source = ThsSource.__new__(ThsSource)
    rows = source.parse_finance_flash(make_flash(), backfill_years=1)
    # 只保留近 1 年（2026-1=2025，含 2025-12-31 年报）
    assert [r['stat_date'] for r in rows] == ['20260630', '20260331', '20251231']


def test_risk_control_captcha_marks():
    source = ThsSource({'review_sync': {'ths_interval': 0}})

    class FakeResp:
        content = '请输入验证码'.encode('utf-8')
        encoding = 'utf-8'

    with pytest.raises(Exception) as exc_info:
        source._check_risk_control(FakeResp())
    assert '验证码' in str(exc_info.value)


def test_blocked_error_cooldown():
    from src.data_sources.ths_source import ThsBlockedError
    err = ThsBlockedError('测试')
    assert err.cooldown_seconds == 900
