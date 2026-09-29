"""公告分类与业绩预告解析测试（方案 4.6/4.13：标题正则、净利润区间、报告期）"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.sync.announcement_manager import DEFAULT_TYPE_KEYWORDS, AnnouncementManager


def make_manager():
    """不连数据库的构造：绕过 __init__，只装配分类所需字段"""
    mgr = AnnouncementManager.__new__(AnnouncementManager)
    mgr.type_keywords = [(k, list(v)) for k, v in DEFAULT_TYPE_KEYWORDS]
    return mgr


def test_classify_periodic():
    mgr = make_manager()
    assert mgr.classify_title('2025年年度报告') == '定期报告'
    assert mgr.classify_title('2026年第一季度报告') == '定期报告'
    assert mgr.classify_title('2025年半年度报告（更新版）') == '定期报告'


def test_classify_forecast_and_flash():
    mgr = make_manager()
    assert mgr.classify_title('2026年第三季度业绩预告') == '业绩预告'
    assert mgr.classify_title('关于2025年年度业绩预增的公告') == '业绩预告'
    assert mgr.classify_title('2025年年度业绩快报') == '业绩快报'


def test_classify_other_types():
    mgr = make_manager()
    assert mgr.classify_title('2025年年度权益分派实施公告') == '分红送转'
    assert mgr.classify_title('关于股东减持股份计划的公告') == '增减持'
    assert mgr.classify_title('关于重大资产重组停牌的公告') == '重大事项'
    assert mgr.classify_title('召开2025年年度股东大会的通知') == '其他'


def test_forecast_type_match():
    assert AnnouncementManager._match_forecast_type('年度业绩扭亏为盈的公告') == '扭亏'
    assert AnnouncementManager._match_forecast_type('业绩预减公告') == '预减'
    assert AnnouncementManager._match_forecast_type('年度报告') is None


def test_profit_range_yuan_range():
    lo, hi = AnnouncementManager._parse_profit_range(
        '预计2026年半年度净利润1.5亿元~2.0亿元，同比增长')
    assert lo == pytest.approx(1.5e8)
    assert hi == pytest.approx(2.0e8)


def test_profit_range_wan_and_reversed():
    lo, hi = AnnouncementManager._parse_profit_range('预计净利润 12000万–8000万')
    assert lo == pytest.approx(8000e4)
    assert hi == pytest.approx(12000e4)


def test_profit_range_not_parseable():
    assert AnnouncementManager._parse_profit_range('预计净利润为正') == (None, None)


def test_parse_report_period():
    assert AnnouncementManager._parse_report_period('2026年第一季度业绩预告') == '20260331'
    assert AnnouncementManager._parse_report_period('2025年半年度业绩预告') == '20250630'
    assert AnnouncementManager._parse_report_period('2026年第三季度业绩预告') == '20260930'
    assert AnnouncementManager._parse_report_period('2025年年度业绩预告') == '20251231'
    assert AnnouncementManager._parse_report_period('业绩预告') is None


def test_ann_row_conversion():
    mgr = make_manager()
    row = mgr._to_announcement_row({
        'announcementId': '1225583316',
        'announcementTitle': '关于公司重大资产重组停牌的公告',
        'announcementTime': 1790265600000,
        'secCode': '600095',
        'secName': '湘财股份',
        'adjunctUrl': 'finalpage/2026-09-25/1225583316.PDF',
    })
    assert row['announcement_id'] == '1225583316'
    assert row['ts_code'] == 'sh.600095'
    assert row['ann_type'] == '重大事项'
    assert row['ann_date'].startswith('2026')
    assert row['ann_url'].startswith('http://static.cninfo.com.cn/')


def test_ann_row_excluded():
    mgr = make_manager()
    assert mgr._to_announcement_row({
        'announcementId': '1', 'announcementTitle': '2025年年度报告摘要',
    }) is None
    assert mgr._to_announcement_row({'announcementTitle': '无ID标题'}) is None
