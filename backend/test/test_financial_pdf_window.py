"""财报PDF采集 manager 报告期窗口（cninfo.pdf_recent_years）单测。

覆盖：窗口对全模式生效（含 init）、显式 --year-from 优先、
增量/回补逻辑在窗口启用时被取代、seDate 起点收窄。
"""
from datetime import date
from pathlib import Path
import sys

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pytest

from src.data_sources.cninfo_source import CninfoSource
from src.sync.financial_pdf_manager import FinancialPdfManager


def _make_manager(recent_years, backfill_years=5):
    m = FinancialPdfManager.__new__(FinancialPdfManager)  # 跳过 __init__（DB/目录依赖）
    m.config = {'cninfo': {'pdf_recent_years': recent_years,
                           'backfill_years': backfill_years}}
    m.backfill_years = backfill_years
    m.recent_years = recent_years
    m.source = CninfoSource({'cninfo': {}})
    return m


def _ann(title, when=20240401):
    return {'announcementTitle': title, 'adjunctUrl': f'{title}.pdf',
            'announcementTime': when, 'announcementId': str(when)}


def _titles():
    return [_ann('2015年年度报告'), _ann('2016年年度报告（更新后）'),
            _ann('2017年第一季度报告'), _ann('2023年年度报告'), _ann('2026年半年度报告')]


class TestRecentYearsWindow:
    def test_init_mode_respects_window(self):
        """init 全历史模式下窗口同样生效（用户决策：仅近 10 年）"""
        m = _make_manager(recent_years=10)  # 2026 → floor 2017
        plan, non_pdf = m._build_plan(_titles(), init_mode=True,
                                      year_from=None, year_to=None, last_stat=None)
        years = [p['year'] for p in plan]
        assert min(years) >= 2017
        assert 2015 not in years and 2016 not in years
        assert 2023 in years and 2026 in years

    def test_explicit_year_from_overrides_window(self):
        """显式 --year-from 优先于配置窗口"""
        m = _make_manager(recent_years=10)
        plan, _ = m._build_plan(_titles(), init_mode=False,
                                year_from=2015, year_to=None, last_stat=None)
        years = [p['year'] for p in plan]
        assert 2015 in years and 2016 in years

    def test_window_disabled_keeps_full_history(self):
        """pdf_recent_years=0 时 init 恢复全历史行为"""
        m = _make_manager(recent_years=0)
        plan, _ = m._build_plan(_titles(), init_mode=True,
                                year_from=None, year_to=None, last_stat=None)
        assert {p['year'] for p in plan} == {2015, 2016, 2017, 2023, 2026}

    def test_backfill_superseded_by_window(self):
        """窗口启用时，无进度回补的下限取更宽的窗口（2017），而非 backfill_years（2021）"""
        m = _make_manager(recent_years=10)
        plan, _ = m._build_plan(_titles(), init_mode=False,
                                year_from=None, year_to=None, last_stat=None)
        years = [p['year'] for p in plan]
        assert 2017 in years and 2015 not in years

    def test_incremental_still_uses_progress(self):
        """有进度时增量语义不变：只采报告期晚于进度的财报"""
        m = _make_manager(recent_years=10)
        plan, _ = m._build_plan(_titles(), init_mode=False,
                                year_from=None, year_to=None,
                                last_stat=date(2023, 12, 31))
        years = [p['year'] for p in plan]
        assert 2023 not in years and 2026 in years

    def test_se_start_narrowed(self):
        """seDate 起点随窗口收窄（init 模式不再回溯 1990）"""
        m = _make_manager(recent_years=10)
        assert m._resolve_se_start(init_mode=True, year_from=None,
                                   last_stat=None) == '2017-01-01'
        assert m._resolve_se_start(init_mode=False, year_from=None,
                                   last_stat=None) == '2017-01-01'
        m0 = _make_manager(recent_years=0)
        assert m0._resolve_se_start(init_mode=True, year_from=None,
                                    last_stat=None) == '1990-01-01'


class TestInitConfigWiring:
    def test_real_configmanager_loads_window(self):
        """回归：__init__ 必须用嵌套 dict 取 cninfo 配置——点路径 get 恒落默认值
        （backfill_years 的旧写法因默认值恰好等于配置值而潜伏未暴露）"""
        from src.config.config_manager import ConfigManager
        m = FinancialPdfManager(ConfigManager())
        assert m.recent_years >= 0
        # 仓库 config 当前 pdf_recent_years=10（用户决策 2026-10-01）
        assert m.recent_years == 10
        assert m._window_floor() == 2017
        m.db.close() if hasattr(m.db, 'close') else None
