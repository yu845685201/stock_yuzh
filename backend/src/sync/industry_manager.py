"""
行业分类同步（每日复盘缺失数据采集方案 V1.0 20260928 §3）

双口径设计（方案 §3.3）：
- 主口径（source=sina_hy）：新浪行业板块（~50 个自定行业），保底、日更，
  日更顺带覆盖当日新股的行业归属（全量仅 ~150 请求、2s 限速约 4 分钟）
- 增强口径（source=sw_2021）：申万 2021 版一级行业 31 个（官方标准口径，全市场覆盖），
  经 swsresearch.com 下载中心（2026-09-28 浏览器探路通过）——报告行业榜优先用申万口径

背景修正：复盘系统方案 V1.0 曾假定"行业分类已具备"，2026-09-28 实测
base_stock_info 的 industry/sector 字段全空、库内无任何行业数据——本 manager 补齐该缺口。

入库：sector_concept / sector_concept_map（快照 diff 见基类）；两口径共用同表，source 区分。
"""

import logging
from typing import Any, Dict, List

from ..data_sources.sw_source import SwSectorSource
from .sector_sync_base import ReviewSyncError, SectorNodeManagerBase

logger = logging.getLogger(__name__)


class IndustryManager(SectorNodeManagerBase):
    """行业分类：新浪行业板块（sina_hy）+ 申万一级行业（sw_2021，可配置）"""

    source_tag = 'sina_hy'
    label = '行业分类'
    report_title = '行业分类同步报告'
    report_prefix = 'industry_sync_report'
    config_section = 'industry'

    def __init__(self, config_manager):
        super().__init__(config_manager)
        section = ((self.config.get('review_sync', {}) or {})
                   .get(self.config_section, {}) or {})
        self.sw_enabled = bool(section.get('sw_enabled', False))
        self.sw_source = SwSectorSource(self.config) if self.sw_enabled else None

    def fetch_nodes(self) -> Dict[str, Dict[str, Any]]:
        return self.source.fetch_industry_list()

    # ---------- 增强口径：申万 2021 一级行业 ----------

    def run_extra_snapshots(self, dry_run: bool, today: str,
                            stats: Dict[str, Any]) -> None:
        if not self.sw_enabled:
            return
        names = self.sw_source.fetch_industry_names()
        if not names:
            raise ReviewSyncError('申万一级行业清单为空（接口结构变更），本轮 sw_2021 不落库')
        members_by_node: Dict[str, List[str]] = {}
        for name in names:
            members_by_node[name] = self.sw_source.fetch_industry_members(name)
        nodes = {name: {'name': name, 'count': None} for name in names}
        frag = self.apply_snapshot('sw_2021', nodes, members_by_node,
                                   dry_run=dry_run, today=today)
        stats['sw_names'] = frag['concepts']
        stats['sw_members'] = frag['members_active']
        stats['sw_new'] = frag['members_new']
        stats['sw_out'] = frag['members_out']

    def report_extra_lines(self, stats: Dict[str, Any]) -> List[str]:
        if 'sw_names' not in stats:
            return []
        return [
            f"增强口径：申万 2021 一级行业 {stats['sw_names']} 个 / "
            f"在册 {stats['sw_members']} 条（新进 {stats['sw_new']}，退出 {stats['sw_out']}）；"
            f"报告行业榜优先本口径",
        ]
