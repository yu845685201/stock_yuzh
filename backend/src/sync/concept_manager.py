"""
概念板块成分同步（每日复盘缺失数据采集方案 V1.0 20260928 §4；改造自待补 stub）

原探路结论（2026-09-27）：同花顺概念清单可取 361 个，成分翻页被 hexin-v 拦（401），
残缺数据不可入库 -> 标待补。
本次换源（2026-09-28 实测）：新浪概念接口清单+成分翻页全部可用（方案 §2.1 P2-P4），
主源切换为新浪（source=sina_gn），本 manager 由此恢复采集。

- 兜底链（新浪失败时）：同花顺 basic 子域个股 concept.html 倒排（无 hexin-v，实测 200）
  / 通达信 block_gn.dat——均为后续增强，当前未实现，报告中如实标注
- 同花顺 361 概念清单仅作报告层名称对齐字典（source='ths_gn'，无成分），后续一次性导入
- 频率：周频全量（weekly_refresh_weekday），入库快照 diff 见 sector_sync_base
"""

import logging
from typing import Any, Dict

from .sector_sync_base import SectorNodeManagerBase

logger = logging.getLogger(__name__)


class ConceptManager(SectorNodeManagerBase):
    """概念板块：新浪概念板块 -> sector_concept(source=sina_gn)"""

    source_tag = 'sina_gn'
    label = '概念板块'
    report_title = '概念板块同步报告'
    report_prefix = 'concept_sync_report'
    config_section = 'concept'

    def fetch_nodes(self) -> Dict[str, Dict[str, Any]]:
        return self.source.fetch_concept_list()
