"""
板块节点同步基类（每日复盘缺失数据采集方案 V1.0 20260928 §3.4/§4.3）

行业分类与概念板块共用同一结构：清单（节点->名称）+ 每节点成分翻页，
入库同用 sector_concept / sector_concept_map 两表，以 source 列区分口径
（sina_hy=新浪行业 / sina_gn=新浪概念），in_date/out_date 留痕支持历史回看。

快照 diff 语义（upsert_concept_maps 配套）：
- 本次在册且此前不在册（out_date IS NULL 视角）-> in_date=刷新日，out_date=NULL
- 本次在册且此前已在册 -> 行不动（冲突分支只刷 out_date=NULL，in_date 保留首次）
- 此前在册、本次缺席 -> out_date=刷新日（保留历史行，不删除）

数据真实性原则：任一节点抓取失败则整轮失败、全部不落库（不做残缺快照）；
风控急停（SinaSectorBlockedError）交编排器冷却重试。
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.sina_sector_source import SinaSectorSource
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

logger = logging.getLogger(__name__)

# 成分数 vs 清单 count 的容差（仅用于触发重抓，不做硬校验——实测清单 count 与
# 翻页实抓口径存在恒定差，如 new_qtxy 清单 202/实抓 171，不可作为截断依据）
NODE_COUNT_TOLERANCE = 2
# 节点成分较库内在册数异常缩减比例（超过则判定翻页截断，整轮不落库）
NODE_SHRINK_RATIO = 0.3


class SectorNodeManagerBase:
    """行业/概念板块同步模板：子类只需提供 source_tag / label / fetch_nodes"""

    source_tag: str = ''
    label: str = ''
    report_prefix: str = ''
    report_title: str = ''

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        section = review.get(self.config_section, {}) or {}
        self.enabled = bool(section.get('enabled', True))
        self.source = SinaSectorSource(self.config)

    config_section: str = ''

    # ---------- 子类实现 ----------

    def fetch_nodes(self) -> Dict[str, Dict[str, Any]]:
        """板块清单 -> {node: {name, count}}"""
        raise NotImplementedError

    # ---------- 主流程 ----------

    def execute(self, dry_run: bool = False) -> Dict[str, Any]:
        started = datetime.now()
        today = datetime.now().strftime('%Y%m%d')
        stats: Dict[str, Any] = {
            'enabled': self.enabled, 'nodes_total': 0, 'nodes_ok': 0,
            'members_active': 0, 'members_new': 0, 'members_out': 0,
            'concepts': 0, 'rows': 0,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'skipped_disabled': not self.enabled}
        if not self.enabled:
            result['errors'].append(f'配置停用（review_sync.{self.config_section}.enabled=false）')
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
            return result

        try:
            nodes = self.fetch_nodes()
            if not nodes:
                raise ReviewSyncError('板块清单解析为空（接口结构变更或风控页），本轮不落库')
            stats['nodes_total'] = len(nodes)
            active_before = self.db.fetch_concept_active_codes(self.source_tag)
            members_by_node: Dict[str, List[str]] = {}
            for node, info in nodes.items():
                expected = info.get('count')
                members = self.source.fetch_node_members(node, expected_count=expected)
                if expected and len(members) < int(expected) - NODE_COUNT_TOLERANCE:
                    time.sleep(self.source.request_interval * 2)
                    members = self.source.fetch_node_members(node, expected_count=expected)
                # 截断判定以库内在册数为基准（清单 count 口径不可靠）：
                # 异常缩减（>30%，如 2026-09-29 盘中实测 247→40）先重抓、仍缩减则整轮不落库
                prev_n = len(active_before.get(node, []))
                if prev_n and len(members) < prev_n * (1 - NODE_SHRINK_RATIO):
                    time.sleep(self.source.request_interval * 2)
                    members = self.source.fetch_node_members(node, expected_count=expected)
                if prev_n and len(members) < prev_n * (1 - NODE_SHRINK_RATIO):
                    raise ReviewSyncError(
                        f'节点 {node} 实抓成分 {len(members)} 较库内在册 {prev_n} '
                        f'异常缩减超 {NODE_SHRINK_RATIO:.0%}（疑似翻页截断），本轮不落库')
                members_by_node[node] = [m['ts_code'] for m in members]
                stats['nodes_ok'] += 1
            stats.update(self.apply_snapshot(self.source_tag, nodes, members_by_node,
                                             dry_run=dry_run, today=today,
                                             active_before=active_before))
            result['success'] = True
            # 增强口径（如申万一级行业）随主口径成功后顺带刷新；失败只记 errors 不回滚主口径
            if result['success']:
                try:
                    self.run_extra_snapshots(dry_run, today, stats)
                except ReviewBlockedError as e:
                    result['errors'].append(f'增强口径风控急停（主口径不受影响）: {e}')
                except Exception as e:
                    result['errors'].append(f'增强口径失败: {e}')
                    logger.exception(f'{self.label} 增强口径失败: {e}')
        except ReviewBlockedError as e:
            result['blocked'] = True
            result['errors'].append(f'新浪板块源风控急停（本轮未落库，冷却后重跑）: {e}')
        except Exception as e:
            result['errors'].append(str(e))
            logger.exception(f'{self.label} 同步失败: {e}')
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 快照（主口径与增强口径共用） ----------

    def apply_snapshot(self, source_tag: str, nodes: Dict[str, Dict[str, Any]],
                       members_by_node: Dict[str, List[str]], *,
                       dry_run: bool, today: str,
                       active_before: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
        """单口径快照 diff + 入库；nodes 附带名称，members_by_node 为 ts_code 列表。

        active_before 可传入 execute 已查得的在册映射（避免重复查询）；缺省自查。
        diff 语义见模块头注释；返回统计片段（concepts/members_active/members_new/
        members_out/rows）。
        """
        if active_before is None:
            active_before = self.db.fetch_concept_active_codes(source_tag)
        concept_rows: List[Dict[str, Any]] = []
        map_rows: List[Dict[str, Any]] = []
        frag: Dict[str, Any] = {'members_new': 0, 'members_out': 0}
        for node, info in nodes.items():
            ts_codes = members_by_node.get(node) or []
            concept_rows.append({
                'concept_code': node, 'concept_name': info.get('name'),
                'source': source_tag,
            })
            current = set(ts_codes)
            known = set(active_before.get(node, []))
            for ts in ts_codes:
                is_new = ts not in known
                map_rows.append({
                    'concept_code': node, 'ts_code': ts,
                    'in_date': today if is_new else None,
                    'out_date': None,
                })
                if is_new:
                    frag['members_new'] += 1
            for gone in known - current:
                map_rows.append({
                    'concept_code': node, 'ts_code': gone,
                    'in_date': None, 'out_date': today,
                })
                frag['members_out'] += 1
        frag['concepts'] = len(concept_rows)
        frag['members_active'] = sum(len(v) for v in
                                     self._merge_active(active_before, map_rows).values())
        frag['rows'] = frag['members_active']
        if not dry_run:
            self.db.upsert_concepts(concept_rows)
            self.db.upsert_concept_maps(map_rows)
        return frag

    def run_extra_snapshots(self, dry_run: bool, today: str, stats: Dict[str, Any]) -> None:
        """增强口径钩子（子类覆写，如行业 manager 挂申万一级行业）；默认无增强口径"""
        return None

    def report_extra_lines(self, stats: Dict[str, Any]) -> List[str]:
        """报告追加行钩子（与 run_extra_snapshots 对应）；默认无"""
        return []

    # ---------- 工具 ----------

    @staticmethod
    def _merge_active(active_before: Dict[str, List[str]],
                      map_rows: List[Dict[str, Any]]) -> Dict[str, set]:
        """dry-run/统计用：按本次 map_rows 重算各节点在册集合（out_date=NULL 视角）"""
        merged: Dict[str, set] = {k: set(v) for k, v in active_before.items()}
        for r in map_rows:
            bucket = merged.setdefault(r['concept_code'], set())
            if r['out_date']:
                bucket.discard(r['ts_code'])
            else:
                bucket.add(r['ts_code'])
        return merged

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        if stats.get('enabled') is False or result.get('skipped_disabled'):
            lines = [f"- 探路结论与来源：新浪 vip 板块接口（方案 §2.1 实测，成分翻页可用）",
                     f"- enabled：False（config review_sync.{self.config_section}.enabled）"]
            status = '待补（配置停用）'
        else:
            lines = [
                f"- 来源：新浪 vip 板块接口，口径 source={self.source_tag}（非官方口径，报告中如实标注）",
                f"- 板块：清单 {stats['nodes_total']} 个，抓全 {stats['nodes_ok']} 个",
                f"- 在册成分 {stats['members_active']} 条（新进 {stats['members_new']}，"
                f"退出 {stats['members_out']}，退出仅置 out_date 保留历史）",
            ]
            lines.extend(self.report_extra_lines(stats))
            if result['errors']:
                lines.append(f"- 错误：{result['errors']}")
            status = '风控急停' if result['blocked'] else ('成功' if result['success'] else '失败')
        return write_review_report(self.report_title, self.report_prefix,
                                   status, lines, result.get('duration'))


class ReviewSyncError(RuntimeError):
    """板块同步业务失败（清单为空等，区别于风控急停）"""
