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
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.cnindex_industry_source import CnindexIndustrySource
from ..data_sources.sw_source import SwSectorSource
from .review_common import latest_closed_trade_date, mark_blocked
from .sector_sync_base import ReviewSyncError, SectorNodeManagerBase

logger = logging.getLogger(__name__)


class IndustryManager(SectorNodeManagerBase):
    """行业分类：新浪行业板块（sina_hy）+ 申万一级行业（sw_2021，可配置）+
    国证一级日期快照（cnindex_level1，§6 批次 B——独立运行，不依赖新浪成功）"""

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
        self.cnindex_enabled = bool(section.get('cnindex_enabled', True))
        self.cnindex_source = CnindexIndustrySource(self.config) if self.cnindex_enabled else None

    def fetch_nodes(self) -> Dict[str, Dict[str, Any]]:        return self.source.fetch_industry_list()

    # ---------- 增强口径：申万 2021 一级行业 ----------

    def run_extra_snapshots(self, dry_run: bool, today: str,
                            stats: Dict[str, Any]) -> None:
        if not self.sw_enabled:
            return
        # G 项改良（V1.4）：单请求全量下载（含时点日期）优先；普通异常回退现行
        # 31 次逐行业下载路径。明确被拒（ReviewBlockedError）不触发同域批量文件
        # 回退（限流方案 20261120 §3 P0：清单+逐行业回退只用于可解释的格式/端点故障）
        try:
            rows = self.sw_source.fetch_all_classification()
            self._sw_apply_all_classification(rows, dry_run=dry_run, stats=stats)
            stats['sw_mode'] = 'all_download'
            return
        except ReviewBlockedError:
            raise
        except Exception as e:
            logger.warning(f'申万单请求路径失败，回退逐行业下载: {e}')
            stats['sw_mode'] = 'per_industry_fallback'
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

    def _register_extra_snapshot_gap(self, e: Exception, today: str, dry_run: bool) -> None:
        """申万增强失败 → 登记子项缺口（限流方案 §3 P0：新浪成功而申万失败时，
        保留新浪结果不变，补采只执行增强部分，不整源重跑）"""
        if dry_run or not self.sw_enabled:
            return
        try:
            self.db.register_retry(
                dataset='industry', source='industry', target_date=today, market='CN',
                action='SCOPE_RETRY', reason='SW_ENHANCE_GAP', sub_key='sw_2021',
                missing_scope={'scope': 'sw_2021'}, domain=None,
                last_error=f'{today} 申万增强失败: {str(e)[:200]}')
        except Exception as we:
            logger.warning(f'申万缺口登记失败（报告仍记录）: {we}')

    def enhance_only(self, dry_run: bool = False,
                     today: Optional[str] = None) -> Dict[str, Any]:
        """只执行申万增强口径（SCOPE_RETRY 子项补采；新浪主口径已验收不重跑）。

        today 缺省当日；跨午夜补采时由补采分发传入原待办锚定日（限流方案 §4.1：
        待办保存原作业锚定日），成功后按同日结清缺口。"""
        run_date = today or datetime.now().strftime('%Y%m%d')
        started = datetime.now()
        stats: Dict[str, Any] = {'sw_mode': None, 'rows': 0}
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'backfill': True}
        try:
            self.run_extra_snapshots(dry_run, run_date, stats)
            result['success'] = True
            if not dry_run:
                try:
                    self.db.complete_scope_retry('industry', run_date, 'sw_2021')
                except Exception as e:
                    logger.debug(f'申万缺口结清检查失败（忽略）: {e}')
        except ReviewBlockedError as e:
            mark_blocked(result, e, f'申万增强风控急停: {e}')
        except Exception as e:
            result['errors'].append(f'申万增强失败: {e}')
            logger.exception(f'申万增强失败: {e}')
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 国证一级日期快照（§6 批次 B） ----------

    def execute_cnindex(self, trade_date: Optional[str] = None,
                        dry_run: bool = False) -> Dict[str, Any]:
        """国证一级日期快照采集（独立于新浪/申万——解除新浪成功才能刷新其他体系的依赖）。

        trade_date 缺省按交易日历取最近已收盘交易日；目录未发布（404/无 id=4 组）
        记待补非失败。全部 11 个行业页级对账闭合后，一事务发布整套当日快照
        （§6：临时数据残缺时不产生大批"退出行业"）。
        """
        started = datetime.now()
        if trade_date:
            target = str(trade_date).replace('-', '')[:8]
        else:
            latest = latest_closed_trade_date(self.db)
            target = str(latest).replace('-', '')
        stats: Dict[str, Any] = {'trade_date': target, 'industries': 0,
                                 'unique_codes': 0, 'published': False,
                                 'missing': []}
        result: Dict[str, Any] = {'success': False, 'stats': stats,
                                  'errors': [], 'blocked': False}
        try:
            if not self.cnindex_source:
                result['errors'].append('国证源未启用（industry.cnindex_enabled=false）')
                return result
            snap = self.cnindex_source.fetch_market_snapshot(target)
            stats['industries'] = len(snap['catalog']['industries'])
            stats['unique_codes'] = snap['unique_codes']
            stats['catalog_date'] = snap['catalog']['catalog_date']
            # 当日证券池差集解释（§6：11-24 应为零；差异单列不阻塞快照）
            try:
                local_codes = {str(r['ts_code']) for r in self.db.execute_query(
                    "SELECT ts_code FROM base_stock_info")}
                snap_codes = set(snap['members'].keys())
                stats['local_not_in_snap'] = sorted(local_codes - snap_codes)[:20]
                stats['snap_not_in_local'] = sorted(snap_codes - local_codes)[:20]
                if local_codes - snap_codes or snap_codes - local_codes:
                    stats['missing'].append(
                        f'本地-快照差集 {len(local_codes - snap_codes)} / '
                        f'快照-本地 {len(snap_codes - local_codes)}')
            except Exception as e:
                stats['missing'].append(f'本地截面差集核对失败: {e}')
            if not dry_run:
                self.db.ensure_industry_snapshot_tables()
                rows = []
                for ts_code, r in snap['members'].items():
                    rows.append({
                        'trade_date': target, 'scheme': 'cnindex', 'level': 1,
                        'ts_code': ts_code,
                        'industry_code': r.get('industryCode1'),
                        'industry_name': r.get('industryName1'),
                        'source': 'cnindex', 'revision_id': None,
                    })
                with self.db.get_connection() as conn:
                    try:
                        self.db.replace_industry_snapshot_tx(
                            conn, target, 'cnindex', 1, rows)
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                stats['published'] = True
            result['success'] = True
        except ReviewBlockedError as e:
            mark_blocked(result, e, f'国证行业风控急停: {e}')
        except ValueError as e:
            # 目录未发布/结构变更——待补，不算已确认空
            result['errors'].append(f'{target} 国证行业待补: {e}')
            stats['missing'].append(str(e)[:160])
        except Exception as e:
            logger.exception(f'国证行业采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
        return result

    def _sw_apply_all_classification(self, rows, dry_run: bool,
                                     stats: Dict[str, Any]) -> None:
        """单请求路径入库：成分带官方起始/结束日期（时点成分，与概念映射口径对齐）。

        SwClass 的起始/结束日期即官方成员任期——退出再回归的股票 in_date 也随
        文件刷新（upsert_concept_maps_explicit，非快照 diff 语义）。
        """
        concept_rows: Dict[str, Dict[str, Any]] = {}
        map_rows: List[Dict[str, Any]] = []
        for r in rows:
            name = r['industry']
            concept_rows.setdefault(name, {'concept_code': name, 'concept_name': name,
                                           'source': 'sw_2021'})
            map_rows.append({
                'concept_code': name, 'ts_code': r['code6'],
                'in_date': r.get('in_date') or None,
                'out_date': r.get('out_date') or None,
            })
        if not dry_run:
            self.db.upsert_concepts(list(concept_rows.values()))
            self.db.upsert_concept_maps_explicit(map_rows)
        active = sum(1 for m in map_rows if not m['out_date'])
        stats['sw_names'] = len(concept_rows)
        stats['sw_members'] = active
        stats['sw_total_rows'] = len(map_rows)

    def report_extra_lines(self, stats: Dict[str, Any]) -> List[str]:
        if 'sw_names' not in stats:
            return []
        mode = stats.get('sw_mode', 'per_industry')
        if mode == 'all_download':
            return [
                f"增强口径：申万 2021 一级行业 {stats['sw_names']} 个 / "
                f"在册 {stats['sw_members']} 条（共 {stats.get('sw_total_rows', 0)} 行含历史任期，"
                f"单请求路径含时点成分）；报告行业榜优先本口径",
            ]
        return [
            f"增强口径：申万 2021 一级行业 {stats['sw_names']} 个 / "
            f"在册 {stats['sw_members']} 条（新进 {stats['sw_new']}，退出 {stats['sw_out']}，"
            f"{'单请求失败回退' if mode == 'per_industry_fallback' else '逐行业'}路径）；"
            f"报告行业榜优先本口径",
        ]
