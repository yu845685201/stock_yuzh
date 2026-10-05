"""
申万二级行业成分同步（延后项补齐方案 N3，2026-10-04 探路锁定并实施）

路径：THS basic.10jqka.com.cn/{code}/company.html 服务端渲染
"所属申万行业：{一级} — {二级}"（与 ths_source 财务接口同站同管道，
普通 UA + Referer，无 hexin-v；4 样本格式统一、GBK 编码）。

入库口径：
- concept_code 用「{一级}—{二级}」组合码（如"银行—股份制银行Ⅱ"）：一级概念以
  code=名称入库（industry_manager sw_2021 规则），裸二级名会与之撞 code 触发
  ON CONFLICT 把一级行的 source 覆盖（2026-10-04 实跑事故，已修复）；组合码天然无冲突；
  concept_name = 二级名原文，source='sw_2021_l2_ths'；
- THS 个别页面存在一级名占两段的异常版式（如「钢铁 — 钢铁」）——不可作二级口径，
  如实计缺（l1==l2 校验）；
- upsert_concept_maps_explicit（in_date=快照日 yyyyMMdd, out_date=NULL）——不用快照
  diff 模式，避免临时抓取失败被误记退出；
- 口径局限（报告标注）：THS 为当前快照、无 in_date/out_date 历史——二级成分仅
  "当前时点"准确，历史行业切换不可回溯（一级已有官方时点日期）；
- 北交所股票无申万分类属正常，如实计缺（探路实测部分 BSE 股有申万分类，有则采）。

增量：目标集 = base_stock_info 全部 ts_code − sector_concept_map(source=sw_2021_l2_ths)
在册 ts_code，重跑自动跳过已抓；--init 全量（首次 ~5,300 只 × 2s 限速 ≈ 3 小时）。
单股失败/无申万分类均不阻塞（分别记 failed/missing 清单入报告，次轮增量自动补）。

安全红线：串行限速（ths_interval）、正常 UA/Referer、不破解任何技术措施、
风控识别即退避冷却（ThsBlockedError）、仅自用不分发。
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.ths_source import ThsBlockedError, ThsSource
from ..database.connection import DatabaseConnection
from .review_common import ts_code_to_code6, write_review_report

logger = logging.getLogger(__name__)

SOURCE_TAG = 'sw_2021_l2_ths'
# 分批落库间隔（股）：中途断点保留进度，重跑自动续
FLUSH_EVERY = 200


class ThsIndustryL2Manager:
    """申万二级行业：THS 公司概况页 -> sector_concept(_map)，source=sw_2021_l2_ths"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        section = review.get('ths_l2', {}) or {}
        self.enabled = bool(section.get('enabled', False))
        self.source = ThsSource(self.config)

    # ---------- 目标集 ----------

    def _target_codes(self, init_mode: bool, codes: Optional[List[str]]) -> List[str]:
        """目标 ts_code 清单：--init 全量 / 缺省增量（新上市 + 在册缺失）；codes 显式指定优先"""
        if codes:
            return list(dict.fromkeys(codes))
        universe = [r['ts_code'] for r in self.db.fetch_stock_basic()]
        if init_mode:
            return universe
        mapped: set = set()
        for members in self.db.fetch_concept_active_codes(source=SOURCE_TAG).values():
            mapped.update(members)
        return [ts for ts in universe if ts not in mapped]

    # ---------- 主流程 ----------

    def execute(self, init_mode: bool = False, codes: Optional[List[str]] = None,
                dry_run: bool = False) -> Dict[str, Any]:
        """抓取目标股申万二级并入库（串行限速，单股失败不阻塞）"""
        started = datetime.now()
        stats: Dict[str, Any] = {
            'mode': 'init 全量' if init_mode else '增量',
            'targets': 0, 'ok': 0, 'rows': 0, 'concepts': 0,
            'missing': [], 'failed': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        if not self.enabled:
            result['errors'].append('配置停用（review_sync.ths_l2.enabled=false）')
            result['skipped_disabled'] = True
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
            return result
        try:
            targets = self._target_codes(init_mode, codes)
            stats['targets'] = len(targets)
            if not targets:
                result['success'] = True
                return self._finish(result, started)

            today = datetime.now().strftime('%Y%m%d')   # sector_concept_map.in_date 为 varchar(8)
            pending_concepts: Dict[str, Dict[str, Any]] = {}
            pending_maps: List[Dict[str, Any]] = []
            concept_names: set = set()
            for i, ts_code in enumerate(targets, 1):
                code6 = ts_code_to_code6(ts_code)
                try:
                    pair = self.source.fetch_sw_industry(code6)
                except ThsBlockedError:
                    # 风控急停：已完成批次照常落库（幂等），急停上抛由编排器冷却重试
                    self._flush(pending_concepts, pending_maps, dry_run)
                    raise
                except Exception as e:
                    stats['failed'].append(f'{ts_code}: {e}')
                    continue
                if pair is None:
                    # 无申万分类（页面缺失/格式变更同口径，如实计缺不入库）
                    stats['missing'].append(ts_code)
                elif pair[0] == pair[1]:
                    # 页面异常版式（一级名占两段，如「钢铁 — 钢铁」）：不可作二级口径，计缺
                    stats['missing'].append(ts_code)
                else:
                    l1, l2 = pair
                    code = f'{l1}—{l2}'
                    if code not in concept_names:
                        concept_names.add(code)
                        pending_concepts[code] = {'concept_code': code,
                                                  'concept_name': l2,
                                                  'source': SOURCE_TAG}
                    pending_maps.append({'concept_code': code, 'ts_code': ts_code,
                                         'in_date': today, 'out_date': None})
                    stats['ok'] += 1
                if i % FLUSH_EVERY == 0 and pending_maps:
                    self._flush(pending_concepts, pending_maps, dry_run)
                    logger.info(f'申万二级进度 {i}/{len(targets)}（已落库分批）')
            self._flush(pending_concepts, pending_maps, dry_run)
            stats['concepts'] = len(concept_names)
            stats['rows'] = stats['ok']
            result['success'] = stats['ok'] > 0 or not stats['failed']
        except ThsBlockedError as e:
            result['blocked'] = True
            result['errors'].append(f'同花顺风控急停（已完成批次已入库，重跑自动续）: {e}')
        except Exception as e:
            logger.exception(f'申万二级行业采集失败: {e}')
            result['errors'].append(str(e))
        return self._finish(result, started)

    def _flush(self, concepts: Dict[str, Dict[str, Any]], maps: List[Dict[str, Any]],
               dry_run: bool) -> None:
        """分批落库（concept 去重后随 map 同批；dry_run 只累计不写库）"""
        if dry_run or not maps:
            return
        if concepts:
            self.db.upsert_concepts(list(concepts.values()))
        self.db.upsert_concept_maps_explicit(maps)
        concepts.clear()
        maps.clear()

    def _finish(self, result: Dict[str, Any], started: datetime) -> Dict[str, Any]:
        result['duration'] = (datetime.now() - started).total_seconds()
        result['report_path'] = self._write_report(result)
        return result

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        if result.get('skipped_disabled'):
            lines = [f"- enabled：False（config review_sync.ths_l2.enabled）"]
            status = '待补（配置停用）'
        else:
            lines = [
                f"- 模式：{stats['mode']}；目标 {stats['targets']} 只，"
                f"成功 {stats['ok']}（二级 {stats['concepts']} 个）",
                f"- 无申万分类 {len(stats['missing'])} 只（如实计缺；北交所无申万属正常）",
                f"- 口径：THS 当前快照，二级成分无历史时点（一级已有官方时点日期）",
            ]
            if stats['failed']:
                lines.append(f"- 失败 {len(stats['failed'])} 只（次轮增量自动补）："
                             f"{stats['failed'][:20]}")
            if result['errors']:
                lines.append(f"- 错误：{result['errors']}")
            status = '风控急停' if result['blocked'] else ('成功' if result['success'] else '失败')
        return write_review_report('申万二级行业同步报告', 'ths_industry_l2_sync_report',
                                   status, lines, result.get('duration'))
