"""财报版本行管理（盘后采集数据源整合详细技术方案 V1.0 §7.3，批次 C 接线第一步）。

两路写入 financial_report_version（表结构见批次 C 迁移）：
- **公告版本行**（source='announcement'）：披露证据链——announcement_daily 中
  定期报告/业绩快报（批次 C 已把"业绩说明会预告"等分到非正文公告，不再误入），
  经标题解析得 (ts_code, stat_date, report_kind)；财务数值列全 NULL（数值由
  季度包/THS 源提供），published_at 取公告日（精度 date，§7.1）。
- **季度包版本行**（source='tdx_gpcw'）：数值版本——gpcw 季度包解析结果
  （tdx_reference_files），source_version=包 SHA-256 前缀（更正换文件内容时
  同一期缓存用内容哈希区分，§7.2），字段按 TARGET_FIELDS 换算落值。

版本选择语义（§7.3）：唯一约束 (ts_code,stat_date,source,source_version,content_hash)
天然去重；当前投影仍按 (ts_code,stat_date) 由既有 base_financial_report 提供，
历史查询走 asof 读方法（fetch_financial_versions_asof，批次 C 接线第二步）。
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.cninfo_source import CninfoSource
from ..data_sources.tdx_reference_files import (TARGET_FIELDS, fetch_pack,
                                                fetch_pack_list, pack_to_rows)
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

# 报告期 NN -> MMDD（与 cninfo_source.NN_PERIOD_END 同口径，紧凑格式）
_NN_MD = {'01': '0331', '02': '0630', '03': '0930', '04': '1231'}


class FinancialVersionManager:
    """财报版本行：公告证据行 + 季度包数值行 -> financial_report_version"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_financial_version_tables()
        cninfo_cfg = dict(self.config.get('data_sources', {}).get('cninfo', {}) or {})
        cninfo_cfg.setdefault('query_base_url', 'http://www.cninfo.com.cn')
        self.cninfo = CninfoSource(cninfo_cfg)
        review = self.config.get('review_sync', {}) or {}
        # 季度包字段权威次序的消费由 asof 查询承担；本管理器只按包内容哈希写行
        self.gpcw_enabled = bool(review.get('financial_report_gpcw_enabled', True))

    # ---------- 公告 → 版本行 ----------

    def build_announcement_versions(self, days_back: int = 10,
                                    dry_run: bool = False) -> Dict[str, Any]:
        """announcement_daily 窗口内定期报告/业绩快报 → 披露证据版本行。

        排除：非正文公告（说明会/预约/取消——§7.1 优先识别）、摘要/英文版。
        标题解析失败或报告期缺失的行不硬造（跳过并计数）。
        """
        rows: List[Dict[str, Any]] = []
        stats: Dict[str, Any] = {'scanned': 0, 'rows': 0, 'skipped_non_report': 0,
                                 'skipped_unparsed': 0}
        ann_rows = self.db.execute_query("""
            SELECT announcement_id, ann_date, ts_code, title, ann_type
            FROM announcement_daily
            WHERE ann_type IN ('定期报告', '业绩快报')
              AND ann_date >= TO_CHAR(NOW() - (%s || ' days')::interval, 'YYYYMMDD')
            ORDER BY ann_date
        """, (str(days_back),))
        stats['scanned'] = len(ann_rows)
        for a in ann_rows:
            title = str(a['title'] or '')
            if any(k in title for k in ('摘要', '英文版', '说明会', '预约', '取消',
                                        '提示性公告')):
                stats['skipped_non_report'] += 1
                continue
            parsed = CninfoSource.parse_report_title(title)
            if parsed is None:
                stats['skipped_unparsed'] += 1
                continue
            year, nn, is_revision = parsed
            stat_date = f'{year}{_NN_MD[nn]}'
            kind = 'revised' if is_revision else 'formal'
            if a['ann_type'] == '业绩快报':
                kind = 'express'
            rows.append({
                'version_id': str(uuid.uuid4()),
                'ts_code': a['ts_code'],
                'stat_date': stat_date,
                'announcement_id': a['announcement_id'],
                'report_kind': kind,
                'source': 'announcement',
                'source_version': str(a['announcement_id']),
                'published_at': _date_to_tz(a['ann_date']),
                'published_precision': 'date',
                'first_observed_at': _date_to_tz(a['ann_date']),
                'availability_basis': 'system_observed',
                'content_hash': _content_hash(a['ts_code'], stat_date, kind,
                                              a['announcement_id'], title),
            })
        stats['rows'] = len(rows)
        if rows and not dry_run:
            self.db.upsert_financial_report_versions(rows)
        return {'success': True, 'stats': stats}

    # ---------- 季度包 → 版本行 ----------

    def build_pack_versions(self, periods: Optional[List[str]] = None,
                            dry_run: bool = False) -> Dict[str, Any]:
        """季度包解析结果 → 数值版本行（按包内容哈希区分版本；同包不重复下载解析）。

        periods 缺省取清单前 8 期（最新两年度）；解析失败/包缺失记失败不阻塞。
        """
        stats: Dict[str, Any] = {'packs': 0, 'rows': 0, 'failed': []}
        entries = fetch_pack_list()
        if not periods:
            periods = [e['name'][4:12] for e in entries[:8]
                       if e['name'].startswith('gpcw')]
        rows: List[Dict[str, Any]] = []
        for period in periods:
            try:
                pack = fetch_pack(period)
            except Exception as e:
                stats['failed'].append(f'{period}: {str(e)[:100]}')
                continue
            src_version = f'gpcw:{pack.sha256[:16]}'
            for r in pack_to_rows(pack):
                code6 = r['stock_code']
                ts_code = _code6_to_ts(code6)
                if not ts_code:
                    continue
                item = {
                    'version_id': str(uuid.uuid4()),
                    'ts_code': ts_code,
                    'stat_date': period,
                    'announcement_id': None,
                    'report_kind': 'quarterly_pack',
                    'source': 'tdx_gpcw',
                    'source_version': src_version,
                    'published_at': None,
                    'published_precision': None,
                    'first_observed_at': _now_tz(),
                    'availability_basis': 'system_observed',
                    'content_hash': _content_hash(ts_code, period, 'quarterly_pack',
                                                  src_version,
                                                  str({k: r.get(k) for k in
                                                       ('net_profit', 'revenue',
                                                        'net_assets')})),
                }
                provenance: Dict[str, Any] = {}
                for f in TARGET_FIELDS:
                    if f == 'report_date':
                        if r.get('report_date'):
                            provenance['report_date_evidence'] = r['report_date']
                        continue
                    item[f] = r.get(f)
                item['field_provenance'] = provenance or None
                rows.append(item)
            stats['packs'] += 1
        stats['rows'] = len(rows)
        if rows and not dry_run:
            self.db.upsert_financial_report_versions(rows)
        return {'success': not stats['failed'] or bool(rows), 'stats': stats}

    def run(self, days_back: int = 10, periods: Optional[List[str]] = None,
            dry_run: bool = False) -> Dict[str, Any]:
        """双路回填（公告证据行 + 季度包数值行）"""
        r1 = self.build_announcement_versions(days_back=days_back, dry_run=dry_run)
        r2 = self.build_pack_versions(periods=periods, dry_run=dry_run) \
            if self.gpcw_enabled else {'stats': {'skipped': 'gpcw disabled'}}
        return {'success': bool(r1.get('success')) and bool(r2.get('success')),
                'announcement': r1['stats'], 'packs': r2.get('stats')}


def _date_to_tz(ann_date: Any) -> Any:
    """'YYYYMMDD' -> TIMESTAMPTZ（当日 00:00 +08；精度 date 语义，§7.1）"""
    s = str(ann_date or '').replace('-', '')[:8]
    try:
        return datetime.strptime(s, '%Y%m%d')
    except ValueError:
        return None


def _now_tz() -> datetime:
    return datetime.now()


def _content_hash(*parts: Any) -> str:
    return hashlib.sha1('|'.join(str(p) for p in parts).encode('utf-8')).hexdigest()[:16]


def _code6_to_ts(code6: str) -> Optional[str]:
    try:
        from .review_common import code6_to_ts_code
        return code6_to_ts_code(code6)
    except Exception:
        return None
