"""
XDXR 股本采集管理器（baostock 下线换源；doc/数据源迁移方案-baostock下线-V1.0-20261025）

来源：tdx-api /api/xdxr（TDX 协议 0x000F，每股一次请求取全历史除权除息事件，
实测单请求约 50ms，退市股全历史可得）。事件字段：
- category 1=除权除息（股本字段全 0，无股本信息，弃用）
- category 2-10 股本变动类（送配股上市/非流通上市/股本变化/增发/回购/转债等），
  携带 qianzongguben/houzongguben/panqianliutong/panhouliutong，单位【万股】

行生成（写 base_fundamentals_info，冲突键 ts_code+stat_date）：
- stat_date = disclosure_date = 事件日。下游消费（kline_normalize.match_fundamentals
  的 bisect 区间匹配、fetch_total_shares_asof 估值、解禁 ratio、ths_finance 最新
  总股本）均为"≤某日期取最近一条"语义，事件日=股本生效日，比 baostock 财报
  披露日更精确且无前视
- total_share/float_share 换算为股（×1e4）；事件未变动的一侧（字段 0）延续前值
- 基线行：首事件前值，日期取 list_date（缺失取首事件日），防首事件前的
  历史K线 join 落空
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..config import ConfigManager
from ..data_sources.tdx_api_source import TdxApiSource, TdxApiBusinessError
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

# category 2-10 为股本变动类事件（1=除权除息无股本字段，见模块注释）
SHARE_EVENT_CATEGORIES = frozenset(range(2, 11))

# 连续业务失败达到该值判定 TDX 链路断死，中止本轮（断点已存，重启中间件后续跑）
CONSECUTIVE_FAILURE_ABORT = 30

DEFAULT_MANIFEST_DIR = Path(__file__).parent.parent.parent.parent / 'tmp'

_UPSERT_SQL = """
INSERT INTO base_fundamentals_info
(ts_code, stock_code, stock_name, stat_date, disclosure_date, total_share, float_share, create_time, update_time)
VALUES (%(ts_code)s, %(stock_code)s, %(stock_name)s, %(stat_date)s, %(disclosure_date)s, %(total_share)s, %(float_share)s, NOW(), NOW())
ON CONFLICT (ts_code, stat_date)
DO UPDATE SET
    stock_name = EXCLUDED.stock_name,
    disclosure_date = EXCLUDED.disclosure_date,
    total_share = EXCLUDED.total_share,
    float_share = EXCLUDED.float_share,
    update_time = NOW()
"""


def _to_share(wan: Optional[float], last: Optional[float]) -> Optional[float]:
    """万股转股；0/None 视为该侧未变动，延续前值 last（两侧均无则 None）"""
    if wan:
        return round(float(wan) * 10000)
    return last


def _norm_ymd(value: Any) -> Optional[str]:
    s = str(value or '').strip().replace('-', '')
    return s if len(s) == 8 else None


def xdxr_events_to_rows(
    ts_code: str,
    stock_code: str,
    stock_name: str,
    events: List[Dict[str, Any]],
    list_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """XDXR 事件序列 → base_fundamentals_info 行（纯函数，日期升序输出）

    events: /api/xdxr 返回的事件列表（date=YYYYMMDD，万股单位）
    list_date: 上市日期（YYYYMMDD 或 YYYY-MM-DD，用于首事件前基线行）
    """
    share_events = sorted(
        (e for e in events
         if e.get('category') in SHARE_EVENT_CATEGORIES
         and (e.get('houzongguben') or e.get('panhouliutong')
              or e.get('qianzongguben') or e.get('panqianliutong'))),
        key=lambda e: str(e.get('date') or ''),
    )
    if not share_events:
        return []

    rows: List[Dict[str, Any]] = []
    last_total: Optional[float] = None
    last_float: Optional[float] = None

    # 基线行：首事件前值（防首事件前历史K线 join 落空；前值缺失回退首事件后值）
    first = share_events[0]
    base_total = _to_share(first.get('qianzongguben'), None) or _to_share(first.get('houzongguben'), None)
    base_float = _to_share(first.get('panqianliutong'), None) or _to_share(first.get('panhouliutong'), None)
    base_date = _norm_ymd(list_date) or str(first.get('date'))
    first_date = str(first.get('date'))
    if base_date > first_date:
        base_date = first_date
    rows.append({
        'ts_code': ts_code, 'stock_code': stock_code, 'stock_name': stock_name,
        'stat_date': base_date, 'disclosure_date': base_date,
        'total_share': base_total, 'float_share': base_float,
    })
    last_total, last_float = base_total, base_float

    for e in share_events:
        d = str(e.get('date') or '')
        if not d:
            continue
        total = _to_share(e.get('houzongguben'), last_total)
        flt = _to_share(e.get('panhouliutong'), last_float)
        rows.append({
            'ts_code': ts_code, 'stock_code': stock_code, 'stock_name': stock_name,
            'stat_date': d, 'disclosure_date': d,
            'total_share': total, 'float_share': flt,
        })
        last_total, last_float = total, flt

    return rows


class XdxrShareManager:
    """XDXR 股本采集：全量重导 / 每日增量刷新（同一 execute，全刷约 5-15 分钟）"""

    def __init__(self, config_manager: ConfigManager = None, manifest_path: Optional[Path] = None):
        self.config = config_manager or ConfigManager()
        self.db = DatabaseConnection(self.config)
        self.tdx = TdxApiSource(self.config.get('data_sources.tdx_api', {}))
        self.logger = logging.getLogger(__name__)
        self.interval = float(self.config.get('sync.shares_xdxr_interval', 0.1))
        self.manifest_path = Path(manifest_path) if manifest_path else DEFAULT_MANIFEST_DIR / 'xdxr_share_manifest.json'

    # ---------- manifest 断点（复用 fundamentals_rebuild 模式：原子落盘） ----------

    def _load_manifest(self) -> Dict[str, Any]:
        if self.manifest_path.exists():
            try:
                return json.loads(self.manifest_path.read_text(encoding='utf-8'))
            except Exception as e:
                self.logger.warning(f"manifest 读取失败，视为空重开: {e}")
        return {'completed': {}, 'no_data': {}, 'failed': {}, 'started_at': None, 'updated_at': None}

    def _save_manifest(self, manifest: Dict[str, Any]) -> None:
        manifest['updated_at'] = datetime.now().isoformat()
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.manifest_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding='utf-8')
        tmp.replace(self.manifest_path)

    # ---------- 股票清单 ----------

    def _load_stocks(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """全量 type='1'（含退市——XDXR 退市股全历史可得，2026-10-25 实测）"""
        query = """
            SELECT ts_code, stock_code, stock_name, list_date
            FROM base_stock_info WHERE type = '1'
        """
        params: List[Any] = []
        if ts_codes:
            query += " AND ts_code = ANY(%s)"
            params.append(ts_codes)
        query += " ORDER BY ts_code"
        return self.db.execute_query(query, tuple(params))

    # ---------- 单股替换（事务：DELETE + 批量 INSERT） ----------

    def _replace_stock_rows(self, stock: Dict[str, Any], rows: List[Dict[str, Any]]) -> int:
        ts_code = stock['ts_code']
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM base_fundamentals_info WHERE ts_code = %s", (ts_code,))
                cur.executemany(_UPSERT_SQL, rows)
            conn.commit()
        return len(rows)

    # ---------- 主流程 ----------

    def execute(
        self,
        ts_codes: Optional[List[str]] = None,
        resume: bool = True,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        stocks = self._load_stocks(ts_codes)
        manifest = self._load_manifest()
        if not resume or ts_codes:
            manifest = {'completed': {}, 'no_data': {}, 'failed': {}, 'started_at': datetime.now().isoformat(), 'updated_at': None}
        elif not manifest.get('started_at'):
            manifest['started_at'] = datetime.now().isoformat()

        stats = {'total': len(stocks), 'ok': 0, 'no_data': 0, 'failed': 0, 'rows': 0, 'skipped': 0}
        start_time = time.time()
        consecutive_bad = 0
        self.logger.info(f"XDXR 股本采集开始：目标 {len(stocks)} 只（断点 {len(manifest['completed'])}，间隔 {self.interval}s）")

        try:
            for i, stock in enumerate(stocks, 1):
                ts_code = stock['ts_code']
                if ts_code in manifest['completed']:
                    stats['skipped'] += 1
                    continue
                code = str(stock['stock_code'] or ts_code.split('.')[-1])
                try:
                    events = self.tdx.get_xdxr_strict(code)
                    consecutive_bad = 0
                    rows = xdxr_events_to_rows(
                        ts_code, code, stock.get('stock_name'), events,
                        list_date=stock.get('list_date'),
                    )
                    if rows:
                        if not dry_run:
                            stats['rows'] += self._replace_stock_rows(stock, rows)
                        else:
                            stats['rows'] += len(rows)
                        manifest['completed'][ts_code] = {'events': len(events), 'rows': len(rows)}
                        manifest['no_data'].pop(ts_code, None)
                        manifest['failed'].pop(ts_code, None)
                        stats['ok'] += 1
                    else:
                        # 查询成功但无事件（北交所部分股票等）：保留原有行
                        manifest['no_data'][ts_code] = {'events': len(events or [])}
                        manifest['failed'].pop(ts_code, None)
                        stats['no_data'] += 1
                except TdxApiBusinessError as e:
                    # 瞬时故障（TDX 链路断开/超时）：记 failed 待续跑，连续达标熔断中止
                    consecutive_bad += 1
                    manifest['failed'][ts_code] = str(e)
                    stats['failed'] += 1
                    self.logger.warning(f"{ts_code} 业务失败({consecutive_bad}/{CONSECUTIVE_FAILURE_ABORT}): {e}")
                    if consecutive_bad >= CONSECUTIVE_FAILURE_ABORT:
                        self.logger.error(
                            f"连续 {CONSECUTIVE_FAILURE_ABORT} 次业务失败，判定 TDX 链路断死，中止本轮。"
                            "处置：docker restart tdx-stock-web 后原命令断点续跑"
                        )
                        break
                except Exception as e:
                    manifest['failed'][ts_code] = str(e)
                    stats['failed'] += 1
                    self.logger.warning(f"{ts_code} 采集失败: {e}")

                if i % 200 == 0 or i == len(stocks):
                    self._save_manifest(manifest)
                    elapsed = time.time() - start_time
                    done = stats['ok'] + stats['no_data'] + stats['failed']
                    eta = elapsed / max(done, 1) * (len(stocks) - i) if done else 0
                    self.logger.info(
                        f"进度 {i}/{len(stocks)} ok={stats['ok']} no_data={stats['no_data']} "
                        f"failed={stats['failed']} rows={stats['rows']} 已用={elapsed:.0f}s ETA={eta:.0f}s"
                    )
                if self.interval > 0:
                    time.sleep(self.interval)
        except KeyboardInterrupt:
            self.logger.warning("手动中断，断点已保存")
        finally:
            self._save_manifest(manifest)

        stats['duration'] = round(time.time() - start_time, 1)
        stats['manifest'] = str(self.manifest_path)
        return stats


def sync_shares_xdxr(config_manager: ConfigManager = None, **options) -> Dict[str, Any]:
    """编排器/CLI 入口（与其它 sync_fundamentals* 入口同构）"""
    return XdxrShareManager(
        config_manager,
        manifest_path=Path(options['manifest_path']) if options.get('manifest_path') else None,
    ).execute(
        ts_codes=options.get('codes'),
        resume=bool(options.get('resume', True)),
        dry_run=bool(options.get('dry_run', False)),
    )
