"""
股票列表维护管理器（baostock 下线换源；doc/数据源迁移方案-baostock下线-V1.0-20261025）

数据源：tdx-api /api/codes（中间件每日 09:00 从 TDX 0x0450 自动刷新本地
SQLite，请求秒级；仅含在市证券，退市股不在列表）。

维护范围聚焦 type='1' 股票（全部消费方只读 type='1' / list_status / 名称 /
list_date / delist_date；指数/ETF/可转债存量行保留冻结——etf_scale 等独立采集）：
- 新股：快照新增且库中无 → INSERT；list_date 取 /api/kline-all 日K首根日期
  （首日K线=上市日，IPO 精确）；cnspell 复用 DataTransformer.generate_pinyin
- 名称变更：快照与库中名称不一致 → UPDATE（ST 摘帽/戴帽跟随）
- list_date 回填：仅库中为 NULL 的行（如北交所历史行），用日K首根日期补
- 退市：库中 'L' 连续 N 日（默认 5）缺席快照，且该股最后日K早于 90 天前
  （长期停市判据，防停牌误杀）→ 置 'D'，delist_date=最后日K日期；
  缺席计数持久化 tmp/stock_list_absence.json
- 首次运行默认仅产出 diff 报告（tmp/stock_list_diff_report_*.md）人工过目，
  --apply 才落库
"""

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..config import ConfigManager
from ..data_sources.tdx_api_source import TdxApiSource
from ..database.connection import DatabaseConnection
from ..utils.data_transformer import DataTransformer

logger = logging.getLogger(__name__)

TMP_DIR = Path(__file__).parent.parent.parent.parent / 'tmp'

# 交换所元数据（DataTransformer.get_market_info 不支持 bj，此处本地维护）
MARKET_META = {
    'sh': ('sh', '上海证券交易所', 'SSE'),
    'sz': ('sz', '深圳证券交易所', 'SZSE'),
    'bj': ('bj', '北京证券交易所', 'BJSE'),
}

# A 股代码段白名单（快照过滤用；排除 038 深市优先股等特殊证券——实测快照混入
# sz.038043 康佳KJP1；302/689 为创业板扩容段/存托凭证，属正常股票）
STOCK_CODE_PREFIXES = (
    '000', '001', '002', '003', '300', '301', '302',          # sz
    '600', '601', '603', '605', '688', '689',                  # sh
    '430', '83', '87', '92',                                   # bj（快照暂不含，防御性保留）
)

# 行情名临时除权标记（交易所惯例：XD除息/XR除权/DR除权除息日名称前缀，数日后
# 自动消失；C 为上市初期标记。带标记的名称不参与改名判定，防止除息季反复改写）
_NAME_MARKERS = ('XD', 'XR', 'DR', 'C')


def normalize_name(name: str) -> str:
    """名称归一化：全角转半角（NFKC）+ 去所有空格"""
    import unicodedata
    return unicodedata.normalize('NFKC', str(name or '')).replace(' ', '').replace('\u3000', '').strip()


def has_temp_marker(name: str) -> bool:
    n = normalize_name(name)
    return any(n.startswith(m) for m in _NAME_MARKERS)


def is_truncated_name(db_name: str, snap_name: str) -> bool:
    """TDX 名称字段按 4 个汉字截断（实测：华润新能源→华润新能）；旧名以新名
    （归一化后）为前缀且新名无临时标记 → 视为截断，保持库内全名不改"""
    old, new = normalize_name(db_name), normalize_name(snap_name)
    return bool(old and new and len(new) < len(old) and old.startswith(new))

_INSERT_SQL = """
INSERT INTO base_stock_info (
    ts_code, stock_code, stock_name, cnspell, market_code, market_name,
    exchange_code, sector_code, sector_name, industry_code, industry_name,
    list_status, list_date, delist_date, type
)
VALUES (%(ts_code)s, %(stock_code)s, %(stock_name)s, %(cnspell)s, %(market_code)s,
        %(market_name)s, %(exchange_code)s, NULL, NULL, NULL, NULL,
        'L', %(list_date)s, NULL, '1')
ON CONFLICT (ts_code) DO UPDATE SET
    stock_name = EXCLUDED.stock_name,
    cnspell = EXCLUDED.cnspell,
    list_status = 'L',
    list_date = COALESCE(base_stock_info.list_date, EXCLUDED.list_date),
    delist_date = NULL
"""


def build_stock_row(code: str, name: str, exchange: str, list_date: Optional[str]) -> Dict[str, Any]:
    """快照项 → base_stock_info 行 dict（type='1'，exchange ∈ sh/sz/bj）"""
    market_code, market_name, exchange_code = MARKET_META.get(exchange, (exchange, exchange, exchange.upper()))
    return {
        'ts_code': f'{exchange}.{code}',
        'stock_code': code,
        'stock_name': name,
        'cnspell': DataTransformer.generate_pinyin(name),
        'market_code': market_code,
        'market_name': market_name,
        'exchange_code': exchange_code,
        'list_date': list_date,
    }


def kline_first_last_date(tdx: TdxApiSource, code: str) -> Optional[Dict[str, str]]:
    """日K全历史首末日期（YYYYMMDD；'Time' RFC3339 → 取前 10 位去横线）"""
    bars = tdx.get_kline_all(code, 'day')
    dates = sorted(str((b or {}).get('Time') or '')[:10].replace('-', '') for b in bars if (b or {}).get('Time'))
    if not dates:
        return None
    return {'first': dates[0], 'last': dates[-1]}


class StockListManager:
    """股票列表快照维护（新股/改名/退市标记/list_date 回填）"""

    def __init__(self, config_manager: ConfigManager = None):
        self.config = config_manager or ConfigManager()
        self.db = DatabaseConnection(self.config)
        self.tdx = TdxApiSource(self.config.get('data_sources.tdx_api', {}))
        self.logger = logging.getLogger(__name__)
        self.absence_path = TMP_DIR / 'stock_list_absence.json'
        self.delist_absent_days = int(self.config.get('sync.stock_list_delist_absent_days', 5))
        self.delist_stale_kline_days = int(self.config.get('sync.stock_list_delist_stale_days', 90))

    # ---------- 快照与存量 ----------

    def _fetch_snapshot(self) -> List[Dict[str, Any]]:
        snap = self.tdx.get_market_stock_codes()
        out = []
        for item in snap:
            code = str(item.get('code') or '').replace('.', '').strip()
            exchange = str(item.get('exchange') or '').strip().lower()
            name = str(item.get('name') or '').strip()
            if len(code) == 6 and exchange in MARKET_META and name and code.startswith(STOCK_CODE_PREFIXES):
                out.append({'ts_code': f'{exchange}.{code}', 'code': code,
                            'exchange': exchange, 'name': name})
        return out

    def _load_db_stocks(self) -> Dict[str, Dict[str, Any]]:
        rows = self.db.execute_query(
            "SELECT ts_code, stock_code, stock_name, cnspell, market_code, market_name, "
            "exchange_code, list_status, list_date, delist_date, type "
            "FROM base_stock_info WHERE type = '1'"
        )
        return {r['ts_code']: r for r in rows}

    # ---------- 缺席计数 ----------

    def _load_absence(self) -> Dict[str, int]:
        if self.absence_path.exists():
            try:
                return json.loads(self.absence_path.read_text(encoding='utf-8'))
            except Exception:
                pass
        return {}

    def _save_absence(self, data: Dict[str, int]) -> None:
        self.absence_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.absence_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
        tmp.replace(self.absence_path)

    # ---------- 主流程 ----------

    def execute(self, apply: bool = False) -> Dict[str, Any]:
        started = time.time()
        snapshot = self._fetch_snapshot()
        snap_by_ts = {s['ts_code']: s for s in snapshot}
        db_stocks = self._load_db_stocks()
        self.logger.info(f"快照 {len(snapshot)} 只 vs 库内 type='1' {len(db_stocks)} 只")

        new_stocks = [s for ts, s in snap_by_ts.items() if ts not in db_stocks]
        renamed = []
        for ts, s in snap_by_ts.items():
            db = db_stocks.get(ts)
            if not db or not db.get('stock_name'):
                continue
            # 带临时除权标记（XD/XR/DR/C）的行情名不参与改名判定（见 _NAME_MARKERS 注释）
            if has_temp_marker(s['name']) or has_temp_marker(db['stock_name']):
                continue
            if is_truncated_name(db['stock_name'], s['name']):
                continue
            if normalize_name(s['name']) != normalize_name(db['stock_name']):
                renamed.append((ts, db['stock_name'], s['name']))

        # 缺席统计与退市判定（快照仅覆盖沪深——TDX sh/sz 服务器不含北交所，
        # bj.* 不参与缺席统计，否则会被误判退市；北交所维护走 bj_kline_manager 既有链路）
        absent = {ts: db for ts, db in db_stocks.items()
                  if db.get('list_status') == 'L' and ts not in snap_by_ts
                  and not ts.startswith('bj.')}
        absence_counts = self._load_absence()
        counted = {ts: absence_counts.get(ts, 0) + 1 for ts in absent}
        absence_counts = {ts: c for ts, c in counted.items() if c > 0}

        delist_candidates = [ts for ts, c in counted.items() if c >= self.delist_absent_days]
        delisted, kept_suspect = [], []
        today = datetime.now().date()
        for ts in delist_candidates:
            db = db_stocks[ts]
            span = self.db.execute_query(
                "SELECT MAX(trade_date) AS d FROM his_kline_day WHERE ts_code = %s", (ts,))
            last_raw = str(span[0]['d']).replace('-', '') if span and span[0].get('d') else None
            if last_raw and len(last_raw) == 8:
                last_kline = date(int(last_raw[:4]), int(last_raw[4:6]), int(last_raw[6:8]))
                stale = (today - last_kline).days > self.delist_stale_kline_days
                delist_date = last_raw
            else:
                # 无日K记录：缺席次数本身已≥N，且无任何近期成交痕迹 → 判退市
                stale = True
                delist_date = None
            if stale:
                delisted.append({'ts_code': ts, 'delist_date': delist_date,
                                 'last_name': db.get('stock_name')})
            else:
                kept_suspect.append(ts)

        # list_date 缺失回填（仅 NULL 行，快照在市的才补，避免退市股逐只拉K线）
        backfill = [ts for ts, db in db_stocks.items()
                    if ts in snap_by_ts and db.get('list_status') == 'L' and not db.get('list_date')]

        report = {
            'snapshot': len(snapshot), 'db_stocks': len(db_stocks),
            'new_stocks': [{'ts_code': s['ts_code'], 'name': s['name']} for s in new_stocks],
            'renamed': [{'ts_code': ts, 'from': f, 'to': t} for ts, f, t in renamed],
            'delisted': delisted,
            'absent_watch': sorted(counted.items(), key=lambda x: -x[1])[:50],
            'kept_suspect': kept_suspect,
            'list_date_backfill': backfill,
            'apply': apply,
            '_snapshot_rows': snapshot,  # 供调用方（sync_stocks）取快照明细
        }

        if apply:
            self._apply(new_stocks, renamed, delisted, backfill, snap_by_ts)
        self._save_absence(absence_counts)

        report_path = self._write_report(report)
        report['report_path'] = str(report_path)
        report['duration'] = round(time.time() - started, 1)
        return report

    def _apply(self, new_stocks, renamed, delisted, backfill, snap_by_ts) -> None:
        # 新股：逐只拉日K推导 list_date（数量少）
        with self.db.get_connection() as conn:
            with conn.cursor() as cur:
                for s in new_stocks:
                    span = kline_first_last_date(self.tdx, s['code'])
                    row = build_stock_row(s['code'], s['name'], s['exchange'],
                                          span['first'] if span else None)
                    if row['list_date'] is None:
                        self.logger.warning(f"{s['ts_code']} 无日K，list_date 留空")
                    cur.execute(_INSERT_SQL, row)
                    time.sleep(0.1)
                for ts, _from, _to in renamed:
                    cur.execute(
                        "UPDATE base_stock_info SET stock_name = %s, cnspell = %s WHERE ts_code = %s",
                        (snap_by_ts[ts]['name'], DataTransformer.generate_pinyin(snap_by_ts[ts]['name']), ts))
                for d in delisted:
                    cur.execute(
                        "UPDATE base_stock_info SET list_status = 'D', delist_date = %s WHERE ts_code = %s",
                        (d['delist_date'], d['ts_code']))
                for ts in backfill:
                    code = ts.split('.')[-1]
                    span = kline_first_last_date(self.tdx, code)
                    if span:
                        cur.execute(
                            "UPDATE base_stock_info SET list_date = %s WHERE ts_code = %s AND list_date IS NULL",
                            (span['first'], ts))
                    time.sleep(0.1)
            conn.commit()
        self.logger.info(
            f"落库完成：新股 {len(new_stocks)}，改名 {len(renamed)}，退市 {len(delisted)}，"
            f"list_date 回填 {len(backfill)}")

    def _write_report(self, report: Dict[str, Any]) -> Path:
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        path = TMP_DIR / f"stock_list_diff_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        mode = '**已落库**' if report['apply'] else '**仅报告（--apply 落库）**'
        lines = [
            f"# 股票列表快照 diff 报告（{datetime.now().isoformat(timespec='seconds')}）",
            f"模式：{mode}；快照 {report['snapshot']} vs 库内 {report['db_stocks']}",
            f"- 新股（{len(report['new_stocks'])}）：",
            *[f"  - {s['ts_code']} {s['name']}" for s in report['new_stocks'][:100]],
            f"- 名称变更（{len(report['renamed'])}）：",
            *[f"  - {r['ts_code']}: {r['from']} → {r['to']}" for r in report['renamed'][:100]],
            f"- 退市判定（{len(report['delisted'])}，缺席≥{self.delist_absent_days} 次且日K停更>{self.delist_stale_kline_days} 天）：",
            *[f"  - {d['ts_code']} {d['last_name']} 末K {d['delist_date']}" for d in report['delisted'][:100]],
            f"- 缺席观察 TOP（未达退市线）：{report['absent_watch'][:20]}",
            f"- list_date 缺失回填候选（{len(report['list_date_backfill'])}）",
        ]
        path.write_text('\n'.join(lines), encoding='utf-8')
        return path


def sync_stock_list(config_manager: ConfigManager = None, **options) -> Dict[str, Any]:
    """CLI 入口：diff_only 默认，apply=True 落库"""
    return StockListManager(config_manager or ConfigManager()).execute(
        apply=bool(options.get('apply', False)))
