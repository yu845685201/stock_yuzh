"""
his_kline_day DAO：upsert / 整股替换 / 前收盘查询（自 connection.py 拆分，R-06 纯物理移动）
"""

from psycopg2.extras import execute_values as pg_execute_values
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..utils.data_transformer import DataTransformer

# his_kline_day 22 列清单（upsert 与 replace 两处 SQL 共用同一常量，逐字取自原 SQL 列清单；
# is_st 原为死列——建表即有但从未写入，P0-1（复盘报告层与 P0 修复技术方案 §1.2）纳入写入列）
KLINE_DAY_COLUMNS = (
    'ts_code', 'stock_code', 'stock_name', 'trade_date',
    'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'raw_close', 'adjust_flag',
    'change_rate', 'turnover_rate', 'is_st',
    'fundamentals_disclosure_date', 'total_share', 'float_share', 'source', 'create_time', 'update_time',
)

_KLINE_DAY_COLUMN_SQL = "(" + ", ".join(KLINE_DAY_COLUMNS) + ")"


def derive_is_st(kline_data: List[Dict[str, Any]]) -> None:
    """写入前统一派生 ``is_st``（P0-1 单一出口，两条写路径共用）。

    口径：**该行同步时点的名称含 ST**（复用 DataTransformer.check_is_st），
    非「该交易日时点的真实 ST 状态」——历史行的戴帽/摘帽切换日前存在误标，
    见 doc/数据模型.MD 口径声明。原地覆盖行内 is_st（含调用方误传值，改名/摘帽
    后重刷以名称为准翻转）。
    """
    for row in kline_data:
        row['is_st'] = DataTransformer.check_is_st(row.get('stock_name') or '')


class KlineDayDaoMixin:
    """日K线 DAO"""

    def upsert_his_kline_day(self, kline_data: List[Dict[str, Any]]) -> int:
        """
        批量upsert 日K线数据

        Args:
            kline_data: 日K线数据列表

        Returns:
            影响的行数
        """
        if not kline_data:
            return 0

        derive_is_st(kline_data)

        # F10（数据采集链路风险修复方案 V1.0）：高价值可空列 COALESCE——源端缺字段
        # （如 merge 层 THS Amount 恒 None、raw 域缺失日 RawClose=None）不得把库内
        # 已有值覆盖为 NULL；close/open/high/low/preclose 保持强制覆盖（前复权重基
        # 必须能改正旧值，这是 refetch 语义本体，不可 COALESCE）。
        upsert_sql = f"""
        INSERT INTO his_kline_day
        {_KLINE_DAY_COLUMN_SQL}
        VALUES %s
        ON CONFLICT (ts_code, trade_date)
        DO UPDATE SET
            stock_code = EXCLUDED.stock_code,
            stock_name = EXCLUDED.stock_name,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            preclose = EXCLUDED.preclose,
            volume = COALESCE(EXCLUDED.volume, his_kline_day.volume),
            amount = COALESCE(EXCLUDED.amount, his_kline_day.amount),
            raw_close = COALESCE(EXCLUDED.raw_close, his_kline_day.raw_close),
            adjust_flag = EXCLUDED.adjust_flag,
            change_rate = EXCLUDED.change_rate,
            turnover_rate = COALESCE(EXCLUDED.turnover_rate, his_kline_day.turnover_rate),
            is_st = EXCLUDED.is_st,
            fundamentals_disclosure_date = COALESCE(EXCLUDED.fundamentals_disclosure_date, his_kline_day.fundamentals_disclosure_date),
            total_share = COALESCE(EXCLUDED.total_share, his_kline_day.total_share),
            float_share = COALESCE(EXCLUDED.float_share, his_kline_day.float_share),
            source = EXCLUDED.source,
            update_time = NOW()
        """

        params_list = []
        now = datetime.now()
        for item in kline_data:
            params_list.append((
                item['ts_code'],
                item['stock_code'],
                item['stock_name'],
                item['trade_date'],
                item.get('open'),
                item.get('high'),
                item.get('low'),
                item.get('close'),
                item.get('preclose'),
                item.get('volume'),
                item.get('amount'),
                item.get('raw_close'),
                item.get('adjust_flag'),
                item.get('change_rate'),
                item.get('turnover_rate'),
                item.get('is_st'),
                item.get('fundamentals_disclosure_date'),
                item.get('total_share'),
                item.get('float_share'),
                item.get('source'),
                item.get('create_time', now),
                now
            ))

        return self.execute_values(upsert_sql, params_list, page_size=2000)

    def replace_his_kline_day(self, ts_code: str, kline_data: List[Dict[str, Any]]) -> int:
        """
        整股原子替换日K线：单事务内先删该股全部行再批量插入（全量初始化/除权重刷用）

        F3（数据采集链路风险修复方案 V1.0）：替换前新旧行数比对——新集合行数
        < 库内行数 × min_ratio（config sync.kline_day_replace_min_ratio，默认 0.9，
        0=关闭校验）时拒绝替换并抛错，防止全量回源被截断后整股历史被部分数据
        永久覆盖（调用链 safe_process_one_stock 已隔离：该股记 failed、旧数据原样保留）。
        init 首次建库 old_count=0 自然放行；除权重拉不改行数，不受 0.9 影响。

        Args:
            ts_code: 股票ts_code
            kline_data: 该股全量日K数据列表

        Returns:
            插入的行数
        """
        if not kline_data:
            return 0

        # F3 完整性校验：DELETE 之前先比行数
        min_ratio = 0.9
        try:
            if self.config_manager is not None:
                min_ratio = float(self.config_manager.get('sync.kline_day_replace_min_ratio', 0.9) or 0)
        except Exception:
            min_ratio = 0.9
        if min_ratio > 0:
            old_row = self.fetch_one(
                "SELECT COUNT(*) AS n FROM his_kline_day WHERE ts_code = %s", (ts_code,))
            old_count = int(old_row.get('n') or 0) if old_row else 0
            if old_count > 0 and len(kline_data) < old_count * min_ratio:
                raise RuntimeError(
                    f"整股替换被拒绝: {ts_code} 新{len(kline_data)}行 < 旧{old_count}行×{min_ratio}"
                    f"（疑似全量回源截断，旧数据已保留；确认无误可用 "
                    f"sync.kline_day_replace_min_ratio=0 临时关闭校验）")

        derive_is_st(kline_data)

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM his_kline_day WHERE ts_code = %s", (ts_code,))
                insert_sql = f"""
                INSERT INTO his_kline_day
                {_KLINE_DAY_COLUMN_SQL}
                VALUES %s
                """
                params_list = []
                now = datetime.now()
                for item in kline_data:
                    params_list.append((
                        item['ts_code'],
                        item['stock_code'],
                        item['stock_name'],
                        item['trade_date'],
                        item.get('open'),
                        item.get('high'),
                        item.get('low'),
                        item.get('close'),
                        item.get('preclose'),
                        item.get('volume'),
                        item.get('amount'),
                        item.get('raw_close'),
                        item.get('adjust_flag'),
                        item.get('change_rate'),
                        item.get('turnover_rate'),
                        item.get('is_st'),
                        item.get('fundamentals_disclosure_date'),
                        item.get('total_share'),
                        item.get('float_share'),
                        item.get('source'),
                        item.get('create_time', now),
                        now
                    ))
                pg_execute_values(cursor, insert_sql, params_list, page_size=2000)
            conn.commit()
        return len(kline_data)

    def fetch_prev_his_kline_day_close(self, ts_code: str, trade_date: str) -> Optional[float]:
        """
        获取指定股票在当前交易日之前最近一条日K线的close
        """
        query = """
        SELECT close
        FROM his_kline_day
        WHERE ts_code = %s
          AND trade_date < %s
        ORDER BY trade_date DESC
        LIMIT 1
        """
        result = self.fetch_one(query, (ts_code, trade_date))
        if result:
            value = result.get('close')
            return float(value) if value is not None else None
        return None

    # ---------- raw 兜底台账（日K线raw兜底与自动恢复方案 V1.0） ----------

    RAW_FALLBACK_TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS kline_day_raw_fallback (
        id bigserial PRIMARY KEY,
        ts_code varchar(16) NOT NULL,
        trade_date varchar(8) NOT NULL,
        status varchar(16) NOT NULL,
        reason varchar(32),
        q_factor numeric,
        raw_close numeric,
        fallback_at timestamptz DEFAULT now(),
        recovered_at timestamptz,
        recover_source varchar(24),
        -- 批次2（数据采集链路耗时优化技术方案 V1.1 §4）：可空的上次尝试时刻与下次
        -- 可尝试时刻——旧的打开记录两列均为 NULL，首次运行即可领取；领取按
        -- "已到期、最久未尝试优先"；源可达但目标日未发布的记录单独退避
        last_attempt_at timestamptz,
        next_retry_at timestamptz,
        attempts integer NOT NULL DEFAULT 0,
        last_error text,
        CONSTRAINT kline_day_raw_fallback_pk UNIQUE (ts_code, trade_date)
    )
    """

    # 存量库补列（批次2 迁移；幂等，ensure 时执行）
    RAW_FALLBACK_MIGRATE_DDL = [
        "ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS last_attempt_at timestamptz",
        "ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS next_retry_at timestamptz",
        "ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0",
        "ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS last_error text",
        "CREATE INDEX IF NOT EXISTS idx_rawfb_claim ON kline_day_raw_fallback "
        "(status, COALESCE(next_retry_at, '1970-01-01'::timestamptz))",
    ]

    def ensure_raw_fallback_table(self) -> None:
        """幂等创建 raw 兜底台账表 + 存量补列（每次 sync_kline_day 启动时调用）"""
        self.execute_update(self.RAW_FALLBACK_TABLE_DDL)
        for ddl in self.RAW_FALLBACK_MIGRATE_DDL:
            self.execute_update(ddl)

    def upsert_raw_fallback_records(self, records: List[Dict[str, Any]]) -> int:
        """
        批量写兜底台账（幂等）。status: 'fallback'（已兜底待恢复）| 'skip_exrights'（除权校验未过跳过）。
        已 recovered 的记录再次兜底时重置回未恢复状态（状态机允许回卷）。

        批次2（方案 V1.1 §4）：幂等重写同一条兜底期间保持其首次兜底时间——仅当旧记录
        已 recovered（回卷）时刷新 fallback_at，仍为 fallback 的保留首次兜底时间
        （前复权恢复延迟按"从 raw 兜底入库到 qfq 转正"口径统计）；重写时重置
        next_retry_at=NULL（立即可领，调度状态不阻塞新写入）。
        """
        if not records:
            return 0
        sql = """
        INSERT INTO kline_day_raw_fallback
            (ts_code, trade_date, status, reason, q_factor, raw_close)
        VALUES %s
        ON CONFLICT (ts_code, trade_date) DO UPDATE SET
            status = EXCLUDED.status,
            reason = EXCLUDED.reason,
            q_factor = EXCLUDED.q_factor,
            raw_close = EXCLUDED.raw_close,
            fallback_at = CASE
                WHEN kline_day_raw_fallback.status = 'fallback'
                    THEN kline_day_raw_fallback.fallback_at   -- 保留首次兜底时间
                ELSE NOW() END,                               -- recovered 回卷 → 新一次兜底
            recovered_at = NULL,
            recover_source = NULL,
            next_retry_at = NULL,
            last_error = NULL
        """
        params_list = [
            (
                r['ts_code'], r['trade_date'], r['status'],
                r.get('reason'), r.get('q_factor'), r.get('raw_close')
            )
            for r in records
        ]
        return self.execute_values(sql, params_list)

    def fetch_open_raw_fallback(self) -> List[Dict[str, Any]]:
        """查询未恢复的兜底记录（status='fallback'），按日期升序"""
        return self.execute_query(
            "SELECT ts_code, trade_date FROM kline_day_raw_fallback "
            "WHERE status = 'fallback' ORDER BY trade_date, ts_code"
        )

    # ---------- 批次2（方案 V1.1 §4）：独立恢复作业的领取/原子销账/退避 ----------

    def claim_raw_fallback_batch(self, limit: int = 200,
                                 max_attempts: Optional[int] = None) -> List[Dict[str, Any]]:
        """领取一批已到期的打开台账（短事务，方案 §4"外部请求期间不开长事务"）。

        领取规则：已到期（next_retry_at IS NULL 或 <= NOW()）、最久未尝试优先
        （COALESCE(last_attempt_at, fallback_at) 升序）；同一股票的日期在调用方合并为
        一次 qfq 请求。领取即更新 last_attempt_at/attempts（幂等安全：进程崩溃后
        余项仍能重领）。FOR UPDATE SKIP LOCKED 保证多消费者不重复领取。

        ⚠️ 领取语句含数据修改 CTE（UPDATE + RETURNING），必须走自提交短事务——
        2026-11-20 实战教训：误用不提交的 execute_query 导致领取痕迹（attempts/
        last_attempt_at）被回滚，已处理记录被反复领取 ~7 倍，预算大半耗在重复劳动。

        Returns:
            [{'ts_code', 'trade_date', 'attempts'}]
        """
        where_extra = ''
        params: tuple = (limit,)
        if max_attempts is not None:
            where_extra = 'AND attempts < %s'
            params = (max_attempts, limit)
        sql = f"""
        WITH picked AS (
            SELECT id FROM kline_day_raw_fallback
            WHERE status = 'fallback'
              AND (next_retry_at IS NULL OR next_retry_at <= NOW())
              {where_extra}
            ORDER BY COALESCE(last_attempt_at, fallback_at) ASC, trade_date ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE kline_day_raw_fallback f
        SET last_attempt_at = NOW(), attempts = f.attempts + 1
        FROM picked
        WHERE f.id = picked.id
        RETURNING f.ts_code, f.trade_date, f.attempts, f.last_error
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(sql, params)
                rows = [dict(r) for r in cursor.fetchall()]
            conn.commit()  # 领取痕迹必须落库（幂等重领与重复领取防线的基准）
        return rows

    def recover_raw_fallback_row_atomic(
        self, ts_code: str, trade_date: str,
        open_v: Optional[float], high_v: Optional[float], low_v: Optional[float],
        close_v: Optional[float], preclose_v: Optional[float],
        change_rate_v: Optional[float], recover_source: str = 'recovery_job'
    ) -> str:
        """单业务键原子恢复（方案 §4"并发与事务"：短事务内锁定并复核台账仍打开、
        行情行仍是 TDXAPI_RAWFB，然后更新价格、来源和销账状态并提交）。

        - 行已回归 TDXAPI（正常增量覆盖）→ 不写价格，仅处置对应台账（销账）
        - 行仍是 TDXAPI_RAWFB → 条件覆盖价格 + 销账（拒绝较早获取的 raw 回写覆盖
          已验收 qfq：WHERE source='TDXAPI_RAWFB' 条件写入）
        - 行不存在（异常）→ 台账保留打开，返回 ROW_MISSING

        Returns:
            'RECOVERED_PRICE' | 'RECOVERED_LEDGER_ONLY' | 'ROW_MISSING' | 'ALREADY_CLOSED'
        """
        outcome = 'ROW_MISSING'
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT id FROM kline_day_raw_fallback "
                        "WHERE ts_code = %s AND trade_date = %s AND status = 'fallback' "
                        "FOR UPDATE",
                        (ts_code, trade_date))
                    if not cursor.fetchone():
                        outcome = 'ALREADY_CLOSED'
                    else:
                        cursor.execute(
                            "SELECT source FROM his_kline_day "
                            "WHERE ts_code = %s AND trade_date = %s",
                            (ts_code, trade_date))
                        row = cursor.fetchone()
                        row_source = row[0] if row else None
                        if row is None:
                            outcome = 'ROW_MISSING'  # 台账保留打开，报告提示人工核查
                        elif row_source == 'TDXAPI':
                            # 正常路径已写回权威 qfq → 同步处置对应台账，不动价格
                            cursor.execute(
                                "UPDATE kline_day_raw_fallback "
                                "SET status='recovered', recovered_at=NOW(), recover_source=%s "
                                "WHERE ts_code=%s AND trade_date=%s AND status='fallback'",
                                (recover_source, ts_code, trade_date))
                            outcome = 'RECOVERED_LEDGER_ONLY'
                        else:
                            # 仍是兜底行 → 条件覆盖价格（拒绝覆盖已验收 qfq）
                            cursor.execute(
                                "UPDATE his_kline_day "
                                "SET open=%s, high=%s, low=%s, close=%s, preclose=%s, "
                                "    change_rate=%s, source='TDXAPI', update_time=NOW() "
                                "WHERE ts_code=%s AND trade_date=%s "
                                "  AND (source='TDXAPI_RAWFB' OR source IS NULL)",
                                (open_v, high_v, low_v, close_v, preclose_v,
                                 change_rate_v, ts_code, trade_date))
                            price_updated = cursor.rowcount > 0
                            cursor.execute(
                                "UPDATE kline_day_raw_fallback "
                                "SET status='recovered', recovered_at=NOW(), recover_source=%s "
                                "WHERE ts_code=%s AND trade_date=%s AND status='fallback'",
                                (recover_source, ts_code, trade_date))
                            outcome = 'RECOVERED_PRICE' if price_updated else 'ROW_MISSING'
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return outcome

    def defer_raw_fallback_rows(self, pairs: List[tuple], backoff_seconds: int,
                                last_error: Optional[str] = None) -> int:
        """退避未恢复记录（方案 §4"源可达但目标日期尚未发布的记录单独退避"）：
        next_retry_at = NOW() + backoff。仍打开，到期后可重领。"""
        if not pairs:
            return 0
        sql = """
        UPDATE kline_day_raw_fallback
        SET next_retry_at = NOW() + (%s || ' seconds')::interval, last_error = %s
        WHERE ts_code = %s AND trade_date = %s AND status = 'fallback'
        """
        params_list = [(str(backoff_seconds), last_error, ts_code, trade_date)
                       for ts_code, trade_date in pairs]
        return self.execute_batch(sql, params_list)

    def raw_fallback_backlog_stats(self) -> Dict[str, Any]:
        """积压统计（方案 §2 指标"前复权恢复延迟"：期初/期末积压与最老账龄）"""
        rows = self.execute_query("""
            SELECT COUNT(*) AS open_n,
                   MIN(fallback_at) AS oldest_fallback_at,
                   COUNT(*) FILTER (WHERE next_retry_at IS NOT NULL AND next_retry_at > NOW()) AS deferred_n
            FROM kline_day_raw_fallback WHERE status = 'fallback'
        """)
        r = rows[0] if rows else {}
        return {
            'open_n': int(r.get('open_n') or 0),
            'deferred_n': int(r.get('deferred_n') or 0),
            'oldest_fallback_at': r.get('oldest_fallback_at'),
        }

    def mark_raw_fallback_recovered(self, pairs: List[tuple], recover_source: str = 'recovery_stage') -> int:
        """批量销账：status='recovered'，recovered_at=now()。仅作用于仍为 fallback 的记录。"""
        if not pairs:
            return 0
        sql = """
        UPDATE kline_day_raw_fallback
        SET status = 'recovered', recovered_at = NOW(), recover_source = %s
        WHERE ts_code = %s AND trade_date = %s AND status = 'fallback'
        """
        params_list = [(recover_source, ts_code, trade_date) for ts_code, trade_date in pairs]
        return self.execute_batch(sql, params_list)

    def update_raw_fallback_row_prices(
        self, ts_code: str, trade_date: str,
        open_v: Optional[float], high_v: Optional[float], low_v: Optional[float],
        close_v: Optional[float], preclose_v: Optional[float], change_rate_v: Optional[float]
    ) -> int:
        """
        恢复阶段的定点行更新：用 qfq 值覆盖兜底行的价格字段并回归 source='TDXAPI'。
        只动价格/preclose/change_rate/source——volume/amount/turnover/基本面等保持原值
        （无除权时 qfq 与 raw 数值相同；若期间发生除权重基，价格修正后由既有
        除权检测机制在后续同步中触发整股重拉彻底修复）。
        """
        sql = """
        UPDATE his_kline_day
        SET open = %s, high = %s, low = %s, close = %s,
            preclose = %s, change_rate = %s,
            source = 'TDXAPI', update_time = NOW()
        WHERE ts_code = %s AND trade_date = %s
        """
        return self.execute_update(sql, (
            open_v, high_v, low_v, close_v, preclose_v, change_rate_v, ts_code, trade_date
        ))
