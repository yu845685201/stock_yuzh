"""补采待办与域冷却 DAO（数据采集链路耗时优化技术方案 V1.1 §5，批次3）

两张控制表：

- ``sync_collection_retry``：持久化待补任务（按 数据集+源+业务分区 唯一）。主流程
  遇到风控急停/失败/数据滞后时**不再原地 sleep**，登记后释放工作线程；补采作业
  （sync-review-pending）按到期记录领取执行。
- ``sync_domain_cooldown``：按共享请求域/资源组保存 ``blocked_until``。主流程、
  手动入口及补采进程共用冷却，交易所域（disclosure/lhb/margin/etf_scale）串行。

与既有 DAO 的边界（同 dao_collection_lineage）：

- ``*_tx`` 方法只在调用者事务内执行、绝不 commit——登记分区结果与新待办用同一事务
  （§5.1"登记分区结果与新待办用同一事务"）；
- 领取用短事务 + FOR UPDATE SKIP LOCKED + 有时限租约 + lease_token；
  外部请求（runner）在事务外执行，确认完成前校验 token（§5.1）。
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import psycopg2.extras

logger = logging.getLogger(__name__)

# missing_scope 列表键并集上限（超出截断并记数，防极端场景 JSONB 膨胀）
_MISSING_SCOPE_LIST_MAX = 2000


def _j(value: Any) -> str:
    return json_dumps(value)


def json_dumps(value: Any) -> str:
    import json
    return json.dumps(value, ensure_ascii=False, default=str)


def _merge_missing_scope(existing: Any,
                         incoming: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """missing_scope 幂等合并（限流方案 §4.3：子项按稳定业务键合并）。

    标量键新值覆盖；列表键并集去重（原 SQL ``||`` 浅合并会让两批子项
    在同一字段下互相覆盖，如 {'detail':...} 与 {'seats':[...]} 之外的
    同键重复登记）。超上限截断并记数。
    """
    if incoming is None:
        return existing if isinstance(existing, dict) else None
    if not isinstance(existing, dict):
        return dict(incoming)
    merged = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, list):
            old = merged.get(key)
            if isinstance(old, list):
                seen = {json_dumps(x) for x in old}
                for item in value:
                    sig = json_dumps(item)
                    if sig not in seen:
                        seen.add(sig)
                        old.append(item)
                if len(old) > _MISSING_SCOPE_LIST_MAX:
                    overflow = len(old) - _MISSING_SCOPE_LIST_MAX
                    merged[key] = old[:_MISSING_SCOPE_LIST_MAX] + [{'_truncated': overflow}]
                else:
                    merged[key] = old
            else:
                merged[key] = value
        else:
            merged[key] = value
    return merged


# 交易所域（共用同一出口 IP，2026-11-17 403 风控教训）：披露/龙虎榜/两融/ETF 份额/期权
# （限流方案 20261120 §3 P0：期权补入覆盖范围；新登记按主机级域（domain_gate），
#  此源级映射保留为兜底与旧记录兼容）
EXCHANGE_DOMAIN_SOURCES = {'disclosure', 'lhb', 'margin', 'etf_scale', 'option'}
DOMAIN_EXCHANGE = 'exchange'


def domain_for_source(source: str) -> Optional[str]:
    """源 → 共享请求域；交易所系源归 exchange 域，其余各自独立（无共享域冷却）"""
    return DOMAIN_EXCHANGE if source in EXCHANGE_DOMAIN_SOURCES else None


class CollectionRetryDaoMixin:
    """补采待办（sync_collection_retry）与域冷却（sync_domain_cooldown）"""

    _RETRY_TABLE_DDL: Dict[str, str] = {
        'sync_collection_retry': """
            CREATE TABLE IF NOT EXISTS sync_collection_retry (
                retry_id VARCHAR(36) PRIMARY KEY,
                retry_key VARCHAR(300) NOT NULL,
                dataset VARCHAR(64) NOT NULL,
                source VARCHAR(64),
                target_date VARCHAR(8) NOT NULL,
                market VARCHAR(16) DEFAULT 'CN',
                partition_key VARCHAR(256),
                action VARCHAR(24) NOT NULL DEFAULT 'SOURCE_RETRY',
                reason VARCHAR(32),
                missing_scope JSONB,
                domain VARCHAR(32),
                retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                lease_token VARCHAR(36),
                lease_owner VARCHAR(128),
                lease_until TIMESTAMPTZ,
                parent_job_id VARCHAR(36),
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """,
        'sync_domain_cooldown': """
            CREATE TABLE IF NOT EXISTS sync_domain_cooldown (
                domain VARCHAR(32) PRIMARY KEY,
                blocked_until TIMESTAMPTZ,
                reason TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """,
    }

    # 域门列（限流方案 20261120 §2：共享发送节拍 + 发送时隙；幂等补列）
    _GATE_ALTER_DDL: List[str] = [
        "ALTER TABLE sync_domain_cooldown ADD COLUMN IF NOT EXISTS next_send_at TIMESTAMPTZ",
        "ALTER TABLE sync_domain_cooldown ADD COLUMN IF NOT EXISTS slot_owner VARCHAR(128)",
        "ALTER TABLE sync_domain_cooldown ADD COLUMN IF NOT EXISTS slot_until TIMESTAMPTZ",
    ]

    _RETRY_INDEX_DDL: List[str] = [
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_sync_retry_key "
        "ON sync_collection_retry (retry_key)",
        # 到期任务索引（§5.1：对到期任务建索引）
        "CREATE INDEX IF NOT EXISTS idx_sync_retry_due "
        "ON sync_collection_retry (status, retry_at)",
        "CREATE INDEX IF NOT EXISTS idx_sync_retry_lease "
        "ON sync_collection_retry (status, lease_until)",
    ]

    def ensure_collection_retry_tables(self) -> None:
        """补采待办/域冷却建表兜底（幂等；DDL 同步维护在 doc/migrations/20261120_*.sql）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for ddl in self._RETRY_TABLE_DDL.values():
                    cursor.execute(ddl)
                for idx in self._RETRY_INDEX_DDL:
                    cursor.execute(idx)
                for ddl in self._GATE_ALTER_DDL:
                    cursor.execute(ddl)
            conn.commit()
        logger.info('补采控制表 ensure 完成（sync_collection_retry/sync_domain_cooldown）')

    # ---------- 登记（*_tx：调用方事务内执行） ----------

    def register_retry_tx(self, conn: Any, *, dataset: str, target_date: str,
                          retry_id: Optional[str] = None, source: Optional[str] = None,
                          market: str = 'CN', partition_key: Optional[str] = None,
                          action: str = 'SOURCE_RETRY', reason: Optional[str] = None,
                          missing_scope: Optional[Dict[str, Any]] = None,
                          domain: Optional[str] = None,
                          retry_at: Optional[datetime] = None,
                          max_attempts: int = 3,
                          parent_job_id: Optional[str] = None,
                          last_error: Optional[str] = None,
                          sub_key: Optional[str] = None) -> None:
        """登记/合并补采任务（§5.1：同一缺口重复登记幂等合并，保留尚未处理的范围）。

        幂等合并语义：已存在同 retry_key 的 PENDING/LEASED 任务时——retry_at 取更早
        （尽快可试）、missing_scope 合并（标量覆盖/列表并集，防两批子项互覆）、
        attempts 不重置；DONE 的任务重新打开（新一轮缺口）。

        ``sub_key``：子分区键（附件/席位/年度窗等子项缺口，方案 §4.3），
        非空时 retry_key 为 ``{dataset}|{market}|{target_date}|{sub_key}``。
        """
        retry_key = f'{dataset}|{market}|{target_date}'
        if sub_key:
            retry_key = f'{retry_key}|{sub_key}'
        with conn.cursor() as cursor:
            # 幂等合并：先锁行取已有 missing_scope，Python 合并后写回
            cursor.execute(
                "SELECT missing_scope FROM sync_collection_retry "
                "WHERE retry_key = %s FOR UPDATE", (retry_key,))
            row = cursor.fetchone()
            merged_scope = _merge_missing_scope(row[0] if row else None, missing_scope)
            cursor.execute("""
                INSERT INTO sync_collection_retry
                    (retry_id, retry_key, dataset, source, target_date, market,
                     partition_key, action, reason, missing_scope, domain,
                     retry_at, max_attempts, parent_job_id, last_error, status)
                VALUES (%(retry_id)s, %(retry_key)s, %(dataset)s, %(source)s,
                        %(target_date)s, %(market)s, %(partition_key)s, %(action)s,
                        %(reason)s, %(missing_scope)s, %(domain)s,
                        COALESCE(%(retry_at)s, NOW()), %(max_attempts)s,
                        %(parent_job_id)s, %(last_error)s, 'PENDING')
                ON CONFLICT (retry_key) DO UPDATE SET
                    target_date = EXCLUDED.target_date,
                    reason = EXCLUDED.reason,
                    domain = EXCLUDED.domain,
                    parent_job_id = EXCLUDED.parent_job_id,
                    last_error = EXCLUDED.last_error,
                    missing_scope = EXCLUDED.missing_scope,
                    retry_at = LEAST(sync_collection_retry.retry_at,
                                     COALESCE(EXCLUDED.retry_at, NOW())),
                    status = CASE WHEN sync_collection_retry.status = 'DONE'
                                  THEN 'PENDING' ELSE sync_collection_retry.status END,
                    updated_at = NOW()
            """, {
                'retry_id': retry_id or str(uuid.uuid4()),
                'retry_key': retry_key,
                'dataset': dataset, 'source': source, 'target_date': target_date,
                'market': market, 'partition_key': partition_key,
                'action': action,
                'reason': reason,
                'missing_scope': _j(merged_scope) if merged_scope is not None else None,
                'domain': domain, 'retry_at': retry_at,
                'max_attempts': max_attempts, 'parent_job_id': parent_job_id,
                'last_error': last_error,
            })

    def register_domain_cooldown_tx(self, conn: Any, domain: str,
                                    blocked_until: datetime,
                                    reason: Optional[str] = None) -> None:
        """登记/延长域冷却（blocked_until 取更晚；§5.1 域冷却记录）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO sync_domain_cooldown (domain, blocked_until, reason, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (domain) DO UPDATE SET
                    blocked_until = GREATEST(sync_domain_cooldown.blocked_until, EXCLUDED.blocked_until),
                    reason = EXCLUDED.reason,
                    updated_at = NOW()
            """, (domain, blocked_until, reason))

    # ---------- 独立短事务包装（管理器即时登记；方案 §2"真实拒绝一经确认即独立提交冷却"） ----------

    def register_retry(self, **kwargs: Any) -> None:
        """独立短事务登记补采任务/子项缺口（不等编排汇总；参数同 register_retry_tx）"""
        with self.get_connection() as conn:
            self.register_retry_tx(conn, **kwargs)
            conn.commit()

    def register_domain_cooldown(self, domain: str, blocked_until: datetime,
                                 reason: Optional[str] = None) -> None:
        """独立短事务登记/延长域冷却"""
        with self.get_connection() as conn:
            self.register_domain_cooldown_tx(conn, domain, blocked_until, reason=reason)
            conn.commit()

    def check_domain_cooldown(self, domain: Optional[str]) -> int:
        """域剩余冷却秒数（0=无冷却；兼容旧 exchange 域记录直到其过期）"""
        if not domain:
            return 0
        rows = self.execute_query(
            "SELECT MAX(blocked_until) AS mx FROM sync_domain_cooldown "
            "WHERE domain IN (%s, %s) AND blocked_until > NOW()",
            (domain, DOMAIN_EXCHANGE))
        mx = rows[0].get('mx') if rows else None
        if not mx:
            return 0
        return int((mx - datetime.now(timezone.utc)).total_seconds())

    # ---------- 子项缺口（SCOPE_RETRY）读取与就地结清（限流方案 §4.3） ----------

    def pending_scope_retry(self, dataset: str, target_date: str,
                            sub_key: str) -> Optional[Dict[str, Any]]:
        """查询未结子项缺口任务（PENDING/LEASED；席位/增强口径等待办存在性判断）"""
        rows = self.execute_query(
            "SELECT retry_id, retry_key, dataset, target_date, action, missing_scope, "
            "attempts, max_attempts FROM sync_collection_retry "
            "WHERE retry_key = %s AND status IN ('PENDING', 'LEASED') "
            "ORDER BY updated_at DESC LIMIT 1",
            (f'{dataset}|CN|{target_date}|{sub_key}',))
        return rows[0] if rows else None

    def scope_retry_missing_scope(self, dataset: str, target_date: str,
                                  sub_key: str) -> Optional[Dict[str, Any]]:
        """取未结子项缺口的 missing_scope（补采只处理缺失范围的依据）"""
        task = self.pending_scope_retry(dataset, target_date, sub_key)
        if not task:
            return None
        return task.get('missing_scope') or None

    def complete_scope_retry(self, dataset: str, target_date: str, sub_key: str) -> bool:
        """子项缺口结清（就地 DONE；幂等，仅对未结任务生效）。

        注意：若该任务正被补采领取器租约执行，此处仍会结清——双方都以
        "实际补齐"为准，领取器侧 complete_retry 对非 LEASED 行返回 STALE 丢弃。"""
        rows = self.execute_query(
            "UPDATE sync_collection_retry SET status = 'DONE', lease_token = NULL, "
            "lease_owner = NULL, lease_until = NULL, updated_at = NOW() "
            "WHERE retry_key = %s AND status IN ('PENDING', 'LEASED') "
            "RETURNING retry_id",
            (f'{dataset}|CN|{target_date}|{sub_key}',))
        return bool(rows)

    # ---------- 领取与完成（短事务；§5.1 租约与 token） ----------

    def claim_pending_retry(self, lease_owner: str, lease_seconds: int = 900,
                            limit: int = 1) -> List[Dict[str, Any]]:
        """领取到期且域冷却已过的补采任务（短事务原子领取 + 有时限租约）。

        - 只领 PENDING（或租约过期的 LEASED——过期租约可重领，§5.1）
        - retry_at <= NOW() 且 domain 冷却已过（无域冷却记录视为无冷却）
        - FOR UPDATE SKIP LOCKED：多领取器并发不重复领取
        """
        lease_until = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        rows: List[Dict[str, Any]] = []
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT r.retry_id, r.retry_key, r.dataset, r.source, r.target_date,
                           r.market, r.partition_key, r.action, r.reason, r.missing_scope,
                           r.domain, r.attempts, r.max_attempts, r.parent_job_id, r.last_error
                    FROM sync_collection_retry r
                    LEFT JOIN sync_domain_cooldown d ON d.domain = r.domain
                    WHERE (r.status = 'PENDING'
                           OR (r.status = 'LEASED' AND r.lease_until < NOW()))
                      AND r.retry_at <= NOW()
                      AND r.attempts < r.max_attempts
                      AND (r.domain IS NULL OR d.blocked_until IS NULL
                           OR d.blocked_until <= NOW())
                    ORDER BY r.retry_at ASC
                    LIMIT %s
                    FOR UPDATE OF r SKIP LOCKED
                """, (limit,))
                candidates = cursor.fetchall()
                token = str(uuid.uuid4())
                for c in candidates:
                    cursor.execute("""
                        UPDATE sync_collection_retry
                        SET status = 'LEASED', lease_token = %s, lease_owner = %s,
                            lease_until = %s, attempts = attempts + 1, updated_at = NOW()
                        WHERE retry_id = %s
                          AND (status = 'PENDING'
                               OR (status = 'LEASED' AND lease_until < NOW()))
                        RETURNING retry_id
                    """, (token, lease_owner, lease_until, c['retry_id']))
                    if cursor.fetchone():
                        row = dict(c)
                        row['lease_token'] = token
                        row['lease_until'] = lease_until
                        rows.append(row)
            conn.commit()
        return rows

    def complete_retry(self, retry_id: str, lease_token: str, success: bool,
                       error: Optional[str] = None,
                       requeue_delay_seconds: Optional[int] = None) -> str:
        """确认完成（§5.1：确认完成前在同一事务校验有效 token，失效响应直接丢弃）。

        Returns:
            'DONE' | 'REQUEUED' | 'FAILED_MAX' | 'STALE'（token 失效或已非 LEASED）
        """
        outcome = 'STALE'
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT attempts, max_attempts FROM sync_collection_retry "
                        "WHERE retry_id = %s AND status = 'LEASED' AND lease_token = %s "
                        "FOR UPDATE",
                        (retry_id, lease_token))
                    row = cursor.fetchone()
                    if row:
                        attempts, max_attempts = row
                        if success:
                            cursor.execute("""
                                UPDATE sync_collection_retry
                                SET status = 'DONE', lease_token = NULL,
                                    lease_owner = NULL, lease_until = NULL,
                                    last_error = NULL, updated_at = NOW()
                                WHERE retry_id = %s
                            """, (retry_id,))
                            outcome = 'DONE'
                        elif attempts >= max_attempts:
                            # 到重试上限后保留缺口和错误，报告提示人工处理（§5.1）
                            cursor.execute("""
                                UPDATE sync_collection_retry
                                SET status = 'FAILED', lease_token = NULL,
                                    lease_owner = NULL, lease_until = NULL,
                                    last_error = %s, updated_at = NOW()
                                WHERE retry_id = %s
                            """, (error, retry_id))
                            outcome = 'FAILED_MAX'
                        else:
                            delay = requeue_delay_seconds or 1800
                            cursor.execute("""
                                UPDATE sync_collection_retry
                                SET status = 'PENDING', lease_token = NULL,
                                    lease_owner = NULL, lease_until = NULL,
                                    retry_at = NOW() + (%s || ' seconds')::interval,
                                    last_error = %s, updated_at = NOW()
                                WHERE retry_id = %s
                            """, (str(delay), error, retry_id))
                            outcome = 'REQUEUED'
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return outcome

    def pending_retry_stats(self) -> Dict[str, Any]:
        """待办三组数量（§5.2：尚未到期、到期未齐、超过截止）"""
        rows = self.execute_query("""
            SELECT
                COUNT(*) FILTER (WHERE status IN ('PENDING','LEASED')
                                 AND retry_at > NOW()) AS not_due,
                COUNT(*) FILTER (WHERE status = 'PENDING' AND retry_at <= NOW()) AS due_waiting,
                COUNT(*) FILTER (WHERE status = 'LEASED') AS leased,
                COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_max,
                COUNT(*) FILTER (WHERE status = 'DONE') AS done
            FROM sync_collection_retry
        """)
        r = rows[0] if rows else {}
        return {k: int(r.get(k) or 0) for k in
                ('not_due', 'due_waiting', 'leased', 'failed_max', 'done')}

    def domain_cooldowns(self) -> List[Dict[str, Any]]:
        """当前域冷却状态（报告用）"""
        return self.execute_query(
            "SELECT domain, blocked_until, reason FROM sync_domain_cooldown "
            "WHERE blocked_until > NOW() ORDER BY domain")