"""采集闭环台账 DAO（每日复盘采集与分析闭环技术方案 V1.0 §4.1/§4.3，D01）

四张控制表：``sync_job`` / ``sync_partition`` / ``dataset_revision`` / ``recompute_task``。

与既有 DAO 的关键差别（技术方案 §1.1 必须改造的边界）：

- 既有通用写方法（execute_batch/execute_update）**自行提交事务**，不能串接它们来
  保证"业务数据写入 + 修订记账 + 重算任务"原子性；
- 本模块提供两层 API：
  - ``*_tx(conn, ...)``：接收**外部连接**，只在调用者事务内执行，绝不 commit——
    供 :meth:`apply_partition` 与未来 manager 级事务组装；
  - ``apply_partition(...)``：§4.3 分区写入事务模板（锁 → 父版本 → 差异 → 业务行
    → 分区验收 + 修订 + 重算任务 → 提交），业务行通过回调写入（回调不得 commit）。

快照文件先于数据库引用落盘（§4.3 红线）：本模块不负责文件 IO，调用方在进入
apply_partition 前完成快照落盘与内容校验；事务失败留下的无引用文件由调用方回收。
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def _j(value: Any) -> str:
    """JSONB 列参数（dict/list → JSON 字符串）"""
    return json.dumps(value, ensure_ascii=False, default=str)


class CollectionLineageDaoMixin:
    """采集作业/分区/修订/重算任务四表（建表 + 显式事务 API + apply_partition 模板）"""

    _COLLECTION_TABLE_DDL: Dict[str, str] = {
        'sync_job': """
            CREATE TABLE IF NOT EXISTS sync_job (
                job_id VARCHAR(36) PRIMARY KEY,
                parent_job_id VARCHAR(36),
                target_date VARCHAR(8) NOT NULL,
                run_mode VARCHAR(16) NOT NULL DEFAULT 'DAILY',
                cutoff_at TIMESTAMPTZ,
                calendar_version VARCHAR(64),
                config_hash VARCHAR(64),
                requested_sources JSONB,
                status VARCHAR(16) NOT NULL DEFAULT 'RUNNING',
                execution_status VARCHAR(16),
                data_readiness JSONB,
                result_summary JSONB,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ
            )
        """,
        'sync_partition': """
            CREATE TABLE IF NOT EXISTS sync_partition (
                partition_id VARCHAR(36) PRIMARY KEY,
                job_id VARCHAR(36) NOT NULL,
                dataset VARCHAR(64) NOT NULL,
                source VARCHAR(64),
                market VARCHAR(16) NOT NULL,
                partition_key VARCHAR(256) NOT NULL,
                expected_date VARCHAR(8),
                source_business_date VARCHAR(8),
                quote_at TIMESTAMPTZ,
                source_published_at TIMESTAMPTZ,
                first_observed_at TIMESTAMPTZ,
                availability_basis VARCHAR(32),
                status VARCHAR(24) NOT NULL,
                reason_codes JSONB,
                expected_codes INTEGER,
                observed_codes INTEGER,
                invalid_codes INTEGER,
                missing_codes INTEGER,
                page_evidence JSONB,
                quality JSONB,
                recoverability VARCHAR(32),
                revision_id VARCHAR(36),
                error_detail TEXT,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        'dataset_revision': """
            CREATE TABLE IF NOT EXISTS dataset_revision (
                revision_id VARCHAR(36) PRIMARY KEY,
                dataset VARCHAR(64) NOT NULL,
                partition_key VARCHAR(256) NOT NULL,
                parent_revision_id VARCHAR(36),
                job_id VARCHAR(36),
                content_hash VARCHAR(64),
                quality_hash VARCHAR(64),
                schema_version VARCHAR(32),
                snapshot_uri TEXT,
                changed_keys JSONB,
                changed_fields JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """,
        'recompute_task': """
            CREATE TABLE IF NOT EXISTS recompute_task (
                task_id VARCHAR(36) PRIMARY KEY,
                module VARCHAR(64) NOT NULL,
                module_version VARCHAR(32) NOT NULL DEFAULT 'v1',
                anchor_start VARCHAR(8) NOT NULL,
                anchor_end VARCHAR(8) NOT NULL,
                codes JSONB,
                scope_hash VARCHAR(64) NOT NULL DEFAULT '',
                trigger_revisions JSONB,
                required_input_hash VARCHAR(64),
                status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                attempt_token VARCHAR(36),
                lease_owner VARCHAR(128),
                lease_until TIMESTAMPTZ,
                result_hash VARCHAR(64),
                last_error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """,
        # 逻辑分区发布指针（盘后采集数据源整合详细技术方案 V1.0 §3.1 批次 A）：
        # 按 (dataset, partition_key, contract_version) 保存发布指针与首轮验收时刻。
        # latest_partition_status 会把不同子项混在一起，本表按完整逻辑键区分；
        # 首达时间/作业号 COALESCE 固定，不回填倒签；历史旧记录由回放复核后再建指针。
        'sync_partition_current': """
            CREATE TABLE IF NOT EXISTS sync_partition_current (
                dataset              VARCHAR(64)  NOT NULL,
                partition_key        VARCHAR(256) NOT NULL,
                business_date        VARCHAR(8)   NOT NULL,
                contract_version     VARCHAR(32)  NOT NULL,
                first_check_at       TIMESTAMPTZ,
                deadline_at          TIMESTAMPTZ,
                first_daily_job_id   VARCHAR(36),
                first_ready_job_id   VARCHAR(36),
                first_ready_at       TIMESTAMPTZ,
                accepted_revision_id VARCHAR(36) REFERENCES dataset_revision(revision_id),
                accepted_status      VARCHAR(24)
                    CHECK (accepted_status IN ('COMPLETE', 'CONFIRMED_EMPTY')),
                accepted_source      VARCHAR(64),
                accepted_at          TIMESTAMPTZ,
                PRIMARY KEY (dataset, partition_key, contract_version)
            )
        """,
    }

    _COLLECTION_INDEX_DDL: List[str] = [
        "CREATE INDEX IF NOT EXISTS idx_sync_job_date ON sync_job (target_date, started_at)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_sync_partition_job "
        "ON sync_partition (job_id, dataset, source, market, partition_key)",
        "CREATE INDEX IF NOT EXISTS idx_sync_partition_lookup "
        "ON sync_partition (dataset, market, expected_date, status)",
        "CREATE INDEX IF NOT EXISTS idx_dataset_revision_chain "
        "ON dataset_revision (dataset, partition_key, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_partition_current_day "
        "ON sync_partition_current (business_date, dataset, contract_version)",
        # required_input_hash 可为 NULL：唯一索引用 COALESCE 把 NULL 归一到空串，
        # 避免 PG 唯一约束对 NULL 不去重（技术方案 §6.3 唯一键语义）
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_recompute_task_scope "
        "ON recompute_task (module, module_version, anchor_start, anchor_end, scope_hash, "
        "COALESCE(required_input_hash, ''))",
        "CREATE INDEX IF NOT EXISTS idx_recompute_task_lease "
        "ON recompute_task (status, lease_until)",
    ]

    def ensure_collection_tables(self) -> None:
        """闭环控制表建表兜底（幂等；DDL 同步维护在 doc/migrations/20261119_*.sql
        与 20261126_source_integration_up.sql）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for ddl in self._COLLECTION_TABLE_DDL.values():
                    cursor.execute(ddl)
                for idx in self._COLLECTION_INDEX_DDL:
                    cursor.execute(idx)
            conn.commit()
        logger.info('闭环控制表 ensure 完成（sync_job/sync_partition/dataset_revision/'
                    'recompute_task/sync_partition_current）')

    # ---------- 显式事务 API（*_tx：调用方事务内执行，不 commit） ----------

    def create_job_tx(self, conn: Any, job_id: str, target_date: str,
                      run_mode: str = 'DAILY', cutoff_at: Optional[datetime] = None,
                      parent_job_id: Optional[str] = None,
                      calendar_version: Optional[str] = None,
                      config_hash: Optional[str] = None,
                      requested_sources: Optional[List[str]] = None) -> None:
        """创建采集作业（RUNNING）。job_id 由应用生成（new_id）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO sync_job (job_id, parent_job_id, target_date, run_mode,
                                      cutoff_at, calendar_version, config_hash,
                                      requested_sources, status, started_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'RUNNING', NOW())
            """, (job_id, parent_job_id, target_date, run_mode, cutoff_at,
                  calendar_version, config_hash, _j(requested_sources or [])))

    def finish_job_tx(self, conn: Any, job_id: str, status: str,
                      execution_status: Optional[str] = None,
                      data_readiness: Optional[Dict[str, Any]] = None,
                      result_summary: Optional[Dict[str, Any]] = None) -> None:
        """收尾作业：status ∈ RUNNING/SUCCEEDED/PARTIAL/FAILED"""
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE sync_job
                SET status = %s, execution_status = %s, data_readiness = %s,
                    result_summary = %s, finished_at = NOW()
                WHERE job_id = %s
            """, (status, execution_status,
                  _j(data_readiness) if data_readiness is not None else None,
                  _j(result_summary) if result_summary is not None else None,
                  job_id))

    def latest_partition_status(self, dataset: str, market: str,
                                target_date: str) -> Optional[str]:
        """查某数据集某分区最近一次验收状态（限流方案 20261120 §4.2：
        补采前先查已验收分区，COMPLETE/CONFIRMED_EMPTY 不再重跑外发）。

        以 expected_date（= 目标补采日）+ dataset + market 定位，
        取 first_observed_at 最新一条；无记录返回 None。"""
        rows = self.execute_query(
            "SELECT status FROM sync_partition "
            "WHERE dataset = %s AND market = %s AND expected_date = %s "
            "ORDER BY first_observed_at DESC LIMIT 1",
            (dataset, market, target_date))
        return rows[0]['status'] if rows else None

    def upsert_partition_tx(self, conn: Any, row: Dict[str, Any]) -> None:
        """分区验收结果 upsert（唯一键 job_id+dataset+source+market+partition_key）。

        缺键字段以 None 兜底（正常链路走 PartitionResult.to_row() 全键；直接
        构造 dict 调用时允许只填关注字段）。"""
        defaults = {
            'partition_id': None, 'job_id': None, 'dataset': None, 'source': None,
            'market': None, 'partition_key': None, 'expected_date': None,
            'source_business_date': None, 'quote_at': None, 'source_published_at': None,
            'first_observed_at': None, 'availability_basis': None, 'status': None,
            'reason_codes': [], 'expected_codes': None, 'observed_codes': None,
            'invalid_codes': None, 'missing_codes': None, 'page_evidence': None,
            'quality': None, 'recoverability': None, 'revision_id': None,
            'error_detail': None,
        }
        row = {**defaults, **row}
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO sync_partition
                (partition_id, job_id, dataset, source, market, partition_key,
                 expected_date, source_business_date, quote_at, source_published_at,
                 first_observed_at, availability_basis, status, reason_codes,
                 expected_codes, observed_codes, invalid_codes, missing_codes,
                 page_evidence, quality, recoverability, revision_id, error_detail)
                VALUES (%(partition_id)s, %(job_id)s, %(dataset)s, %(source)s, %(market)s,
                        %(partition_key)s, %(expected_date)s, %(source_business_date)s,
                        %(quote_at)s, %(source_published_at)s, %(first_observed_at)s,
                        %(availability_basis)s, %(status)s, %(reason_codes)s,
                        %(expected_codes)s, %(observed_codes)s, %(invalid_codes)s,
                        %(missing_codes)s, %(page_evidence)s, %(quality)s,
                        %(recoverability)s, %(revision_id)s, %(error_detail)s)
                ON CONFLICT (job_id, dataset, source, market, partition_key)
                DO UPDATE SET expected_date = EXCLUDED.expected_date,
                    source_business_date = EXCLUDED.source_business_date,
                    quote_at = EXCLUDED.quote_at,
                    source_published_at = EXCLUDED.source_published_at,
                    first_observed_at = EXCLUDED.first_observed_at,
                    availability_basis = EXCLUDED.availability_basis,
                    status = EXCLUDED.status,
                    reason_codes = EXCLUDED.reason_codes,
                    expected_codes = EXCLUDED.expected_codes,
                    observed_codes = EXCLUDED.observed_codes,
                    invalid_codes = EXCLUDED.invalid_codes,
                    missing_codes = EXCLUDED.missing_codes,
                    page_evidence = EXCLUDED.page_evidence,
                    quality = EXCLUDED.quality,
                    recoverability = EXCLUDED.recoverability,
                    revision_id = EXCLUDED.revision_id,
                    error_detail = EXCLUDED.error_detail,
                    update_time = NOW()
            """, {**row,
                  'reason_codes': _j(row.get('reason_codes') or []),
                  'page_evidence': _j(row['page_evidence']) if row.get('page_evidence') is not None else None,
                  'quality': _j(row['quality']) if row.get('quality') is not None else None})

    def latest_revision_tx(self, conn: Any, dataset: str,
                           partition_key: str) -> Optional[Dict[str, Any]]:
        """当前父版本（修订链尾）；无修订返回 None"""
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("""
                SELECT * FROM dataset_revision
                WHERE dataset = %s AND partition_key = %s
                ORDER BY created_at DESC LIMIT 1
            """, (dataset, partition_key))
            row = cursor.fetchone()
            return dict(row) if row else None

    def insert_revision_tx(self, conn: Any, revision: Dict[str, Any]) -> None:
        """新增修订记录（父版本不分叉由 apply_partition 的锁 + parent 校验保证）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dataset_revision
                (revision_id, dataset, partition_key, parent_revision_id, job_id,
                 content_hash, quality_hash, schema_version, snapshot_uri,
                 changed_keys, changed_fields, created_at)
                VALUES (%(revision_id)s, %(dataset)s, %(partition_key)s,
                        %(parent_revision_id)s, %(job_id)s, %(content_hash)s,
                        %(quality_hash)s, %(schema_version)s, %(snapshot_uri)s,
                        %(changed_keys)s, %(changed_fields)s, NOW())
            """, {**revision,
                  'changed_keys': _j(revision['changed_keys']) if revision.get('changed_keys') is not None else None,
                  'changed_fields': _j(revision['changed_fields']) if revision.get('changed_fields') is not None else None})

    def insert_recompute_tasks_tx(self, conn: Any, tasks: List[Dict[str, Any]]) -> int:
        """登记重算任务（唯一键去重：内容与输入版本相同则不重复排任务）"""
        if not tasks:
            return 0
        with conn.cursor() as cursor:
            for t in tasks:
                cursor.execute("""
                    INSERT INTO recompute_task
                    (task_id, module, module_version, anchor_start, anchor_end,
                     codes, scope_hash, trigger_revisions, required_input_hash, status)
                    VALUES (%(task_id)s, %(module)s, %(module_version)s, %(anchor_start)s,
                            %(anchor_end)s, %(codes)s, %(scope_hash)s, %(trigger_revisions)s,
                            %(required_input_hash)s, 'PENDING')
                    ON CONFLICT (module, module_version, anchor_start, anchor_end,
                                 scope_hash, COALESCE(required_input_hash, ''))
                    DO NOTHING
                """, {**t,
                      'codes': _j(t.get('codes') or []),
                      'trigger_revisions': _j(t.get('trigger_revisions') or [])})
        return len(tasks)

    # ---------- sync_partition_current 逻辑分区发布指针（§3.1 批次 A） ----------

    def ensure_partition_current_tx(self, conn: Any, dataset: str,
                                    partition_key: str, business_date: str,
                                    contract_version: str = 'v1',
                                    first_check_at: Optional[datetime] = None,
                                    deadline_at: Optional[datetime] = None) -> None:
        """计划时创建当前合同期望行（ON CONFLICT DO NOTHING——已有指针不覆盖）。

        某天作业整轮漏跑时，依据交易日历重建的应到集合中缺失指针算未达（§3.1）。
        """
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO sync_partition_current
                (dataset, partition_key, business_date, contract_version,
                 first_check_at, deadline_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset, partition_key, contract_version)
                DO NOTHING
            """, (dataset, partition_key, business_date, contract_version,
                  first_check_at, deadline_at))

    def accept_partition_current_tx(self, conn: Any, dataset: str,
                                    partition_key: str, business_date: str,
                                    job_id: str, status: str,
                                    source: Optional[str] = None,
                                    revision_id: Optional[str] = None,
                                    contract_version: str = 'v1',
                                    accepted_at: Optional[datetime] = None,
                                    first_daily: bool = False) -> None:
        """提交验收指针（仅 COMPLETE/CONFIRMED_EMPTY；PARTIAL/FAILED 只记 sync_partition）。

        - 首达时间/作业号 COALESCE 固定（first_daily_job_id/first_ready_job_id/
          first_ready_at——首次正式作业写 first_daily_job_id，第一次合格提交固定
          首达，不回填倒签）；
        - 后续来源修订只更新当前指针（accepted_*）；
        - 调用方须持有该逻辑分区锁（apply_partition 同款 advisory lock）。
        """
        if status not in ('COMPLETE', 'CONFIRMED_EMPTY'):
            raise ValueError(f'发布指针仅接受 COMPLETE/CONFIRMED_EMPTY，收到 {status}')
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO sync_partition_current
                (dataset, partition_key, business_date, contract_version,
                 first_daily_job_id, first_ready_job_id, first_ready_at,
                 accepted_revision_id, accepted_status, accepted_source, accepted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset, partition_key, contract_version)
                DO UPDATE SET
                    business_date = EXCLUDED.business_date,
                    first_daily_job_id = COALESCE(sync_partition_current.first_daily_job_id,
                                                  EXCLUDED.first_daily_job_id),
                    first_ready_job_id = COALESCE(sync_partition_current.first_ready_job_id,
                                                  EXCLUDED.first_ready_job_id),
                    first_ready_at = COALESCE(sync_partition_current.first_ready_at,
                                              EXCLUDED.first_ready_at),
                    accepted_revision_id = EXCLUDED.accepted_revision_id,
                    accepted_status = EXCLUDED.accepted_status,
                    accepted_source = EXCLUDED.accepted_source,
                    accepted_at = EXCLUDED.accepted_at
            """, (dataset, partition_key, business_date, contract_version,
                  job_id if first_daily else None,
                  job_id,
                  accepted_at or datetime.now(timezone.utc),
                  revision_id, status, source,
                  accepted_at or datetime.now(timezone.utc)))

    def list_partition_current_by_date(self, dataset: str, business_date: str,
                                       contract_version: str = 'v1'
                                       ) -> List[Dict[str, Any]]:
        """按业务日查逻辑分区指针（补采判定/截止完整率核对；完整逻辑键防子项混排）"""
        rows = self.execute_query(
            "SELECT partition_key, business_date, accepted_status, accepted_source, "
            "first_ready_at, deadline_at, first_daily_job_id, first_ready_job_id "
            "FROM sync_partition_current "
            "WHERE dataset = %s AND business_date = %s AND contract_version = %s "
            "ORDER BY partition_key",
            (dataset, business_date, contract_version))
        return [dict(r) for r in rows]

    # ---------- §4.3 分区写入事务模板 ----------

    def apply_partition(self, dataset: str, partition_key: str,
                        partition_row: Dict[str, Any],
                        rows: List[Dict[str, Any]],
                        row_writer: Optional[Callable[[Any, List[Dict[str, Any]]], None]],
                        revision_row: Optional[Dict[str, Any]] = None,
                        tasks: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """分区写入事务（技术方案 §4.3，核心闭环写入点）。

        步骤（调用方职责见模块 docstring——快照文件必须已先行落盘）：
        1. 开事务，取 (dataset, partition_key) 事务级 advisory lock；
        2. 读当前父版本；本次内容的 content_hash 与 quality_hash 均与父版本一致
           → 不新增有效修订、不重复排重算任务，只记本次验收（§4.3 第 3 条；
           质量从待核变完整时 quality_hash 变化 → 仍生成修订，让下游重新判断）；
        3. 业务行经 row_writer(conn, rows) 写入（回调内禁止 commit/rollback）；
        4. 同一事务写分区验收 + 修订记录 + 重算任务；
        5. 提交；业务写入失败整体回滚。

        Returns:
            {'revision_id': 新修订 ID 或 None（未变化/无修订意图）,
             'committed': True, 'unchanged': bool}
        Raises:
            任何异常原样抛出（事务已回滚）；调用方保留原始错误类别。
        """
        new_revision: Optional[Dict[str, Any]] = None
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    # 事务级锁：hashtext 双参数版本，降低碰撞概率
                    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))",
                                   (dataset, partition_key))
                parent = self.latest_revision_tx(conn, dataset, partition_key)
                unchanged = False
                if revision_row is not None:
                    if (parent is not None
                            and parent.get('content_hash') == revision_row.get('content_hash')
                            and parent.get('quality_hash') == revision_row.get('quality_hash')):
                        unchanged = True
                    else:
                        new_revision = {
                            'revision_id': revision_row.get('revision_id') or str(uuid.uuid4()),
                            'dataset': dataset,
                            'partition_key': partition_key,
                            'parent_revision_id': (parent or {}).get('revision_id'),
                            'job_id': revision_row.get('job_id'),
                            'content_hash': revision_row.get('content_hash'),
                            'quality_hash': revision_row.get('quality_hash'),
                            'schema_version': revision_row.get('schema_version'),
                            'snapshot_uri': revision_row.get('snapshot_uri'),
                            'changed_keys': revision_row.get('changed_keys'),
                            'changed_fields': revision_row.get('changed_fields'),
                        }
                if rows and row_writer is not None:
                    row_writer(conn, rows)          # 业务行（不 commit）
                self.upsert_partition_tx(conn, {**partition_row,
                                                'revision_id': (new_revision or {}).get('revision_id')
                                                if new_revision else partition_row.get('revision_id')})
                if new_revision is not None:
                    self.insert_revision_tx(conn, new_revision)
                    # 修订触发重算任务（§6.3：quality_hash 变化也让下游重新判断）
                    self.insert_recompute_tasks_tx(conn, tasks or [])
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return {'committed': True, 'revision_id': (new_revision or {}).get('revision_id'),
                'unchanged': unchanged}

    # ---------- §6.3 重算任务租约 ----------

    def claim_recompute_tasks(self, lease_owner: str, lease_seconds: int = 300,
                              limit: int = 10) -> List[Dict[str, Any]]:
        """领取重算任务（PENDING → RUNNING + 新 attempt_token + 租约；过期任务可被重新领取）"""
        token = str(uuid.uuid4())
        lease_until = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        rows: List[Dict[str, Any]] = []
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT task_id, module, module_version, anchor_start, anchor_end,
                           codes, scope_hash, trigger_revisions, required_input_hash
                    FROM recompute_task
                    WHERE (status = 'PENDING'
                           OR (status = 'RUNNING' AND lease_until < NOW()))
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (limit,))
                candidates = cursor.fetchall()
                for c in candidates:
                    cursor.execute("""
                        UPDATE recompute_task
                        SET status = 'RUNNING', attempt_token = %s, lease_owner = %s,
                            lease_until = %s, attempts = attempts + 1, updated_at = NOW()
                        WHERE task_id = %s
                          AND (status = 'PENDING'
                               OR (status = 'RUNNING' AND lease_until < NOW()))
                        RETURNING task_id
                    """, (token, lease_owner, lease_until, c['task_id']))
                    if cursor.fetchone():
                        c = dict(c)
                        c['attempt_token'] = token
                        rows.append(c)
            conn.commit()
        return rows

    def complete_recompute_task(self, task_id: str, attempt_token: str,
                                success: bool, result_hash: Optional[str] = None,
                                error: Optional[str] = None) -> bool:
        """条件更新收尾：核对 RUNNING + token，防止过期 worker 覆盖新结果（§6.3）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE recompute_task
                    SET status = %s, result_hash = %s, last_error = %s,
                        lease_owner = NULL, lease_until = NULL, updated_at = NOW()
                    WHERE task_id = %s AND status = 'RUNNING' AND attempt_token = %s
                """, ('SUCCEEDED' if success else 'FAILED', result_hash, error,
                      task_id, attempt_token))
                updated = cursor.rowcount > 0
            conn.commit()
        return updated
