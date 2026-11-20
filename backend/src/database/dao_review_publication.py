"""发布批次 DAO（每日复盘采集与分析闭环技术方案 V1.0 §4.1/§9，D01 建表 + 基础 API）

两张表：

- ``review_batch``：已发布批次（batch_id 身份；首发唯一 + current 指针部分唯一索引）；
- ``review_verification_version``：核验结果按批次版本保留（旧 review_verification_daily
  降级为 legacy 数据源，不再按日期覆盖）。

状态机（§9.1）：``BUILDING → VALIDATED → PUBLISHED``，失败为 ``FAILED``。
只有 ``PUBLISHED`` 批次能设为 current。发布后的载荷（facts/candidates/predictions/
artifact_manifest）不可原地重建覆盖——本 DAO 不提供 UPDATE payload 的 API。

kind ∈ FIRST / REVISION / LEGACY / RESEARCH：历史研究批次不得抢占真实首发身份
（is_original_first 的部分唯一索引同时约束 kind=FIRST 与有证据恢复的 LEGACY）。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import psycopg2.extras

logger = logging.getLogger(__name__)


def _j(value: Any) -> str:
    import json
    return json.dumps(value, ensure_ascii=False, default=str)


class ReviewPublicationDaoMixin:
    """发布批次与核验版本表（建表 + 显式事务/条件更新 API）"""

    _PUBLICATION_TABLE_DDL: Dict[str, str] = {
        'review_batch': """
            CREATE TABLE IF NOT EXISTS review_batch (
                batch_id VARCHAR(36) PRIMARY KEY,
                anchor_date VARCHAR(8) NOT NULL,
                kind VARCHAR(16) NOT NULL DEFAULT 'FIRST',
                first_batch_id VARCHAR(36),
                revision_of VARCHAR(36),
                status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',
                is_original_first BOOLEAN NOT NULL DEFAULT FALSE,
                is_current BOOLEAN NOT NULL DEFAULT FALSE,
                cutoff_at TIMESTAMPTZ,
                input_manifest JSONB,
                snapshot_id VARCHAR(64),
                rules_hash VARCHAR(64),
                config_hash VARCHAR(64),
                code_hash VARCHAR(64),
                skill_hash VARCHAR(64),
                batch_key VARCHAR(64),
                facts JSONB,
                candidates JSONB,
                predictions JSONB,
                artifact_manifest JSONB,
                published_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """,
        'review_verification_version': """
            CREATE TABLE IF NOT EXISTS review_verification_version (
                verification_id VARCHAR(36) PRIMARY KEY,
                prediction_batch_id VARCHAR(36) NOT NULL,
                evaluation_date VARCHAR(8) NOT NULL,
                scope VARCHAR(32) NOT NULL DEFAULT '',
                metric_id VARCHAR(64) NOT NULL DEFAULT '',
                horizon VARCHAR(16) NOT NULL DEFAULT '',
                result_snapshot_id VARCHAR(64) NOT NULL DEFAULT '',
                metric_version VARCHAR(32) NOT NULL DEFAULT 'v1',
                payload JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """,
    }

    _PUBLICATION_INDEX_DDL: List[str] = [
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_review_batch_key "
        "ON review_batch (batch_key) WHERE batch_key IS NOT NULL",
        # 每锚日一个已发布原始首发（含 LEGACY 恢复首发，不只 kind=FIRST）
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_review_batch_original "
        "ON review_batch (anchor_date) "
        "WHERE status = 'PUBLISHED' AND is_original_first = TRUE",
        # 每锚日一个 current 指针
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_review_batch_current "
        "ON review_batch (anchor_date) WHERE is_current = TRUE",
        "CREATE INDEX IF NOT EXISTS idx_review_batch_anchor "
        "ON review_batch (anchor_date, status)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uk_review_verification_version "
        "ON review_verification_version (prediction_batch_id, evaluation_date, scope, "
        "metric_id, horizon, result_snapshot_id, metric_version)",
    ]

    def ensure_review_publication_tables(self) -> None:
        """发布批次表建表兜底（幂等；DDL 同步维护在 doc/migrations/20261119_*.sql）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for ddl in self._PUBLICATION_TABLE_DDL.values():
                    cursor.execute(ddl)
                for idx in self._PUBLICATION_INDEX_DDL:
                    cursor.execute(idx)
            conn.commit()
        logger.info('发布批次表 ensure 完成（review_batch/review_verification_version）')

    # ---------- 批次状态机（条件更新，禁止改 payload） ----------

    def create_review_batch_tx(self, conn: Any, batch_id: str, anchor_date: str,
                               kind: str = 'FIRST', cutoff_at: Optional[Any] = None,
                               input_manifest: Optional[Dict[str, Any]] = None,
                               snapshot_id: Optional[str] = None,
                               first_batch_id: Optional[str] = None,
                               revision_of: Optional[str] = None) -> None:
        """创建 BUILDING 批次（is_original_first 在发布时按首发规则写入）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO review_batch
                (batch_id, anchor_date, kind, first_batch_id, revision_of, status, cutoff_at,
                 input_manifest, snapshot_id)
                VALUES (%s, %s, %s, %s, %s, 'BUILDING', %s, %s, %s)
            """, (batch_id, anchor_date, kind, first_batch_id, revision_of, cutoff_at,
                  _j(input_manifest) if input_manifest is not None else None, snapshot_id))

    def update_batch_payload_tx(self, conn: Any, batch_id: str,
                                facts: Optional[Dict[str, Any]] = None,
                                candidates: Optional[Dict[str, Any]] = None,
                                predictions: Optional[Dict[str, Any]] = None,
                                artifact_manifest: Optional[Dict[str, Any]] = None,
                                batch_key: Optional[str] = None,
                                rules_hash: Optional[str] = None,
                                config_hash: Optional[str] = None,
                                code_hash: Optional[str] = None,
                                skill_hash: Optional[str] = None) -> None:
        """BUILDING 阶段填充载荷（仅 status=BUILDING 时允许；发布后禁止）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE review_batch SET
                    facts = COALESCE(%s, facts),
                    candidates = COALESCE(%s, candidates),
                    predictions = COALESCE(%s, predictions),
                    artifact_manifest = COALESCE(%s, artifact_manifest),
                    batch_key = COALESCE(%s, batch_key),
                    rules_hash = COALESCE(%s, rules_hash),
                    config_hash = COALESCE(%s, config_hash),
                    code_hash = COALESCE(%s, code_hash),
                    skill_hash = COALESCE(%s, skill_hash),
                    updated_at = NOW()
                WHERE batch_id = %s AND status = 'BUILDING'
            """, (_j(facts) if facts is not None else None,
                  _j(candidates) if candidates is not None else None,
                  _j(predictions) if predictions is not None else None,
                  _j(artifact_manifest) if artifact_manifest is not None else None,
                  batch_key, rules_hash, config_hash, code_hash, skill_hash, batch_id))

    def mark_batch_validated_tx(self, conn: Any, batch_id: str) -> bool:
        """校验通过：BUILDING → VALIDATED（批次键必须在进入 VALIDATED 前已计算，§9.2）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE review_batch SET status = 'VALIDATED', updated_at = NOW()
                WHERE batch_id = %s AND status = 'BUILDING'
                  AND batch_key IS NOT NULL
            """, (batch_id,))
            return cursor.rowcount > 0

    def mark_batch_failed_tx(self, conn: Any, batch_id: str) -> None:
        """失败批次（未发布的允许恢复到 BUILDING，§9.1）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE review_batch SET status = 'FAILED', updated_at = NOW()
                WHERE batch_id = %s AND status IN ('BUILDING', 'VALIDATED')
            """, (batch_id,))

    def publish_batch_tx(self, conn: Any, batch_id: str,
                         is_original_first: Optional[bool] = None,
                         first_batch_id: Optional[str] = None) -> bool:
        """VALIDATED → PUBLISHED（发布锁由调用方按锚定日 advisory lock 获取，
        首发唯一约束兜底两进程同时发布，§9.1）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE review_batch SET
                    status = 'PUBLISHED', published_at = NOW(), updated_at = NOW(),
                    is_original_first = COALESCE(%s, is_original_first),
                    first_batch_id = COALESCE(%s, first_batch_id,
                        CASE WHEN %s::boolean IS TRUE THEN batch_id ELSE first_batch_id END)
                WHERE batch_id = %s AND status = 'VALIDATED'
            """, (is_original_first, first_batch_id, is_original_first, batch_id))
            return cursor.rowcount > 0

    def set_current_batch_tx(self, conn: Any, batch_id: str) -> bool:
        """切换 current 指针（仅 PUBLISHED 批次；先清旧再置新，同事务原子）"""
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("SELECT anchor_date, status FROM review_batch WHERE batch_id = %s",
                           (batch_id,))
            row = cursor.fetchone()
            if not row or row['status'] != 'PUBLISHED':
                return False
            cursor.execute("UPDATE review_batch SET is_current = FALSE WHERE anchor_date = %s",
                           (row['anchor_date'],))
            cursor.execute("UPDATE review_batch SET is_current = TRUE WHERE batch_id = %s "
                           "AND status = 'PUBLISHED'", (batch_id,))
            return cursor.rowcount > 0

    # ---------- 查询 ----------

    def get_review_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        rows = self.execute_query(
            "SELECT * FROM review_batch WHERE batch_id = %s", (batch_id,))
        return rows[0] if rows else None

    def get_current_review_batch(self, anchor_date: str) -> Optional[Dict[str, Any]]:
        """current 批次（读者入口与自动消费先解析指针，再访问批次目录，§9.2）"""
        rows = self.execute_query(
            "SELECT * FROM review_batch WHERE anchor_date = %s AND is_current = TRUE",
            (anchor_date,))
        return rows[0] if rows else None

    def get_original_first_batch(self, anchor_date: str) -> Optional[Dict[str, Any]]:
        """首发批次（正式核验使用已发布首发名单，§1 约束 4）"""
        rows = self.execute_query(
            "SELECT * FROM review_batch WHERE anchor_date = %s "
            "AND status = 'PUBLISHED' AND is_original_first = TRUE",
            (anchor_date,))
        return rows[0] if rows else None

    def insert_verification_version_tx(self, conn: Any, verification_id: str,
                                       prediction_batch_id: str, evaluation_date: str,
                                       scope: str = '', metric_id: str = '',
                                       horizon: str = '',
                                       result_snapshot_id: str = '',
                                       metric_version: str = 'v1',
                                       payload: Optional[Dict[str, Any]] = None) -> None:
        """核验结果按版本落库（唯一键去重：同批次同指标同输入不重复计样本，§7.3）"""
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO review_verification_version
                (verification_id, prediction_batch_id, evaluation_date, scope, metric_id,
                 horizon, result_snapshot_id, metric_version, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (prediction_batch_id, evaluation_date, scope, metric_id,
                             horizon, result_snapshot_id, metric_version)
                DO NOTHING
            """, (verification_id, prediction_batch_id, evaluation_date, scope, metric_id,
                  horizon, result_snapshot_id, metric_version,
                  _j(payload) if payload is not None else None))
