-- ============================================================
-- 每日复盘采集与分析闭环 - 升级迁移（技术方案 V1.0 §4，D01）
-- 日期：2026-11-19
-- 对应回退脚本：20261119_review_closed_loop_down.sql
--
-- 内容：
--   四张采集控制表：sync_job / sync_partition / dataset_revision / recompute_task
--   两张发布控制表：review_batch / review_verification_version
--   一张业务表：    market_quote_snapshot（多时点不可变报价，§4.2）
--
-- 约定：
--   - ID 由应用生成 UUID（VARCHAR(36)），不依赖数据库扩展；
--   - 日期边界 VARCHAR(8) 'YYYYMMDD'（与业务表一致）；时间 TIMESTAMPTZ；
--   - 与应用内 ensure_collection_tables()/ensure_review_publication_tables()
--     幂等兼容：本脚本可重复执行（IF NOT EXISTS）；
--   - 历史脏数据修订走独立迁移作业，本脚本不做数据回填。
-- ============================================================

-- ---------- 1. 采集控制表 ----------

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
);

CREATE INDEX IF NOT EXISTS idx_sync_job_date ON sync_job (target_date, started_at);

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
);

-- 同一作业内同数据集×源×市场×分区键唯一（含 source 于身份：分区键不区分源选择
-- 策略时，备用源替换沿用同一修订链，source 仅作记录列；此唯一键防同作业重复记账）
CREATE UNIQUE INDEX IF NOT EXISTS uk_sync_partition_job
    ON sync_partition (job_id, dataset, source, market, partition_key);

CREATE INDEX IF NOT EXISTS idx_sync_partition_lookup
    ON sync_partition (dataset, market, expected_date, status);

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
);

CREATE INDEX IF NOT EXISTS idx_dataset_revision_chain
    ON dataset_revision (dataset, partition_key, created_at);

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
);

-- required_input_hash 可空：COALESCE 归一后建唯一索引，保证 NULL 也参与去重
CREATE UNIQUE INDEX IF NOT EXISTS uk_recompute_task_scope
    ON recompute_task (module, module_version, anchor_start, anchor_end, scope_hash,
                       COALESCE(required_input_hash, ''));

CREATE INDEX IF NOT EXISTS idx_recompute_task_lease
    ON recompute_task (status, lease_until);

-- ---------- 2. 发布控制表 ----------

CREATE TABLE IF NOT EXISTS review_batch (
    batch_id VARCHAR(36) PRIMARY KEY,
    anchor_date VARCHAR(8) NOT NULL,
    kind VARCHAR(16) NOT NULL DEFAULT 'FIRST',   -- FIRST/REVISION/LEGACY/RESEARCH
    first_batch_id VARCHAR(36),
    revision_of VARCHAR(36),
    status VARCHAR(16) NOT NULL DEFAULT 'BUILDING',  -- BUILDING/VALIDATED/PUBLISHED/FAILED
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
);

-- batch_key 非空唯一（同输入稳定批次恢复原 batch_id，不另建同内容批次）
CREATE UNIQUE INDEX IF NOT EXISTS uk_review_batch_key
    ON review_batch (batch_key) WHERE batch_key IS NOT NULL;

-- 每锚日一个已发布原始首发（含 LEGACY 恢复首发，不只 kind=FIRST）
CREATE UNIQUE INDEX IF NOT EXISTS uk_review_batch_original
    ON review_batch (anchor_date)
    WHERE status = 'PUBLISHED' AND is_original_first = TRUE;

-- 每锚日一个 current 指针
CREATE UNIQUE INDEX IF NOT EXISTS uk_review_batch_current
    ON review_batch (anchor_date) WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_review_batch_anchor
    ON review_batch (anchor_date, status);

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
);

-- 核验按批次版本唯一（同批次同指标同输入不重复计样本）
CREATE UNIQUE INDEX IF NOT EXISTS uk_review_verification_version
    ON review_verification_version (prediction_batch_id, evaluation_date, scope,
                                    metric_id, horizon, result_snapshot_id, metric_version);

-- ---------- 3. 业务表：多时点不可变报价（技术方案 §4.2） ----------

CREATE TABLE IF NOT EXISTS market_quote_snapshot (
    quote_id VARCHAR(36) PRIMARY KEY,
    market_code VARCHAR(32) NOT NULL,
    source VARCHAR(64),
    source_trade_date VARCHAR(8),
    quote_at TIMESTAMPTZ,
    source_published_at TIMESTAMPTZ,
    first_observed_at TIMESTAMPTZ,
    availability_basis VARCHAR(32),
    payload JSONB,
    payload_hash VARCHAR(64),
    quality_state VARCHAR(24),
    job_id VARCHAR(36)
);

-- 同源同代码同报价时刻同载荷去重；源对同一时刻的修正保留新版本（payload_hash 不同）
CREATE UNIQUE INDEX IF NOT EXISTS uk_market_quote_snapshot
    ON market_quote_snapshot (market_code, source, source_trade_date, quote_at, payload_hash);

CREATE INDEX IF NOT EXISTS idx_market_quote_lookup
    ON market_quote_snapshot (market_code, source_trade_date, quote_at DESC);

-- 旧 overseas_market_daily 继续作为最新视图，不再承担盘前历史证据（§4.2），
-- 本迁移不改动其结构。
