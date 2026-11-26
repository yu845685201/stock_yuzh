-- ============================================================
-- 盘后采集数据源整合 - 批次 C 升级迁移（详细技术方案 V1.0 §7.3/§8）
-- 日期：2026-11-26
-- 对应回退脚本：20261126_source_integration_batch_c_down.sql
--
-- 内容（§7.3 财报版本、§8 除权事件与兼容因子候选）：
--   financial_report_version      财报数值版本行（ts_code,stat_date 保留
--                                 历史版本；source_version 区分公告原件/季度包哈希）
--   stock_action_event_version    除权等公司行为事件（source+版本+事件键唯一）
--   adjust_factor_candidate       兼容因子候选序列（series_version 同锚点序列）
--
-- 约定：与应用 ensure 幂等兼容；批次 C 消费端接线（asof 查询/估值重算）为
--   后续批次，本迁移仅建结构。旧 base_financial_report / base_adjust_factor
--   保持正式键不动（§7.3 当前表投影、§8 只从已验收序列投影）。
-- ============================================================

CREATE TABLE IF NOT EXISTS financial_report_version (
    version_id          VARCHAR(36) PRIMARY KEY,
    ts_code             VARCHAR(12) NOT NULL,
    stat_date           VARCHAR(8)  NOT NULL,
    announcement_id     VARCHAR(64),
    report_kind         VARCHAR(16),
    source              VARCHAR(24) NOT NULL,
    source_version      VARCHAR(128) NOT NULL,
    published_at        TIMESTAMPTZ,
    published_precision VARCHAR(8),
    first_observed_at   TIMESTAMPTZ,
    availability_basis  VARCHAR(32),
    supersedes_id       VARCHAR(36),
    content_hash        VARCHAR(64),
    revision_id         VARCHAR(36),
    field_provenance    JSONB,
    net_profit          NUMERIC(24,4),
    operate_cashflow    NUMERIC(24,4),
    net_assets          NUMERIC(24,4),
    revenue             NUMERIC(24,4),
    revenue_yoy         NUMERIC(16,4),
    net_profit_yoy      NUMERIC(16,4),
    eps                 NUMERIC(16,4),
    roe                 NUMERIC(16,4),
    gross_margin        NUMERIC(16,4),
    update_time         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ts_code, stat_date, source, source_version, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_fin_ver_asof
    ON financial_report_version (ts_code, stat_date, published_at, first_observed_at);

CREATE TABLE IF NOT EXISTS stock_action_event_version (
    source            VARCHAR(16)  NOT NULL,
    source_version    VARCHAR(64)  NOT NULL,
    ts_code           VARCHAR(12)  NOT NULL,
    effective_date    VARCHAR(8)   NOT NULL,
    event_key         VARCHAR(96)  NOT NULL,
    category          SMALLINT,
    raw_event_type    VARCHAR(64),
    cash_dividend     NUMERIC(16,8),
    cash_reference    NUMERIC(16,8),
    bonus_ratio       NUMERIC(16,8),
    transfer_ratio    NUMERIC(16,8),
    rights_issue_ratio NUMERIC(16,8),
    rights_price      NUMERIC(16,8),
    raw_unit          VARCHAR(16),
    announcement_id   VARCHAR(64),
    observed_at       TIMESTAMPTZ,
    supersedes        VARCHAR(64),
    row_digest        TEXT,
    update_time       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source, source_version, ts_code, effective_date, event_key)
);

CREATE INDEX IF NOT EXISTS idx_action_event_day
    ON stock_action_event_version (effective_date, source);

CREATE TABLE IF NOT EXISTS adjust_factor_candidate (
    series_version     VARCHAR(64) NOT NULL,
    ts_code            VARCHAR(12) NOT NULL,
    trade_date         VARCHAR(8)  NOT NULL,
    qfq_factor         NUMERIC(24,10),
    hfq_factor         NUMERIC(24,10),
    reference_raw_close NUMERIC(24,8),
    anchor_date        VARCHAR(8),
    algo_version       VARCHAR(24),
    based_event_version VARCHAR(96),
    check_result       VARCHAR(16),
    check_detail       JSONB,
    update_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (series_version, ts_code, trade_date)
);
