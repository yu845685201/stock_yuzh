-- ============================================================
-- 盘后采集数据源整合 - 批次 A 升级迁移（详细技术方案 V1.0 §3.1）
-- 日期：2026-11-26
-- 对应回退脚本：20261126_source_integration_down.sql
--
-- 内容：
--   逻辑分区发布指针表 sync_partition_current：
--     - 按逻辑分区（dataset + partition_key + contract_version）保存发布指针
--       与首轮验收时刻，解决 latest_partition_status 子项混排问题；
--     - 计划时创建当前合同期望行（INSERT ... ON CONFLICT DO NOTHING）；
--     - 首次合格提交用 COALESCE 固定首达时间/作业号（first_daily_* /
--       first_ready_* 不可回填倒签）；后续来源修订只更新当前指针（accepted_*）。
--
-- 约定：
--   - 日期边界 VARCHAR(8) 'YYYYMMDD'；时刻 TIMESTAMPTZ（Asia/Shanghai 业务钟）；
--   - 与应用内 ensure_collection_tables() 幂等兼容：本脚本可重复执行
--     （IF NOT EXISTS）；旧 sync_partition 记录由回放复核后再建立指针
--     （§3.1：不回填历史"按时成功"）；
--   - dataset_revision 须先存在（外键引用）——本脚本在其后执行。
-- ============================================================

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
);

CREATE INDEX IF NOT EXISTS idx_partition_current_day
    ON sync_partition_current (business_date, dataset, contract_version);
