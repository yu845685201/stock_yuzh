-- ============================================================
-- 每日复盘采集与分析闭环 - 回退迁移（技术方案 V1.0 §4，D01）
-- 日期：2026-11-19
-- 对应升级脚本：20261119_review_closed_loop_up.sql
--
-- 回退原则（技术方案 §11.4）：
--   - 结构回退以旧读取路径兼容为主，**不删除已发布证据**——生产回退时
--     通常只回退应用版本，本脚本仅在确认需要物理清退新表时使用；
--   - 数据修订撤回不静默恢复脏行：物理删表前必须已备份
--     （pg_dump 或 CREATE TABLE ... AS SELECT）。
-- ============================================================

-- ---------- 1. 发布控制表 ----------
DROP INDEX IF EXISTS uk_review_verification_version;
DROP INDEX IF EXISTS idx_review_batch_anchor;
DROP INDEX IF EXISTS uk_review_batch_current;
DROP INDEX IF EXISTS uk_review_batch_original;
DROP INDEX IF EXISTS uk_review_batch_key;
DROP TABLE IF EXISTS review_verification_version;
DROP TABLE IF EXISTS review_batch;

-- ---------- 2. 业务表 ----------
DROP INDEX IF EXISTS idx_market_quote_lookup;
DROP INDEX IF EXISTS uk_market_quote_snapshot;
DROP TABLE IF EXISTS market_quote_snapshot;

-- ---------- 3. 采集控制表 ----------
DROP INDEX IF EXISTS idx_recompute_task_lease;
DROP INDEX IF EXISTS uk_recompute_task_scope;
DROP INDEX IF EXISTS idx_dataset_revision_chain;
DROP INDEX IF EXISTS idx_sync_partition_lookup;
DROP INDEX IF EXISTS uk_sync_partition_job;
DROP INDEX IF EXISTS idx_sync_job_date;
DROP TABLE IF EXISTS recompute_task;
DROP TABLE IF EXISTS dataset_revision;
DROP TABLE IF EXISTS sync_partition;
DROP TABLE IF EXISTS sync_job;
