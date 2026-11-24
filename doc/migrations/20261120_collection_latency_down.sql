-- 数据采集链路耗时优化（V1.1-20261120）迁移 DOWN（回滚）
-- 保留新增数据（待补任务/域冷却/台账重试记录）至对账完成后再执行（方案 §6 回滚：
-- 数据库迁移采用增量字段与新表，已有待办、分区证据和已修正行情保留至对账完成）

DROP INDEX IF EXISTS idx_rawfb_claim;
ALTER TABLE kline_day_raw_fallback DROP COLUMN IF EXISTS last_attempt_at;
ALTER TABLE kline_day_raw_fallback DROP COLUMN IF EXISTS next_retry_at;
ALTER TABLE kline_day_raw_fallback DROP COLUMN IF EXISTS attempts;
ALTER TABLE kline_day_raw_fallback DROP COLUMN IF EXISTS last_error;

DROP INDEX IF EXISTS idx_sync_retry_due;
DROP INDEX IF EXISTS idx_sync_retry_lease;
DROP INDEX IF EXISTS uk_sync_retry_key;
DROP TABLE IF EXISTS sync_collection_retry;
DROP TABLE IF EXISTS sync_domain_cooldown;
