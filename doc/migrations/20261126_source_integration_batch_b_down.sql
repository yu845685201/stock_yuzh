-- ============================================================
-- 盘后采集数据源整合 - 批次 B 回退迁移（详细技术方案 V1.0 §5.2）
-- 日期：2026-11-26
-- 对应升级脚本：20261126_source_integration_batch_b_up.sql
--
-- 回退说明：仅删除批次 B 新增的 lhb_event_seat / lhb_event；旧表
-- lhb_stock_daily / lhb_detail 及读端不受影响。
-- ============================================================

DROP INDEX IF EXISTS idx_lhb_event_seat_name;
DROP TABLE IF EXISTS lhb_event_seat;
DROP INDEX IF EXISTS idx_lhb_event_reason;
DROP INDEX IF EXISTS idx_lhb_event_day;
DROP TABLE IF EXISTS lhb_event;
DROP INDEX IF EXISTS idx_industry_snapshot_ind;
DROP TABLE IF EXISTS industry_member_snapshot;
