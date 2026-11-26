-- ============================================================
-- 盘后采集数据源整合 - 批次 A 回退迁移（详细技术方案 V1.0 §3.1）
-- 日期：2026-11-26
-- 对应升级脚本：20261126_source_integration_up.sql
--
-- 回退说明：
--   - 仅删除本批次新增的 sync_partition_current 表与索引；
--   - 表内指针数据删除后不可自动重建——回退前如需保留验收轨迹，
--     先按 §12 导出（business_date/dataset/accepted_status/first_ready_at）；
--   - sync_job / sync_partition / dataset_revision / recompute_task 等共享表
--     不在本脚本范围（回退不清业务与台账数据）。
-- ============================================================

DROP INDEX IF EXISTS idx_partition_current_day;
DROP TABLE IF EXISTS sync_partition_current;
