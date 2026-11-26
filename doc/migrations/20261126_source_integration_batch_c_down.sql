-- ============================================================
-- 盘后采集数据源整合 - 批次 C 回退迁移（详细技术方案 V1.0 §7.3/§8）
-- 日期：2026-11-26
-- 对应升级脚本：20261126_source_integration_batch_c_up.sql
-- ============================================================

DROP TABLE IF EXISTS adjust_factor_candidate;
DROP INDEX IF EXISTS idx_action_event_day;
DROP TABLE IF EXISTS stock_action_event_version;
DROP INDEX IF EXISTS idx_fin_ver_asof;
DROP TABLE IF EXISTS financial_report_version;
