-- =====================================================================
-- init-V2.sql 财报PDF采集增量DDL
-- 变更内容：新增财报PDF采集进度表 sync_financial_pdf_progress
-- 关联方案：财报PDF采集（巨潮资讯网，文件落地+进度表+增量采集）
-- 说明：PDF 本体落 data/financial_pdf/ 文件系统，数据库仅记录采集进度
-- 幂等：代码启动时亦执行 CREATE TABLE IF NOT EXISTS 兜底（见 dao_financial_pdf.py）
-- =====================================================================

CREATE TABLE IF NOT EXISTS sync_financial_pdf_progress (
    stock_code     VARCHAR(16) PRIMARY KEY,   -- 股票编码（如 600519）
    last_stat_date DATE NOT NULL,             -- 最后财报期（成功落盘的最大报告期，如 2025-06-30）
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  sync_financial_pdf_progress IS '财报PDF采集进度表（巨潮资讯网，股票编码+最后财报期）';
COMMENT ON COLUMN sync_financial_pdf_progress.stock_code     IS '股票编码（6位数字，如 600519）';
COMMENT ON COLUMN sync_financial_pdf_progress.last_stat_date IS '最后财报期：已成功落盘的最大报告期结束日（01->03-31, 02->06-30, 03->09-30, 04->12-31）';
