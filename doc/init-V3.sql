-- ============================================================
-- init-V3.sql  每日复盘数据采集新表 DDL（每日复盘数据采集技术方案-V1.0-20260927 第五章）
-- 惯例：代码运行时 dao_review_sync.py ensure_review_tables() 兜底建表（本文件为 DDL 归档，两者保持一致）
-- 全部表带 update_time 并按项目惯例用 ON CONFLICT ... DO UPDATE upsert
-- ============================================================

-- ---------- 通用游标表（替代每源一张进度表；全量任务用 tmp/manifest 不进此表） ----------
-- ---------- sync_cursor ----------
CREATE TABLE IF NOT EXISTS sync_cursor (
    source_name   VARCHAR(50)  NOT NULL,
    cursor_key    VARCHAR(50)  NOT NULL,
    cursor_value  VARCHAR(200),
    updated_at    TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (source_name, cursor_key)
);

-- ---------- base_adjust_factor ----------
CREATE TABLE IF NOT EXISTS base_adjust_factor (
    id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    qfq_factor NUMERIC(20, 8),
    hfq_factor NUMERIC(20, 8),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE base_adjust_factor ADD CONSTRAINT uk_base_adjust_factor_review UNIQUE (ts_code, trade_date);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- base_financial_report ----------
CREATE TABLE IF NOT EXISTS base_financial_report (
    id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    stat_date VARCHAR(8) NOT NULL,
    revenue NUMERIC(24, 4),
    revenue_yoy NUMERIC(14, 4),
    net_profit NUMERIC(24, 4),
    net_profit_yoy NUMERIC(14, 4),
    roe NUMERIC(14, 4),
    gross_margin NUMERIC(14, 4),
    operate_cashflow NUMERIC(24, 4),
    eps NUMERIC(14, 4),
    net_assets NUMERIC(24, 4),
    disclosure_date VARCHAR(8),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE base_financial_report ADD CONSTRAINT uk_base_financial_report_review UNIQUE (ts_code, stat_date);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- base_disclosure_calendar ----------
CREATE TABLE IF NOT EXISTS base_disclosure_calendar (
    id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    report_period VARCHAR(8) NOT NULL,
    plan_disclosure_date VARCHAR(8),
    actual_disclosure_date VARCHAR(8),
    exchange VARCHAR(10),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE base_disclosure_calendar ADD CONSTRAINT uk_base_disclosure_calendar_review UNIQUE (ts_code, report_period);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- index_valuation_daily ----------
CREATE TABLE IF NOT EXISTS index_valuation_daily (
    id BIGSERIAL PRIMARY KEY,
    trade_date VARCHAR(8) NOT NULL,
    index_code VARCHAR(20) NOT NULL,
    index_name VARCHAR(50),
    pe_ttm NUMERIC(16, 4),
    pe_static NUMERIC(16, 4),
    dividend_yield NUMERIC(12, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE index_valuation_daily ADD CONSTRAINT uk_index_valuation_daily_review UNIQUE (index_code, trade_date);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- stock_valuation_daily ----------
CREATE TABLE IF NOT EXISTS stock_valuation_daily (
    id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    trade_date VARCHAR(8) NOT NULL,
    total_mv NUMERIC(24, 4),
    pe_ttm NUMERIC(20, 4),
    pb NUMERIC(20, 4),
    insufficient_note VARCHAR(200),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE stock_valuation_daily ADD CONSTRAINT uk_stock_valuation_daily_review UNIQUE (ts_code, trade_date);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- performance_forecast ----------
CREATE TABLE IF NOT EXISTS performance_forecast (
    id BIGSERIAL PRIMARY KEY,
    announcement_id VARCHAR(50) NOT NULL,
    ts_code VARCHAR(20),
    stock_name VARCHAR(30),
    ann_date VARCHAR(8),
    report_period VARCHAR(8),
    forecast_type VARCHAR(20),
    net_profit_min NUMERIC(24, 4),
    net_profit_max NUMERIC(24, 4),
    summary VARCHAR(500),
    ann_url VARCHAR(300),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE performance_forecast ADD CONSTRAINT uk_performance_forecast_review UNIQUE (announcement_id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- announcement_daily ----------
CREATE TABLE IF NOT EXISTS announcement_daily (
    id BIGSERIAL PRIMARY KEY,
    announcement_id VARCHAR(50) NOT NULL,
    ann_date VARCHAR(8),
    ts_code VARCHAR(20),
    stock_name VARCHAR(30),
    title VARCHAR(500),
    ann_type VARCHAR(20),
    ann_url VARCHAR(300),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE announcement_daily ADD CONSTRAINT uk_announcement_daily_review UNIQUE (announcement_id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- sector_concept ----------
CREATE TABLE IF NOT EXISTS sector_concept (
    id BIGSERIAL PRIMARY KEY,
    concept_code VARCHAR(30) NOT NULL,
    concept_name VARCHAR(100),
    source VARCHAR(20),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE sector_concept ADD CONSTRAINT uk_sector_concept_review UNIQUE (concept_code);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- sector_concept_map ----------
CREATE TABLE IF NOT EXISTS sector_concept_map (
    id BIGSERIAL PRIMARY KEY,
    concept_code VARCHAR(30) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    in_date VARCHAR(8),
    out_date VARCHAR(8),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE sector_concept_map ADD CONSTRAINT uk_sector_concept_map_review UNIQUE (concept_code, ts_code);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- lhb_stock_daily ----------
CREATE TABLE IF NOT EXISTS lhb_stock_daily (
    id BIGSERIAL PRIMARY KEY,
    trade_date VARCHAR(8) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(30),
    reason VARCHAR(200),
    buy_amt NUMERIC(24, 4),
    sell_amt NUMERIC(24, 4),
    net_amt NUMERIC(24, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE lhb_stock_daily ADD CONSTRAINT uk_lhb_stock_daily_review UNIQUE (trade_date, ts_code, reason);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- lhb_detail ----------
CREATE TABLE IF NOT EXISTS lhb_detail (
    id BIGSERIAL PRIMARY KEY,
    trade_date VARCHAR(8) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    seat_name VARCHAR(100) NOT NULL,
    side VARCHAR(10) NOT NULL,
    amount NUMERIC(24, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE lhb_detail ADD CONSTRAINT uk_lhb_detail_review UNIQUE (trade_date, ts_code, seat_name, side);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- margin_trade_daily ----------
CREATE TABLE IF NOT EXISTS margin_trade_daily (
    id BIGSERIAL PRIMARY KEY,
    trade_date VARCHAR(8) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    rzye NUMERIC(24, 4),
    rzmre NUMERIC(24, 4),
    rzche NUMERIC(24, 4),
    rqye NUMERIC(24, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE margin_trade_daily ADD CONSTRAINT uk_margin_trade_daily_review UNIQUE (trade_date, ts_code);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- share_unlock_calendar ----------
CREATE TABLE IF NOT EXISTS share_unlock_calendar (
    id BIGSERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(30),
    unlock_date VARCHAR(8) NOT NULL,
    unlock_shares NUMERIC(24, 4),
    unlock_ratio NUMERIC(12, 6),
    unlock_type VARCHAR(50),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE share_unlock_calendar ADD CONSTRAINT uk_share_unlock_calendar_review UNIQUE (ts_code, unlock_date);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- overseas_market_daily ----------
CREATE TABLE IF NOT EXISTS overseas_market_daily (
    id BIGSERIAL PRIMARY KEY,
    trade_date VARCHAR(8) NOT NULL,
    market_code VARCHAR(30) NOT NULL,
    market_name VARCHAR(50),
    close NUMERIC(20, 4),
    change_pct NUMERIC(14, 4),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE overseas_market_daily ADD CONSTRAINT uk_overseas_market_daily_review UNIQUE (trade_date, market_code);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- money_flow_daily ----------
CREATE TABLE IF NOT EXISTS money_flow_daily (
    id BIGSERIAL PRIMARY KEY,
    trade_date VARCHAR(8) NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    total_amt NUMERIC(24, 4),
    big_buy_amt NUMERIC(24, 4),
    big_sell_amt NUMERIC(24, 4),
    big_net_amt NUMERIC(24, 4),
    big_net_pct NUMERIC(14, 6),
    stock_count INTEGER,
    calib_note VARCHAR(50) DEFAULT '近似口径:分笔分桶',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DO $$
BEGIN
    ALTER TABLE money_flow_daily ADD CONSTRAINT uk_money_flow_daily_review UNIQUE (trade_date, ts_code);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- ---------- trade_index_info 迁移（方案 4.1/5.2） ----------
-- 涨跌家数参考列（精确家数仍由全市场日K自算）
ALTER TABLE trade_index_info ADD COLUMN IF NOT EXISTS up_count INTEGER,
                 ADD COLUMN IF NOT EXISTS down_count INTEGER;

-- (ts_code, trade_date) 业务唯一键（upsert 依赖；若存在重复行需先清理，运行时 ensure 会保留每组 id 最大者）
CREATE UNIQUE INDEX IF NOT EXISTS uk_trade_index_info_ts_code_trade_date ON trade_index_info (ts_code, trade_date);

