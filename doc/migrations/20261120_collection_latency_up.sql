-- 数据采集链路耗时优化（V1.1-20261120）迁移 UP
-- 对应批次：2 恢复扫描拆分（台账重试字段）、3 补采接管冷却（待补表及域状态）
-- 说明：Python 侧 ensure_raw_fallback_table / ensure_collection_retry_tables 亦幂等建列建表，
--       本 SQL 用于显式迁移控制（与 D01 惯例一致）；可在 uat/prod 手工执行或依赖 ensure 自动兜底。

-- ===== 批次2：raw 兜底台账重试调度字段（方案 §4 预算和公平性）=====
ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS last_attempt_at timestamptz;
ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS next_retry_at  timestamptz;
ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS attempts       integer NOT NULL DEFAULT 0;
ALTER TABLE kline_day_raw_fallback ADD COLUMN IF NOT EXISTS last_error     text;

-- 领取索引：到期（next_retry_at 空 = 立即可领）+ 最久未尝试优先
CREATE INDEX IF NOT EXISTS idx_rawfb_claim
    ON kline_day_raw_fallback (status, COALESCE(next_retry_at, '1970-01-01'::timestamptz));

-- ===== 批次3：补采待办表（方案 §5.1 拟增 sync_collection_retry）=====
CREATE TABLE IF NOT EXISTS sync_collection_retry (
    retry_id VARCHAR(36) PRIMARY KEY,
    retry_key VARCHAR(300) NOT NULL,
    dataset VARCHAR(64) NOT NULL,
    source VARCHAR(64),
    target_date VARCHAR(8) NOT NULL,
    market VARCHAR(16) DEFAULT 'CN',
    partition_key VARCHAR(256),
    action VARCHAR(24) NOT NULL DEFAULT 'SOURCE_RETRY',
    reason VARCHAR(32),
    missing_scope JSONB,
    domain VARCHAR(32),
    retry_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    lease_token VARCHAR(36),
    lease_owner VARCHAR(128),
    lease_until TIMESTAMPTZ,
    parent_job_id VARCHAR(36),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_sync_retry_key ON sync_collection_retry (retry_key);
CREATE INDEX IF NOT EXISTS idx_sync_retry_due   ON sync_collection_retry (status, retry_at);
CREATE INDEX IF NOT EXISTS idx_sync_retry_lease ON sync_collection_retry (status, lease_until);

-- ===== 批次3：域冷却记录（方案 §5.1 拟增域冷却记录）=====
CREATE TABLE IF NOT EXISTS sync_domain_cooldown (
    domain VARCHAR(32) PRIMARY KEY,
    blocked_until TIMESTAMPTZ,
    reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ===== 存量对账（方案 §4"迁移时对账存量 raw 行与打开台账"，执行前先跑核对查询）=====
-- 1) 核实已转正行（行情行已回归 TDXAPI 但台账仍打开 → 应销账）：
--    SELECT f.ts_code, f.trade_date FROM kline_day_raw_fallback f
--    JOIN his_kline_day h ON h.ts_code = f.ts_code AND h.trade_date = f.trade_date
--    WHERE f.status = 'fallback' AND h.source = 'TDXAPI';
--    → 核对无误后执行：
--    UPDATE kline_day_raw_fallback f SET status='recovered', recovered_at=NOW(),
--           recover_source='migration_reconcile'
--    FROM his_kline_day h WHERE h.ts_code=f.ts_code AND h.trade_date=f.trade_date
--      AND f.status='fallback' AND h.source='TDXAPI';
-- 2) 补登缺账（行情行仍是兜底标记但台账无记录 → 补登打开台账）：
--    INSERT INTO kline_day_raw_fallback (ts_code, trade_date, status, reason, fallback_at)
--    SELECT h.ts_code, h.trade_date, 'fallback', 'migration_backfill', h.update_time
--    FROM his_kline_day h
--    WHERE h.source = 'TDXAPI_RAWFB'
--      AND NOT EXISTS (SELECT 1 FROM kline_day_raw_fallback f
--                      WHERE f.ts_code=h.ts_code AND f.trade_date=h.trade_date);
--    注：his_kline_day 无 update_time 行（兜底行必有 update_time）不适用；按实际执行结果核对计数。
-- 3) 存量打开记录 last_attempt_at/next_retry_at 保持 NULL → 首次运行即可领取（方案 §4）。
