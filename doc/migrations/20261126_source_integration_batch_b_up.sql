-- ============================================================
-- 盘后采集数据源整合 - 批次 B 升级迁移（详细技术方案 V1.0 §5.2 龙虎榜事件/席位）
-- 日期：2026-11-26
-- 对应回退脚本：20261126_source_integration_batch_b_down.sql
--
-- 内容：
--   lhb_event        榜单事件层（event_id 由归一身份生成：披露日+交易所+板块+
--                    代码+原因规则+披露区间；sec_tx_amount 为披露区间竞价成交额）
--   lhb_event_seat   席位层（(event_id,side,seat_rank) 主键；买卖同机构、
--                    同股多原因不碰撞；名称原文、方向内及对侧金额、来源与精度）
--
-- 约定：与 ensure 同源幂等（IF NOT EXISTS）；旧表 lhb_stock_daily/lhb_detail
--   保留并行投影（§5.2 兼容查询），历史旧行不迁移。
-- ============================================================

CREATE TABLE IF NOT EXISTS lhb_event (
    event_id       VARCHAR(64) PRIMARY KEY,
    trade_date     VARCHAR(8)  NOT NULL,
    exchange       VARCHAR(16) NOT NULL,
    board          VARCHAR(16) NOT NULL,
    ts_code        VARCHAR(12) NOT NULL,
    security_type  VARCHAR(16),
    reason_code    VARCHAR(64) NOT NULL,
    reason_text    TEXT,
    period_start   VARCHAR(8),
    period_end     VARCHAR(8),
    sec_tx_amount  NUMERIC(24,4),
    buy_amt        NUMERIC(24,4),
    sell_amt       NUMERIC(24,4),
    net_amt        NUMERIC(24,4),
    source_keys    JSONB,
    revision_id    VARCHAR(36),
    update_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lhb_event_day ON lhb_event (trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_lhb_event_reason ON lhb_event (trade_date, exchange, board, reason_code);

CREATE TABLE IF NOT EXISTS lhb_event_seat (
    event_id    VARCHAR(64) NOT NULL REFERENCES lhb_event(event_id),
    side        CHAR(1)     NOT NULL CHECK (side IN ('B', 'S')),
    seat_rank   SMALLINT    NOT NULL CHECK (seat_rank > 0),
    seat_name   TEXT        NOT NULL,
    buy_amount  NUMERIC(24,4),
    sell_amount NUMERIC(24,4),
    amount_unit VARCHAR(8)  NOT NULL DEFAULT 'yuan',
    source      VARCHAR(24),
    PRIMARY KEY (event_id, side, seat_rank)
);

CREATE INDEX IF NOT EXISTS idx_lhb_event_seat_name ON lhb_event_seat (seat_name);

-- ---------- 国证行业日期快照（§6 批次 B） ----------

CREATE TABLE IF NOT EXISTS industry_member_snapshot (
    trade_date    VARCHAR(8)  NOT NULL,
    scheme        VARCHAR(24) NOT NULL,
    level         SMALLINT    NOT NULL,
    ts_code       VARCHAR(12) NOT NULL,
    industry_code VARCHAR(32),
    industry_name TEXT,
    source        VARCHAR(16) NOT NULL DEFAULT 'cnindex',
    revision_id   VARCHAR(36),
    update_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, scheme, level, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_industry_snapshot_ind
    ON industry_member_snapshot (scheme, level, trade_date, industry_code);
