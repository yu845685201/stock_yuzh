"""
review 相关 DAO mixin（每日复盘数据采集技术方案 V1.0 第五章）

- 全部新表 ensure（IF NOT EXISTS，幂等；DDL 同步维护在 doc/init-V3.sql）
- 通用游标表 sync_cursor：get_cursor / set_cursor（日频/周频源增量下界；全量任务走 tmp/manifest）
- 各表 upsert 全部 ON CONFLICT DO UPDATE + update_time = NOW()（幂等重跑安全）
- trade_index_info 迁移：补 up_count/down_count 两列 + (ts_code, trade_date) 唯一索引（若缺）

金额单位：元；成交量单位：股；涨跌幅/占比单位：百分点或小数按列注释。
"""

import json
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


class ReviewDaoMixin:
    """每日复盘数据采集 DAO"""

    # ---------- 建表兜底 ----------

    _REVIEW_TABLE_DDL: Dict[str, str] = {
        'sync_cursor': """
        CREATE TABLE IF NOT EXISTS sync_cursor (
            source_name   VARCHAR(50)  NOT NULL,
            cursor_key    VARCHAR(50)  NOT NULL,
            cursor_value  VARCHAR(200),
            updated_at    TIMESTAMP    DEFAULT NOW(),
            PRIMARY KEY (source_name, cursor_key)
        )
        """,
        'base_adjust_factor': """
        CREATE TABLE IF NOT EXISTS base_adjust_factor (
            id BIGSERIAL PRIMARY KEY,
            ts_code VARCHAR(20) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            qfq_factor NUMERIC(20, 8),
            hfq_factor NUMERIC(20, 8),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'base_financial_report': """
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
        )
        """,
        'base_disclosure_calendar': """
        CREATE TABLE IF NOT EXISTS base_disclosure_calendar (
            id BIGSERIAL PRIMARY KEY,
            ts_code VARCHAR(20) NOT NULL,
            report_period VARCHAR(8) NOT NULL,
            plan_disclosure_date VARCHAR(8),
            actual_disclosure_date VARCHAR(8),
            exchange VARCHAR(10),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'index_valuation_daily': """
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
        )
        """,
        'stock_valuation_daily': """
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
        )
        """,
        'performance_forecast': """
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
        )
        """,
        'announcement_daily': """
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
        )
        """,
        'sector_concept': """
        CREATE TABLE IF NOT EXISTS sector_concept (
            id BIGSERIAL PRIMARY KEY,
            concept_code VARCHAR(30) NOT NULL,
            concept_name VARCHAR(100),
            source VARCHAR(20),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'sector_concept_map': """
        CREATE TABLE IF NOT EXISTS sector_concept_map (
            id BIGSERIAL PRIMARY KEY,
            concept_code VARCHAR(30) NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            in_date VARCHAR(8),
            out_date VARCHAR(8),
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'lhb_stock_daily': """
        CREATE TABLE IF NOT EXISTS lhb_stock_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            stock_name VARCHAR(30),
            reason VARCHAR(200),
            buy_amt NUMERIC(24, 4),
            sell_amt NUMERIC(24, 4),
            net_amt NUMERIC(24, 4),
            sec_tx_amount NUMERIC(24, 4),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'lhb_detail': """
        CREATE TABLE IF NOT EXISTS lhb_detail (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            seat_name VARCHAR(100) NOT NULL,
            side VARCHAR(10) NOT NULL,
            amount NUMERIC(24, 4),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'margin_trade_daily': """
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
        )
        """,
        'macro_indicator_daily': """
        CREATE TABLE IF NOT EXISTS macro_indicator_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            indicator_code VARCHAR(30) NOT NULL,
            value NUMERIC(24, 6),
            unit VARCHAR(20),
            period VARCHAR(20),
            source VARCHAR(30),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'option_daily': """
        CREATE TABLE IF NOT EXISTS option_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            contract_code VARCHAR(20) NOT NULL,
            contract_symbol VARCHAR(40),
            underlying_code VARCHAR(12),
            call_put VARCHAR(4),
            strike NUMERIC(12, 4),
            expiry_date VARCHAR(8),
            iv NUMERIC(12, 6),
            delta NUMERIC(12, 6),
            gamma NUMERIC(12, 6),
            theta NUMERIC(12, 6),
            vega NUMERIC(12, 6),
            rho NUMERIC(12, 6),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'option_underlying_daily': """
        CREATE TABLE IF NOT EXISTS option_underlying_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            underlying_code VARCHAR(12) NOT NULL,
            underlying_name VARCHAR(40),
            contract_count INTEGER,
            total_amount NUMERIC(24, 4),
            total_volume NUMERIC(24, 4),
            call_volume NUMERIC(24, 4),
            put_volume NUMERIC(24, 4),
            put_call_ratio NUMERIC(12, 4),
            oi_total NUMERIC(24, 4),
            oi_call NUMERIC(24, 4),
            oi_put NUMERIC(24, 4),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'option_contract_master': """
        CREATE TABLE IF NOT EXISTS option_contract_master (
            contract_code VARCHAR(20) PRIMARY KEY,
            contract_symbol VARCHAR(40),
            underlying_code VARCHAR(12),
            underlying_name VARCHAR(40),
            call_put VARCHAR(4),
            strike NUMERIC(12, 4),
            expiry_date VARCHAR(8),
            first_seen VARCHAR(8),
            last_seen VARCHAR(8),
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'north_flow_detail_daily': """
        CREATE TABLE IF NOT EXISTS north_flow_detail_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            hold_shares NUMERIC(24, 4),
            hold_market_value NUMERIC(24, 4),
            hold_ratio NUMERIC(12, 6),
            change_shares NUMERIC(24, 4),
            change_amount NUMERIC(24, 4),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'share_unlock_calendar': """
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
        )
        """,
        'overseas_market_daily': """
        CREATE TABLE IF NOT EXISTS overseas_market_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            market_code VARCHAR(30) NOT NULL,
            market_name VARCHAR(50),
            close NUMERIC(20, 4),
            change_pct NUMERIC(14, 4),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'overseas_kline_day': """
        CREATE TABLE IF NOT EXISTS overseas_kline_day (
            id BIGSERIAL PRIMARY KEY,
            market_code VARCHAR(20) NOT NULL,
            trade_date VARCHAR(8) NOT NULL,
            open NUMERIC(20, 4),
            high NUMERIC(20, 4),
            low NUMERIC(20, 4),
            close NUMERIC(20, 4),
            volume NUMERIC(24, 4),
            adj_type VARCHAR(8) DEFAULT 'raw',
            source VARCHAR(16) DEFAULT 'ifzq',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'money_flow_daily': """
        CREATE TABLE IF NOT EXISTS money_flow_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            total_amt NUMERIC(24, 4),
            big_buy_amt NUMERIC(24, 4),
            big_sell_amt NUMERIC(24, 4),
            big_net_amt NUMERIC(24, 4),
            big_net_pct NUMERIC(14, 6),
            super_big_buy_amt NUMERIC(24, 4),
            super_big_sell_amt NUMERIC(24, 4),
            super_big_net_amt NUMERIC(24, 4),
            mid_buy_amt NUMERIC(24, 4),
            mid_sell_amt NUMERIC(24, 4),
            mid_net_amt NUMERIC(24, 4),
            small_buy_amt NUMERIC(24, 4),
            small_sell_amt NUMERIC(24, 4),
            small_net_amt NUMERIC(24, 4),
            stock_count INTEGER,
            calib_note VARCHAR(50) DEFAULT '近似口径:分笔分桶',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'sentiment_metrics_daily': """
        CREATE TABLE IF NOT EXISTS sentiment_metrics_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            stock_count INT,
            up_count INT,
            down_count INT,
            flat_count INT,
            limit_up_count INT,
            limit_down_count INT,
            burst_count INT,
            burst_rate NUMERIC(6, 4),
            max_streak INT,
            ladder JSONB,
            premium_mean NUMERIC(8, 2),
            premium_median NUMERIC(8, 2),
            premium_count INT,
            premium_continued INT,
            promotion JSONB,
            sentiment_stage VARCHAR(16),
            rules_version VARCHAR(16),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'limit_seal_stats_daily': """
        CREATE TABLE IF NOT EXISTS limit_seal_stats_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            ts_code VARCHAR(20) NOT NULL,
            limit_price NUMERIC(12, 4),
            first_seal_time VARCHAR(8),
            last_seal_time VARCHAR(8),
            open_count INT,
            sealed_ratio NUMERIC(6, 4),
            source VARCHAR(20) DEFAULT 'tdx_tick',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'hkex_connect_daily': """
        CREATE TABLE IF NOT EXISTS hkex_connect_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            channel VARCHAR(16) NOT NULL,
            total_turnover NUMERIC(24, 4),
            buy_turnover NUMERIC(24, 4),
            sell_turnover NUMERIC(24, 4),
            etf_turnover NUMERIC(24, 4),
            trade_count BIGINT,
            source VARCHAR(24) DEFAULT 'hkex_dailystat',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'etf_scale_daily': """
        CREATE TABLE IF NOT EXISTS etf_scale_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            exchange VARCHAR(4) NOT NULL,
            fund_code VARCHAR(10) NOT NULL,
            fund_name VARCHAR(64),
            etf_type VARCHAR(20),
            shares_wan NUMERIC(24, 4),
            source VARCHAR(20),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'review_prediction_daily': """
        CREATE TABLE IF NOT EXISTS review_prediction_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            payload JSONB NOT NULL,
            version INT DEFAULT 1,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'review_verification_daily': """
        CREATE TABLE IF NOT EXISTS review_verification_daily (
            id BIGSERIAL PRIMARY KEY,
            trade_date VARCHAR(8) NOT NULL,
            prev_date VARCHAR(8),
            payload JSONB NOT NULL,
            version INT DEFAULT 1,
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        'news_telegraph': """
        CREATE TABLE IF NOT EXISTS news_telegraph (
            id BIGSERIAL PRIMARY KEY,
            news_id VARCHAR(64) NOT NULL,
            publish_time VARCHAR(19),
            trade_date VARCHAR(8),
            title VARCHAR(500),
            content TEXT,
            subject VARCHAR(100),
            importance SMALLINT DEFAULT 0,
            source VARCHAR(20) DEFAULT 'sina_7x24',
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    }

    _REVIEW_UNIQUE_KEYS: Dict[str, str] = {
        'base_adjust_factor': 'UNIQUE (ts_code, trade_date)',
        'base_financial_report': 'UNIQUE (ts_code, stat_date)',
        'base_disclosure_calendar': 'UNIQUE (ts_code, report_period)',
        'index_valuation_daily': 'UNIQUE (index_code, trade_date)',
        'stock_valuation_daily': 'UNIQUE (ts_code, trade_date)',
        'performance_forecast': 'UNIQUE (announcement_id)',
        'announcement_daily': 'UNIQUE (announcement_id)',
        'sector_concept': 'UNIQUE (concept_code)',
        'sector_concept_map': 'UNIQUE (concept_code, ts_code)',
        'lhb_stock_daily': 'UNIQUE (trade_date, ts_code, reason)',
        'lhb_detail': 'UNIQUE (trade_date, ts_code, seat_name, side)',
        'margin_trade_daily': 'UNIQUE (trade_date, ts_code)',
        'macro_indicator_daily': 'UNIQUE (trade_date, indicator_code)',
        'option_daily': 'UNIQUE (trade_date, contract_code)',
        'option_underlying_daily': 'UNIQUE (trade_date, underlying_code)',
        'north_flow_detail_daily': 'UNIQUE (trade_date, ts_code)',
        'share_unlock_calendar': 'UNIQUE (ts_code, unlock_date)',
        'overseas_market_daily': 'UNIQUE (trade_date, market_code)',
        'overseas_kline_day': 'UNIQUE (market_code, trade_date)',
        'money_flow_daily': 'UNIQUE (trade_date, ts_code)',
        'sentiment_metrics_daily': 'UNIQUE (trade_date)',
        'limit_seal_stats_daily': 'UNIQUE (trade_date, ts_code)',
        'hkex_connect_daily': 'UNIQUE (trade_date, channel)',
        'etf_scale_daily': 'UNIQUE (trade_date, exchange, fund_code)',
        'review_prediction_daily': 'UNIQUE (trade_date)',
        'review_verification_daily': 'UNIQUE (trade_date)',
        'news_telegraph': 'UNIQUE (news_id)',
    }

    def ensure_review_tables(self) -> None:
        """全部 review 新表建表兜底 + 业务唯一索引（幂等；DDL 同步维护在 doc/init-V3.sql）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for ddl in self._REVIEW_TABLE_DDL.values():
                    cursor.execute(ddl)
                # 存量库列宽兼容：side 存官网原文（买1/卖1…含排名，方案键修订 2026-09-28
                # ——"机构专用"对应多个不同机构席位，原 (ts_code, seat_name, side) 键会丢行）
                cursor.execute("ALTER TABLE lhb_detail ALTER COLUMN side TYPE VARCHAR(10)")
                # 存量库加列（CREATE IF NOT EXISTS 不会给已存在的表加列，开发项 J）：
                # sec_tx_amount = 个股当日总成交额（沪市 secTxAmount / 深市列表成交金额）
                cursor.execute("ALTER TABLE lhb_stock_daily "
                               "ADD COLUMN IF NOT EXISTS sec_tx_amount NUMERIC(24, 4)")
                # P2 分桶扩列（存量库加列，量价资金方案 V1.1 2026-10-05；存量行 NULL
                # 不回填——历史分笔不可得，calib_note 区分行级口径）
                for _col in ('super_big_buy_amt', 'super_big_sell_amt', 'super_big_net_amt',
                             'mid_buy_amt', 'mid_sell_amt', 'mid_net_amt',
                             'small_buy_amt', 'small_sell_amt', 'small_net_amt'):
                    cursor.execute(f"ALTER TABLE money_flow_daily "
                                   f"ADD COLUMN IF NOT EXISTS {_col} NUMERIC(24, 4)")
                # news_telegraph 存量表补 update_time（首版 DDL 缺列，2026-10-07 修）
                cursor.execute("ALTER TABLE news_telegraph "
                               "ADD COLUMN IF NOT EXISTS update_time TIMESTAMP "
                               "DEFAULT CURRENT_TIMESTAMP")
                # 披露日 COALESCE 回填的查询路径索引（回填实测教训：34.6 万财报行 ×
                # 公告相关子查询，无此索引时单次回填可达小时级/更久）
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_announcement_ts_date "
                               "ON announcement_daily (ts_code, ann_date)")
                for table, key in self._REVIEW_UNIQUE_KEYS.items():
                    conname = f'uk_{table}_review'
                    # ADD CONSTRAINT 无 IF NOT EXISTS：用 DO 块吞掉重复添加异常（幂等）
                    cursor.execute(f"""
                        DO $$
                        BEGIN
                            ALTER TABLE {table} ADD CONSTRAINT {conname} {key};
                        EXCEPTION
                            WHEN duplicate_object THEN NULL;
                            WHEN duplicate_table THEN NULL;
                        END $$;
                    """)
                conn.commit()
        # trade_index_info 迁移单独处理（含可能的重复行清理，日志可追溯）
        self.ensure_trade_index_info_upgrade()

    def ensure_trade_index_info_upgrade(self) -> None:
        """trade_index_info：表缺失则兜底建表（doc/init.sql 口径），补 up_count/down_count
        列与 (ts_code, trade_date) 唯一索引（若缺）

        建唯一索引前如存在 (ts_code, trade_date) 重复行，保留 id 最大的一条（与新数据
        upsert 语义一致），清理动作记录日志。重复行超 100 条视为异常，不自动清理、
        仅告警（提示人工排查，避免误删真实数据）。
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS trade_index_info (
                        id BIGSERIAL PRIMARY KEY,
                        ts_code VARCHAR(20),
                        index_code VARCHAR(20),
                        index_name VARCHAR(20),
                        trade_date VARCHAR(8),
                        open NUMERIC(20, 4),
                        high NUMERIC(20, 4),
                        low NUMERIC(20, 4),
                        close NUMERIC(20, 4),
                        preclose NUMERIC(20, 4),
                        volume NUMERIC(20, 0),
                        amount NUMERIC(20, 4),
                        change_rate NUMERIC(10, 6),
                        create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cursor.execute("""
                    ALTER TABLE trade_index_info ADD COLUMN IF NOT EXISTS up_count INTEGER,
                    ADD COLUMN IF NOT EXISTS down_count INTEGER
                """)
                conn.commit()
                cursor.execute("""
                    SELECT 1 FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND indexname = 'uk_trade_index_info_ts_code_trade_date'
                """)
                if cursor.fetchone():
                    return
                cursor.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT ts_code, trade_date FROM trade_index_info
                        GROUP BY ts_code, trade_date HAVING COUNT(*) > 1
                    ) d
                """)
                dup = cursor.fetchone()[0]
                if dup > 0:
                    if dup > 100:
                        logger.warning(
                            f'trade_index_info 存在 {dup} 组 (ts_code, trade_date) 重复，'
                            f'超自动清理上限，唯一索引未创建，请人工排查')
                        return
                    cursor.execute("""
                        DELETE FROM trade_index_info a USING trade_index_info b
                        WHERE a.ts_code = b.ts_code AND a.trade_date = b.trade_date
                          AND a.id < b.id
                    """)
                    logger.info(f'trade_index_info 清理重复行 {dup} 组（保留每组 id 最大者）')
                cursor.execute("""
                    CREATE UNIQUE INDEX uk_trade_index_info_ts_code_trade_date
                    ON trade_index_info (ts_code, trade_date)
                """)
                conn.commit()
                logger.info('trade_index_info 已补建 (ts_code, trade_date) 唯一索引')

    # ---------- 通用游标 ----------

    def get_cursor(self, source_name: str, cursor_key: str) -> Optional[str]:
        rows = self.execute_query(
            "SELECT cursor_value FROM sync_cursor WHERE source_name = %s AND cursor_key = %s",
            (source_name, cursor_key))
        return rows[0]['cursor_value'] if rows else None

    def set_cursor(self, source_name: str, cursor_key: str, cursor_value: str) -> None:
        self.execute_update("""
            INSERT INTO sync_cursor (source_name, cursor_key, cursor_value, updated_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (source_name, cursor_key)
            DO UPDATE SET cursor_value = EXCLUDED.cursor_value, updated_at = NOW()
        """, (source_name, cursor_key, cursor_value))

    # ---------- 指数日K（trade_index_info，已有表） ----------

    def upsert_index_klines(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO trade_index_info
        (ts_code, index_code, index_name, trade_date, open, high, low, close, preclose,
         volume, amount, change_rate, up_count, down_count)
        VALUES (%(ts_code)s, %(index_code)s, %(index_name)s, %(trade_date)s, %(open)s, %(high)s,
                %(low)s, %(close)s, %(preclose)s, %(volume)s, %(amount)s, %(change_rate)s,
                %(up_count)s, %(down_count)s)
        ON CONFLICT (ts_code, trade_date)
        DO UPDATE SET
            index_code = EXCLUDED.index_code,
            index_name = EXCLUDED.index_name,
            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
            preclose = EXCLUDED.preclose, volume = EXCLUDED.volume, amount = EXCLUDED.amount,
            change_rate = EXCLUDED.change_rate,
            up_count = EXCLUDED.up_count, down_count = EXCLUDED.down_count,
            update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_index_kline_prev_close(self, ts_code: str, before_date: str) -> Optional[float]:
        rows = self.execute_query("""
            SELECT close FROM trade_index_info
            WHERE ts_code = %s AND trade_date < %s AND close IS NOT NULL
            ORDER BY trade_date DESC LIMIT 1
        """, (ts_code, before_date))
        return float(rows[0]['close']) if rows else None

    # ---------- 复权因子 ----------

    def upsert_adjust_factors(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO base_adjust_factor (ts_code, trade_date, qfq_factor, hfq_factor)
        VALUES (%(ts_code)s, %(trade_date)s, %(qfq_factor)s, %(hfq_factor)s)
        ON CONFLICT (ts_code, trade_date)
        DO UPDATE SET qfq_factor = EXCLUDED.qfq_factor, hfq_factor = EXCLUDED.hfq_factor,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    # ---------- 财报指标 ----------

    def upsert_financial_reports(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO base_financial_report
        (ts_code, stat_date, revenue, revenue_yoy, net_profit, net_profit_yoy, roe,
         gross_margin, operate_cashflow, eps, net_assets, disclosure_date)
        VALUES (%(ts_code)s, %(stat_date)s, %(revenue)s, %(revenue_yoy)s, %(net_profit)s,
                %(net_profit_yoy)s, %(roe)s, %(gross_margin)s, %(operate_cashflow)s,
                %(eps)s, %(net_assets)s, %(disclosure_date)s)
        ON CONFLICT (ts_code, stat_date)
        DO UPDATE SET
            revenue = COALESCE(EXCLUDED.revenue, base_financial_report.revenue),
            revenue_yoy = COALESCE(EXCLUDED.revenue_yoy, base_financial_report.revenue_yoy),
            net_profit = COALESCE(EXCLUDED.net_profit, base_financial_report.net_profit),
            net_profit_yoy = COALESCE(EXCLUDED.net_profit_yoy, base_financial_report.net_profit_yoy),
            roe = COALESCE(EXCLUDED.roe, base_financial_report.roe),
            gross_margin = COALESCE(EXCLUDED.gross_margin, base_financial_report.gross_margin),
            operate_cashflow = COALESCE(EXCLUDED.operate_cashflow, base_financial_report.operate_cashflow),
            eps = COALESCE(EXCLUDED.eps, base_financial_report.eps),
            net_assets = COALESCE(EXCLUDED.net_assets, base_financial_report.net_assets),
            disclosure_date = COALESCE(EXCLUDED.disclosure_date, base_financial_report.disclosure_date),
            update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def backfill_financial_disclosure_dates(self) -> int:
        """披露日回填：disclosure_date = COALESCE(公告 ann_date, 官网 actual_disclosure_date)

        独立 UPDATE 步骤（方案 4.3 披露日口径：真实 pubDate 优先）。
        公告匹配：定期报告/业绩快报公告发布于报告期结束后 200 天窗口内（跨期公告安全）；
        官网匹配：base_disclosure_calendar 按 (ts_code, report_period) 精确对齐。
        """
        return self.execute_update("""
            WITH periods AS (
                SELECT id, ts_code,
                       to_date(substring(stat_date from 1 for 4) ||
                               CASE substring(stat_date from 5 for 2)
                                   WHEN '03' THEN '0331' WHEN '06' THEN '0630'
                                   WHEN '09' THEN '0930' ELSE '1231' END,
                               'YYYYMMDD') AS period_end
                FROM base_financial_report
            ),
            backfill AS (
                SELECT p.id,
                       COALESCE(
                           (SELECT MIN(a.ann_date)
                            FROM announcement_daily a
                            WHERE a.ts_code = p.ts_code
                              AND a.ann_type IN ('定期报告', '业绩快报')
                              AND a.ann_date IS NOT NULL
                              AND to_date(a.ann_date, 'YYYYMMDD') > p.period_end
                              AND to_date(a.ann_date, 'YYYYMMDD') <= p.period_end + 200),
                           (SELECT c.actual_disclosure_date
                            FROM base_disclosure_calendar c
                            WHERE c.ts_code = p.ts_code
                              AND c.report_period = to_char(p.period_end, 'YYYYMMDD'))
                       ) AS disclosure_date
                FROM periods p
            )
            UPDATE base_financial_report f
            SET disclosure_date = b.disclosure_date
            FROM backfill b
            WHERE f.id = b.id
              AND f.disclosure_date IS DISTINCT FROM b.disclosure_date
        """)

    def fetch_recent_financial_reports(self, periods: int = 8) -> List[Dict[str, Any]]:
        """每只股票最近 N 期财报（PE(TTM)/PB 自算输入）"""
        return self.execute_query(f"""
            SELECT ts_code, stat_date, net_profit, net_assets FROM (
                SELECT ts_code, stat_date, net_profit, net_assets,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY stat_date DESC) AS rn
                FROM base_financial_report
                WHERE net_profit IS NOT NULL
            ) t WHERE rn <= {int(periods)}
        """)

    def fetch_latest_total_shares(self) -> Dict[str, float]:
        """每只股票最新总股本（base_fundamentals_info 最新披露口径，单位：股）"""
        rows = self.execute_query("""
            SELECT DISTINCT ON (ts_code) ts_code, total_share
            FROM base_fundamentals_info
            WHERE total_share IS NOT NULL
            ORDER BY ts_code, disclosure_date DESC, stat_date DESC
        """)
        return {r['ts_code']: float(r['total_share']) for r in rows}

    def fetch_financial_reports_asof(self, as_of: str, periods: int = 8) -> List[Dict[str, Any]]:
        """每只股票在 as_of 日**已披露**的最近 N 期财报（点时口径，估值历史回填用，开发项 A）。

        过滤 disclosure_date IS NOT NULL AND disclosure_date <= as_of——杜绝"目标日之后
        才披露的财报"流入历史估值（前视偏差，探路 X-6：无此过滤时 as-of 20240628 可算 0 只）；
        ROW_NUMBER 按 stat_date DESC 取前 N（与 fetch_recent_financial_reports 同构）。
        对当日而言 as-of=当日，行为与 fetch_recent_financial_reports 等价（口径统一）。
        """
        return self.execute_query(f"""
            SELECT ts_code, stat_date, net_profit, net_assets FROM (
                SELECT ts_code, stat_date, net_profit, net_assets,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY stat_date DESC) AS rn
                FROM base_financial_report
                WHERE net_profit IS NOT NULL
                  AND disclosure_date IS NOT NULL AND disclosure_date <= %s
            ) t WHERE rn <= {int(periods)}
        """, (as_of,))

    def fetch_financial_reports_from_versions(self, as_of: str,                                              periods: int = 8) -> List[Dict[str, Any]]:
        """版本表口径的估值输入（§7.3 批次 C 接线第三步；与
        fetch_financial_reports_asof 同构返回 ts_code/stat_date/net_profit/net_assets）。

        cutoff = as_of（YYYYMMDD）当日 15:00+08 收盘可知时刻——严格回放：仅
        first_observed_at <= cutoff 的版本参与（"旧可知时点不读新值"）。
        影子/publish 模式共用；publish 前须完成 shadow 差异对比归零。
        """
        cutoff = f'{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]} 15:00:00+08'
        rows = self.fetch_financial_versions_asof(cutoff, limit_periods=periods)
        return [{'ts_code': r['ts_code'], 'stat_date': r['stat_date'],
                 'net_profit': r.get('net_profit'), 'net_assets': r.get('net_assets'),
                 '_sources': r.get('_sources')}
                for r in rows
                if r.get('net_profit') is not None]

    def fetch_total_shares_asof(self, as_of: str) -> Dict[str, float]:
        """每只股票在 as_of 日已披露的最新总股本（disclosure_date <= as_of，开发项 A 回退口径）"""
        rows = self.execute_query("""
            SELECT DISTINCT ON (ts_code) ts_code, total_share
            FROM base_fundamentals_info
            WHERE total_share IS NOT NULL
              AND disclosure_date IS NOT NULL AND disclosure_date <= %s
            ORDER BY ts_code, disclosure_date DESC, stat_date DESC
        """, (as_of,))
        return {r['ts_code']: float(r['total_share']) for r in rows}

    def fetch_valuation_row_count(self, trade_date: str) -> int:
        """stock_valuation_daily 某日行数（估值回填断点续跑判定用，开发项 A）"""
        rows = self.execute_query(
            "SELECT COUNT(*) AS n FROM stock_valuation_daily WHERE trade_date = %s",
            (trade_date,))
        return int(rows[0]['n']) if rows else 0

    def fetch_margin_row_count(self, trade_date: str) -> int:
        """margin_trade_daily 某日行数（两融回填断点续跑判定，开发项 C/X-14：只信查库）"""
        rows = self.execute_query(
            "SELECT COUNT(*) AS n FROM margin_trade_daily WHERE trade_date = %s",
            (trade_date,))
        return int(rows[0]['n']) if rows else 0

    def fetch_margin_row_counts_by_exchange(self, trade_date: str) -> Dict[str, int]:
        """两融某日沪深分行数（限流方案 20261120 §4.2：以目标市场+日期为补采单位，
        已完整分区跳过、单所缺失只补单所；ts_code 前缀 sh./sz. 区分交易所）"""
        rows = self.execute_query(
            "SELECT COUNT(*) FILTER (WHERE ts_code LIKE 'sh.%') AS sse, "
            "COUNT(*) FILTER (WHERE ts_code LIKE 'sz.%') AS szse "
            "FROM margin_trade_daily WHERE trade_date = %s",
            (trade_date,))
        r = rows[0] if rows else {}
        return {'sse': int(r.get('sse') or 0), 'szse': int(r.get('szse') or 0)}

    def fetch_lhb_row_count(self, trade_date: str) -> int:
        """lhb_stock_daily 某日行数（龙虎榜回填断点续跑判定，开发项 C）"""
        rows = self.execute_query(
            "SELECT COUNT(*) AS n FROM lhb_stock_daily WHERE trade_date = %s",
            (trade_date,))
        return int(rows[0]['n']) if rows else 0

    # ---------- 披露日历 ----------

    def upsert_disclosure_calendar(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO base_disclosure_calendar
        (ts_code, report_period, plan_disclosure_date, actual_disclosure_date, exchange)
        VALUES (%(ts_code)s, %(report_period)s, %(plan_disclosure_date)s,
                %(actual_disclosure_date)s, %(exchange)s)
        ON CONFLICT (ts_code, report_period)
        DO UPDATE SET
            plan_disclosure_date = COALESCE(EXCLUDED.plan_disclosure_date, base_disclosure_calendar.plan_disclosure_date),
            actual_disclosure_date = COALESCE(EXCLUDED.actual_disclosure_date, base_disclosure_calendar.actual_disclosure_date),
            exchange = EXCLUDED.exchange,
            update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_disclosure_actual_between(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """[start, end] 内有实际披露记录的清单（THS 财报增量抓取范围）"""
        return self.execute_query("""
            SELECT ts_code, report_period, actual_disclosure_date
            FROM base_disclosure_calendar
            WHERE actual_disclosure_date >= %s AND actual_disclosure_date <= %s
        """, (start_date, end_date))

    # ---------- 指数估值 ----------

    def upsert_index_valuations(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO index_valuation_daily
        (trade_date, index_code, index_name, pe_ttm, pe_static, dividend_yield)
        VALUES (%(trade_date)s, %(index_code)s, %(index_name)s, %(pe_ttm)s, %(pe_static)s,
                %(dividend_yield)s)
        ON CONFLICT (index_code, trade_date)
        DO UPDATE SET index_name = EXCLUDED.index_name,
                      pe_ttm = EXCLUDED.pe_ttm, pe_static = EXCLUDED.pe_static,
                      dividend_yield = EXCLUDED.dividend_yield,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    # ---------- 公告 / 业绩预告 ----------

    def upsert_announcements(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO announcement_daily
        (announcement_id, ann_date, ts_code, stock_name, title, ann_type, ann_url)
        VALUES (%(announcement_id)s, %(ann_date)s, %(ts_code)s, %(stock_name)s,
                %(title)s, %(ann_type)s, %(ann_url)s)
        ON CONFLICT (announcement_id)
        DO UPDATE SET ann_date = EXCLUDED.ann_date, ts_code = EXCLUDED.ts_code,
                      stock_name = EXCLUDED.stock_name, title = EXCLUDED.title,
                      ann_type = EXCLUDED.ann_type, ann_url = EXCLUDED.ann_url,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def upsert_performance_forecasts(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO performance_forecast
        (announcement_id, ts_code, stock_name, ann_date, report_period, forecast_type,
         net_profit_min, net_profit_max, summary, ann_url)
        VALUES (%(announcement_id)s, %(ts_code)s, %(stock_name)s, %(ann_date)s,
                %(report_period)s, %(forecast_type)s, %(net_profit_min)s, %(net_profit_max)s,
                %(summary)s, %(ann_url)s)
        ON CONFLICT (announcement_id)
        DO UPDATE SET ts_code = EXCLUDED.ts_code, stock_name = EXCLUDED.stock_name,
                      ann_date = EXCLUDED.ann_date, report_period = EXCLUDED.report_period,
                      forecast_type = EXCLUDED.forecast_type,
                      net_profit_min = EXCLUDED.net_profit_min,
                      net_profit_max = EXCLUDED.net_profit_max,
                      summary = EXCLUDED.summary, ann_url = EXCLUDED.ann_url,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_announcement_codes_on(self, ann_date: str) -> List[str]:
        """当日公告中含定期报告/业绩快报的股票代码（THS 财报增量目标，方案 4.3 标题匹配口径）"""
        rows = self.execute_query("""
            SELECT DISTINCT ts_code FROM announcement_daily
            WHERE ann_date = %s AND ts_code IS NOT NULL
              AND ann_type IN ('定期报告', '业绩快报')
        """, (ann_date,))
        return [r['ts_code'] for r in rows]

    # ---------- 个股估值 ----------

    def upsert_stock_valuations(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO stock_valuation_daily
        (ts_code, trade_date, total_mv, pe_ttm, pb, insufficient_note)
        VALUES (%(ts_code)s, %(trade_date)s, %(total_mv)s, %(pe_ttm)s, %(pb)s,
                %(insufficient_note)s)
        ON CONFLICT (ts_code, trade_date)
        DO UPDATE SET total_mv = EXCLUDED.total_mv, pe_ttm = EXCLUDED.pe_ttm,
                      pb = EXCLUDED.pb, insufficient_note = EXCLUDED.insufficient_note,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    # ---------- 概念板块 ----------

    def upsert_concepts(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO sector_concept (concept_code, concept_name, source, updated_at)
        VALUES (%(concept_code)s, %(concept_name)s, %(source)s, NOW())
        ON CONFLICT (concept_code)
        DO UPDATE SET concept_name = EXCLUDED.concept_name, source = EXCLUDED.source,
                      updated_at = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def upsert_concept_maps(self, rows: List[Dict[str, Any]]) -> int:
        """成分留痕：新进写 in_date；退出由 manager 比对后将 out_date 置为刷新日"""
        if not rows:
            return 0
        sql = """
        INSERT INTO sector_concept_map (concept_code, ts_code, in_date, out_date)
        VALUES (%(concept_code)s, %(ts_code)s, %(in_date)s, %(out_date)s)
        ON CONFLICT (concept_code, ts_code)
        DO UPDATE SET out_date = EXCLUDED.out_date, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def upsert_concept_maps_explicit(self, rows: List[Dict[str, Any]]) -> int:
        """成分显式时点 upsert（G 项改良）：in_date/out_date 全部以来源文件为准覆盖。

        与快照 diff 语义（upsert_concept_maps）不同：SwClass 自带起始/结束日期，
        即官方时点成分——退出再回归的股票 in_date 也应刷新为最新起始日。
        """
        if not rows:
            return 0
        sql = """
        INSERT INTO sector_concept_map (concept_code, ts_code, in_date, out_date)
        VALUES (%(concept_code)s, %(ts_code)s, %(in_date)s, %(out_date)s)
        ON CONFLICT (concept_code, ts_code)
        DO UPDATE SET in_date = EXCLUDED.in_date, out_date = EXCLUDED.out_date,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_concept_active_codes(self, source: Optional[str] = None) -> Dict[str, List[str]]:
        """当前在成分内的 concept_code -> [ts_code]（out_date IS NULL）

        source：按 sector_concept.source 过滤口径（sina_hy=行业/sina_gn=概念）；
        None 时不过滤（历史行为，行业与概念混算）。
        """
        if source:
            rows = self.execute_query("""
                SELECT m.concept_code, m.ts_code
                FROM sector_concept_map m
                JOIN sector_concept c ON c.concept_code = m.concept_code
                WHERE m.out_date IS NULL AND c.source = %s
            """, (source,))
        else:
            rows = self.execute_query("""
                SELECT concept_code, ts_code FROM sector_concept_map
                WHERE out_date IS NULL
            """)
        result: Dict[str, List[str]] = {}
        for r in rows:
            result.setdefault(r['concept_code'], []).append(r['ts_code'])
        return result

    # ---------- 龙虎榜 ----------

    def upsert_lhb_stocks(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO lhb_stock_daily
        (trade_date, ts_code, stock_name, reason, buy_amt, sell_amt, net_amt, sec_tx_amount)
        VALUES (%(trade_date)s, %(ts_code)s, %(stock_name)s, %(reason)s, %(buy_amt)s,
                %(sell_amt)s, %(net_amt)s, %(sec_tx_amount)s)
        ON CONFLICT (trade_date, ts_code, reason)
        DO UPDATE SET stock_name = EXCLUDED.stock_name, buy_amt = EXCLUDED.buy_amt,
                      sell_amt = EXCLUDED.sell_amt, net_amt = EXCLUDED.net_amt,
                      sec_tx_amount = EXCLUDED.sec_tx_amount,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def delete_sse_lhb_legacy(self, trade_date: str) -> int:
        """删除沪市旧格式龙虎榜行（X-1 修复重抓配套，X-18）。

        旧接口把 reason 存为 '披露原因代码{N}'；新映射用官网文案（如'涨幅偏离'）。
        唯一键 (trade_date, ts_code, reason) 下同股旧新两行会并存致 D2 同股重复上榜，
        重抓日期须先删后插。仅删沪市旧格式行（深市 reason 为官网原文，不受影响）。
        """
        return self.execute_update(
            "DELETE FROM lhb_stock_daily WHERE trade_date = %s "
            "AND ts_code LIKE 'sh.%%' AND reason LIKE '披露原因代码%%'",
            (trade_date,))

    def upsert_lhb_details(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO lhb_detail
        (trade_date, ts_code, seat_name, side, amount)
        VALUES (%(trade_date)s, %(ts_code)s, %(seat_name)s, %(side)s, %(amount)s)
        ON CONFLICT (trade_date, ts_code, seat_name, side)
        DO UPDATE SET amount = EXCLUDED.amount, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    # ---------- 龙虎榜事件层（盘后采集数据源整合详细技术方案 V1.0 §5.2 批次 B） ----------

    _LHB_EVENT_DDL = (
        "CREATE TABLE IF NOT EXISTS lhb_event ("
        " event_id VARCHAR(64) PRIMARY KEY,"
        " trade_date VARCHAR(8) NOT NULL,"
        " exchange VARCHAR(16) NOT NULL,"
        " board VARCHAR(16) NOT NULL,"
        " ts_code VARCHAR(12) NOT NULL,"
        " security_type VARCHAR(16),"
        " reason_code VARCHAR(64) NOT NULL,"
        " reason_text TEXT,"
        " period_start VARCHAR(8),"
        " period_end VARCHAR(8),"
        " sec_tx_amount NUMERIC(24,4),"
        " buy_amt NUMERIC(24,4),"
        " sell_amt NUMERIC(24,4),"
        " net_amt NUMERIC(24,4),"
        " source_keys JSONB,"
        " revision_id VARCHAR(36),"
        " update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        "CREATE INDEX IF NOT EXISTS idx_lhb_event_day ON lhb_event (trade_date, ts_code)",
        "CREATE INDEX IF NOT EXISTS idx_lhb_event_reason "
        "ON lhb_event (trade_date, exchange, board, reason_code)",
        "CREATE TABLE IF NOT EXISTS lhb_event_seat ("
        " event_id VARCHAR(64) NOT NULL REFERENCES lhb_event(event_id),"
        " side CHAR(1) NOT NULL CHECK (side IN ('B', 'S')),"
        " seat_rank SMALLINT NOT NULL CHECK (seat_rank > 0),"
        " seat_name TEXT NOT NULL,"
        " buy_amount NUMERIC(24,4),"
        " sell_amount NUMERIC(24,4),"
        " amount_unit VARCHAR(8) NOT NULL DEFAULT 'yuan',"
        " source VARCHAR(24),"
        " PRIMARY KEY (event_id, side, seat_rank))",
        "CREATE INDEX IF NOT EXISTS idx_lhb_event_seat_name ON lhb_event_seat (seat_name)",
    )

    def ensure_lhb_event_tables(self) -> None:
        """龙虎榜事件/席位层建表兜底（幂等；DDL 同步维护在
        doc/migrations/20261126_source_integration_batch_b_up.sql）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for ddl in self._LHB_EVENT_DDL:
                    cursor.execute(ddl)
            conn.commit()
        logger.info('龙虎榜事件层 ensure 完成（lhb_event/lhb_event_seat）')

    def replace_lhb_events_tx(self, conn: Any, events: List[Dict[str, Any]],
                              trade_date: str) -> int:
        """事件+席位同事务整日替换（§5.2：按同一事件事务替换；先删该日全部旧
        事件与席位再插入——披露原因或区间更正导致身份变化时，旧事件随本轮
        整体撤下）。调用方持有 conn 且负责 commit/rollback。"""
        import json as _json

        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM lhb_event_seat WHERE event_id IN "
                           "(SELECT event_id FROM lhb_event WHERE trade_date = %s)",
                           (trade_date,))
            cursor.execute("DELETE FROM lhb_event WHERE trade_date = %s", (trade_date,))
            for ev in events or []:
                ev_row = {**ev}
                ev_row.setdefault('revision_id', None)
                ev_row.setdefault('source_keys', {})
                cursor.execute("""
                    INSERT INTO lhb_event
                    (event_id, trade_date, exchange, board, ts_code, security_type,
                     reason_code, reason_text, period_start, period_end,
                     sec_tx_amount, buy_amt, sell_amt, net_amt, source_keys, revision_id)
                    VALUES (%(event_id)s, %(trade_date)s, %(exchange)s, %(board)s,
                            %(ts_code)s, %(security_type)s, %(reason_code)s,
                            %(reason_text)s, %(period_start)s, %(period_end)s,
                            %(sec_tx_amount)s, %(buy_amt)s, %(sell_amt)s,
                            %(net_amt)s, %(source_keys)s, %(revision_id)s)
                """, {**ev_row,
                      'source_keys': _json.dumps(ev_row.get('source_keys') or {},
                                                 ensure_ascii=False, default=str)})
                for seat in ev.get('seats') or []:
                    seat_row = {**seat, 'event_id': ev['event_id']}
                    seat_row.setdefault('amount_unit', 'yuan')
                    seat_row.setdefault('source', None)
                    cursor.execute("""
                        INSERT INTO lhb_event_seat
                        (event_id, side, seat_rank, seat_name, buy_amount,
                         sell_amount, amount_unit, source)
                        VALUES (%(event_id)s, %(side)s, %(seat_rank)s, %(seat_name)s,
                                %(buy_amount)s, %(sell_amount)s, %(amount_unit)s,
                                %(source)s)
                        ON CONFLICT (event_id, side, seat_rank)
                        DO UPDATE SET seat_name = EXCLUDED.seat_name,
                            buy_amount = EXCLUDED.buy_amount,
                            sell_amount = EXCLUDED.sell_amount,
                            source = EXCLUDED.source
                    """, seat_row)
        return len(events or [])

    def fetch_lhb_events_day(self, trade_date: str,
                             exchange: Optional[str] = None) -> List[Dict[str, Any]]:
        """某日事件清单（读端'逐事件榜单'查询；seat 行内嵌返回）"""
        rows = self.execute_query(
            "SELECT * FROM lhb_event WHERE trade_date = %s "
            + ("AND exchange = %s " if exchange else "")
            + "ORDER BY ts_code, reason_code",
            (trade_date, exchange) if exchange else (trade_date,))
        out = [dict(r) for r in rows]
        if out:
            seat_rows = self.execute_query(
                "SELECT s.* FROM lhb_event_seat s JOIN lhb_event e "
                "ON e.event_id = s.event_id WHERE e.trade_date = %s "
                "ORDER BY s.event_id, s.side, s.seat_rank", (trade_date,))
            by_event: Dict[str, List[Dict[str, Any]]] = {}
            for s in seat_rows:
                by_event.setdefault(s['event_id'], []).append(dict(s))
            for ev in out:
                ev['seats'] = by_event.get(ev['event_id'], [])
        return out

    # ---------- 国证行业日期快照（§6 批次 B） ----------

    _INDUSTRY_SNAPSHOT_DDL = (
        "CREATE TABLE IF NOT EXISTS industry_member_snapshot ("
        " trade_date VARCHAR(8) NOT NULL,"
        " scheme VARCHAR(24) NOT NULL,"
        " level SMALLINT NOT NULL,"
        " ts_code VARCHAR(12) NOT NULL,"
        " industry_code VARCHAR(32),"
        " industry_name TEXT,"
        " source VARCHAR(16) NOT NULL DEFAULT 'cnindex',"
        " revision_id VARCHAR(36),"
        " update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        " PRIMARY KEY (trade_date, scheme, level, ts_code))",
        "CREATE INDEX IF NOT EXISTS idx_industry_snapshot_ind "
        "ON industry_member_snapshot (scheme, level, trade_date, industry_code)",
    )

    def ensure_industry_snapshot_tables(self) -> None:
        """国证行业快照建表兜底（幂等；DDL 同步维护在
        doc/migrations/20261126_source_integration_batch_b_up.sql）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for ddl in self._INDUSTRY_SNAPSHOT_DDL:
                    cursor.execute(ddl)
            conn.commit()
        logger.info('国证行业快照 ensure 完成（industry_member_snapshot）')

    def replace_industry_snapshot_tx(self, conn: Any, trade_date: str, scheme: str,
                                     level: int, rows: List[Dict[str, Any]]) -> int:
        """整日整套快照事务替换（§6：11 个行业页级对账全过后一事务发布）。
        回填历史日只写对应快照行（主键含 trade_date，不影响其他日期）。"""
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM industry_member_snapshot "
                           "WHERE trade_date = %s AND scheme = %s AND level = %s",
                           (trade_date, scheme, level))
            for r in rows or []:
                cursor.execute("""
                    INSERT INTO industry_member_snapshot
                    (trade_date, scheme, level, ts_code, industry_code,
                     industry_name, source, revision_id)
                    VALUES (%(trade_date)s, %(scheme)s, %(level)s, %(ts_code)s,
                            %(industry_code)s, %(industry_name)s, %(source)s,
                            %(revision_id)s)
                    ON CONFLICT (trade_date, scheme, level, ts_code)
                    DO UPDATE SET industry_code = EXCLUDED.industry_code,
                        industry_name = EXCLUDED.industry_name,
                        revision_id = EXCLUDED.revision_id, update_time = NOW()
                """, r)
        return len(rows or [])

    # ---------- 财报版本行（§7.3 批次 C 接线） ----------

    _FINANCIAL_VERSION_DDL = (
        "CREATE TABLE IF NOT EXISTS financial_report_version ("
        " version_id VARCHAR(36) PRIMARY KEY,"
        " ts_code VARCHAR(12) NOT NULL,"
        " stat_date VARCHAR(8) NOT NULL,"
        " announcement_id VARCHAR(64),"
        " report_kind VARCHAR(16),"
        " source VARCHAR(24) NOT NULL,"
        " source_version VARCHAR(128) NOT NULL,"
        " published_at TIMESTAMPTZ,"
        " published_precision VARCHAR(8),"
        " first_observed_at TIMESTAMPTZ,"
        " availability_basis VARCHAR(32),"
        " supersedes_id VARCHAR(36),"
        " content_hash VARCHAR(64),"
        " revision_id VARCHAR(36),"
        " field_provenance JSONB,"
        " net_profit NUMERIC(24,4),"
        " operate_cashflow NUMERIC(24,4),"
        " net_assets NUMERIC(24,4),"
        " revenue NUMERIC(24,4),"
        " revenue_yoy NUMERIC(16,4),"
        " net_profit_yoy NUMERIC(16,4),"
        " eps NUMERIC(16,4),"
        " roe NUMERIC(16,4),"
        " gross_margin NUMERIC(16,4),"
        " update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
        " UNIQUE (ts_code, stat_date, source, source_version, content_hash))",
        "CREATE INDEX IF NOT EXISTS idx_fin_ver_asof "
        "ON financial_report_version (ts_code, stat_date, published_at, first_observed_at)",
    )

    def ensure_financial_version_tables(self) -> None:
        """财报版本表建表兜底（幂等；DDL 同步维护在
        doc/migrations/20261126_source_integration_batch_c_up.sql）"""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for ddl in self._FINANCIAL_VERSION_DDL:
                    cursor.execute(ddl)
            conn.commit()
        logger.info('财报版本表 ensure 完成（financial_report_version）')

    _FIN_VER_VALUE_COLS = ('net_profit', 'operate_cashflow', 'net_assets', 'revenue',
                           'revenue_yoy', 'net_profit_yoy', 'eps', 'roe', 'gross_margin')

    def upsert_financial_report_versions(self, rows: List[Dict[str, Any]]) -> int:
        """版本行 upsert（唯一键 ts_code+stat_date+source+source_version+content_hash
        天然去重——同包/同公告重跑幂等；数值列 COALESCE 保留已写入值）。"""
        if not rows:
            return 0
        val_cols = self._FIN_VER_VALUE_COLS
        set_clause = ', '.join(
            f'{c} = COALESCE(EXCLUDED.{c}, financial_report_version.{c})' for c in val_cols)
        col_list = ('version_id, ts_code, stat_date, announcement_id, report_kind, source,'
                    ' source_version, published_at, published_precision, first_observed_at,'
                    ' availability_basis, content_hash, field_provenance, '
                    + ', '.join(val_cols))
        placeholders = ('%(version_id)s, %(ts_code)s, %(stat_date)s, %(announcement_id)s,'
                        ' %(report_kind)s, %(source)s, %(source_version)s,'
                        ' %(published_at)s, %(published_precision)s, %(first_observed_at)s,'
                        ' %(availability_basis)s, %(content_hash)s, %(field_provenance)s, '
                        + ', '.join(f'%({c})s' for c in val_cols))
        sql = f"""
        INSERT INTO financial_report_version
        ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (ts_code, stat_date, source, source_version, content_hash)
        DO UPDATE SET {set_clause},
            published_at = COALESCE(EXCLUDED.published_at, financial_report_version.published_at),
            first_observed_at = COALESCE(EXCLUDED.first_observed_at,
                                         financial_report_version.first_observed_at),
            field_provenance = EXCLUDED.field_provenance, update_time = NOW()
        """
        import json as _json
        prepared = []
        for r in rows:
            item = {**r}
            fp = item.get('field_provenance')
            item['field_provenance'] = (_json.dumps(fp, ensure_ascii=False, default=str)
                                        if fp else None)
            item.setdefault('announcement_id', None)
            item.setdefault('report_kind', None)
            item.setdefault('published_precision', None)
            item.setdefault('availability_basis', None)
            item.setdefault('content_hash', None)
            for c in val_cols:
                item.setdefault(c, None)
            prepared.append(item)
        return self.execute_batch(sql, prepared)

    def fetch_financial_versions_asof(self, cutoff_at: str,
                                      codes: Optional[List[str]] = None,
                                      periods: Optional[List[str]] = None,
                                      limit_periods: int = 8) -> List[Dict[str, Any]]:
        """截止时刻版本选读（§7.3：先按截止过滤 → 每股每期选一个完整可用版本 →
        截取最近 N 个报告期）。

        选版规则（本实现）：
        - 只有 first_observed_at <= cutoff_at 的版本参与（严格回放红线）；
        - 同股同期同 source 取 first_observed_at 最新的一行；
        - 跨 source 字段权威次序：数值字段优先 tdx_gpcw（有值），公告版本行
          提供披露时点；两源合并为每股每期一行（_asof_source 标注来源组合）；
        - 每股每期一行后按 stat_date 降序截 limit_periods。
        """
        cond_codes = ''
        params: List[Any] = [cutoff_at]
        if codes:
            cond_codes = 'AND ts_code = ANY(%s)'
            params.append(codes)
        if periods:
            cond_codes += ' AND stat_date = ANY(%s)'
            params.append(periods)
        rows = self.execute_query(f"""
            SELECT DISTINCT ON (ts_code, stat_date, source)
                   ts_code, stat_date, source, source_version, announcement_id,
                   report_kind, published_at, published_precision, first_observed_at,
                   availability_basis, content_hash, field_provenance,
                   net_profit, operate_cashflow, net_assets, revenue,
                   revenue_yoy, net_profit_yoy, eps, roe, gross_margin
            FROM financial_report_version
            WHERE first_observed_at <= %s {cond_codes}
            ORDER BY ts_code, stat_date, source, first_observed_at DESC
        """, tuple(params))
        by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for r in rows:
            key = (r['ts_code'], r['stat_date'])
            cur = by_key.get(key)
            if cur is None:
                merged = {**r, '_sources': [r['source']]}
                by_key[key] = merged
                continue
            # 字段权威合并：数值列 gpcw 有值优先（cur 若已是 gpcw 则保持）
            merged = cur
            if r['source'] == 'tdx_gpcw' and cur.get('_sources') == ['announcement']:
                for c in self._FIN_VER_VALUE_COLS:
                    if r.get(c) is not None:
                        merged[c] = r[c]
                merged['published_at'] = merged.get('published_at') or r.get('published_at')
                merged['_sources'] = ['announcement', 'tdx_gpcw']
            elif r['source'] == 'announcement' and 'announcement' not in merged['_sources']:
                merged['published_at'] = merged.get('published_at') or r.get('published_at')
                merged['announcement_id'] = merged.get('announcement_id') or r.get('announcement_id')
                merged['_sources'].append('announcement')
        per_code: Dict[str, List[Dict[str, Any]]] = {}
        for (ts, _sd), v in by_key.items():
            per_code.setdefault(ts, []).append(v)
        out: List[Dict[str, Any]] = []
        for ts, items in per_code.items():
            items.sort(key=lambda x: x['stat_date'], reverse=True)
            out.extend(items[:limit_periods])
        return out

    # ---------- 兼容因子候选（§8 批次 C 接线） ----------

    def upsert_adjust_factor_candidates(self, rows: List[Dict[str, Any]]) -> int:
        """候选行 upsert（主键 series_version+ts_code+trade_date；不动正式键
        base_adjust_factor——§8 只从已验收序列投影）"""
        if not rows:
            return 0
        sql = """
        INSERT INTO adjust_factor_candidate
        (series_version, ts_code, trade_date, qfq_factor, hfq_factor,
         reference_raw_close, anchor_date, algo_version, based_event_version,
         check_result, check_detail)
        VALUES (%(series_version)s, %(ts_code)s, %(trade_date)s, %(qfq_factor)s,
                %(hfq_factor)s, %(reference_raw_close)s, %(anchor_date)s,
                %(algo_version)s, %(based_event_version)s, %(check_result)s,
                %(check_detail)s)
        ON CONFLICT (series_version, ts_code, trade_date)
        DO UPDATE SET qfq_factor = EXCLUDED.qfq_factor,
            hfq_factor = EXCLUDED.hfq_factor,
            reference_raw_close = EXCLUDED.reference_raw_close,
            algo_version = EXCLUDED.algo_version,
            based_event_version = EXCLUDED.based_event_version,
            check_detail = EXCLUDED.check_detail, update_time = NOW()
        """
        import json as _json
        prepared = []
        for r in rows:
            item = {**r}
            cd = item.get('check_detail')
            item['check_detail'] = (_json.dumps(cd, ensure_ascii=False, default=str)
                                    if cd else None)
            item.setdefault('check_result', None)
            prepared.append(item)
        return self.execute_batch(sql, prepared)

    def upsert_stock_action_events(self, rows: List[Dict[str, Any]]) -> int:
        """权益事件 upsert（§8：source+source_version+ts_code+effective_date+event_key
        唯一；同版本重跑/未变化包天然去重）"""
        if not rows:
            return 0
        sql = """
        INSERT INTO stock_action_event_version
        (source, source_version, ts_code, effective_date, event_key, category,
         raw_event_type, cash_dividend, cash_reference, bonus_ratio,
         rights_issue_ratio, raw_unit, observed_at)
        VALUES (%(source)s, %(source_version)s, %(ts_code)s, %(effective_date)s,
                %(event_key)s, %(category)s, %(raw_event_type)s, %(cash_dividend)s,
                %(cash_reference)s, %(bonus_ratio)s, %(rights_issue_ratio)s,
                %(raw_unit)s, %(observed_at)s)
        ON CONFLICT (source, source_version, ts_code, effective_date, event_key)
        DO NOTHING
        """
        return self.execute_batch(sql, rows)

    # ---------- 两融 ----------

    def upsert_hkex_connect(self, rows: List[Dict[str, Any]]) -> int:
        """互联互通日度（北向总额+南向净额）upsert（P3，量价资金方案 V1.1）"""
        if not rows:
            return 0
        sql = """
        INSERT INTO hkex_connect_daily
        (trade_date, channel, total_turnover, buy_turnover, sell_turnover,
         etf_turnover, trade_count, source)
        VALUES (%(trade_date)s, %(channel)s, %(total_turnover)s, %(buy_turnover)s,
                %(sell_turnover)s, %(etf_turnover)s, %(trade_count)s, %(source)s)
        ON CONFLICT (trade_date, channel)
        DO UPDATE SET total_turnover = EXCLUDED.total_turnover,
                      buy_turnover = EXCLUDED.buy_turnover,
                      sell_turnover = EXCLUDED.sell_turnover,
                      etf_turnover = EXCLUDED.etf_turnover,
                      trade_count = EXCLUDED.trade_count,
                      source = EXCLUDED.source, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def upsert_etf_scale(self, rows: List[Dict[str, Any]]) -> int:
        """ETF 份额日度（沪深两所，单位万份）upsert（P3）"""
        if not rows:
            return 0
        sql = """
        INSERT INTO etf_scale_daily
        (trade_date, exchange, fund_code, fund_name, etf_type, shares_wan, source)
        VALUES (%(trade_date)s, %(exchange)s, %(fund_code)s, %(fund_name)s,
                %(etf_type)s, %(shares_wan)s, %(source)s)
        ON CONFLICT (trade_date, exchange, fund_code)
        DO UPDATE SET fund_name = EXCLUDED.fund_name, etf_type = EXCLUDED.etf_type,
                      shares_wan = EXCLUDED.shares_wan, source = EXCLUDED.source
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def upsert_margin_trades(self, rows: List[Dict[str, Any]]) -> int:
        """两融明细 upsert（§4.1：NULL 不清空已验证值——官网缺沪市 rqye 时保留
        巨潮已验证金额，避免交替重跑把值清空；显式新值/正式更正常规覆盖）。"""
        if not rows:
            return 0
        sql = """
        INSERT INTO margin_trade_daily
        (trade_date, ts_code, rzye, rzmre, rzche, rqye)
        VALUES (%(trade_date)s, %(ts_code)s, %(rzye)s, %(rzmre)s, %(rzche)s, %(rqye)s)
        ON CONFLICT (trade_date, ts_code)
        DO UPDATE SET rzye = COALESCE(EXCLUDED.rzye, margin_trade_daily.rzye),
                      rzmre = COALESCE(EXCLUDED.rzmre, margin_trade_daily.rzmre),
                      rzche = COALESCE(EXCLUDED.rzche, margin_trade_daily.rzche),
                      rqye  = COALESCE(EXCLUDED.rqye,  margin_trade_daily.rqye),
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    # ---------- 宏观指标（第三阶段-1） ----------

    def upsert_macro_indicators(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO macro_indicator_daily
        (trade_date, indicator_code, value, unit, period, source)
        VALUES (%(trade_date)s, %(indicator_code)s, %(value)s, %(unit)s,
                %(period)s, %(source)s)
        ON CONFLICT (trade_date, indicator_code)
        DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit,
                      period = EXCLUDED.period, source = EXCLUDED.source,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_macro_indicator_dates(self, start_date: str,
                                    end_date: str) -> Dict[str, List[str]]:
        """区间内已有指标日期 {indicator_code: [trade_date,...]}（断点续跑判定）"""
        rows = self.execute_query(
            "SELECT DISTINCT indicator_code, trade_date FROM macro_indicator_daily "
            "WHERE trade_date BETWEEN %s AND %s ORDER BY indicator_code, trade_date",
            (start_date, end_date))
        out: Dict[str, List[str]] = {}
        for r in rows:   # RealDictCursor：dict 行
            out.setdefault(str(r['indicator_code']), []).append(str(r['trade_date']))
        return out

    def fetch_macro_latest(self, indicator_code: str) -> Optional[Dict[str, Any]]:
        rows = self.execute_query(
            "SELECT trade_date, value, unit FROM macro_indicator_daily "
            "WHERE indicator_code = %s ORDER BY trade_date DESC LIMIT 1",
            (indicator_code,))
        if not rows:
            return None
        r = rows[0]
        return {'trade_date': str(r['trade_date']), 'value': r['value'],
                'unit': r['unit']}

    def fetch_macro_indicators_as_of(self, trade_date: str) -> List[Dict[str, Any]]:
        """某日全部指标行（报告层宏观锚消费）"""
        rows = self.execute_query(
            "SELECT indicator_code, value, unit, period FROM macro_indicator_daily "
            "WHERE trade_date = %s ORDER BY indicator_code", (trade_date,))
        return [{'indicator_code': str(r['indicator_code']), 'value': r['value'],
                 'unit': r['unit'], 'period': r['period']} for r in rows]

    # ---------- ETF 期权（第三阶段-2） ----------

    def upsert_option_daily(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO option_daily
        (trade_date, contract_code, contract_symbol, underlying_code, call_put,
         strike, expiry_date, iv, delta, gamma, theta, vega, rho)
        VALUES (%(trade_date)s, %(contract_code)s, %(contract_symbol)s,
                %(underlying_code)s, %(call_put)s, %(strike)s, %(expiry_date)s,
                %(iv)s, %(delta)s, %(gamma)s, %(theta)s, %(vega)s, %(rho)s)
        ON CONFLICT (trade_date, contract_code)
        DO UPDATE SET iv = EXCLUDED.iv, delta = EXCLUDED.delta,
                      gamma = EXCLUDED.gamma, theta = EXCLUDED.theta,
                      vega = EXCLUDED.vega, rho = EXCLUDED.rho,
                      contract_symbol = EXCLUDED.contract_symbol,
                      underlying_code = EXCLUDED.underlying_code,
                      call_put = EXCLUDED.call_put, strike = EXCLUDED.strike,
                      expiry_date = EXCLUDED.expiry_date, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def upsert_option_underlying_daily(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO option_underlying_daily
        (trade_date, underlying_code, underlying_name, contract_count, total_amount,
         total_volume, call_volume, put_volume, put_call_ratio, oi_total, oi_call,
         oi_put)
        VALUES (%(trade_date)s, %(underlying_code)s, %(underlying_name)s,
                %(contract_count)s, %(total_amount)s, %(total_volume)s,
                %(call_volume)s, %(put_volume)s, %(put_call_ratio)s,
                %(oi_total)s, %(oi_call)s, %(oi_put)s)
        ON CONFLICT (trade_date, underlying_code)
        DO UPDATE SET underlying_name = EXCLUDED.underlying_name,
                      contract_count = EXCLUDED.contract_count,
                      total_amount = EXCLUDED.total_amount,
                      total_volume = EXCLUDED.total_volume,
                      call_volume = EXCLUDED.call_volume,
                      put_volume = EXCLUDED.put_volume,
                      put_call_ratio = EXCLUDED.put_call_ratio,
                      oi_total = EXCLUDED.oi_total, oi_call = EXCLUDED.oi_call,
                      oi_put = EXCLUDED.oi_put, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def upsert_option_contracts(self, rows: List[Dict[str, Any]]) -> int:
        """合约主数据 upsert（first_seen 保首见日，last_seen 滚动刷新）"""
        if not rows:
            return 0
        sql = """
        INSERT INTO option_contract_master
        (contract_code, contract_symbol, underlying_code, underlying_name,
         call_put, strike, expiry_date, first_seen, last_seen)
        VALUES (%(contract_code)s, %(contract_symbol)s, %(underlying_code)s,
                %(underlying_name)s, %(call_put)s, %(strike)s, %(expiry_date)s,
                %(first_seen)s, %(last_seen)s)
        ON CONFLICT (contract_code)
        DO UPDATE SET contract_symbol = EXCLUDED.contract_symbol,
                      underlying_code = EXCLUDED.underlying_code,
                      underlying_name = EXCLUDED.underlying_name,
                      call_put = EXCLUDED.call_put, strike = EXCLUDED.strike,
                      expiry_date = EXCLUDED.expiry_date,
                      last_seen = EXCLUDED.last_seen, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_option_row_count(self, trade_date: str) -> int:
        rows = self.execute_query(
            "SELECT COUNT(*) AS n FROM option_daily WHERE trade_date = %s",
            (trade_date,))
        return int(rows[0]['n']) if rows else 0

    def fetch_option_stats_row_count(self, trade_date: str) -> int:
        """option_underlying_daily 某日行数（§4.2：期权按缺失子表补齐的 stats 断点）"""
        rows = self.execute_query(
            "SELECT COUNT(*) AS n FROM option_underlying_daily WHERE trade_date = %s",
            (trade_date,))
        return int(rows[0]['n']) if rows else 0

    def fetch_option_underlying_as_of(self, trade_date: str) -> List[Dict[str, Any]]:
        rows = self.execute_query(
            "SELECT underlying_code, underlying_name, contract_count, total_amount, "
            "total_volume, call_volume, put_volume, put_call_ratio, oi_total "
            "FROM option_underlying_daily WHERE trade_date = %s "
            "ORDER BY total_amount DESC", (trade_date,))
        return [{'underlying_code': str(r['underlying_code']),
                 'underlying_name': r['underlying_name'],
                 'contract_count': r['contract_count'],
                 'total_amount': r['total_amount'],
                 'total_volume': r['total_volume'],
                 'call_volume': r['call_volume'], 'put_volume': r['put_volume'],
                 'put_call_ratio': r['put_call_ratio'],
                 'oi_total': r['oi_total']} for r in rows]

    # ---------- 北向个股明细（第三阶段-3） ----------

    def upsert_north_flow_detail(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO north_flow_detail_daily
        (trade_date, ts_code, hold_shares, hold_market_value, hold_ratio,
         change_shares, change_amount)
        VALUES (%(trade_date)s, %(ts_code)s, %(hold_shares)s,
                %(hold_market_value)s, %(hold_ratio)s, %(change_shares)s,
                %(change_amount)s)
        ON CONFLICT (trade_date, ts_code)
        DO UPDATE SET hold_shares = EXCLUDED.hold_shares,
                      hold_market_value = EXCLUDED.hold_market_value,
                      hold_ratio = EXCLUDED.hold_ratio,
                      change_shares = EXCLUDED.change_shares,
                      change_amount = EXCLUDED.change_amount,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_north_flow_detail_dates(self, start_date: str,
                                      end_date: str) -> List[str]:
        rows = self.execute_query(
            "SELECT DISTINCT trade_date FROM north_flow_detail_daily "
            "WHERE trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (start_date, end_date))
        return [str(r['trade_date']) for r in rows]

    def fetch_north_flow_detail_max_date(self) -> Optional[str]:
        rows = self.execute_query(
            "SELECT MAX(trade_date) AS d FROM north_flow_detail_daily")
        return str(rows[0]['d']) if rows and rows[0]['d'] else None

    def fetch_north_flow_detail_top(self, trade_date: str,
                                    limit: int = 10) -> List[Dict[str, Any]]:
        """某日北向持股市值 Top N（报告层活跃度锚）"""
        rows = self.execute_query(
            "SELECT ts_code, hold_shares, hold_market_value, hold_ratio "
            "FROM north_flow_detail_daily WHERE trade_date = %s "
            "ORDER BY hold_market_value DESC NULLS LAST LIMIT %s",
            (trade_date, limit))
        return [{'ts_code': str(r['ts_code']), 'hold_shares': r['hold_shares'],
                 'hold_market_value': r['hold_market_value'],
                 'hold_ratio': r['hold_ratio']} for r in rows]

    def fetch_north_flow_detail_date_count(self) -> int:
        rows = self.execute_query(
            "SELECT COUNT(DISTINCT trade_date) AS n FROM north_flow_detail_daily")
        return int(rows[0]['n']) if rows else 0

    # ---------- 解禁 ----------

    def upsert_share_unlocks(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO share_unlock_calendar
        (ts_code, stock_name, unlock_date, unlock_shares, unlock_ratio, unlock_type)
        VALUES (%(ts_code)s, %(stock_name)s, %(unlock_date)s, %(unlock_shares)s,
                %(unlock_ratio)s, %(unlock_type)s)
        ON CONFLICT (ts_code, unlock_date)
        DO UPDATE SET stock_name = EXCLUDED.stock_name, unlock_shares = EXCLUDED.unlock_shares,
                      unlock_ratio = EXCLUDED.unlock_ratio, unlock_type = EXCLUDED.unlock_type,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def backfill_unlock_ratios(self) -> int:
        """解禁 ratio 推导回填（开发项 L，X-11）：unlock_ratio = unlock_shares ÷ 总股本 × 100。

        总股本取 his_kline_day 最新 total_share 快照，回退 base_fundamentals_info
        最新披露；仅填 unlock_ratio IS NULL 且 unlock_shares > 0 的行（幂等，仅补空）。
        单位口径：百分数（与 PDF 正文解析值一致，如 5.48 = 5.48%）。
        """
        n1 = self.execute_update("""
            UPDATE share_unlock_calendar u
            SET unlock_ratio = round(sub.ratio, 6)
            FROM (
                SELECT u2.id, u2.unlock_shares / NULLIF(k.total_share, 0) * 100 AS ratio
                FROM share_unlock_calendar u2
                CROSS JOIN LATERAL (
                    SELECT k.total_share FROM his_kline_day k
                    WHERE k.ts_code = u2.ts_code AND k.total_share IS NOT NULL
                    ORDER BY k.trade_date DESC LIMIT 1
                ) k
                WHERE u2.unlock_shares > 0 AND u2.unlock_ratio IS NULL
            ) sub
            WHERE u.id = sub.id
        """)
        n2 = self.execute_update("""
            UPDATE share_unlock_calendar u
            SET unlock_ratio = round(sub.ratio, 6)
            FROM (
                SELECT u2.id, u2.unlock_shares / NULLIF(f.total_share, 0) * 100 AS ratio
                FROM share_unlock_calendar u2
                CROSS JOIN LATERAL (
                    SELECT f.total_share FROM base_fundamentals_info f
                    WHERE f.ts_code = u2.ts_code AND f.total_share IS NOT NULL
                    ORDER BY f.disclosure_date DESC, f.stat_date DESC LIMIT 1
                ) f
                WHERE u2.unlock_shares > 0 AND u2.unlock_ratio IS NULL
            ) sub
            WHERE u.id = sub.id
        """)
        return n1 + n2

    # ---------- 外围市场 ----------

    def upsert_overseas_markets(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO overseas_market_daily
        (trade_date, market_code, market_name, close, change_pct)
        VALUES (%(trade_date)s, %(market_code)s, %(market_name)s, %(close)s, %(change_pct)s)
        ON CONFLICT (trade_date, market_code)
        DO UPDATE SET market_name = EXCLUDED.market_name, close = EXCLUDED.close,
                      change_pct = EXCLUDED.change_pct, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    # ---------- 外围日K（overseas_kline_day，技术方案 V1.0 20261004 §5） ----------

    def upsert_overseas_klines(self, rows: List[Dict[str, Any]]) -> int:
        """外围日K upsert：trade_date 为市场自身交易日（非 A 股日历），幂等可重跑"""
        if not rows:
            return 0
        sql = """
        INSERT INTO overseas_kline_day
        (market_code, trade_date, open, high, low, close, volume, adj_type, source)
        VALUES (%(market_code)s, %(trade_date)s, %(open)s, %(high)s, %(low)s, %(close)s,
                %(volume)s, COALESCE(%(adj_type)s, 'raw'), COALESCE(%(source)s, 'ifzq'))
        ON CONFLICT (market_code, trade_date)
        DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                      close = EXCLUDED.close, volume = EXCLUDED.volume,
                      adj_type = EXCLUDED.adj_type, source = EXCLUDED.source,
                      update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_overseas_kline_latest_date(self, market_code: str) -> Optional[str]:
        """某代码库内最新市场交易日（增量判定下界；无行返回 None）"""
        rows = self.execute_query(
            "SELECT MAX(trade_date) AS latest FROM overseas_kline_day WHERE market_code = %s",
            (market_code,))
        return rows[0]['latest'] if rows and rows[0].get('latest') else None

    def fetch_overseas_klines_since(self, market_code: str, start_date: str) -> Dict[str, float]:
        """某代码 start_date（含）之后的 {trade_date: close}（qfq 重述检测的库内基准）"""
        rows = self.execute_query(
            "SELECT trade_date, close FROM overseas_kline_day "
            "WHERE market_code = %s AND trade_date >= %s AND close IS NOT NULL",
            (market_code, start_date))
        return {r['trade_date']: float(r['close']) for r in rows}

    def fetch_overseas_kline_recent_closes(self, market_code: str, limit: int = 3) -> List[float]:
        """某代码最近 N 个交易日收盘（冻结 tripwire：连续同值判定）"""
        rows = self.execute_query(
            "SELECT close FROM overseas_kline_day WHERE market_code = %s AND close IS NOT NULL "
            "ORDER BY trade_date DESC LIMIT %s", (market_code, limit))
        return [float(r['close']) for r in rows]


    # ---------- 分笔资金流 ----------

    def upsert_money_flows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO money_flow_daily
        (trade_date, ts_code, total_amt, big_buy_amt, big_sell_amt, big_net_amt,
         big_net_pct, super_big_buy_amt, super_big_sell_amt, super_big_net_amt,
         mid_buy_amt, mid_sell_amt, mid_net_amt,
         small_buy_amt, small_sell_amt, small_net_amt,
         stock_count, calib_note)
        VALUES (%(trade_date)s, %(ts_code)s, %(total_amt)s, %(big_buy_amt)s, %(big_sell_amt)s,
                %(big_net_amt)s, %(big_net_pct)s,
                %(super_big_buy_amt)s, %(super_big_sell_amt)s, %(super_big_net_amt)s,
                %(mid_buy_amt)s, %(mid_sell_amt)s, %(mid_net_amt)s,
                %(small_buy_amt)s, %(small_sell_amt)s, %(small_net_amt)s,
                %(stock_count)s, COALESCE(%(calib_note)s, '近似口径:分笔分桶'))
        ON CONFLICT (trade_date, ts_code)
        DO UPDATE SET total_amt = EXCLUDED.total_amt, big_buy_amt = EXCLUDED.big_buy_amt,
                      big_sell_amt = EXCLUDED.big_sell_amt, big_net_amt = EXCLUDED.big_net_amt,
                      big_net_pct = EXCLUDED.big_net_pct,
                      super_big_buy_amt = EXCLUDED.super_big_buy_amt,
                      super_big_sell_amt = EXCLUDED.super_big_sell_amt,
                      super_big_net_amt = EXCLUDED.super_big_net_amt,
                      mid_buy_amt = EXCLUDED.mid_buy_amt, mid_sell_amt = EXCLUDED.mid_sell_amt,
                      mid_net_amt = EXCLUDED.mid_net_amt,
                      small_buy_amt = EXCLUDED.small_buy_amt,
                      small_sell_amt = EXCLUDED.small_sell_amt,
                      small_net_amt = EXCLUDED.small_net_amt,
                      stock_count = EXCLUDED.stock_count,
                      calib_note = EXCLUDED.calib_note, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_money_flow_codes(self, trade_date: str, calib_note: Optional[str] = None) -> set:
        """当日已入库资金流的股票集合。

        calib_note 过滤（2026-11-01）：full_market 重跑精确跳过——只跳过口径相同的
        既有行；历史/异动子集口径行（calib_note 不同）仍重算覆盖，保留原"旧行可能
        缺分桶列，重算幂等覆盖"的防御语义。
        """
        if calib_note is None:
            rows = self.execute_query(
                "SELECT ts_code FROM money_flow_daily WHERE trade_date = %s", (trade_date,))
        else:
            rows = self.execute_query(
                "SELECT ts_code FROM money_flow_daily WHERE trade_date = %s AND calib_note = %s",
                (trade_date, calib_note))
        return {r['ts_code'] for r in rows}

    # ---------- 分笔封板指标（开发项 F） ----------

    def upsert_seal_stats(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        sql = """
        INSERT INTO limit_seal_stats_daily
        (trade_date, ts_code, limit_price, first_seal_time, last_seal_time,
         open_count, sealed_ratio, source)
        VALUES (%(trade_date)s, %(ts_code)s, %(limit_price)s, %(first_seal_time)s,
                %(last_seal_time)s, %(open_count)s, %(sealed_ratio)s,
                COALESCE(%(source)s, 'tdx_tick'))
        ON CONFLICT (trade_date, ts_code)
        DO UPDATE SET limit_price = EXCLUDED.limit_price,
                      first_seal_time = EXCLUDED.first_seal_time,
                      last_seal_time = EXCLUDED.last_seal_time,
                      open_count = EXCLUDED.open_count,
                      sealed_ratio = EXCLUDED.sealed_ratio,
                      source = EXCLUDED.source, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_seal_stats_codes(self, trade_date: str) -> set:
        """当日已有封板指标的代码集（F 回填断点续跑判定）"""
        rows = self.execute_query(
            "SELECT ts_code FROM limit_seal_stats_daily WHERE trade_date = %s", (trade_date,))
        return {r['ts_code'] for r in rows}

    def fetch_seal_map(self, trade_date: str) -> Dict[str, Dict[str, Any]]:
        """当日封板指标映射（报告层"首封/开板"列）"""
        rows = self.execute_query(
            "SELECT ts_code, first_seal_time, open_count FROM limit_seal_stats_daily "
            "WHERE trade_date = %s", (trade_date,))
        return {r['ts_code']: {'first_seal_time': r['first_seal_time'],
                               'open_count': r['open_count']} for r in rows}

    # ---------- 情绪指标日频表（开发项 B） ----------

    def upsert_sentiment_metrics(self, rows: List[Dict[str, Any]]) -> int:
        """情绪指标日频 upsert（ladder/promotion 由调用方序列化为 JSON 字符串）"""
        if not rows:
            return 0
        sql = """
        INSERT INTO sentiment_metrics_daily
        (trade_date, stock_count, up_count, down_count, flat_count,
         limit_up_count, limit_down_count, burst_count, burst_rate,
         max_streak, ladder, premium_mean, premium_median, premium_count,
         premium_continued, promotion)
        VALUES (%(trade_date)s, %(stock_count)s, %(up_count)s, %(down_count)s, %(flat_count)s,
                %(limit_up_count)s, %(limit_down_count)s, %(burst_count)s, %(burst_rate)s,
                %(max_streak)s, %(ladder)s, %(premium_mean)s, %(premium_median)s,
                %(premium_count)s, %(premium_continued)s, %(promotion)s)
        ON CONFLICT (trade_date)
        DO UPDATE SET stock_count = EXCLUDED.stock_count, up_count = EXCLUDED.up_count,
                      down_count = EXCLUDED.down_count, flat_count = EXCLUDED.flat_count,
                      limit_up_count = EXCLUDED.limit_up_count,
                      limit_down_count = EXCLUDED.limit_down_count,
                      burst_count = EXCLUDED.burst_count, burst_rate = EXCLUDED.burst_rate,
                      max_streak = EXCLUDED.max_streak, ladder = EXCLUDED.ladder,
                      premium_mean = EXCLUDED.premium_mean,
                      premium_median = EXCLUDED.premium_median,
                      premium_count = EXCLUDED.premium_count,
                      premium_continued = EXCLUDED.premium_continued,
                      promotion = EXCLUDED.promotion, update_time = NOW()
        """
        return self.execute_batch(sql, [self._strip(r) for r in rows])

    def fetch_sentiment_row(self, trade_date: str) -> Optional[Dict[str, Any]]:
        """单日情绪指标行（报告层先查表路径，开发项 B）；无行返回 None"""
        rows = self.execute_query(
            "SELECT * FROM sentiment_metrics_daily WHERE trade_date = %s", (trade_date,))
        return rows[0] if rows else None

    def fetch_sentiment_recent(self, trade_date: str, days: int = 250) -> List[Dict[str, Any]]:
        """截至 trade_date（含）最近 N 个交易日情绪行（分位注记用），按日期升序返回"""
        return self.execute_query("""
            SELECT * FROM sentiment_metrics_daily
            WHERE trade_date <= %s
            ORDER BY trade_date DESC LIMIT %s
        """, (trade_date, days))

    # ---------- 复盘预期落库与核验（四期方案 §1） ----------

    def upsert_review_prediction(self, trade_date: str, payload: Dict[str, Any],
                                 version: int = 1) -> int:
        """复盘预期落库（payload 由调用方 json.dumps；同锚定日重生成覆盖）"""
        return self.execute_batch(
            """
            INSERT INTO review_prediction_daily (trade_date, payload, version)
            VALUES (%s, %s, %s)
            ON CONFLICT (trade_date)
            DO UPDATE SET payload = EXCLUDED.payload, version = EXCLUDED.version,
                          update_time = NOW()
            """,
            [(trade_date, json.dumps(payload, ensure_ascii=False), version)])

    def fetch_review_prediction(self, trade_date: str) -> Optional[Dict[str, Any]]:
        """单日预测载荷（JSONB 自动还原 dict）；无行返回 None"""
        rows = self.execute_query(
            "SELECT payload FROM review_prediction_daily WHERE trade_date = %s",
            (trade_date,))
        return rows[0]['payload'] if rows else None

    def upsert_review_verification(self, trade_date: str, prev_date: Optional[str],
                                   payload: Dict[str, Any], version: int = 1) -> int:
        """核验结果落库（同锚定日重生成覆盖）"""
        return self.execute_batch(
            """
            INSERT INTO review_verification_daily (trade_date, prev_date, payload, version)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (trade_date)
            DO UPDATE SET prev_date = EXCLUDED.prev_date, payload = EXCLUDED.payload,
                          version = EXCLUDED.version, update_time = NOW()
            """,
            [(trade_date, prev_date, json.dumps(payload, ensure_ascii=False), version)])

    def list_review_verifications(self, end_date: str, limit: int = 30) -> List[Dict[str, Any]]:
        """截至 end_date（含）最近 limit 个核验记录（降序；滚动战绩输入）"""
        return self.execute_query(
            """
            SELECT trade_date, prev_date, payload FROM review_verification_daily
            WHERE trade_date <= %s
            ORDER BY trade_date DESC LIMIT %s
            """, (end_date, limit))

    # ---------- 消息催化新闻（四期方案 §4） ----------

    def upsert_news_telegraph(self, rows: List[Dict[str, Any]]) -> int:
        """财联社电报 upsert（news_id 唯一；幂等可中断重跑）"""
        if not rows:
            return 0
        return self.execute_batch(
            """
            INSERT INTO news_telegraph
            (news_id, publish_time, trade_date, title, content, subject, importance, source)
            VALUES (%(news_id)s, %(publish_time)s, %(trade_date)s, %(title)s,
                    %(content)s, %(subject)s, %(importance)s, %(source)s)
            ON CONFLICT (news_id)
            DO UPDATE SET title = EXCLUDED.title, content = EXCLUDED.content,
                          subject = EXCLUDED.subject, importance = EXCLUDED.importance,
                          update_time = NOW()
            """, [self._strip(r) for r in rows])

    def fetch_latest_news_id(self) -> Optional[str]:
        """最新入库 news_id（增量游标）"""
        rows = self.execute_query(
            "SELECT news_id FROM news_telegraph ORDER BY publish_time DESC LIMIT 1")
        return rows[0]['news_id'] if rows else None

    # ---------- 日K快照（money_flow 子集 / valuation_calc 输入） ----------

    def fetch_day_kline_snapshot(self, trade_date: str) -> List[Dict[str, Any]]:
        """全市场某交易日日K快照：涨跌停/异动子集与估值自算的统一输入。

        total_share 取 his_kline_day 当日快照，缺失由调用方回退 base_fundamentals_info。
        """
        return self.execute_query("""
            SELECT ts_code, stock_code, stock_name, trade_date, close, preclose, high, low,
                   amount, volume, change_rate, is_st, total_share
            FROM his_kline_day
            WHERE trade_date = %s
        """, (trade_date,))

    # ---------- 内部 ----------

    @staticmethod
    def _strip(row: Dict[str, Any]) -> Dict[str, Any]:
        """行参数直通（占位收敛点：如需统一清洗在此扩展，调用方模板键必须齐全）"""
        return row
