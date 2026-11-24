-- 域门列升级（第三方数据采集限流与补采优化方案 20261120 §2）
-- sync_domain_cooldown 增加共享发送节拍与发送时隙列；
-- 下游 domain_gate.acquire 在短事务内原子推进 next_send_at、占用 slot。
-- 幂等：可重复执行。回滚见 20261120_domain_gate_down.sql

ALTER TABLE sync_domain_cooldown
    ADD COLUMN IF NOT EXISTS next_send_at TIMESTAMPTZ;
ALTER TABLE sync_domain_cooldown
    ADD COLUMN IF NOT EXISTS slot_owner VARCHAR(128);
ALTER TABLE sync_domain_cooldown
    ADD COLUMN IF NOT EXISTS slot_until TIMESTAMPTZ;
