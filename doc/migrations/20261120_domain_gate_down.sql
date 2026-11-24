-- 域门列回滚（对应 20261120_domain_gate_up.sql）
-- 注意：回滚前确认没有采集进程正在使用域门 DB 模式
-- （review_sync.domain_gate_enabled=false 或相关进程已停止）。

ALTER TABLE sync_domain_cooldown DROP COLUMN IF EXISTS slot_until;
ALTER TABLE sync_domain_cooldown DROP COLUMN IF EXISTS slot_owner;
ALTER TABLE sync_domain_cooldown DROP COLUMN IF EXISTS next_send_at;
