"""采集端共享异常定义（第三方数据采集限流与补采优化方案 20261120 §1）

ReviewBlockedError 自 http_collector_base 迁移至此（原路径保留 re-export，
既有 import 不受影响）。新增结构化字段随异常保留到单源报告、编排和补采：

- kind: rate_limited(429) / forbidden(403,401) / bad_request(400) /
        blocked_page(验证码、非 JSON、拦截页) / cooldown_active(本地域冷却拦回)
- host: 实际外发主机（域冷却按主机登记，替代源级字符串推断）
- endpoint: 端点标识（路径末两段 + 白名单参数摘要，有界截断）
- retry_after: 服务端 Retry-After 提示秒数（无则 None）

DomainCooldownError 为域门本地拦回（冷却未到期，请求**未发出**）：单列"未发出"，
不耗尽外部尝试次数，也不据此延长冷却截止时间。
"""

from __future__ import annotations

from typing import Optional

# kind 取值常量（与 domain_gate 共用）
KIND_RATE_LIMITED = 'rate_limited'
KIND_FORBIDDEN = 'forbidden'
KIND_BAD_REQUEST = 'bad_request'
KIND_BLOCKED_PAGE = 'blocked_page'
KIND_COOLDOWN_ACTIVE = 'cooldown_active'


class ReviewBlockedError(RuntimeError):
    """风控基类异常：该源本轮急停，冷却 cooldown_seconds 后可由编排器决定是否重试一次

    各源子类化（如 ThsBlockedError），子类可覆写默认冷却时长。
    """

    cooldown_seconds: int = 600

    def __init__(self, message: str, cooldown_seconds: Optional[int] = None, *,
                 kind: Optional[str] = None, host: Optional[str] = None,
                 endpoint: Optional[str] = None, retry_after: Optional[int] = None):
        super().__init__(message)
        if cooldown_seconds is not None:
            self.cooldown_seconds = int(cooldown_seconds)
        self.kind = kind
        self.host = host
        self.endpoint = endpoint
        self.retry_after = retry_after


class DomainCooldownError(ReviewBlockedError):
    """域门本地拦回：共享冷却未到期，请求未发出（单列"未发出"，不外发）"""
