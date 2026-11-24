"""请求级准入域门（第三方数据采集限流与补采优化方案 20261120 §2）

按**实际外发主机**统一准入：冷却判断、共享发送节拍、发送时隙占用。
主流程、单源 CLI、补采、多进程共用 PostgreSQL ``sync_domain_cooldown`` 行；
短事务原子判断冷却/推进节拍/占用时隙，**网络调用前释放数据库锁**（不跨事务持锁）。

- DB 模式（``review_sync.domain_gate_enabled: true``，显式开启）：跨进程共享。
- 本地模式（缺省）：仅保留源内串行节拍，与旧行为一致——代码先行上线、
  配置灰度开启（方案 §5 发布顺序）。
- fail-safe：控制面（DB）不可用时，高风险域暂缓采集并报告，低风险域退化为
  本地节拍（方案 §2 故障行）。
- 升级期兼容：旧 ``exchange`` 域冷却记录仍被识别，直到其过期。
- 冷却分级：429/拦截页 → 主机级；403 → 端点级（同轮多端点被拒才升级主机级）；
  400 → 端点级诊断暂停，不触发全域频控统计（方案 §1 表）。
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse

from .errors import (
    KIND_BAD_REQUEST,
    KIND_BLOCKED_PAGE,
    KIND_COOLDOWN_ACTIVE,
    KIND_FORBIDDEN,
    KIND_RATE_LIMITED,
    DomainCooldownError,
)

logger = logging.getLogger(__name__)

# 主机 → 共享请求域（方案 §2「域与端点」表）；未列出主机不进共享域，仅本地节拍
DOMAIN_BY_HOST: Dict[str, str] = {
    'www.szse.cn': 'szse',
    'query.sse.com.cn': 'sse',
    'www.sse.com.cn': 'sse',
    'www.cninfo.com.cn': 'cninfo_query',
    'static.cninfo.com.cn': 'cninfo_static',
    'vip.stock.finance.sina.com.cn': 'sina_vip',
    'basic.10jqka.com.cn': 'ths_basic',
    'www.csindex.com.cn': 'csindex',
    'oss-ch.csindex.com.cn': 'csindex_oss',
    'www.swsresearch.com': 'sws',
    'datacenter-web.eastmoney.com': 'eastmoney_dc',
    'datacenter-api.jin10.com': 'jin10',
    'cdn.jin10.com': 'jin10',
}

# 高风险域：共享状态不可读时暂缓该主机采集（方案 §2 故障行）；其余退化为本地节拍
CRITICAL_DOMAINS = {'szse', 'sse', 'cninfo_query', 'cninfo_static',
                    'sina_vip', 'ths_basic', 'csindex'}

# 升级期兼容：旧交易所域记录（2026-11-17 起）仍被识别，直到其过期
LEGACY_EXCHANGE_DOMAIN = 'exchange'

RATE_LIMIT_DEFAULT_SECONDS = 1800        # 429/拦截页：主机级冷却（无可信 Retry-After 时）
ENDPOINT_PAUSE_FORBIDDEN_SECONDS = 900   # 403 未知原因：端点级暂停
ENDPOINT_PAUSE_BAD_REQUEST_SECONDS = 300  # 400：端点级诊断暂停（不计频控）

# 端点摘要白名单键（方案 §1：报告只保留允许记录的业务参数）
_ENDPOINT_PARAM_KEYS = ('sqlId', 'CATALOGID', 'TABKEY', 'bulletinType')
_ENDPOINT_TAG_MAX = 26
_LOCAL_BEAT_KEY_MAX = 64


def domain_for_host(host: Optional[str]) -> Optional[str]:
    """实际外发主机 → 共享请求域（未映射主机返回 None，仅本地节拍）"""
    return DOMAIN_BY_HOST.get((host or '').strip().lower())


def endpoint_domain(host_domain: Optional[str], endpoint: Optional[str]) -> Optional[str]:
    """域 + 端点 → 端点级冷却键（``szse:1842_detal`` 形式，超长截断）"""
    if not host_domain:
        return None
    if not endpoint:
        return host_domain
    return f'{host_domain}:{endpoint}'[:32]


def build_endpoint_tag(url: str, params: Optional[Dict[str, Any]] = None,
                       data: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """端点标识：路径末两段 + 白名单参数摘要（有界，供报告与端点级冷却）"""
    try:
        path = urlparse(url).path or ''
    except Exception:
        return None
    segs = [s for s in path.split('/') if s]
    base = '/'.join(segs[-2:]) if len(segs) >= 2 else (segs[-1] if segs else path)
    extras = []
    for k in _ENDPOINT_PARAM_KEYS:
        v = (params or {}).get(k) or (data or {}).get(k)
        if v:
            extras.append(str(v))
    tag = base + ('#' + '#'.join(extras) if extras else '')
    return tag[:_ENDPOINT_TAG_MAX] or None


def parse_retry_after(value: Any) -> Optional[int]:
    """解析 Retry-After：秒数或 HTTP 日期 → 剩余秒数；非法/缺失返回 None"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        remaining = int((dt - datetime.now(timezone.utc)).total_seconds())
        return max(0, remaining)
    except Exception:
        return None


class DomainGate:
    """按实际外发主机的请求级准入（冷却/节拍/时隙；跨进程经 DB 共享）

    Args:
        enabled: true 走 DB 共享模式；false 仅本地节拍（与旧行为一致）
        db_factory: 惰性返回 DatabaseConnection 的工厂（缺省自建独立连接）
        critical_domains: 共享状态不可读时需暂缓采集的域集合
    """

    def __init__(self, *, enabled: bool = False,
                 db_factory: Optional[Callable[[], Any]] = None,
                 critical_domains: Optional[set] = None):
        self.enabled = bool(enabled)
        self.critical_domains = set(critical_domains
                                    if critical_domains is not None else CRITICAL_DOMAINS)
        self._db_factory = db_factory
        self._db: Any = None
        self._db_failed = False
        self._slot_timeout = 120  # 发送时隙自愈时长（请求超时上限 30s × 富余）
        self._owner = f'{socket.gethostname()}:{os.getpid()}'
        self._lock = threading.Lock()
        self._local_next_send: Dict[str, float] = {}
        self._tls = threading.local()
        self._rejected_endpoints: Dict[str, set] = {}

    # ---------- 单例（进程内共享） ----------

    _SHARED: Optional['DomainGate'] = None
    _SHARED_LOCK = threading.Lock()

    @classmethod
    def shared(cls, config: Optional[Dict[str, Any]] = None) -> 'DomainGate':
        with cls._SHARED_LOCK:
            if cls._SHARED is None:
                review = (config or {}).get('review_sync', {}) or {}
                cls._SHARED = cls(enabled=bool(review.get('domain_gate_enabled', False)))
            return cls._SHARED

    # ---------- 请求前准入 ----------

    def acquire(self, url: str, *, min_interval: float = 0.0,
                endpoint: Optional[str] = None) -> None:
        """请求前准入：必要时本地等待共享节拍；冷却未到期抛 DomainCooldownError。

        本地等待发生在数据库锁释放之后（短事务提交 → sleep → 外发）。
        """
        host = urlparse(url).netloc.lower() if url else ''
        domain = domain_for_host(host)
        if not self.enabled or domain is None:
            self._local_wait(domain or host or 'default', min_interval)
            return
        conn = self._conn()
        if conn is None:
            self._fail_safe(domain, host, endpoint, min_interval)
            return
        try:
            wait_seconds, _ = self._acquire_db(conn, domain, host, endpoint,
                                               min_interval)
        except DomainCooldownError:
            raise
        except Exception as e:
            self._reset_db(e)
            self._fail_safe(domain, host, endpoint, min_interval)
            return
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    # ---------- 请求收尾 ----------

    def report(self, url: str, *, endpoint: Optional[str] = None, kind: Optional[str] = None,
               retry_after: Optional[int] = None, ok: bool = True) -> None:
        """请求收尾：成功/网络类失败释放时隙；显式拒绝**立即独立提交冷却**
        （不等整轮汇总，方案 §2）。登记失败仅告警——编排层汇总兜底登记。"""
        if not self.enabled:
            return
        host = urlparse(url).netloc.lower() if url else ''
        last = getattr(self._tls, 'last', None) or {}
        domain = last.get('domain') or domain_for_host(host)
        if not domain:
            return
        endpoint = endpoint or last.get('endpoint')
        conn = self._conn()
        if conn is None:
            return
        try:
            self._report_db(conn, domain, endpoint, kind, retry_after, ok)
        except Exception as e:
            self._reset_db(e)
            logger.warning(f'域门收尾失败（编排层将兜底登记冷却）: {e}')

    def remaining_for_url(self, url: str) -> int:
        """查询该 URL 所属域的剩余冷却秒数（0=无冷却；DB 不可用返回 0）。

        供管理器在任务启动前预检（夜间续跑脚本读取暂停截止时间的实现基础）。
        """
        if not self.enabled:
            return 0
        host = urlparse(url).netloc.lower() if url else ''
        domain = domain_for_host(host)
        if not domain:
            return 0
        conn = self._conn()
        if conn is None:
            return 0
        try:
            return self._query_remaining(conn, domain)
        except Exception as e:
            self._reset_db(e)
            return 0

    # ---------- 数据库实现 ----------

    def _conn(self) -> Any:
        if not self._db_factory:
            def _default():
                from ..config.config_manager import ConfigManager
                from ..database.connection import DatabaseConnection
                return DatabaseConnection(ConfigManager())
            self._db_factory = _default
        if self._db_failed:
            return None
        if self._db is None:
            try:
                self._db = self._db_factory()
            except Exception as e:
                self._db_failed = True
                logger.warning(f'域门数据库不可用（退化本地节拍/暂缓高风险域）: {e}')
                return None
        return self._db

    def _reset_db(self, e: Exception) -> None:
        self._db_failed = True
        logger.warning(f'域门数据库访问失败（本轮退化本地节拍）: {e}')

    def _fail_safe(self, domain: str, host: str, endpoint: Optional[str],
                   min_interval: float = 0.0) -> None:
        """控制面不可用：高风险域暂缓采集，低风险域退化本地节拍"""
        if domain in self.critical_domains:
            raise DomainCooldownError(
                f'[{domain}] 域门共享状态不可读，暂缓该主机大批量采集',
                kind=KIND_COOLDOWN_ACTIVE, host=host, endpoint=endpoint)
        self._local_wait(domain, min_interval)

    def _acquire_db(self, conn: Any, domain: str, host: str,
                    endpoint: Optional[str], min_interval: float):
        """短事务：原子判断冷却 + 推进共享节拍 + 占用时隙；提交后仅本地 sleep"""
        now = datetime.now(timezone.utc)
        send_at = now
        ekey = domain
        with conn.get_connection() as c:
            try:
                with c.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO sync_domain_cooldown (domain) VALUES (%s) "
                        "ON CONFLICT (domain) DO NOTHING", (domain,))
                    cursor.execute(
                        "SELECT blocked_until, next_send_at, slot_owner, slot_until "
                        "FROM sync_domain_cooldown WHERE domain = %s FOR UPDATE",
                        (domain,))
                    row = cursor.fetchone()
                    blocked_until = row[0] if row else None
                    if blocked_until is not None and blocked_until > now:
                        remaining = int((blocked_until - now).total_seconds())
                        c.rollback()
                        raise DomainCooldownError(
                            f'[{domain}] 域冷却中（剩余 {remaining}s），请求未发出',
                            cooldown_seconds=remaining, kind=KIND_COOLDOWN_ACTIVE,
                            host=host, endpoint=endpoint)
                    # 升级期兼容：旧 exchange 域记录仍生效（直到其自然过期）
                    cursor.execute(
                        "SELECT blocked_until FROM sync_domain_cooldown "
                        "WHERE domain = %s", (LEGACY_EXCHANGE_DOMAIN,))
                    legacy = cursor.fetchone()
                    if legacy and legacy[0] and legacy[0] > now:
                        remaining = int((legacy[0] - now).total_seconds())
                        c.rollback()
                        raise DomainCooldownError(
                            f'[exchange] 旧域冷却继承中（剩余 {remaining}s），请求未发出',
                            cooldown_seconds=remaining, kind=KIND_COOLDOWN_ACTIVE,
                            host=host, endpoint=endpoint)
                    # 端点级暂停（403/400 局部暂停）：仅拦该端点，主机其余端点照常
                    ekey = endpoint_domain(domain, endpoint) or domain
                    if ekey != domain:
                        cursor.execute(
                            "SELECT blocked_until FROM sync_domain_cooldown "
                            "WHERE domain = %s", (ekey,))
                        erow = cursor.fetchone()
                        if erow and erow[0] and erow[0] > now:
                            remaining = int((erow[0] - now).total_seconds())
                            c.rollback()
                            raise DomainCooldownError(
                                f'[{ekey}] 端点暂停中（剩余 {remaining}s），请求未发出',
                                cooldown_seconds=remaining, kind=KIND_COOLDOWN_ACTIVE,
                                host=host, endpoint=endpoint)
                    # 共享节拍 + 到期放行单请求（他人未释放的时隙顺延本次发送）
                    if row and row[1] and row[1] > now:
                        send_at = row[1]
                    if row and row[2] and row[2] != self._owner \
                            and row[3] and row[3] > now:
                        send_at = max(send_at, row[3])
                    next_send = send_at + timedelta(seconds=max(0.0, min_interval))
                    slot_until = send_at + timedelta(seconds=self._slot_timeout)
                    cursor.execute(
                        "UPDATE sync_domain_cooldown SET next_send_at = %s, "
                        "slot_owner = %s, slot_until = %s, updated_at = NOW() "
                        "WHERE domain = %s",
                        (next_send, self._owner, slot_until, domain))
                c.commit()
            except Exception:
                c.rollback()
                raise
        wait_seconds = max(0.0, (send_at - now).total_seconds())
        self._tls.last = {'domain': domain, 'endpoint': endpoint, 'host': host}
        return wait_seconds, ekey

    def _report_db(self, conn: Any, domain: str, endpoint: Optional[str],
                   kind: Optional[str], retry_after: Optional[int], ok: bool) -> None:
        now = datetime.now(timezone.utc)
        with conn.get_connection() as c:
            try:
                with c.cursor() as cursor:
                    # 释放本进程占用的发送时隙
                    cursor.execute(
                        "UPDATE sync_domain_cooldown SET slot_owner = NULL, "
                        "slot_until = NULL, updated_at = NOW() "
                        "WHERE domain = %s AND slot_owner = %s",
                        (domain, self._owner))
                    if ok or kind is None:
                        c.commit()
                        return
                    if kind == KIND_RATE_LIMITED:
                        seconds = max(RATE_LIMIT_DEFAULT_SECONDS, retry_after or 0)
                        self._register_cooldown(
                            cursor, domain, now + timedelta(seconds=seconds),
                            f'429 服务端限频（Retry-After={retry_after}）')
                    elif kind == KIND_BLOCKED_PAGE:
                        self._register_cooldown(
                            cursor, domain, now + timedelta(seconds=RATE_LIMIT_DEFAULT_SECONDS),
                            '拦截页/验证码/非 JSON（主机级暂停）')
                    elif kind == KIND_FORBIDDEN:
                        ekey = endpoint_domain(domain, endpoint) or domain
                        until = now + timedelta(seconds=ENDPOINT_PAUSE_FORBIDDEN_SECONDS)
                        self._register_cooldown(cursor, ekey, until, '403 访问被拒（端点暂停）')
                        if self._note_endpoint_rejection(domain, endpoint):
                            self._register_cooldown(cursor, domain, until,
                                                    '同轮多端点 403，升级主机级暂停')
                    elif kind == KIND_BAD_REQUEST:
                        ekey = endpoint_domain(domain, endpoint) or domain
                        self._register_cooldown(
                            cursor, ekey, now + timedelta(seconds=ENDPOINT_PAUSE_BAD_REQUEST_SECONDS),
                            '400 请求异常（端点诊断暂停，不计频控）')
                c.commit()
            except Exception:
                c.rollback()
                raise

    @staticmethod
    def _register_cooldown(cursor: Any, domain: str, blocked_until: datetime,
                           reason: str) -> None:
        cursor.execute(
            "INSERT INTO sync_domain_cooldown (domain, blocked_until, reason, updated_at) "
            "VALUES (%s, %s, %s, NOW()) ON CONFLICT (domain) DO UPDATE SET "
            "blocked_until = GREATEST(sync_domain_cooldown.blocked_until, EXCLUDED.blocked_until), "
            "reason = EXCLUDED.reason, updated_at = NOW()",
            (domain, blocked_until, reason))

    def _query_remaining(self, conn: Any, domain: str) -> int:
        rows = conn.execute_query(
            "SELECT MAX(blocked_until) AS mx FROM sync_domain_cooldown "
            "WHERE domain IN (%s, %s) AND blocked_until > NOW()",
            (domain, LEGACY_EXCHANGE_DOMAIN))
        mx = rows[0].get('mx') if rows else None
        if not mx:
            return 0
        return int((mx - datetime.now(timezone.utc)).total_seconds())

    def _note_endpoint_rejection(self, domain: str, endpoint: Optional[str]) -> bool:
        """同轮 ≥2 个不同端点被拒 → True（升级主机级暂停）"""
        if not endpoint:
            return False
        with self._lock:
            seen = self._rejected_endpoints.setdefault(domain, set())
            seen.add(endpoint)
            return len(seen) >= 2

    # ---------- 本地模式 ----------

    def _local_wait(self, key: str, min_interval: float) -> None:
        """本地节拍（无跨进程共享；语义与旧 `_attempt` 请求前 sleep 一致）"""
        if min_interval <= 0:
            return
        key = key[:_LOCAL_BEAT_KEY_MAX]
        now = time.monotonic()
        with self._lock:
            nxt = self._local_next_send.get(key, 0.0)
            wait = max(0.0, nxt - now)
            self._local_next_send[key] = max(now, nxt) + min_interval
        if wait > 0:
            time.sleep(wait)


def get_gate(config: Optional[Dict[str, Any]] = None) -> DomainGate:
    """进程级共享域门（配置 ``review_sync.domain_gate_enabled`` 显式开启 DB 模式）"""
    return DomainGate.shared(config)
