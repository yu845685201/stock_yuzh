"""
统一 HTTP 采集基类（每日复盘数据采集技术方案 V1.0 §2.3，稳定性核心）

现有 cninfo/tdx 源不迁移；全部新增外网源（sse_szse/csindex/ths/sina）继承本基类。

设计要点：
- 会话：模块级唯一 requests.Session()（连接复用）；headers 每请求注入，源间不串
- 超时：default_timeout 配置 [连接5s, 读30s]，防挂死
- 限速：请求级准入域门（domain_gate）统一节拍——DB 模式（review_sync.
  domain_gate_enabled=true）按主机共享冷却/节拍/时隙；本地模式退化为源内
  串行 sleep（与旧行为一致）。单次 headers 与默认 headers 合并，显式覆盖项优先
- 重试：请求级 run_with_retry，指数退避 + 随机抖动 ±20%；
        仅对网络类异常/5xx 重试；风控信号（4xx/验证码页/非 JSON）不重试，
        直接抛 ReviewBlockedError——裸重试是触发封禁升级的主因
- 拒绝分类（限流优化方案 20261120 §1）：429→rate_limited（解析 Retry-After）/
  403,401→forbidden / 400→bad_request / 拦截页·非 JSON→blocked_page；
  kind/host/endpoint/retry_after 随异常保留到单源报告、编排和补采。
  429 或 5xx 携带有效 Retry-After 时遵守服务端等待时间、不再重试
- 风控识别钩子：_check_risk_control(resp)，子类覆写识别验证码页特征串等
- 急停：manager 捕获 ReviewBlockedError → 本轮该源急停（结果 blocked=True）；
  pending_handoff 登记持久化待补后释放线程；进度/断点已落盘可续
- 请求前后打 debug 日志（URL、状态码、耗时、字节数），问题可追溯

安全红线（沿用 cninfo_source.py 文件头，对全部新源生效）：
串行限速、正常 UA/Referer、不破解任何技术措施（含同花顺 hexin-v cookie）、
风控识别即退避冷却、仅自用不分发。
"""

import logging
import random
import time
from typing import Any, Dict, List, Optional, Tuple, Type, Union
from urllib.parse import urlparse

import requests

from ..utils.retry import run_with_retry
from .domain_gate import (
    build_endpoint_tag,
    get_gate,
    parse_retry_after,
)
from .errors import (
    KIND_BAD_REQUEST,
    KIND_BLOCKED_PAGE,
    KIND_FORBIDDEN,
    KIND_RATE_LIMITED,
    ReviewBlockedError,
)

logger = logging.getLogger(__name__)

# 与 cninfo_source 同款 UA 策略（正常浏览器 UA，配置可覆盖）
DEFAULT_UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

# 报错附带的原始响应片段上限（方案 §6.1：结构变更报错必须带原始响应片段）
_SNIPPET_CHARS = 200

# 按主机隔离的会话池（2026-11-26 批次 A 回放实测：深市 XLSX 大响应后共享单例
# Session 上发沪市 queryMargin.do 恒被 nginx 400——连接池状态跨主机残留污染；
# 新建 Session 即恢复。cookie/连接池按 netloc 隔离，headers 仍逐请求注入）
_SESSIONS: Dict[str, requests.Session] = {}


def _session_for(url: str) -> requests.Session:
    host = _host_of(url) or ''
    sess = _SESSIONS.get(host)
    if sess is None:
        sess = requests.Session()
        _SESSIONS[host] = sess
    return sess

__all__ = ['HttpCollectorBase', 'ReviewBlockedError', 'DEFAULT_UA']


def _host_of(url: str) -> Optional[str]:
    try:
        return urlparse(url).netloc.lower() or None
    except Exception:
        return None


def _classify_4xx(resp: requests.Response) -> Tuple[str, Optional[int]]:
    """4xx → (kind, retry_after)，对齐方案 §1 表（400 不触发全域频控统计）"""
    status = resp.status_code
    retry_after = None
    if status == 429:
        retry_after = parse_retry_after(resp.headers.get('Retry-After'))
        return KIND_RATE_LIMITED, retry_after
    if status in (401, 403):
        return KIND_FORBIDDEN, None
    if status == 400:
        return KIND_BAD_REQUEST, None
    return KIND_BLOCKED_PAGE, None


def response_snippet(resp: requests.Response, limit: int = _SNIPPET_CHARS) -> str:
    """截取响应体片段（日志/异常用），二进制安全"""
    try:
        text = resp.content[:limit * 4].decode(resp.encoding or 'utf-8', errors='replace')
    except Exception:
        text = repr(resp.content[:80])
    return ' '.join(text.split())[:limit]


class HttpCollectorBase:
    """外网 HTTP 采集基类：域门准入 + 限速 + 重试 + 风控识别 + debug 日志

    Args:
        config: 全量配置 dict（读取 review_sync 段通用项）
        source_name: 源名（日志与急停标识）
        interval_seconds: 请求间隔（秒，各源从 config.yaml 独立取值后传入；
            域门 DB 模式下作为该源对所属域的节拍贡献）
        referer: Referer（交易所站点强校验，每源必须配置正确）
        ajax: True 时追加 X-Requested-With: XMLHttpRequest（仅 AJAX 类接口）
        headers_override: 追加/覆盖 headers（Cookie 默认不带；THS hexin-v 一律不做——红线）
        blocked_error_class: 该源风控异常类（须为 ReviewBlockedError 子类）
        gate: 请求级准入域门（缺省进程级单例；单测可注入假门）
    """

    blocked_error_class: Type[ReviewBlockedError] = ReviewBlockedError

    def __init__(self, config: Dict[str, Any], source_name: str, *,
                 interval_seconds: float = 1.0,
                 referer: Optional[str] = None,
                 ajax: bool = False,
                 headers_override: Optional[Dict[str, str]] = None,
                 blocked_error_class: Optional[Type[ReviewBlockedError]] = None,
                 verify: Optional[str] = None,
                 gate: Optional[Any] = None):
        review = (config or {}).get('review_sync', {}) or {}
        self.source_name = source_name
        self.request_interval = float(interval_seconds)
        self.blocked_cooldown_seconds = int(review.get('blocked_cooldown_seconds', 600))
        self.retry_attempts = max(1, int(review.get('retry_attempts', 3)))
        timeout = review.get('default_timeout', [5, 30])
        self.request_timeout: Tuple[float, float] = (float(timeout[0]), float(timeout[1]))
        # 代理：默认 False 外网走系统代理；True 时显式绕过（与 tdx 本地绕过逻辑无关）
        self.bypass_proxy = bool(review.get('bypass_proxy', False))
        self.user_agent = review.get('user_agent') or DEFAULT_UA
        # TLS 校验：None 用 requests 默认（certifi）；个别站点证书链不完整
        # （如 swsresearch 只发叶子证书），配本地 CA bundle 路径（含缺失中间证书）
        self.verify = verify
        if blocked_error_class is not None:
            self.blocked_error_class = blocked_error_class
        self.gate = gate if gate is not None else get_gate(config)

        headers = {
            'User-Agent': self.user_agent,
            'Accept': 'application/json,text/html;q=0.9,*/*;q=0.8',
        }
        if referer:
            headers['Referer'] = referer
        if ajax:
            headers['X-Requested-With'] = 'XMLHttpRequest'
        if headers_override:
            headers.update(headers_override)
        self.headers: Dict[str, str] = headers

    # ---------- 请求入口 ----------

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None,
                 headers: Optional[Dict[str, str]] = None,
                 allow_status: Tuple[int, ...] = ()) -> Union[Dict[str, Any], List[Any]]:
        return self._request('GET', url, params=params, headers=headers,
                             expect_json=True, allow_status=allow_status)

    def post_json(self, url: str, data: Optional[Dict[str, Any]] = None,
                  params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None,
                  allow_status: Tuple[int, ...] = ()) -> Union[Dict[str, Any], List[Any]]:
        return self._request('POST', url, params=params, data=data, headers=headers,
                             expect_json=True, allow_status=allow_status)

    def get_text(self, url: str, params: Optional[Dict[str, Any]] = None,
                 headers: Optional[Dict[str, str]] = None,
                 allow_status: Tuple[int, ...] = ()) -> str:
        resp = self._request('GET', url, params=params, headers=headers,
                             expect_json=False, allow_status=allow_status)
        return resp.text

    def get_content(self, url: str, params: Optional[Dict[str, Any]] = None,
                    headers: Optional[Dict[str, str]] = None,
                    allow_status: Tuple[int, ...] = ()) -> bytes:
        resp = self._request('GET', url, params=params, headers=headers,
                             expect_json=False, allow_status=allow_status)
        return resp.content

    # ---------- 内部实现 ----------

    @property
    def _proxies(self) -> Optional[Dict[str, None]]:
        return {'http': None, 'https': None} if self.bypass_proxy else None

    def _backoff_seconds(self, attempt: int) -> float:
        """指数退避 + 随机抖动 ±20%（封顶 30s）"""
        return min(2.0 ** attempt, 30.0) * random.uniform(0.8, 1.2)

    def _attempt(self, method: str, url: str, *, params: Optional[Dict[str, Any]],
                 data: Optional[Dict[str, Any]], headers: Optional[Dict[str, str]],
                 expect_json: bool, allow_status: Tuple[int, ...]) -> Any:
        # 请求级准入：冷却判断 + 共享节拍（冷却中抛 DomainCooldownError，请求不外发）
        endpoint = build_endpoint_tag(url, params, data)
        self.gate.acquire(url, min_interval=self.request_interval, endpoint=endpoint)
        started = time.time()
        # 单次自定义 headers 与默认 headers 合并，显式覆盖项优先
        # （修复：仅传 Referer 的调用丢失采集器 UA/Accept 的既有缺陷）
        merged_headers = {**self.headers, **(headers or {})}
        try:
            resp = _session_for(url).request(method, url, params=params, data=data,
                                             headers=merged_headers,
                                             timeout=self.request_timeout,
                                             proxies=self._proxies,
                                             verify=self.verify)
        except requests.RequestException:
            # 网络类失败：释放时隙，交由重试预算退避
            self.gate.report(url, endpoint=endpoint, ok=True)
            raise
        elapsed_ms = (time.time() - started) * 1000
        logger.debug(f'[{self.source_name}] {method} {url} -> {resp.status_code} '
                     f'({elapsed_ms:.0f}ms, {len(resp.content)}B)')

        host = _host_of(url)
        if resp.status_code >= 500:
            retry_after = parse_retry_after(resp.headers.get('Retry-After'))
            if retry_after is not None:
                # 服务端明确要求等待：遵守等待时间，不再重试（方案 §1 网络类行）
                self.gate.report(url, endpoint=endpoint, kind=KIND_RATE_LIMITED,
                                 retry_after=retry_after, ok=False)
                raise self._make_blocked(
                    f'HTTP {resp.status_code}（服务端要求等待 {retry_after}s）',
                    cooldown_seconds=max(self.blocked_cooldown_seconds, retry_after),
                    kind=KIND_RATE_LIMITED, host=host, endpoint=endpoint,
                    retry_after=retry_after)
            # 服务端/网关类异常：网络类失败，允许重试
            self.gate.report(url, endpoint=endpoint, ok=True)
            resp.raise_for_status()
        if resp.status_code >= 400:
            if resp.status_code in allow_status:
                # 已约定的未发布/缺文件等 404 语义：正常业务路径，非拒绝
                self.gate.report(url, endpoint=endpoint, ok=True)
                return resp
            kind, retry_after = _classify_4xx(resp)
            cooldown = self.blocked_cooldown_seconds
            if kind == KIND_RATE_LIMITED and retry_after:
                cooldown = max(cooldown, retry_after)
            self.gate.report(url, endpoint=endpoint, kind=kind,
                             retry_after=retry_after, ok=False)
            raise self._make_blocked(f'HTTP {resp.status_code}', cooldown_seconds=cooldown,
                                     kind=kind, host=host, endpoint=endpoint,
                                     retry_after=retry_after)
        try:
            self._check_risk_control(resp)
        except ReviewBlockedError:
            self.gate.report(url, endpoint=endpoint, kind=KIND_BLOCKED_PAGE, ok=False)
            raise
        if expect_json:
            try:
                payload = resp.json()
            except ValueError as e:
                self.gate.report(url, endpoint=endpoint, kind=KIND_BLOCKED_PAGE, ok=False)
                raise self._make_blocked(
                    f'返回非 JSON（疑似风控/验证码/改版）: {response_snippet(resp)}',
                    kind=KIND_BLOCKED_PAGE, host=host, endpoint=endpoint) from e
            self.gate.report(url, endpoint=endpoint, ok=True)
            return payload
        self.gate.report(url, endpoint=endpoint, ok=True)
        return resp

    def _request(self, method: str, url: str, *, params: Optional[Dict[str, Any]] = None,
                 data: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None,
                 expect_json: bool = True, allow_status: Tuple[int, ...] = ()) -> Any:
        def _on_error(attempt: int, e: BaseException) -> None:
            logger.warning(f'[{self.source_name}] 第 {attempt} 次请求失败: {url} {e}')

        return run_with_retry(
            lambda: self._attempt(method, url, params=params, data=data, headers=headers,
                                  expect_json=expect_json, allow_status=allow_status),
            attempts=self.retry_attempts,
            backoff=self._backoff_seconds,
            # 仅网络类异常重试；ReviewBlockedError 不在其列 → 风控信号不重试
            retry_on=(requests.RequestException,),
            on_error=_on_error,
        )

    def _check_risk_control(self, resp: requests.Response) -> None:
        """风控识别钩子：默认不识别（4xx 已在 _attempt 拦截）。

        子类覆写识别：验证码页特征串、非预期响应形态等；命中即
        raise self._make_blocked(...)。
        """
        return None

    def _make_blocked(self, message: str,
                      cooldown_seconds: Optional[int] = None, *,
                      kind: Optional[str] = None, host: Optional[str] = None,
                      endpoint: Optional[str] = None,
                      retry_after: Optional[int] = None) -> ReviewBlockedError:
        return self.blocked_error_class(
            f'[{self.source_name}] {message}',
            cooldown_seconds or self.blocked_cooldown_seconds,
            kind=kind, host=host, endpoint=endpoint, retry_after=retry_after)
