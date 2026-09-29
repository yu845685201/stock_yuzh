"""
统一 HTTP 采集基类（每日复盘数据采集技术方案 V1.0 §2.3，稳定性核心）

现有 cninfo/tdx 源不迁移；全部新增外网源（sse_szse/csindex/ths/sina）继承本基类。

设计要点：
- 会话：模块级唯一 requests.Session()（连接复用）；headers 每请求注入，源间不串
- 超时：default_timeout 配置 [连接5s, 读30s]，防挂死
- 限速：每源独立 request_interval（串行 sleep），不用 ApiRateLimiter（与 cninfo 惯例一致）
- 重试：请求级 run_with_retry，指数退避 + 随机抖动 ±20%；
        仅对网络类异常/5xx 重试；风控信号（4xx/验证码页/非 JSON）不重试，直接抛 ReviewBlockedError
        ——裸重试是触发封禁升级的主因（baostock/cninfo 先例均已验证此模式）
- 风控识别钩子：_check_risk_control(resp)，子类覆写识别 403/429、验证码页特征串、
  非 JSON 响应等（对照方案 §6.1 矩阵）；基类默认把 4xx 视为风控/不可用信号
- 急停：manager 捕获 ReviewBlockedError → 本轮该源急停（结果 blocked=True），
  冷却 sleep 后可由编排器决定是否重试一次；进度/断点已落盘可续
- 请求前后打 debug 日志（URL、状态码、耗时、字节数），问题可追溯

安全红线（沿用 cninfo_source.py 文件头，对全部新源生效）：
串行限速、正常 UA/Referer、不破解任何技术措施（含同花顺 hexin-v cookie）、
风控识别即退避冷却、仅自用不分发。
"""

import logging
import random
import time
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import requests

from ..utils.retry import run_with_retry

logger = logging.getLogger(__name__)

# 与 cninfo_source 同款 UA 策略（正常浏览器 UA，配置可覆盖）
DEFAULT_UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

# 报错附带的原始响应片段上限（方案 §6.1：结构变更报错必须带原始响应片段）
_SNIPPET_CHARS = 200

# 模块级唯一会话（连接复用；headers 逐请求注入，源间互不影响）
_SESSION = requests.Session()


def response_snippet(resp: requests.Response, limit: int = _SNIPPET_CHARS) -> str:
    """截取响应体片段（日志/异常用），二进制安全"""
    try:
        text = resp.content[:limit * 4].decode(resp.encoding or 'utf-8', errors='replace')
    except Exception:
        text = repr(resp.content[:80])
    return ' '.join(text.split())[:limit]


class ReviewBlockedError(RuntimeError):
    """风控基类异常：该源本轮急停，冷却 cooldown_seconds 后可由编排器决定是否重试一次

    各源子类化（如 ThsBlockedError），子类可覆写默认冷却时长。
    """

    cooldown_seconds: int = 600

    def __init__(self, message: str, cooldown_seconds: Optional[int] = None):
        super().__init__(message)
        if cooldown_seconds is not None:
            self.cooldown_seconds = int(cooldown_seconds)


class HttpCollectorBase:
    """外网 HTTP 采集基类：限速 + 重试 + 风控识别 + debug 日志

    Args:
        config: 全量配置 dict（读取 review_sync 段通用项）
        source_name: 源名（日志与急停标识）
        interval_seconds: 请求间隔（秒，各源从 config.yaml 独立取值后传入）
        referer: Referer（交易所站点强校验，每源必须配置正确）
        ajax: True 时追加 X-Requested-With: XMLHttpRequest（仅 AJAX 类接口）
        headers_override: 追加/覆盖 headers（Cookie 默认不带；THS hexin-v 一律不做——红线）
        blocked_error_class: 该源风控异常类（须为 ReviewBlockedError 子类）
    """

    blocked_error_class: Type[ReviewBlockedError] = ReviewBlockedError

    def __init__(self, config: Dict[str, Any], source_name: str, *,
                 interval_seconds: float = 1.0,
                 referer: Optional[str] = None,
                 ajax: bool = False,
                 headers_override: Optional[Dict[str, str]] = None,
                 blocked_error_class: Optional[Type[ReviewBlockedError]] = None,
                 verify: Optional[str] = None):
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
        if self.request_interval > 0:
            time.sleep(self.request_interval)
        started = time.time()
        resp = _SESSION.request(method, url, params=params, data=data,
                                headers=headers or self.headers,
                                timeout=self.request_timeout, proxies=self._proxies,
                                verify=self.verify)
        elapsed_ms = (time.time() - started) * 1000
        logger.debug(f'[{self.source_name}] {method} {url} -> {resp.status_code} '
                     f'({elapsed_ms:.0f}ms, {len(resp.content)}B)')

        if resp.status_code >= 500:
            # 服务端/网关类异常：网络类失败，允许重试
            resp.raise_for_status()
        if resp.status_code >= 400:
            # 风控信号（4xx）：不重试，直接急停（子类 _check_risk_control 可更精细识别）
            if resp.status_code in allow_status:
                return resp
            raise self._make_blocked(f'HTTP {resp.status_code}')
        self._check_risk_control(resp)
        if expect_json:
            try:
                return resp.json()
            except ValueError as e:
                raise self._make_blocked(
                    f'返回非 JSON（疑似风控/验证码/改版）: {response_snippet(resp)}') from e
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
                      cooldown_seconds: Optional[int] = None) -> ReviewBlockedError:
        return self.blocked_error_class(f'[{self.source_name}] {message}',
                                        cooldown_seconds or self.blocked_cooldown_seconds)
