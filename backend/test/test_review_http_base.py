"""统一 HTTP 采集基类测试（方案 2.3：风控信号识别、重试边界、冷却传播）"""

import sys
from pathlib import Path

import pytest
import requests

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.http_collector_base import HttpCollectorBase, ReviewBlockedError


class FakeResponse:
    def __init__(self, status_code=200, content=b'', json_data=None, encoding='utf-8'):
        self.status_code = status_code
        self.content = content if isinstance(content, bytes) else content.encode('utf-8')
        self.encoding = encoding
        self.headers = {}
        self._json_data = json_data

    def json(self):
        if self._json_data is None:
            raise ValueError('no json')
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f'HTTP {self.status_code}')

    @property
    def text(self):
        return self.content.decode(self.encoding, errors='replace')


class FakeSession:
    """按序返回预设响应，记录请求次数与间隔调用的真实 sleep"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({'method': method, 'url': url, **kwargs})
        if len(self.responses) == 1:
            return self.responses[0]
        return self.responses.pop(0)


def make_collector(interval=0.0, attempts=3, monkeypatch=None):
    collector = HttpCollectorBase(
        {'review_sync': {'retry_attempts': attempts, 'blocked_cooldown_seconds': 77,
                         'default_timeout': [1, 2], 'request_interval': interval}},
        'test', interval_seconds=interval)
    return collector


def test_4xx_not_retried_raises_blocked(monkeypatch):
    collector = make_collector(attempts=3)
    session = FakeSession([FakeResponse(status_code=403, content='forbidden')])
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    with pytest.raises(ReviewBlockedError) as exc_info:
        collector.get_json('http://example.com/api')
    # 风控信号不重试：仅 1 次请求
    assert len(session.calls) == 1
    assert exc_info.value.cooldown_seconds == 77


def test_429_not_retried(monkeypatch):
    collector = make_collector(attempts=3)
    session = FakeSession([FakeResponse(status_code=429)])
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    with pytest.raises(ReviewBlockedError):
        collector.get_json('http://example.com/api')
    assert len(session.calls) == 1


def test_5xx_retried_then_raises_http_error(monkeypatch):
    collector = make_collector(attempts=3)
    session = FakeSession([FakeResponse(status_code=502)])
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    monkeypatch.setattr('time.sleep', lambda s: None)
    with pytest.raises(requests.HTTPError):
        collector.get_json('http://example.com/api')
    assert len(session.calls) == 3  # 网络类/5xx 走满重试


def test_non_json_raises_blocked(monkeypatch):
    collector = make_collector()
    session = FakeSession([FakeResponse(status_code=200, content='<html>验证码</html>')])
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    with pytest.raises(ReviewBlockedError) as exc_info:
        collector.get_json('http://example.com/api')
    assert '非 JSON' in str(exc_info.value)
    # 报错带原始响应片段（方案 6.1：结构变更报错必须带片段）
    assert '验证码' in str(exc_info.value)


def test_network_error_retried(monkeypatch):
    collector = make_collector(attempts=2)
    session = FakeSession([FakeResponse(status_code=200, json_data={'ok': 1})])

    calls = {'n': 0}

    def _request(*args, **kwargs):
        calls['n'] += 1
        if calls['n'] == 1:
            raise requests.ConnectionError('conn reset')
        return session.responses[0]

    session.request = _request
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    monkeypatch.setattr('time.sleep', lambda s: None)
    assert collector.get_json('http://example.com/api') == {'ok': 1}
    assert calls['n'] == 2


def test_retry_exhausted_network_error(monkeypatch):
    collector = make_collector(attempts=2)
    session = FakeSession([FakeResponse(status_code=200, json_data={'ok': 1})])

    def _request(*args, **kwargs):
        raise requests.Timeout('timed out')

    session.request = _request
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    monkeypatch.setattr('time.sleep', lambda s: None)
    with pytest.raises(requests.Timeout):
        collector.get_json('http://example.com/api')


def test_risk_control_hook_called(monkeypatch):
    collector = make_collector()

    def _hook(resp):
        raise collector._make_blocked('hook 命中')

    collector._check_risk_control = _hook
    session = FakeSession([FakeResponse(status_code=200, json_data={'ok': 1})])
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    with pytest.raises(ReviewBlockedError) as exc_info:
        collector.get_json('http://example.com/api')
    assert 'hook 命中' in str(exc_info.value)


def test_allow_status_passthrough(monkeypatch):
    collector = make_collector()
    session = FakeSession([FakeResponse(status_code=404, content='not found')])
    monkeypatch.setattr('src.data_sources.http_collector_base._SESSION', session)
    resp = collector.get_text('http://example.com/file', allow_status=(404,))
    assert resp == 'not found'
