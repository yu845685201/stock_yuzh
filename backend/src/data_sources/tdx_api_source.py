"""
Tdx API数据源实现 - 用于获取K线数据
"""

import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

from .base import DataSourceBase
from ..utils.api_rate_limiter import ApiRateLimiter
from ..utils.retry import run_with_retry

logger = logging.getLogger(__name__)

# 本地中间件主机名：这些地址必须绕过系统代理
# （沙箱/CI 环境常注入 HTTP_PROXY，会把 127.0.0.1 的请求也劫持到代理，导致大面积 ProxyError）
_LOCAL_HOSTS = {'127.0.0.1', 'localhost', '::1', '0.0.0.0'}


class TdxApiBusinessError(RuntimeError):
    """中间件业务层错误（code=-1）：调用方应视为瞬时故障，与"查询成功但结果为空"区分"""


class TdxApiSource(DataSourceBase):
    """Tdx API数据源，获取K线数据"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = (config.get('base_url') or '').rstrip('/')
        self.timeout = config.get('timeout', 30)
        # 本地中间件地址：请求显式绕过系统代理（见 _LOCAL_HOSTS 注释）
        self._bypass_proxy = urlparse(self.base_url).hostname in _LOCAL_HOSTS
        # 中间件增强路径开关（V3.0 M-C）：true 时尾部/全量请求走 /api/kline-qfq、/api/kline-recent 新接口
        self.use_enhanced_api = bool(config.get('use_enhanced_api', False))

        rate_limit_config = config.get('rate_limit', {})
        if rate_limit_config.get('enabled', True):
            self.rate_limiter = ApiRateLimiter(
                calls_per_period=rate_limit_config.get('calls_per_period', 50),
                sleep_duration=rate_limit_config.get('sleep_duration', 1.0),
                enabled=True
            )
        else:
            self.rate_limiter = ApiRateLimiter(enabled=False)

    def connect(self) -> bool:
        """HTTP API无需显式连接"""
        self._connected = True
        return True

    def disconnect(self) -> None:
        """HTTP API无需显式断开"""
        self._connected = False

    def get_stock_list(self) -> List[Dict[str, Any]]:
        """Tdx API不提供股票列表"""
        return []

    def get_financial_data(self, code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
        """Tdx API不提供基本面数据"""
        return None

    def _request(self, path: str, params: Dict[str, Any], retries: int = 3) -> Optional[Any]:
        if not self.base_url:
            logger.error("Tdx API base_url未配置")
            return None

        url = f"{self.base_url}{path}"

        def _attempt():
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()
            # 本地中间件不走系统代理：显式传 proxies，避免被 HTTP_PROXY 劫持
            resp = requests.get(
                url, params=params, timeout=self.timeout,
                proxies={'http': None, 'https': None} if self._bypass_proxy else None
            )
            resp.raise_for_status()
            return resp.json()

        def _on_error(attempt, e):
            logger.warning(f"Tdx API请求失败(第{attempt}次): {url} params={params} error={e}")

        try:
            data = run_with_retry(
                _attempt,
                attempts=retries,
                backoff=lambda attempt: min(2 ** attempt, 8),
                on_error=_on_error
            )
        except Exception as e:
            logger.error(f"Tdx API重试耗尽: {url} params={params} error={e}")
            return None

        if isinstance(data, dict) and data.get('code') not in (0, None):
            # 中间件业务层错误（code=-1）
            logger.warning(f"Tdx API业务错误: {url} params={params} message={data.get('message')}")
            return None

        if isinstance(data, dict):
            if 'data' in data:
                payload = data['data']
                if isinstance(payload, dict):
                    if 'List' in payload:
                        return payload['List']
                    if 'list' in payload:
                        return payload['list']
                return payload
            if 'result' in data:
                payload = data['result']
                if isinstance(payload, dict):
                    if 'List' in payload:
                        return payload['List']
                    if 'list' in payload:
                        return payload['list']
                return payload
        return data

    def get_kline_qfq_full(self, code: str, refresh: bool = False) -> List[Dict[str, Any]]:
        """前复权日K全量

        - 增强路径（use_enhanced_api=true）：/api/kline-qfq（缓存优先；refresh=true 时
          绕过缓存回源并重建，调用方除权检测命中后的重拉必须传 refresh=true）
        - 传统路径：/api/kline type=day（同花顺源，每次全量回源）
        """
        if self.use_enhanced_api:
            params = {'code': code, 'type': 'day'}
            if refresh:
                params['refresh'] = 1
            data = self._request('/api/kline-qfq', params)
        else:
            data = self._request('/api/kline', {'code': code, 'type': 'day'})
        return data if isinstance(data, list) else []

    def get_kline_raw_full(self, code: str) -> List[Dict[str, Any]]:
        """原始（不复权）日K全量（/api/kline-all type=day，含真实 amount）"""
        data = self._request('/api/kline-all', {'code': code, 'type': 'day'})
        return data if isinstance(data, list) else []

    def get_kline_qfq_tail(self, code: str, limit: int = 5) -> List[Dict[str, Any]]:
        """前复权日K尾部

        - 增强路径：/api/kline-qfq?limit=N（缓存命中约100ms）
        - 传统路径：/api/kline-history（日期参数中间件未实现，仅最近N条，服务端全量回源）
        """
        if self.use_enhanced_api:
            data = self._request('/api/kline-qfq', {'code': code, 'type': 'day', 'limit': limit})
        else:
            data = self._request('/api/kline-history', {'code': code, 'type': 'day', 'limit': limit})
        return data if isinstance(data, list) else []

    def get_kline_raw_tail(self, code: str, limit: int = 5) -> List[Dict[str, Any]]:
        """原始日K尾部（含真实 amount）

        - 增强路径：/api/kline-recent?limit=N（单次协议请求，约50ms）
        - 传统路径：/api/kline-all?limit=N（服务端全量拼接后截取）
        """
        if self.use_enhanced_api:
            data = self._request('/api/kline-recent', {'code': code, 'type': 'day', 'limit': limit})
        else:
            data = self._request('/api/kline-all', {'code': code, 'type': 'day', 'limit': limit})
        return data if isinstance(data, list) else []

    def get_kline_all(self, code: str, kline_type: str = 'minute1') -> List[Dict[str, Any]]:
        """调用 /api/kline-all 获取全量K线"""
        params = {
            'code': code,
            'type': kline_type
        }
        data = self._request('/api/kline-all', params)
        return data if isinstance(data, list) else []

    def get_kline_history(
        self,
        code: str,
        kline_type: str = 'minute1',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 800
    ) -> List[Dict[str, Any]]:
        """调用 /api/kline-history 获取区间K线"""
        params = {
            'code': code,
            'type': kline_type,
            'limit': limit
        }
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date

        data = self._request('/api/kline-history', params)
        return data if isinstance(data, list) else []

    def fetch_kline_all(self, code: str, kline_type: str = 'minute1') -> List[Dict[str, Any]]:
        return self.get_kline_all(code, kline_type)

    def fetch_kline_range(
        self,
        code: str,
        kline_type: str = 'minute1',
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 800
    ) -> List[Dict[str, Any]]:
        return self.get_kline_history(code, kline_type, start_date=start_date, end_date=end_date, limit=limit)

    # ---------- 每日复盘数据采集扩展（方案 4.1/4.2/4.10；tdx-api 侧对应新增只读接口见 3.2/3.3）----------

    def get_index_kline_all(self, code: str, kline_type: str = 'day') -> List[Dict[str, Any]]:
        """指数日K全量历史（/api/index/all，Kline 含 UpCount/DownCount，仅指数有效）"""
        data = self._request('/api/index/all', {'code': code, 'type': kline_type})
        return data if isinstance(data, list) else []

    def get_index_kline_tail(self, code: str, limit: int = 5, kline_type: str = 'day') -> List[Dict[str, Any]]:
        """指数日K尾部增量（/api/index，limit=N）"""
        data = self._request('/api/index', {'code': code, 'type': kline_type, 'limit': limit})
        return data if isinstance(data, list) else []

    def get_ths_factor(self, code: str) -> List[Dict[str, Any]]:
        """复权因子全历史（/api/ths-factor，THS 同源；[{date, q_factor, h_factor}]）"""
        data = self._request('/api/ths-factor', {'code': code})
        return data if isinstance(data, list) else []

    def get_workday_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """区间交易日序列（/api/workday/range；[{iso, numeric}]，仅返回交易日）

        ⚠️ 仅可用于 K线已覆盖区间的尾部补全：中间件对不在K线缓存里的工作日统一
        走"周末排除+硬编码节假日表（仅 2025/2026）"推断，历史节假日会被误判为
        交易日（实测 1991-2024 年均 ~16-24 天误报）。历史交易日真值必须用
        get_trade_calendar 的指数K线口径
        """
        data = self._request('/api/workday/range', {'start': start_date, 'end': end_date})
        return data if isinstance(data, list) else []

    def get_trade_calendar(self, start_year: int, end_year: int) -> List[Dict[str, Any]]:
        """交易日历全日历口径（与原 baostock query_trade_dates 同构，YYYY-MM-DD）

        真值口径（2026-10-25 实测对拍定稿）：
        - ≤ 指数最新bar：sh000001+sz399001 日K日期并集 = 交易日（K线有 bar 即真实
          开市；比 baostock 更准——1991 年深交所周六开市的 45 个交易日 baostock 漏标）
        - > 最新bar 且 ≤ 2026-12-31：中间件 workday 推断（周末+2026 节假日表，
          与官方日历实测零差异）
        - > 2026-12-31：节假日表未覆盖，不落行（等指数K线到达后下次同步修正）；
          因此 end_year 限当年+1
        """
        from datetime import date as _date, timedelta as _timedelta

        def _bar_days(code: str) -> set:
            days = set()
            for x in self.get_index_kline_all(code, 'day'):
                t = str((x or {}).get('Time') or '')[:10]
                if len(t) == 10:
                    days.add(t)
            return days

        index_days = _bar_days('sh000001') | _bar_days('sz399001')
        if not index_days:
            return []
        last_bar = _date.fromisoformat(max(index_days))

        # 尾部（K线未覆盖）用 workday 节假日表推断，仅信任 2026 内（表覆盖范围）
        table_horizon = _date(2026, 12, 31)
        tail_days = set()
        tail_start = last_bar + _timedelta(days=1)
        if tail_start <= table_horizon:
            tail = self.get_workday_range(tail_start.isoformat(), table_horizon.isoformat())
            tail_days = {x.get('iso') for x in tail if isinstance(x, dict) and x.get('iso')}

        walk_start = max(_date(start_year, 1, 1), _date(1990, 12, 19))  # 沪所开市日
        walk_end = _date(end_year, 12, 31)
        one_day = _timedelta(days=1)

        calendar = []
        d = walk_start
        while d <= walk_end:
            iso = d.isoformat()
            if d <= last_bar:
                flag = iso in index_days
            elif d <= table_horizon:
                flag = iso in tail_days
            else:
                d += one_day
                continue  # 节假日表未覆盖的未来区间不落行
            calendar.append({'calendar_date': iso, 'is_trading_day': 1 if flag else 0})
            d += one_day
        return calendar

    def get_market_stock_codes(self) -> List[Dict[str, Any]]:
        """全市场股票代码快照（/api/codes?exchange=all；[{code, name, exchange}]）

        中间件每日 09:00 从 TDX 0x0450 自动刷新，请求走本地 SQLite 缓存；
        仅含在市证券（退市股不在列表），type 按代码前缀推断
        """
        data = self._request('/api/codes', {'exchange': 'all'})
        if isinstance(data, dict):
            codes = data.get('codes')
            return codes if isinstance(codes, list) else []
        return data if isinstance(data, list) else []

    def get_xdxr(self, code: str, limit: int = 0) -> List[Dict[str, Any]]:
        """除权除息事件（/api/xdxr；limit=0 全历史，>0 取尾部 N 条）

        ⚠️ 业务错误与空事件都返回 []（_request 把 code=-1 归为 None）——批量采集
        需区分二者时用 get_xdxr_strict
        """
        params: Dict[str, Any] = {'code': code}
        if limit:
            params['limit'] = limit
        data = self._request('/api/xdxr', params)
        return data if isinstance(data, list) else []

    def get_xdxr_strict(self, code: str, limit: int = 0) -> List[Dict[str, Any]]:
        """同 get_xdxr，但中间件业务错误（code=-1）抛 TdxApiBusinessError

        全量重导等批量场景必须用本方法：TDX 链路断开时中间件持续返回业务错误，
        与"该股确实无事件"的空列表混在一起会把大批股票误记为 no_data
        （2026-10-25 全量重导实测：连接断死 90 分钟无熔断）"""
        if not self.base_url:
            raise TdxApiBusinessError("Tdx API base_url未配置")
        url = f"{self.base_url}/api/xdxr"
        params: Dict[str, Any] = {'code': code}
        if limit:
            params['limit'] = limit

        def _attempt():
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()
            resp = requests.get(
                url, params=params, timeout=self.timeout,
                proxies={'http': None, 'https': None} if self._bypass_proxy else None
            )
            resp.raise_for_status()
            return resp.json()

        try:
            data = run_with_retry(
                _attempt, attempts=2, backoff=lambda attempt: min(2 ** attempt, 8),
                on_error=lambda attempt, e: logger.warning(f"Tdx API请求失败(第{attempt}次): {url} params={params} error={e}")
            )
        except Exception as e:
            raise TdxApiBusinessError(f"网络层失败: {e}") from e

        if not isinstance(data, dict) or data.get('code') not in (0, None):
            raise TdxApiBusinessError(data.get('message') or '未知业务错误')
        payload = (data.get('data') or {})
        lst = payload.get('list') if isinstance(payload, dict) else None
        return lst if isinstance(lst, list) else []

    def get_minute_trade_history(self, code: str, trade_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """全天分时成交（/api/minute-trade-all；不带 date 取当日，带 date 取历史全天，
        中间件内部已分页聚合全量，Go 侧单请求 2s 超时【已验证】）"""
        params: Dict[str, Any] = {'code': code}
        if trade_date:
            params['date'] = trade_date
        data = self._request('/api/minute-trade-all', params)
        return data if isinstance(data, list) else []
