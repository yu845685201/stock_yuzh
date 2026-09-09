"""
Tdx API数据源实现 - 用于获取K线数据
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from .base import DataSourceBase
from ..utils.api_rate_limiter import ApiRateLimiter

logger = logging.getLogger(__name__)


class TdxApiSource(DataSourceBase):
    """Tdx API数据源，获取K线数据"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.base_url = (config.get('base_url') or '').rstrip('/')
        self.timeout = config.get('timeout', 30)
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
        last_error = None
        for attempt in range(1, retries + 1):
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            try:
                resp = requests.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                last_error = e
                logger.warning(f"Tdx API请求失败(第{attempt}次): {url} params={params} error={e}")
                if attempt < retries:
                    time.sleep(min(2 ** attempt, 8))
                continue

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

        logger.error(f"Tdx API重试耗尽: {url} params={params} error={last_error}")
        return None

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
