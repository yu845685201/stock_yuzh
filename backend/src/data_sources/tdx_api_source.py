"""
Tdx API数据源实现 - 用于获取K线数据
"""

import logging
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

    def _request(self, path: str, params: Dict[str, Any]) -> Optional[Any]:
        if not self.base_url:
            logger.error("Tdx API base_url未配置")
            return None

        if self.rate_limiter:
            self.rate_limiter.wait_if_needed()

        url = f"{self.base_url}{path}"
        try:
            resp = requests.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Tdx API请求失败: {url} params={params} error={e}")
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
