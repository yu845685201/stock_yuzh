"""
数据模型定义
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any


@dataclass
class BaseStockInfo:
    """股票基本信息模型 - 严格按照文档要求"""
    id: Optional[int] = None
    ts_code: Optional[str] = None
    stock_code: str = ""
    stock_name: Optional[str] = None
    cnspell: Optional[str] = None
    market_code: Optional[str] = None
    market_name: Optional[str] = None
    exchange_code: Optional[str] = None
    sector_code: Optional[str] = None
    sector_name: Optional[str] = None
    industry_code: Optional[str] = None
    industry_name: Optional[str] = None
    list_status: str = 'L'  # L-上市，D-退市，P-暂停上市
    list_date: Optional[date] = None
    delist_date: Optional[date] = None
    type: Optional[str] = None  # 1：股票，2：指数，3：其它，4：可转债，5：ETF
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseStockInfo':
        """从字典创建BaseStockInfo对象"""
        return cls(
            id=data.get('id'),
            ts_code=data.get('ts_code'),
            stock_code=data.get('stock_code') or data.get('code', ''),
            stock_name=data.get('stock_name') or data.get('name') or data.get('code_name'),
            cnspell=data.get('cnspell'),
            market_code=data.get('market_code'),
            market_name=data.get('market_name'),
            exchange_code=data.get('exchange_code'),
            sector_code=data.get('sector_code'),
            sector_name=data.get('sector_name'),
            industry_code=data.get('industry_code'),
            industry_name=data.get('industry_name') or data.get('industry'),
            list_status=data.get('list_status', 'L'),
            list_date=data.get('list_date'),
            delist_date=data.get('delist_date'),
            type=data.get('type'),
            create_time=data.get('create_time'),
            update_time=data.get('update_time')
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'ts_code': self.ts_code,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'cnspell': self.cnspell,
            'market_code': self.market_code,
            'market_name': self.market_name,
            'exchange_code': self.exchange_code,
            'sector_code': self.sector_code,
            'sector_name': self.sector_name,
            'industry_code': self.industry_code,
            'industry_name': self.industry_name,
            'list_status': self.list_status,
            'list_date': self.list_date,
            'delist_date': self.delist_date,
            'type': self.type,
            'create_time': self.create_time,
            'update_time': self.update_time
        }


# 为了向后兼容，保留Stock别名
Stock = BaseStockInfo
