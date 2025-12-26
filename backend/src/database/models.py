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
            'create_time': self.create_time,
            'update_time': self.update_time
        }


# 为了向后兼容，保留Stock别名
Stock = BaseStockInfo


@dataclass
class HisKlineDay:
    """历史日K线数据模型 - 严格按照文档要求"""
    id: Optional[int] = None
    ts_code: Optional[str] = None
    stock_code: str = ""
    stock_name: Optional[str] = None
    trade_date: date = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    preclose: Optional[float] = None
    volume: Optional[int] = None
    amount: Optional[float] = None
    trade_status: Optional[int] = None  # 1：正常交易，0：停牌
    is_st: Optional[bool] = None  # True：是ST股，False：否
    adjust_flag: Optional[int] = None  # 1：后复权，2：前复权，3：不复权
    change_rate: Optional[float] = None
    turnover_rate: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb_rate: Optional[float] = None
    ps_ttm: Optional[float] = None
    pcf_ttm: Optional[float] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HisKlineDay':
        """从字典创建HisKlineDay对象"""
        return cls(
            id=data.get('id'),
            ts_code=data.get('ts_code'),
            stock_code=data.get('stock_code') or data.get('code', ''),
            stock_name=data.get('stock_name') or data.get('name'),
            trade_date=data.get('trade_date') or data.get('date'),
            open=data.get('open'),
            high=data.get('high'),
            low=data.get('low'),
            close=data.get('close'),
            preclose=data.get('preclose'),
            volume=data.get('volume'),
            amount=data.get('amount'),
            trade_status=data.get('trade_status'),
            is_st=data.get('is_st'),
            adjust_flag=data.get('adjust_flag'),
            change_rate=data.get('change_rate') or data.get('pct_chg') or data.get('pctChg'),
            turnover_rate=data.get('turnover_rate') or data.get('turn'),
            pe_ttm=data.get('pe_ttm'),
            pb_rate=data.get('pb_rate'),
            ps_ttm=data.get('ps_ttm'),
            pcf_ttm=data.get('pcf_ttm'),
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
            'trade_date': self.trade_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'preclose': self.preclose,
            'volume': self.volume,
            'amount': self.amount,
            'trade_status': self.trade_status,
            'is_st': self.is_st,
            'adjust_flag': self.adjust_flag,
            'change_rate': self.change_rate,
            'turnover_rate': self.turnover_rate,
            'pe_ttm': self.pe_ttm,
            'pb_rate': self.pb_rate,
            'ps_ttm': self.ps_ttm,
            'pcf_ttm': self.pcf_ttm,
            'create_time': self.create_time,
            'update_time': self.update_time
        }


# 为了向后兼容，保留DailyData别名
DailyData = HisKlineDay




@dataclass
class HisKline5Min:
    """历史5分钟K线数据模型 - 严格按照文档要求"""
    id: Optional[int] = None
    ts_code: Optional[str] = None
    stock_code: str = ""
    stock_name: Optional[str] = None
    trade_date: date = None
    trade_time: Optional[datetime.time] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    preclose: Optional[float] = None
    volume: Optional[int] = None
    amount: Optional[float] = None
    adjust_flag: Optional[int] = None  # 1：后复权，2：前复权，3：不复权
    change_rate: Optional[float] = None
    turnover_rate: Optional[float] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HisKline5Min':
        """从字典创建HisKline5Min对象"""
        return cls(
            id=data.get('id'),
            ts_code=data.get('ts_code'),
            stock_code=data.get('stock_code') or data.get('code', ''),
            stock_name=data.get('stock_name') or data.get('name'),
            trade_date=data.get('trade_date') or data.get('date'),
            trade_time=data.get('trade_time'),
            open=data.get('open'),
            high=data.get('high'),
            low=data.get('low'),
            close=data.get('close'),
            preclose=data.get('preclose'),
            volume=data.get('volume'),
            amount=data.get('amount'),
            adjust_flag=data.get('adjust_flag'),
            change_rate=data.get('change_rate') or data.get('pct_chg') or data.get('pctChg'),
            turnover_rate=data.get('turnover_rate') or data.get('turn'),
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
            'trade_date': self.trade_date,
            'trade_time': self.trade_time,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'preclose': self.preclose,
            'volume': self.volume,
            'amount': self.amount,
            'adjust_flag': self.adjust_flag,
            'change_rate': self.change_rate,
            'turnover_rate': self.turnover_rate,
            'create_time': self.create_time,
            'update_time': self.update_time
        }


# 为了向后兼容，保留Min5Data别名
Min5Data = HisKline5Min