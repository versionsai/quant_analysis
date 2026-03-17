# -*- coding: utf-8 -*-
"""
股票池管理
"""
from typing import List, Optional, Set, Dict
from utils.logger import get_logger

logger = get_logger(__name__)


class StockPool:
    """股票池管理"""
    
    def __init__(self):
        self._stocks: Set[str] = set()
        self._excluded: Set[str] = set()
        self._metadata: Dict[str, dict] = {}
    
    def add(self, symbols: List[str]) -> "StockPool":
        """添加股票到池中"""
        for s in symbols:
            code = str(s).zfill(6)
            if code not in self._excluded:
                self._stocks.add(code)
        return self
    
    def add_with_metadata(self, products: List[dict]) -> "StockPool":
        """带元数据添加产品"""
        for p in products:
            code = str(p.get("code", "")).zfill(6)
            if code not in self._excluded:
                self._stocks.add(code)
                self._metadata[code] = p
        return self
    
    def remove(self, symbols: List[str]) -> "StockPool":
        """从池中移除股票"""
        for s in symbols:
            code = str(s).zfill(6)
            self._stocks.discard(code)
            self._metadata.pop(code, None)
        return self
    
    def exclude(self, symbols: List[str]) -> "StockPool":
        """排除股票"""
        for s in symbols:
            code = str(s).zfill(6)
            self._stocks.discard(code)
            self._excluded.add(code)
        return self
    
    def filter_by_volume(self, min_volume: float = 0) -> "StockPool":
        """按成交量过滤"""
        if min_volume <= 0:
            return self
        filtered = {}
        for code, meta in self._metadata.items():
            if meta.get("amount", 0) >= min_volume:
                filtered[code] = meta
        self._metadata = filtered
        self._stocks = set(filtered.keys())
        return self
    
    def filter_t0_only(self, prefer_t0: bool = True) -> "StockPool":
        """过滤T+0产品"""
        if prefer_t0:
            filtered = {k: v for k, v in self._metadata.items() if v.get("t0", False)}
            self._metadata = filtered
            self._stocks = set(filtered.keys())
        return self
    
    def get_t0_products_first(self) -> List[dict]:
        """获取T+0产品优先的排序列表"""
        products = list(self._metadata.values())
        products.sort(key=lambda x: (not x.get("t0", False), -x.get("amount", 0)))
        return products
    
    def filter_by_market_cap(self, min_cap: float = 0) -> "StockPool":
        """按市值过滤"""
        return self
    
    def get_all(self) -> List[str]:
        """获取所有股票"""
        return sorted(list(self._stocks))
    
    def get_metadata(self, symbol: str) -> Optional[dict]:
        """获取产品元数据"""
        return self._metadata.get(str(symbol).zfill(6))
    
    def __len__(self) -> int:
        return len(self._stocks)
    
    def __contains__(self, symbol: str) -> bool:
        return str(symbol).zfill(6) in self._stocks


def get_st_pool(name: str, data_source=None) -> StockPool:
    """获取预设股票池"""
    pool = StockPool()
    
    if name == "all_a":
        pool.add([str(i).zfill(6) for i in range(1, 7000)])
    elif name == "sh50":
        pool.add(["600000", "600016", "600019", "600028", "600030",
                  "600031", "600036", "600048", "600050", "600104"])
    elif name == "cyb":
        pool.add([str(i).zfill(6) for i in range(300001, 300900)])
    elif name == "kcb":
        pool.add([str(i).zfill(6) for i in range(688001, 688900)])
    elif name == "etf_lof" and data_source is not None:
        products = data_source.get_etf_lof_pool(min_amount=300000000, prefer_t0=True)
        pool.add_with_metadata(products)
        logger.info(f"加载ETF/LOF股票池: {len(pool)} 只产品")
    
    return pool
