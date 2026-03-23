# -*- coding: utf-8 -*-
"""
股票池管理
"""
from typing import Dict, List, Optional, Set

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
        for symbol in symbols:
            code = str(symbol).zfill(6)
            if code not in self._excluded:
                self._stocks.add(code)
        return self

    def add_with_metadata(self, products: List[dict]) -> "StockPool":
        """带元数据添加产品"""
        for item in products:
            code = str(item.get("code", "")).zfill(6)
            if code not in self._excluded:
                self._stocks.add(code)
                self._metadata[code] = item
        return self

    def remove(self, symbols: List[str]) -> "StockPool":
        """从池中移除股票"""
        for symbol in symbols:
            code = str(symbol).zfill(6)
            self._stocks.discard(code)
            self._metadata.pop(code, None)
        return self

    def exclude(self, symbols: List[str]) -> "StockPool":
        """排除股票"""
        for symbol in symbols:
            code = str(symbol).zfill(6)
            self._stocks.discard(code)
            self._excluded.add(code)
        return self

    def filter_by_volume(self, min_volume: float = 0) -> "StockPool":
        """按成交额过滤"""
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
        """仅保留 T+0 产品"""
        if prefer_t0:
            filtered = {key: value for key, value in self._metadata.items() if value.get("t0", False)}
            self._metadata = filtered
            self._stocks = set(filtered.keys())
        return self

    def get_t0_products_first(self) -> List[dict]:
        """获取 T+0 优先排序结果"""
        products = list(self._metadata.values())
        products.sort(key=lambda item: (not item.get("t0", False), -item.get("amount", 0)))
        return products

    def filter_by_pool_type(self, pool_type: str) -> "StockPool":
        """按池类型过滤"""
        filtered = {key: value for key, value in self._metadata.items() if value.get("pool_type") == pool_type}
        self._metadata = filtered
        self._stocks = set(filtered.keys())
        return self

    def filter_by_risk(self, max_risk: str = "high") -> "StockPool":
        """按风险等级过滤"""
        risk_order = ["low", "medium", "medium_high", "high"]
        if max_risk not in risk_order:
            return self
        max_idx = risk_order.index(max_risk)
        filtered = {
            key: value
            for key, value in self._metadata.items()
            if risk_order.index(value.get("risk_level", "medium")) <= max_idx
        }
        self._metadata = filtered
        self._stocks = set(filtered.keys())
        return self

    def filter_by_market_cap(self, min_cap: float = 0) -> "StockPool":
        """按市值过滤"""
        return self

    def get_all(self) -> List[str]:
        """获取全部代码"""
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
        pool.add(["600000", "600016", "600019", "600028", "600030", "600031", "600036", "600048", "600050", "600104"])
    elif name == "cyb":
        pool.add([str(i).zfill(6) for i in range(300001, 300900)])
    elif name == "kcb":
        pool.add([str(i).zfill(6) for i in range(688001, 688900)])
    elif name == "etf_lof" and data_source is not None:
        products = data_source.get_etf_lof_pool(min_amount=300000000, prefer_t0=True)
        pool.add_with_metadata(products)
        logger.info(f"Loaded ETF/LOF pool: {len(pool)} products")

    return pool


def get_dynamic_pool(pool_type: str = "all", limit: int = 50, db_path: Optional[str] = None) -> StockPool:
    """
    从数据库获取动态股票池
    """
    from .stock_pool_generator import get_pool_generator

    pool = StockPool()
    generator = get_pool_generator(db_path)

    if pool_type in ("etf_lof", "etf", "lof"):
        products = generator.load_pool(pool_type=pool_type, limit=limit)
        pool.add_with_metadata(
            [
                {
                    "code": item.code,
                    "name": item.name,
                    "amount": item.amount,
                    "t0": item.t0,
                    "score": item.score,
                    "risk_level": item.risk_level,
                    "reason": item.reason,
                }
                for item in products
            ]
        )
    elif pool_type == "stock":
        products = generator.load_pool(pool_type="stock", limit=limit)
        pool.add_with_metadata(
            [
                {
                    "code": item.code,
                    "name": item.name,
                    "amount": item.amount,
                    "t0": item.t0,
                    "score": item.score,
                    "risk_level": item.risk_level,
                    "reason": item.reason,
                }
                for item in products
            ]
        )
    else:
        products = generator.load_pool(limit=limit)
        pool.add_with_metadata(
            [
                {
                    "code": item.code,
                    "name": item.name,
                    "amount": item.amount,
                    "t0": item.t0,
                    "pool_type": item.pool_type,
                    "score": item.score,
                    "risk_level": item.risk_level,
                    "reason": item.reason,
                }
                for item in products
            ]
        )

    logger.info(f"Loaded dynamic pool [{pool_type}]: {len(pool)} products")
    return pool
