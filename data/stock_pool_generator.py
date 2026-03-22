# -*- coding: utf-8 -*-
"""
股票池动态生成器

每日自动更新 ETF/LOF 优先池 + A股热点股票池
- ETF/LOF: 趋势分析优先，T+0产品优先，按成交额筛选
- 股票池: 沪深主板（000/001/600/601/603），热点+趋势，中高风险
"""
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass

import pandas as pd
import numpy as np

from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PoolProduct:
    """池产品"""
    code: str
    name: str
    pool_type: str
    t0: bool = False
    amount: float = 0.0
    change_pct: float = 0.0
    score: float = 0.0
    risk_level: str = ""
    sector: str = ""
    reason: str = ""
    trend_score: float = 0.0
    updated_at: str = ""


class StockPoolGenerator:
    """股票池动态生成器"""

    ETF_T0_PREFIXES = ("51", "15")
    LOF_T0_PREFIXES = ("16", "15")
    
    # 沪深主板股票代码前缀（仅保留这些）
    MAIN_BOARD_PREFIXES = ("000", "001", "600", "601", "603")
    # 排除：科创板(688)、创业板(300)、北交所(4xx/8xx/9xx)
    EXCLUDED_PREFIXES = ("688", "300", "430", "830", "872", "4", "8", "9")

    RISK_MAP = {
        "high": ["688"],
        "medium_high": ["002", "300"],
        "medium": ["000", "001", "600", "601", "603"],
    }

    def __init__(self, db_path: str = "./data/recommend.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or "./data", exist_ok=True)

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT,
                pool_type TEXT NOT NULL,
                t0 INTEGER DEFAULT 0,
                amount REAL DEFAULT 0,
                change_pct REAL DEFAULT 0,
                score REAL DEFAULT 0,
                risk_level TEXT DEFAULT '',
                sector TEXT DEFAULT '',
                reason TEXT DEFAULT '',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pool_type ON stock_pool(pool_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_score ON stock_pool(score DESC)")
        conn.commit()
        conn.close()

    def _is_t0_etf(self, code: str) -> bool:
        return code.startswith(self.ETF_T0_PREFIXES)

    def _is_t0_lof(self, code: str) -> bool:
        return code.startswith(self.LOF_T0_PREFIXES)

    def _get_risk_level(self, code: str) -> str:
        for level, prefixes in self.RISK_MAP.items():
            for p in prefixes:
                if code.startswith(p):
                    return level
        return "medium"
    
    def _is_main_board(self, code: str) -> bool:
        """判断是否为沪深主板股票（排除科创/创业/北交所）"""
        code = str(code).strip()
        for p in self.EXCLUDED_PREFIXES:
            if code.startswith(p):
                return False
        for p in self.MAIN_BOARD_PREFIXES:
            if code.startswith(p):
                return True
        return False
    
    def _is_cross_border_etf(self, code: str) -> bool:
        """判断是否为跨境ETF（美股/港股等，受外围市场影响大）"""
        cross_prefixes = (
            "513100", "513500", "513050", "513160", "513080",
            "159631", "159941", "159920", "164824", "000071",
        )
        code = str(code).strip()
        for p in cross_prefixes:
            if code.startswith(p):
                return True
        return False

    def _retry_akshare(self, func, default=None, retries: int = 3):
        """akshare 调用重试"""
        for i in range(retries):
            try:
                return func()
            except Exception as e:
                if i == retries - 1:
                    return default
                import time
                time.sleep(2)

    def _resolve_pool_type(self, code: str) -> str:
        """根据代码推断池类型"""
        code_text = str(code or "").strip()
        if code_text.startswith(("15", "16", "50", "51", "56")):
            return "etf"
        return "stock"

    def _get_strategy_fallback_pool(self) -> List[PoolProduct]:
        """基于持仓与信号池构建回退候选池"""
        self._ensure_table()
        products: List[PoolProduct] = []
        conn = self._get_conn()
        cursor = conn.cursor()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        seen_codes = set()

        try:
            cursor.execute(
                """
                SELECT
                    code,
                    name,
                    SUM(quantity) AS amount_proxy,
                    MAX(target_price) AS target_price,
                    MIN(stop_loss) AS stop_loss
                FROM positions
                WHERE status = 'holding'
                GROUP BY code, name
                """
            )
            for row in cursor.fetchall():
                code = str(row["code"] or "").strip()
                if not code or code in seen_codes:
                    continue
                pool_type = self._resolve_pool_type(code)
                products.append(PoolProduct(
                    code=code,
                    name=str(row["name"] or "").strip(),
                    pool_type=pool_type,
                    t0=pool_type == "etf" and self._is_t0_etf(code),
                    amount=float(row["amount_proxy"] or 0),
                    change_pct=0.0,
                    score=95.0,
                    risk_level="low" if pool_type == "etf" else self._get_risk_level(code),
                    reason="持仓默认池",
                    updated_at=now_str,
                ))
                seen_codes.add(code)

            cursor.execute(
                """
                SELECT code, name, pool_type, signal_type, price, score, reason, updated_at
                FROM signal_pool
                WHERE status IN ('active', 'holding')
                ORDER BY
                    CASE status WHEN 'holding' THEN 1 WHEN 'active' THEN 2 ELSE 9 END,
                    score DESC,
                    updated_at DESC,
                    id DESC
                LIMIT 100
                """
            )
            for row in cursor.fetchall():
                code = str(row["code"] or "").strip()
                if not code or code in seen_codes:
                    continue
                pool_type = str(row["pool_type"] or "").strip() or self._resolve_pool_type(code)
                signal_type = str(row["signal_type"] or "").strip()
                reason_prefix = "信号池默认池"
                if signal_type:
                    reason_prefix = f"{reason_prefix}({signal_type})"
                products.append(PoolProduct(
                    code=code,
                    name=str(row["name"] or "").strip(),
                    pool_type=pool_type,
                    t0=pool_type == "etf" and self._is_t0_etf(code),
                    amount=max(float(row["price"] or 0) * 100000, 0.0),
                    change_pct=0.0,
                    score=max(float(row["score"] or 0), 80.0),
                    risk_level="low" if pool_type == "etf" else self._get_risk_level(code),
                    reason=f"{reason_prefix}: {str(row['reason'] or '').strip()}".strip(": "),
                    updated_at=str(row["updated_at"] or now_str),
                ))
                seen_codes.add(code)
        finally:
            conn.close()

        logger.info(f"基于持仓/信号池生成回退池: {len(products)} 只")
        return products

    def _fetch_etf_pool(self, min_amount: float = 300_000_000) -> List[PoolProduct]:
        """从akshare获取ETF池"""
        products = []
        df = self._retry_akshare(lambda: __import__("akshare").fund_etf_spot_em())
        if df is None or (hasattr(df, "empty") and df.empty):
            return products
        try:
            df.columns = [c.strip() for c in df.columns]
            if "成交额" in df.columns:
                df = df[df["成交额"] >= min_amount]
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip()
                name = str(row.get("名称", "")).strip()
                amount = float(row.get("成交额", 0) or 0)
                change_pct = float(row.get("涨跌幅", 0) or 0)
                if not code:
                    continue
                products.append(PoolProduct(
                    code=code,
                    name=name,
                    pool_type="etf",
                    t0=self._is_t0_etf(code),
                    amount=amount,
                    change_pct=change_pct,
                    risk_level="low",
                    updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ))
            logger.info(f"从akshare获取ETF: {len(products)} 只 (成交额>{min_amount/1e8:.0f}亿)")
        except Exception as e:
            logger.warning(f"解析ETF数据失败: {e}")
        return products

    def _fetch_lof_pool(self, min_amount: float = 200_000_000) -> List[PoolProduct]:
        """从akshare获取LOF池"""
        products = []
        df = self._retry_akshare(lambda: __import__("akshare").fund_lof_spot_em())
        if df is None or (hasattr(df, "empty") and df.empty):
            return products
        try:
            df.columns = [c.strip() for c in df.columns]
            if "成交额" in df.columns:
                df = df[df["成交额"] >= min_amount]
            for _, row in df.iterrows():
                code = str(row.get("代码", "")).strip()
                name = str(row.get("名称", "")).strip()
                amount = float(row.get("成交额", 0) or 0)
                change_pct = float(row.get("涨跌幅", 0) or 0)
                if not code:
                    continue
                products.append(PoolProduct(
                    code=code,
                    name=name,
                    pool_type="lof",
                    t0=self._is_t0_lof(code),
                    amount=amount,
                    change_pct=change_pct,
                    risk_level="low",
                    updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ))
            logger.info(f"从akshare获取LOF: {len(products)} 只 (成交额>{min_amount/1e8:.0f}亿)")
        except Exception as e:
            logger.warning(f"解析LOF数据失败: {e}")
        return products

    def _fetch_hot_stocks(self, top_n: int = 50) -> List[PoolProduct]:
        """获取热点股票 (个股飙升榜 + 资金流排名)"""
        products = []
        seen = set()

        df = self._retry_akshare(lambda: __import__("akshare").stock_hot_up_em())
        if df is not None and not df.empty:
            try:
                df.columns = [c.strip() for c in df.columns]
                for _, row in df.iterrows():
                    code = str(row.get("代码", "")).strip().replace("SH", "").replace("SZ", "")
                    name = str(row.get("股票名称", "")).strip()
                    change_pct = float(row.get("涨跌幅", 0) or 0)
                    if not code or code in seen:
                        continue
                    if not self._is_main_board(code):
                        continue
                    if "ST" in name or "*ST" in name or "S" in name:
                        continue
                    seen.add(code)
                    products.append(PoolProduct(
                        code=code,
                        name=name,
                        pool_type="stock",
                        t0=False,
                        change_pct=change_pct,
                        risk_level=self._get_risk_level(code),
                        reason="个股飙升",
                        updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ))
                logger.info(f"个股飙升榜获取: {len(products)} 只 (仅沪深主板)")
            except Exception as e:
                logger.warning(f"解析个股飙升榜失败: {e}")

        df = self._retry_akshare(lambda: __import__("akshare").stock_individual_fund_flow_rank(indicator="今日"))
        if df is not None and not df.empty:
            try:
                df.columns = [c.strip() for c in df.columns]
                for _, row in df.iterrows():
                    code = str(row.get("代码", "")).strip()
                    name = str(row.get("名称", "")).strip()
                    change_pct = float(row.get("涨跌幅", 0) or 0)
                    if not code or code in seen:
                        continue
                    if not self._is_main_board(code):
                        continue
                    if "ST" in name or "*ST" in name or "S" in name:
                        continue
                    seen.add(code)
                    products.append(PoolProduct(
                        code=code,
                        name=name,
                        pool_type="stock",
                        t0=False,
                        change_pct=change_pct,
                        risk_level=self._get_risk_level(code),
                        reason="资金净流入",
                        updated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ))
                logger.info(f"资金净流入排名获取: {len(products)} 只 (仅沪深主板)")
            except Exception as e:
                logger.warning(f"解析资金流排名失败: {e}")

        df = self._retry_akshare(
            lambda: __import__("akshare").stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        )
        if df is not None and not df.empty:
            try:
                df.columns = [c.strip() for c in df.columns]
                hot_sectors = []
                for _, row in df.iterrows():
                    rank = int(row.get("序号", 0) or 0)
                    if rank > 10:
                        break
                    sector = str(row.get("名称", "")).strip()
                    net_amount = float(row.get("今日主力净流入-净额", 0) or 0)
                    if net_amount > 0 and sector:
                        hot_sectors.append(sector)
                if hot_sectors:
                    logger.info(f"热点行业: {', '.join(hot_sectors)}")
            except Exception as e:
                logger.warning(f"解析行业资金流失败: {e}")

        return products[:top_n]

    def _analyze_etf_trend(self, code: str) -> float:
        """分析ETF趋势打分 (0-40分)"""
        try:
            from .data_source import DataSource
            ds = DataSource()
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
            df = ds.get_kline(code, start, end)
            ds.close()
            if df is None or df.empty or len(df) < 20:
                return 0.0
            
            df = df.tail(30).copy()
            close = df["close"]
            volume = df["volume"]
            
            ma5 = close.rolling(5).mean()
            ma10 = close.rolling(10).mean()
            ma20 = close.rolling(20).mean()
            
            score = 0.0
            
            if ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]:
                score += 15
            
            recent_return = (close.iloc[-1] / close.iloc[-5] - 1) * 100 if len(close) >= 5 else 0
            if recent_return > 3:
                score += 10
            elif recent_return > 0:
                score += 5
            
            vol_ratio = volume.iloc[-1] / volume.tail(5).mean() if len(volume) >= 5 else 1
            if vol_ratio > 1.5:
                score += 10
            elif vol_ratio > 1.2:
                score += 5
            
            if df["close"].iloc[-1] > df["open"].iloc[-1]:
                score += 5
            
            return min(score, 40)
        except Exception:
            return 0.0
    
    def _score_products(self, products: List[PoolProduct]) -> List[PoolProduct]:
        """对产品进行量化打分"""
        for p in products:
            if p.pool_type in ("etf", "lof"):
                score = 0
                if p.t0:
                    score += 30
                score += min(p.amount / 1e9 * 10, 30)
                if self._is_cross_border_etf(p.code):
                    score -= 20
                trend_score = self._analyze_etf_trend(p.code)
                p.trend_score = trend_score
                score += trend_score
                p.score = min(score, 100)
            else:
                score = 0
                if p.change_pct > 9.5:
                    score += 40
                elif p.change_pct > 5:
                    score += 30
                elif p.change_pct > 2:
                    score += 20
                elif p.change_pct > 0:
                    score += 10
                if p.risk_level == "medium_high":
                    score += 20
                if p.reason in ("资金净流入",):
                    score += 15
                if p.reason == "个股飙升":
                    score += 10
                p.score = min(score, 100)
        return products

    def _filter_stock_pool(self, products: List[PoolProduct], max_per_risk: int = 10) -> List[PoolProduct]:
        """过滤股票池，控制中高风险比例"""
        risk_counts = {"high": 0, "medium_high": 0, "medium": 0}
        filtered = []
        for p in sorted(products, key=lambda x: -x.score):
            if risk_counts.get(p.risk_level, 0) >= max_per_risk:
                continue
            risk_counts[p.risk_level] = risk_counts.get(p.risk_level, 0) + 1
            filtered.append(p)
        return filtered

    def generate_etf_lof_pool(self, etf_min_amount: float = 300_000_000, lof_min_amount: float = 200_000_000) -> List[PoolProduct]:
        """生成ETF/LOF池"""
        etfs = self._fetch_etf_pool(etf_min_amount)
        lofs = self._fetch_lof_pool(lof_min_amount)
        products = etfs + lofs
        if not products:
            logger.warning("ETF/LOF API 获取失败，使用持仓/信号池回退候选")
            products = [p for p in self._get_strategy_fallback_pool() if p.pool_type in ("etf", "lof")]
        products = self._score_products(products)
        products.sort(key=lambda x: (-x.score, -x.trend_score, not x.t0))
        return products

    def generate_hot_stock_pool(self, max_stocks: int = 20) -> List[PoolProduct]:
        """生成热点股票池"""
        products = self._fetch_hot_stocks(top_n=100)
        if not products:
            logger.warning("热点股票 API 获取失败，使用持仓/信号池回退候选")
            products = [p for p in self._get_strategy_fallback_pool() if p.pool_type == "stock"]
        products = self._score_products(products)
        products = self._filter_stock_pool(products, max_per_risk=max_stocks // 3 + 1)
        products.sort(key=lambda x: -x.score)
        return products[:max_stocks]

    def save_pool(self, products: List[PoolProduct]):
        """保存股票池到数据库"""
        self._ensure_table()
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stock_pool")
        for p in products:
            cursor.execute("""
                INSERT INTO stock_pool (code, name, pool_type, t0, amount, change_pct, score, risk_level, sector, reason, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (p.code, p.name, p.pool_type, int(p.t0), p.amount, p.change_pct,
                  p.score, p.risk_level, p.sector, p.reason, p.updated_at))
        conn.commit()
        conn.close()
        logger.info(f"股票池已保存: {len(products)} 只")

    def _merge_products(self, existing: List[PoolProduct], new_products: List[PoolProduct]) -> List[PoolProduct]:
        """合并股票池（按代码去重，保留更高分并合并理由）"""
        merged: Dict[str, PoolProduct] = {}

        for product in existing + new_products:
            code = str(product.code).strip()
            if not code:
                continue

            current = merged.get(code)
            if current is None:
                merged[code] = PoolProduct(
                    code=product.code,
                    name=product.name,
                    pool_type=product.pool_type,
                    t0=product.t0,
                    amount=product.amount,
                    change_pct=product.change_pct,
                    score=product.score,
                    risk_level=product.risk_level,
                    sector=product.sector,
                    reason=product.reason,
                    trend_score=product.trend_score,
                    updated_at=product.updated_at,
                )
                continue

            if product.score >= current.score:
                current.name = product.name or current.name
                current.pool_type = product.pool_type or current.pool_type
                current.t0 = bool(product.t0 or current.t0)
                current.amount = max(float(current.amount or 0), float(product.amount or 0))
                current.change_pct = float(product.change_pct or current.change_pct or 0)
                current.score = float(product.score or current.score or 0)
                current.risk_level = product.risk_level or current.risk_level
                current.sector = product.sector or current.sector
                current.trend_score = float(product.trend_score or current.trend_score or 0)
                current.updated_at = product.updated_at or current.updated_at
            else:
                current.amount = max(float(current.amount or 0), float(product.amount or 0))
                current.change_pct = max(float(current.change_pct or 0), float(product.change_pct or 0))
                current.trend_score = max(float(current.trend_score or 0), float(product.trend_score or 0))
                current.updated_at = product.updated_at or current.updated_at

            reasons = []
            for reason in [current.reason, product.reason]:
                text = str(reason or "").strip()
                if text and text not in reasons:
                    reasons.append(text)
            current.reason = " + ".join(reasons)

        products = list(merged.values())
        products.sort(key=lambda x: (-float(x.score or 0), x.pool_type, x.code))
        return products

    def load_pool(self, pool_type: Optional[str] = None, limit: int = 100) -> List[PoolProduct]:
        """从数据库加载股票池"""
        conn = self._get_conn()
        cursor = conn.cursor()
        if pool_type:
            cursor.execute("""
                SELECT * FROM stock_pool WHERE pool_type = ? ORDER BY score DESC LIMIT ?
            """, (pool_type, limit))
        else:
            cursor.execute("SELECT * FROM stock_pool ORDER BY score DESC LIMIT ?", (limit,))
        rows = cursor.fetchall()
        conn.close()
        products = []
        for row in rows:
            products.append(PoolProduct(
                code=row["code"],
                name=row["name"],
                pool_type=row["pool_type"],
                t0=bool(row["t0"]),
                amount=row["amount"] or 0,
                change_pct=row["change_pct"] or 0,
                score=row["score"] or 0,
                risk_level=row["risk_level"] or "",
                sector=row["sector"] or "",
                reason=row["reason"] or "",
                updated_at=row["updated_at"] or "",
            ))
        return products

    def update_daily(self, merge_existing: bool = False) -> Dict[str, List[PoolProduct]]:
        """每日更新股票池"""
        logger.info("=" * 50)
        logger.info("开始每日股票池更新...")
        etf_lof = self.generate_etf_lof_pool()
        hot_stocks = self.generate_hot_stock_pool(max_stocks=20)
        products = etf_lof + hot_stocks

        if merge_existing:
            existing = self.load_pool(limit=500)
            products = self._merge_products(existing, products)
            logger.info(f"股票池采用合并更新: 原有 {len(existing)} 只 -> 合并后 {len(products)} 只")

        self.save_pool(products)
        logger.info(f"更新完成: ETF/LOF {len(etf_lof)} 只, 热点股票 {len(hot_stocks)} 只")
        return {"etf_lof": etf_lof, "stock": hot_stocks, "merged": products}

    def get_pool_summary(self) -> Dict:
        """获取股票池摘要"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT pool_type, COUNT(*) as cnt FROM stock_pool GROUP BY pool_type")
        rows = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) as total, MAX(updated_at) as updated FROM stock_pool")
        meta = cursor.fetchone()
        conn.close()
        summary = {
            "total": meta["total"],
            "updated": meta["updated"],
            "by_type": {},
        }
        for row in rows:
            summary["by_type"][row["pool_type"]] = row["cnt"]
        return summary


_pool_generators: Dict[str, "StockPoolGenerator"] = {}


def get_pool_generator(db_path: str = "./data/recommend.db") -> "StockPoolGenerator":
    global _pool_generators
    normalized_path = str(db_path or "./data/recommend.db")
    if normalized_path not in _pool_generators:
        _pool_generators[normalized_path] = StockPoolGenerator(normalized_path)
    return _pool_generators[normalized_path]
