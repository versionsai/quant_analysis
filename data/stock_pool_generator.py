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

    DEFAULT_ETF_POOL = [
        {"code": "511880", "name": "银华日利ETF", "t0": True},
        {"code": "511990", "name": "华宝添益ETF", "t0": True},
        {"code": "511010", "name": "上证50ETF", "t0": False},
        {"code": "510300", "name": "沪深300ETF", "t0": False},
        {"code": "512480", "name": "半导体ETF", "t0": False},
        {"code": "515790", "name": "光伏ETF", "t0": False},
        {"code": "515000", "name": "智能制造ETF", "t0": False},
        {"code": "513050", "name": "中概互联网ETF", "t0": False},
        {"code": "513100", "name": "纳指ETF", "t0": False},
        {"code": "513500", "name": "标普500ETF", "t0": False},
        {"code": "518880", "name": "黄金ETF", "t0": True},
        {"code": "512400", "name": "有色金属ETF", "t0": False},
        {"code": "512050", "name": "中证科技ETF", "t0": False},
        {"code": "159915", "name": "创业板ETF易方达", "t0": False},
        {"code": "159919", "name": "沪深300ETF嘉实", "t0": False},
        {"code": "159995", "name": "芯片ETF", "t0": False},
        {"code": "515050", "name": "5GETF", "t0": False},
        {"code": "515220", "name": "医疗ETF", "t0": False},
        {"code": "159928", "name": "中证消费ETF", "t0": False},
        {"code": "512690", "name": "酒ETF", "t0": False},
    ]

    DEFAULT_STOCK_POOL = [
        {"code": "300750", "name": "宁德时代"},
        {"code": "300059", "name": "东方财富"},
        {"code": "002594", "name": "比亚迪"},
        {"code": "600519", "name": "贵州茅台"},
        {"code": "600036", "name": "招商银行"},
        {"code": "601318", "name": "中国平安"},
        {"code": "000858", "name": "五粮液"},
        {"code": "002415", "name": "海康威视"},
        {"code": "601012", "name": "隆基绿能"},
        {"code": "300015", "name": "爱尔眼科"},
        {"code": "300760", "name": "迈瑞医疗"},
        {"code": "002475", "name": "立讯精密"},
        {"code": "603259", "name": "药明康德"},
        {"code": "300124", "name": "汇川技术"},
        {"code": "002371", "name": "北方华创"},
        {"code": "688981", "name": "中芯国际"},
        {"code": "688256", "name": "寒武纪"},
        {"code": "300408", "name": "三环集团"},
        {"code": "002456", "name": "欧菲光"},
        {"code": "300274", "name": "阳光电源"},
    ]

    def _get_default_pool(self) -> List[PoolProduct]:
        """获取默认股票池（API不可用时）"""
        products = []
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in self.DEFAULT_ETF_POOL:
            products.append(PoolProduct(
                code=item["code"],
                name=item["name"],
                pool_type="etf",
                t0=item.get("t0", False),
                amount=5e8,
                change_pct=0.0,
                risk_level="low",
                reason="默认池",
                updated_at=now_str,
            ))
        for item in self.DEFAULT_STOCK_POOL:
            products.append(PoolProduct(
                code=item["code"],
                name=item["name"],
                pool_type="stock",
                t0=False,
                amount=5e8,
                change_pct=0.0,
                risk_level=self._get_risk_level(item["code"]),
                reason="默认池",
                updated_at=now_str,
            ))
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
            logger.warning("ETF/LOF API 获取失败，使用默认池")
            products = [p for p in self._get_default_pool() if p.pool_type in ("etf", "lof")]
        products = self._score_products(products)
        products.sort(key=lambda x: (-x.score, -x.trend_score, not x.t0))
        return products

    def generate_hot_stock_pool(self, max_stocks: int = 20) -> List[PoolProduct]:
        """生成热点股票池"""
        products = self._fetch_hot_stocks(top_n=100)
        if not products:
            logger.warning("热点股票 API 获取失败，使用默认池")
            products = [p for p in self._get_default_pool() if p.pool_type == "stock"]
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

    def update_daily(self) -> Dict[str, List[PoolProduct]]:
        """每日更新股票池"""
        logger.info("=" * 50)
        logger.info("开始每日股票池更新...")
        etf_lof = self.generate_etf_lof_pool()
        hot_stocks = self.generate_hot_stock_pool(max_stocks=20)
        self.save_pool(etf_lof + hot_stocks)
        logger.info(f"更新完成: ETF/LOF {len(etf_lof)} 只, 热点股票 {len(hot_stocks)} 只")
        return {"etf_lof": etf_lof, "stock": hot_stocks}

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


_pool_generator: Optional["StockPoolGenerator"] = None


def get_pool_generator(db_path: str = "./data/recommend.db") -> "StockPoolGenerator":
    global _pool_generator
    if _pool_generator is None:
        _pool_generator = StockPoolGenerator(db_path)
    return _pool_generator
