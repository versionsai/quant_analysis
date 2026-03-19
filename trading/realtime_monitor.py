# -*- coding: utf-8 -*-
"""
实时选股监控系统
双策略运行: PriceAction+MACD + 弱转强
双重信号标注
"""
import os
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from data import DataSource, get_dynamic_pool
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    WeakToStrongTimingStrategy,
    WeakToStrongParams,
)
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class StockSignal:
    """股票信号"""
    code: str
    name: str
    price: float
    change_pct: float
    volume: float
    signal_type: str
    target_price: Optional[float] = None
    stop_loss: Optional[float] = None
    reason: str = ""
    score: float = 0.0
    dual_signal: bool = False
    ws_stage: int = 0
    ws_reason: str = ""
    ws_score: float = 0.0


class RealtimeMonitor:
    """实时选股监控"""
    
    def __init__(
        self,
        data_source: DataSource = None,
        etf_count: int = 5,
        stock_count: int = 5,
        db_path: str = None,
    ):
        self.data_source = data_source or DataSource()
        self.etf_count = etf_count
        self.stock_count = stock_count
        self.db_path = db_path or os.environ.get("DATABASE_PATH", "./data/recommend.db")
        
        self.pa_macd_strategy = PriceActionMACDStrategy(
            lookback=20,
            macd_fast=12,
            macd_slow=26,
            macd_signal=9,
        )
        
        self.ws_strategy = WeakToStrongTimingStrategy(
            params=WeakToStrongParams()
        )
        
        self.etf_pool = []
        self.stock_pool = []
        self._load_dynamic_pool()
    
    def _load_dynamic_pool(self):
        """从数据库加载动态股票池"""
        try:
            pool = get_dynamic_pool(pool_type="all", limit=100, db_path=self.db_path)
            
            etf_products = []
            stock_products = []
            
            for code, meta in pool._metadata.items():
                item = {"code": code, "name": meta.get("name", "")}
                ptype = meta.get("pool_type", "")
                if ptype in ("etf", "lof"):
                    etf_products.append(item)
                elif ptype == "stock":
                    stock_products.append(item)
            
            self.etf_pool = etf_products
            self.stock_pool = stock_products
            
            logger.info(f"动态股票池加载: ETF/LOF {len(self.etf_pool)} 只, 股票 {len(self.stock_pool)} 只")
        except Exception as e:
            logger.warning(f"动态股票池加载失败，使用空池: {e}")
            self.etf_pool = []
            self.stock_pool = []
    
    def reload_pool(self):
        """重新加载股票池"""
        self._load_dynamic_pool()
    
    def get_latest_price(self, symbol: str) -> Optional[dict]:
        """获取最新价格（使用最近交易日数据）"""
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            df = self.data_source.get_kline(
                symbol, 
                start_date.strftime("%Y%m%d"), 
                end_date.strftime("%Y%m%d")
            )
            
            if df is None or df.empty:
                return None
            
            latest = df.iloc[-1]
            
            return {
                "price": float(latest.get("close", 0)),
                "change_pct": float(latest.get("pct_change", 0)) if "pct_change" in latest else 0.0,
                "volume": float(latest.get("volume", 0)),
                "date": str(latest.get("date", "")),
            }
            
        except Exception as e:
            logger.warning(f"获取最新价格失败 {symbol}: {e}")
            return None
    
    def analyze_stock(self, symbol: str, name: str, is_stock: bool = True) -> Optional[StockSignal]:
        """分析单只股票（双策略）"""
        try:
            latest_price = self.get_latest_price(symbol)
            
            if latest_price is None or latest_price["price"] <= 0:
                logger.warning(f"无法获取 {symbol} 价格数据")
                return None
            
            price = latest_price["price"]
            change_pct = latest_price["change_pct"]
            volume = latest_price["volume"]
            
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
            
            df = self.data_source.get_kline(symbol, start_date, end_date)
            if df is None or df.empty or len(df) < 30:
                logger.warning(f"历史数据不足 {symbol}")
                return None
            
            pa_signal = self.pa_macd_strategy.on_bar(symbol, df)
            
            ws_signal = None
            ws_stage = 0
            ws_reason = ""
            ws_score = 0.0
            dual_signal = False
            
            if is_stock:
                self.ws_strategy.load_data(symbol, df)
                ws_signal = self.ws_strategy.on_bar(symbol, df)
                ws_info = self.ws_strategy.get_stage_info()
                if ws_info is not None:
                    ws_stage = ws_info.stage
                    ws_reason = ws_info.details
                    ws_score = ws_info.score
            
            pa_signal_val = pa_signal.signal if pa_signal else 0
            ws_signal_val = ws_signal.signal if ws_signal else 0
            
            if pa_signal_val > 0 and ws_signal_val > 0:
                dual_signal = True
            
            if pa_signal_val > 0:
                target_price = price * 1.05
                stop_loss = price * 0.97
                signal_type = "买入"
                reason = self._generate_pa_reason(df, pa_signal)
                score = min(pa_signal.weight + (ws_signal_val * 0.3 if ws_signal_val > 0 else 0), 1.0)
            elif pa_signal_val < 0:
                target_price = None
                stop_loss = None
                signal_type = "卖出"
                reason = self._generate_pa_reason(df, pa_signal)
                score = 1.0
            elif ws_signal_val > 0 and ws_stage >= 3:
                target_price = price * 1.05
                stop_loss = price * 0.97
                signal_type = "买入"
                reason = f"弱转强({ws_reason})"
                score = min(ws_score / 100 + 0.3, 1.0)
            else:
                target_price = None
                stop_loss = None
                signal_type = "观望"
                if ws_stage > 0:
                    reason = f"弱转强{ws_stage}/4阶段"
                else:
                    reason = "无明确信号"
                score = 0.0
            
            return StockSignal(
                code=symbol,
                name=name,
                price=price,
                change_pct=change_pct,
                volume=volume,
                signal_type=signal_type,
                target_price=target_price,
                stop_loss=stop_loss,
                reason=reason,
                score=score,
                dual_signal=dual_signal,
                ws_stage=ws_stage,
                ws_reason=ws_reason,
                ws_score=ws_score,
            )
            
        except Exception as e:
            logger.error(f"分析股票失败 {symbol}: {e}")
            return None
    
    def _generate_pa_reason(self, df, signal) -> str:
        """生成PA+MACD推荐理由"""
        try:
            latest = df.iloc[-1]
            reasons = []
            
            if "macd" in df.columns:
                macd = latest.get("macd", 0)
                macd_signal = latest.get("macd_signal", 0)
                if macd > macd_signal:
                    reasons.append("MACD金叉")
                elif macd < macd_signal:
                    reasons.append("MACD死叉")
            
            if "ema20" in df.columns and "close" in df.columns:
                if latest["close"] > latest["ema20"]:
                    reasons.append("站上20日线")
                else:
                    reasons.append("跌破20日线")
            
            if "volume" in df.columns:
                vol_ma = df["volume"].tail(20).mean()
                if latest["volume"] > vol_ma * 1.5:
                    reasons.append("量能放大")
            
            return ",".join(reasons[:2]) if reasons else "技术面信号"
            
        except Exception:
            return "技术面信号"
    
    def scan_market(self) -> Dict[str, List[StockSignal]]:
        """
        扫描市场 (双策略: PA+MACD + 弱转强)
        
        Returns:
            {
                "etf": [StockSignal, ...],
                "stock": [StockSignal, ...]
            }
        """
        logger.info("开始实时扫描市场 (双策略)...")
        
        etf_signals = []
        stock_signals = []
        dual_count = 0
        
        logger.info(f"扫描ETF池 ({len(self.etf_pool)}只)...")
        for etf in self.etf_pool:
            signal = self.analyze_stock(etf["code"], etf["name"], is_stock=False)
            if signal:
                etf_signals.append(signal)
        
        logger.info(f"扫描A股池 ({len(self.stock_pool)}只, PA+MACD + 弱转强)...")
        for stock in self.stock_pool:
            signal = self.analyze_stock(stock["code"], stock["name"], is_stock=True)
            if signal:
                if signal.dual_signal:
                    dual_count += 1
                stock_signals.append(signal)
        
        etf_signals = sorted(etf_signals, key=lambda x: -x.score)
        stock_signals = sorted(stock_signals, key=lambda x: (-x.score, -x.dual_signal))
        
        logger.info(f"扫描完成: ETF {len(etf_signals)}只, A股 {len(stock_signals)}只, 双重信号 {dual_count}只")
        
        return {
            "etf": etf_signals[:self.etf_count],
            "stock": stock_signals[:self.stock_count],
        }
    
    def get_top_recommends(self, signals: List[StockSignal], top_n: int = 5) -> List[dict]:
        """获取推荐列表"""
        recommends = []
        
        for s in signals[:top_n]:
            dual_tag = "⭐双重信号" if s.dual_signal else ""
            rec = {
                "code": s.code,
                "name": s.name,
                "price": s.price,
                "change_pct": s.change_pct,
                "signal": s.signal_type,
                "target": s.target_price,
                "stop_loss": s.stop_loss,
                "reason": f"{s.reason} {dual_tag}".strip(),
                "dual_signal": s.dual_signal,
                "ws_stage": s.ws_stage,
            }
            recommends.append(rec)
        
        return recommends


def run_realtime_scan():
    """运行实时扫描"""
    from trading.push_service import get_pusher
    
    monitor = RealtimeMonitor(etf_count=5, stock_count=5)
    
    # 扫描市场
    results = monitor.scan_market()
    
    # 推送结果
    pusher = get_pusher()
    
    etf_recs = monitor.get_top_recommends(results["etf"])
    stock_recs = monitor.get_top_recommends(results["stock"])
    
    success = pusher.push_daily_recommend(etf_recs, stock_recs)
    
    if success:
        print("推送成功!")
    else:
        print("推送失败!")
    
    # 打印结果
    print("\n" + "="*50)
    print("ETF推荐:")
    for r in etf_recs:
        print(f"  {r['code']} {r['name']} - {r['signal']} @ {r['price']:.4f}")
    
    print("\nA股推荐:")
    for r in stock_recs:
        print(f"  {r['code']} {r['name']} - {r['signal']} @ {r['price']:.4f}")
    
    return results
