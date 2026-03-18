# -*- coding: utf-8 -*-
"""
实时选股监控系统
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

from data import DataSource
from strategy import PriceActionMACDStrategy, MACDStrategy, PriceActionStrategy
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
    signal_type: str  # 买入/卖出/观望
    target_price: Optional[float] = None
    stop_loss: Optional[float] = None
    reason: str = ""
    score: float = 0.0


class RealtimeMonitor:
    """实时选股监控"""
    
    def __init__(
        self,
        data_source: DataSource = None,
        etf_count: int = 5,
        stock_count: int = 5,
    ):
        self.data_source = data_source or DataSource()
        self.etf_count = etf_count
        self.stock_count = stock_count
        
        self.strategy = PriceActionMACDStrategy(
            lookback=20,
            macd_fast=12,
            macd_slow=26,
            macd_signal=9,
        )
        
        # 默认ETF股票池
        self.etf_pool = [
            {"code": "511880", "name": "银华日利ETF"},
            {"code": "513100", "name": "纳指ETF"},
            {"code": "513050", "name": "中概互联网ETF"},
            {"code": "510300", "name": "沪深300ETF"},
            {"code": "511010", "name": "上证50ETF"},
            {"code": "512480", "name": "半导体ETF"},
            {"code": "515790", "name": "光伏ETF"},
            {"code": "515000", "name": "智能制造ETF"},
        ]
        
        # 默认A股股票池
        self.stock_pool = [
            {"code": "600519", "name": "贵州茅台"},
            {"code": "600036", "name": "招商银行"},
            {"code": "601318", "name": "中国平安"},
            {"code": "000858", "name": "五粮液"},
            {"code": "300750", "name": "宁德时代"},
            {"code": "002594", "name": "比亚迪"},
            {"code": "300059", "name": "东方财富"},
            {"code": "601012", "name": "隆基绿能"},
        ]
    
    def get_latest_price(self, symbol: str) -> Optional[dict]:
        """获取最新价格（使用最近交易日数据）"""
        try:
            # 获取最近20个交易日的数据
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            df = self.data_source.get_kline(
                symbol, 
                start_date.strftime("%Y%m%d"), 
                end_date.strftime("%Y%m%d")
            )
            
            if df is None or df.empty:
                return None
            
            # 获取最后一行数据
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
    
    def analyze_stock(self, symbol: str, name: str) -> Optional[StockSignal]:
        """分析单只股票"""
        try:
            # 获取最新价格
            latest_price = self.get_latest_price(symbol)
            
            if latest_price is None or latest_price["price"] <= 0:
                logger.warning(f"无法获取 {symbol} 价格数据")
                return None
            
            price = latest_price["price"]
            change_pct = latest_price["change_pct"]
            volume = latest_price["volume"]
            
            # 获取历史数据进行技术分析
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
            
            df = self.data_source.get_kline(symbol, start_date, end_date)
            if df is None or df.empty or len(df) < 30:
                logger.warning(f"历史数据不足 {symbol}")
                return None
            
            # 使用策略分析
            signal = self.strategy.on_bar(symbol, df)
            
            if signal is None:
                return StockSignal(
                    code=symbol,
                    name=name,
                    price=price,
                    change_pct=change_pct,
                    volume=volume,
                    signal_type="观望",
                    reason="数据不足",
                )
            
            # 计算目标价和止损价
            if signal.signal > 0:  # 买入信号
                target_price = price * 1.05  # 5%目标
                stop_loss = price * 0.97    # 3%止损
                signal_type = "买入"
                reason = self._generate_reason(df, signal)
                score = signal.weight
            elif signal.signal < 0:  # 卖出信号
                target_price = None
                stop_loss = None
                signal_type = "卖出"
                reason = self._generate_reason(df, signal)
                score = 1.0
            else:
                target_price = None
                stop_loss = None
                signal_type = "观望"
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
            )
            
        except Exception as e:
            logger.error(f"分析股票失败 {symbol}: {e}")
            return None
    
    def _generate_reason(self, df, signal) -> str:
        """生成推荐理由"""
        try:
            latest = df.iloc[-1]
            
            reasons = []
            
            # MACD分析
            if "macd" in df.columns:
                macd = latest.get("macd", 0)
                macd_signal = latest.get("macd_signal", 0)
                if macd > macd_signal:
                    reasons.append("MACD金叉")
                elif macd < macd_signal:
                    reasons.append("MACD死叉")
            
            # 均线分析
            if "ema20" in df.columns and "close" in df.columns:
                if latest["close"] > latest["ema20"]:
                    reasons.append("价格站上20日均线")
                else:
                    reasons.append("价格跌破20日均线")
            
            # 成交量分析
            if "volume" in df.columns:
                vol_ma = df["volume"].tail(20).mean()
                if latest["volume"] > vol_ma * 1.5:
                    reasons.append("成交量放大")
            
            if not reasons:
                return "技术面观望"
            
            return ",".join(reasons[:2])
            
        except Exception as e:
            return "技术分析"
    
    def scan_market(self) -> Dict[str, List[StockSignal]]:
        """
        扫描市场
        
        Returns:
            {
                "etf": [StockSignal, ...],
                "stock": [StockSignal, ...]
            }
        """
        logger.info("开始实时扫描市场...")
        
        etf_signals = []
        stock_signals = []
        
        # 扫描ETF
        logger.info(f"扫描ETF池 ({len(self.etf_pool)}只)...")
        for etf in self.etf_pool:
            signal = self.analyze_stock(etf["code"], etf["name"])
            if signal:
                etf_signals.append(signal)
        
        # 扫描A股
        logger.info(f"扫描A股池 ({len(self.stock_pool)}只)...")
        for stock in self.stock_pool:
            signal = self.analyze_stock(stock["code"], stock["name"])
            if signal:
                stock_signals.append(signal)
        
        # 按信号排序
        etf_signals = sorted(etf_signals, key=lambda x: -x.score)
        stock_signals = sorted(stock_signals, key=lambda x: -x.score)
        
        logger.info(f"扫描完成: ETF {len(etf_signals)}只, A股 {len(stock_signals)}只")
        
        return {
            "etf": etf_signals[:self.etf_count],
            "stock": stock_signals[:self.stock_count],
        }
    
    def get_top_recommends(self, signals: List[StockSignal], top_n: int = 5) -> List[dict]:
        """获取推荐列表"""
        recommends = []
        
        for s in signals[:top_n]:
            rec = {
                "code": s.code,
                "name": s.name,
                "price": s.price,
                "change_pct": s.change_pct,
                "signal": s.signal_type,
                "target": s.target_price,
                "stop_loss": s.stop_loss,
                "reason": s.reason,
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
