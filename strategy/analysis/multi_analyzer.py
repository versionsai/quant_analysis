# -*- coding: utf-8 -*-
"""
多维度综合分析器

整合资金面、情绪面、基本面、技术面进行综合评分

权重设计:
- 资金面: 30%
- 情绪面: 30% (大盘情绪50% + 个股情绪30% + 板块情绪20%)
- 基本面: 25%
- 技术面: 15%
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from strategy.analysis.base_analyzer import BaseAnalyzer, ScoreResult
from strategy.analysis.emotion.market_emotion import MarketEmotionAnalyzer, MarketEmotion
from strategy.analysis.emotion.stock_emotion import StockEmotionAnalyzer, StockEmotion
from strategy.analysis.emotion.sector_emotion import SectorEmotionAnalyzer, SectorEmotion
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class MultiDimScore:
    """多维度评分"""
    symbol: str
    name: str = ""
    
    fund_score: float = 50.0
    emotion_score: float = 50.0
    funda_score: float = 50.0
    tech_score: float = 50.0
    
    market_emotion_score: float = 50.0
    stock_emotion_score: float = 50.0
    sector_emotion_score: float = 50.0
    
    total_score: float = 50.0
    
    buy_signals: List[str] = field(default_factory=list)
    sell_signals: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    recommendation: str = "观望"
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "total_score": self.total_score,
            "fund_score": self.fund_score,
            "emotion_score": self.emotion_score,
            "funda_score": self.funda_score,
            "tech_score": self.tech_score,
            "market_emotion": self.market_emotion_score,
            "stock_emotion": self.stock_emotion_score,
            "sector_emotion": self.sector_emotion_score,
            "buy_signals": self.buy_signals,
            "warnings": self.warnings,
            "recommendation": self.recommendation,
        }


class MultiDimensionalAnalyzer(BaseAnalyzer):
    """多维度综合分析器"""
    
    WEIGHTS = {
        "fund": 0.30,
        "emotion": 0.30,
        "funda": 0.25,
        "tech": 0.15,
    }
    
    EMOTION_WEIGHTS = {
        "market": 0.50,
        "stock": 0.30,
        "sector": 0.20,
    }
    
    def __init__(self):
        super().__init__("MultiDimensional")
        self.market_analyzer = MarketEmotionAnalyzer()
        self.stock_analyzer = StockEmotionAnalyzer()
        self.sector_analyzer = SectorEmotionAnalyzer()
        
        self._market_emotion: Optional[MarketEmotion] = None
    
    def analyze(
        self,
        symbols: List[str],
        names: Dict[str, str] = None,
        date: str = None,
    ) -> List[MultiDimScore]:
        """分析多只股票"""
        if names is None:
            names = {}
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        logger.info(f"开始多维度分析: {len(symbols)} 只股票")
        
        self._market_emotion = self.market_analyzer.get_market_emotion(date)
        logger.info(f"大盘情绪: {self._market_emotion.cycle if self._market_emotion else '未知'}")
        
        results = []
        for symbol in symbols:
            score = self._analyze_single(symbol, names.get(symbol, ""), date)
            if score:
                results.append(score)
        
        results.sort(key=lambda x: -x.total_score)
        
        logger.info(f"分析完成: {len(results)} 只股票")
        return results
    
    def _analyze_single(self, symbol: str, name: str, date: str) -> Optional[MultiDimScore]:
        """分析单只股票"""
        try:
            score = MultiDimScore(symbol=symbol, name=name)
            
            stock_emotion_result = self.stock_analyzer.analyze_stock(symbol, name, date)
            if stock_emotion_result.success:
                stock_emotion = StockEmotion(**stock_emotion_result.raw_data)
                score.stock_emotion_score = stock_emotion.total_score
                score.buy_signals.extend(stock_emotion.signals)
                score.warnings.extend(stock_emotion.warnings)
            
            score.market_emotion_score = self._market_emotion.normalized_score if self._market_emotion else 50
            
            sector_emotion_result = self.sector_analyzer.analyze_sectors(date)
            if sector_emotion_result.success:
                hot_sectors = sector_emotion_result.raw_data.get("hot_sectors", [])
                score.sector_emotion_score = sector_emotion_result.score
            else:
                score.sector_emotion_score = 50
            
            score.emotion_score = (
                self.EMOTION_WEIGHTS["market"] * score.market_emotion_score +
                self.EMOTION_WEIGHTS["stock"] * score.stock_emotion_score +
                self.EMOTION_WEIGHTS["sector"] * score.sector_emotion_score
            )
            
            score.fund_score = self._calc_fund_score(symbol)
            
            score.tech_score = self._calc_tech_score(symbol, date)
            
            score.funda_score = 50
            
            score.total_score = (
                self.WEIGHTS["fund"] * score.fund_score +
                self.WEIGHTS["emotion"] * score.emotion_score +
                self.WEIGHTS["funda"] * score.funda_score +
                self.WEIGHTS["tech"] * score.tech_score
            )
            
            self._generate_recommendation(score)
            
            return score
            
        except Exception as e:
            logger.error(f"分析失败 {symbol}: {e}")
            return None
    
    def _calc_fund_score(self, symbol: str) -> float:
        """计算资金面评分"""
        try:
            from data import DataSource

            data_source = DataSource()
            try:
                flow = data_source.get_individual_capital_flow(symbol)
            finally:
                data_source.close()

            ratio = float(flow.get("main_net_ratio", 0.0) or 0.0)
            if ratio > 10:
                return 80
            if ratio > 5:
                return 65
            if ratio > 0:
                return 50
            if ratio > -5:
                return 40
        except Exception:
            pass
        
        return 50
    
    def _calc_tech_score(self, symbol: str, date: str) -> float:
        """计算技术面评分"""
        try:
            from data import DataSource
            
            data_source = DataSource()
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
            
            df = data_source.get_kline(symbol, start_date, end_date)
            if df is None or df.empty or len(df) < 20:
                return 50
            
            if "close" not in df.columns:
                return 50
            
            close = df["close"]
            ma20 = close.rolling(20).mean().iloc[-1]
            ma60 = close.rolling(60).mean().iloc[-1]
            
            current = close.iloc[-1]
            
            score = 50
            
            if current > ma20 > ma60:
                score += 20
            elif current > ma20:
                score += 10
            
            if "volume" in df.columns:
                vol_ma = df["volume"].rolling(20).mean().iloc[-1]
                current_vol = df["volume"].iloc[-1]
                if current_vol > vol_ma * 1.5:
                    score += 15
                elif current_vol > vol_ma:
                    score += 5
            
            returns_5d = (close.iloc[-1] / close.iloc[-5] - 1) * 100 if len(close) >= 5 else 0
            if returns_5d > 5:
                score += 15
            elif returns_5d > 0:
                score += 5
            
            return max(0, min(100, score))
            
        except Exception:
            return 50
    
    def _generate_recommendation(self, score: MultiDimScore):
        """生成推荐建议"""
        if score.total_score >= 75:
            score.recommendation = "强烈买入"
        elif score.total_score >= 65:
            score.recommendation = "买入"
        elif score.total_score >= 55:
            score.recommendation = "持有"
        elif score.total_score >= 45:
            score.recommendation = "谨慎持有"
        else:
            score.recommendation = "卖出"
        
        if score.market_emotion_score < 30:
            score.recommendation = "市场情绪差，建议观望"
            score.warnings.append("大盘情绪低迷")
    
    def get_recommendations(
        self,
        symbols: List[str],
        names: Dict[str, str] = None,
        date: str = None,
        top_n: int = 10,
    ) -> Dict[str, List[MultiDimScore]]:
        """获取推荐列表"""
        results = self.analyze(symbols, names, date)
        
        buy_list = [s for s in results if s.recommendation in ("强烈买入", "买入")][:top_n]
        hold_list = [s for s in results if s.recommendation in ("持有", "谨慎持有")][:top_n]
        sell_list = [s for s in results if s.recommendation == "卖出"][:top_n]
        
        return {
            "buy": buy_list,
            "hold": hold_list,
            "sell": sell_list,
            "all": results,
        }
    
    def get_market_summary(self, date: str = None) -> Optional[MarketEmotion]:
        """获取大盘情绪摘要"""
        return self._market_emotion
