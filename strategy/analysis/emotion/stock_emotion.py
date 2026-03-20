# -*- coding: utf-8 -*-
"""
个股情绪分析器

基于A股特色的个股情绪因子:
1. 涨停/连板因子 (Limit_Score)
2. 换手率因子 (Turnover_Score)
3. 封单质量 (Seal_Strength)
4. 板块联动 (Sector_Strength)
5. 龙头标记 (Leader_Flag)

Stock_Sentiment = 
  0.30 * Limit_Score +
  0.20 * Turnover_Score +
  0.20 * Seal_Strength +
  0.20 * Sector_Strength +
  0.10 * Leader_Flag
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from strategy.analysis.base_analyzer import BaseAnalyzer, ScoreResult
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class StockEmotion:
    """个股情绪数据"""
    symbol: str
    name: str = ""
    
    is_limit_up: bool = False
    is_limit_down: bool = False
    limit_up_times: int = 0
    continuous_limit_days: int = 0
    
    turnover_rate: float = 0.0
    turnover_rank: float = 0.0
    
    sector_zt_count: int = 0
    sector_total: int = 0
    sector_strength: float = 0.0
    concept_name: str = ""
    
    is_leader: bool = False
    
    seal_amount: float = 0.0
    seal_ratio: float = 0.0
    break_count: int = 0
    
    main_net_inflow: float = 0.0
    main_net_ratio: float = 0.0
    
    limit_score: float = 0.0
    turnover_score: float = 0.0
    seal_score: float = 0.0
    sector_score: float = 0.0
    leader_score: float = 0.0
    
    total_score: float = 50.0
    
    signals: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "is_limit_up": self.is_limit_up,
            "continuous_limit_days": self.continuous_limit_days,
            "turnover_rate": self.turnover_rate,
            "sector_strength": self.sector_strength,
            "concept_name": self.concept_name,
            "seal_ratio": self.seal_ratio,
            "break_count": self.break_count,
            "total_score": self.total_score,
            "signals": self.signals,
        }


class StockEmotionAnalyzer(BaseAnalyzer):
    """个股情绪分析器"""
    
    def __init__(self):
        super().__init__("StockEmotion")
        self._sector_zt_cache: Dict[str, int] = {}
    
    def analyze(self, **kwargs) -> ScoreResult:
        """执行分析（实现基类抽象方法）"""
        symbol = kwargs.get("symbol", "")
        name = kwargs.get("name", "")
        date = kwargs.get("date")
        return self.analyze_stock(symbol, name, date)
    
    def analyze_stock(self, symbol: str, name: str = "", date: str = None) -> ScoreResult:
        """分析单只股票情绪"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        cache_key = f"stock_emotion_{symbol}_{date}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        result = self._analyze_impl(symbol, name, date)
        self._set_cache(cache_key, result)
        return result
    
    def _analyze_impl(self, symbol: str, name: str, date: str) -> ScoreResult:
        """实际分析实现"""
        result = ScoreResult()
        emotion = StockEmotion(symbol=symbol, name=name)
        
        try:
            import akshare as ak
            import akshare_proxy_patch
            akshare_proxy_patch.install_patch(
                "101.201.173.125", auth_token="", retry=30,
                hook_domains=["fund.eastmoney.com", "push2.eastmoney.com"]
            )
            
            self._load_limit_status(emotion, date)
            
            self._load_turnover_data(emotion)

            self._load_concept_strength(emotion, date)
            
            self._load_fund_flow(emotion, symbol)
            
            self._calc_emotion_score(emotion)
            
            result.score = emotion.total_score
            result.raw_data = emotion.to_dict()
            result.signals = emotion.signals
            result.warnings = emotion.warnings
            result.success = True
            
        except Exception as e:
            result.success = False
            result.error_msg = str(e)
            logger.error(f"个股情绪分析失败 {symbol}: {e}")
        
        return result
    
    def _load_limit_status(self, emotion: StockEmotion, date: str):
        """加载涨停状态"""
        try:
            import akshare as ak
            
            df_zt = ak.stock_zt_pool_em(date=date)
            if df_zt is not None and not df_zt.empty:
                if "代码" in df_zt.columns:
                    zt_codes = df_zt["代码"].astype(str).tolist()
                    emotion.is_limit_up = emotion.symbol in zt_codes
                    
                    if emotion.is_limit_up and "连板数" in df_zt.columns:
                        stock_data = df_zt[df_zt["代码"] == emotion.symbol]
                        if not stock_data.empty:
                            emotion.continuous_limit_days = int(stock_data.iloc[0]["连板数"])
                            emotion.seal_amount = float(stock_data.iloc[0].get("封板资金", 0) or 0)
                            turnover = float(stock_data.iloc[0].get("成交额", 0) or 0)
                            emotion.break_count = int(stock_data.iloc[0].get("炸板次数", 0) or 0)
                            emotion.seal_ratio = emotion.seal_amount / turnover if turnover > 0 else 0.0
                            
        except Exception as e:
            logger.debug(f"涨停状态加载失败: {e}")

    def _load_concept_strength(self, emotion: StockEmotion, date: str):
        """加载概念板块强度（概念优先于行业）"""
        try:
            from strategy.analysis.space.space_score import SpaceScoreAnalyzer

            analyzer = SpaceScoreAnalyzer()
            concept_score, concept_name = analyzer.get_symbol_concept_strength(
                symbol=emotion.symbol,
                date=date,
                top_concepts=30,
            )
            emotion.sector_strength = float(np.clip(concept_score, 0.0, 1.0))
            emotion.concept_name = concept_name
        except Exception as e:
            logger.debug(f"概念强度加载失败 {emotion.symbol}: {e}")
    
    def _load_turnover_data(self, emotion: StockEmotion):
        """加载换手率数据"""
        try:
            from data.data_source import DataSource

            data_source = DataSource()
            try:
                df = data_source.get_a_share_market_snapshot()
            finally:
                data_source.close()

            if df is not None and not df.empty:
                code_col = "code" if "code" in df.columns else "代码"
                turnover_col = "turnover_rate" if "turnover_rate" in df.columns else "换手率"
                if code_col in df.columns and turnover_col in df.columns:
                    stock_data = df[df[code_col] == emotion.symbol]
                    if not stock_data.empty:
                        row = stock_data.iloc[0]
                        emotion.turnover_rate = float(row.get(turnover_col, 0) or 0)
                        
                        all_turnovers = pd.to_numeric(df[turnover_col], errors="coerce").dropna()
                        if len(all_turnovers) > 0:
                            avg_turnover = all_turnovers.mean()
                            if avg_turnover > 0:
                                emotion.turnover_rank = emotion.turnover_rate / avg_turnover
                                
        except Exception as e:
            logger.debug(f"换手率加载失败: {e}")
    
    def _load_fund_flow(self, emotion: StockEmotion, symbol: str):
        """加载资金流数据"""
        try:
            import akshare as ak
            
            df = ak.stock_individual_fund_flow(stock=symbol, market="sh" if symbol.startswith(("5", "6")) else "sz")
            if df is not None and not df.empty:
                if "主力净流入" in df.columns:
                    emotion.main_net_inflow = float(df["主力净流入"].iloc[-1] if len(df) > 0 else 0)
                if "成交额" in df.columns:
                    amount = float(df["成交额"].iloc[-1] if len(df) > 0 else 0)
                    if amount > 0:
                        emotion.main_net_ratio = emotion.main_net_inflow / amount * 100
                        
        except Exception as e:
            logger.debug(f"资金流加载失败: {e}")
    
    def _calc_emotion_score(self, emotion: StockEmotion):
        """计算情绪评分"""
        emotion.limit_score = 0.0
        if emotion.is_limit_up:
            if emotion.continuous_limit_days >= 3:
                emotion.limit_score = 100.0
                emotion.signals.append(f"连板{emotion.continuous_limit_days}天")
            elif emotion.continuous_limit_days == 2:
                emotion.limit_score = 80.0
                emotion.signals.append("二连板")
            else:
                emotion.limit_score = 60.0
                emotion.signals.append("涨停")
        
        if emotion.turnover_rank > 3.0:
            emotion.turnover_score = 80.0
            emotion.signals.append(f"高换手{emotion.turnover_rank:.1f}倍")
        elif emotion.turnover_rank > 2.0:
            emotion.turnover_score = 60.0
        elif emotion.turnover_rank > 1.0:
            emotion.turnover_score = 40.0
        else:
            emotion.turnover_score = 20.0
        
        if emotion.seal_ratio > 0.20:
            emotion.seal_score = 85.0
            emotion.signals.append(f"封单强{emotion.seal_ratio:.2f}")
        elif emotion.seal_ratio > 0.10:
            emotion.seal_score = 70.0
            emotion.signals.append(f"封单尚可{emotion.seal_ratio:.2f}")
        elif emotion.main_net_ratio > 10:
            emotion.seal_score = 80.0
            emotion.signals.append(f"主力净流入{emotion.main_net_ratio:.1f}%")
        elif emotion.main_net_ratio > 5:
            emotion.seal_score = 60.0
        elif emotion.main_net_ratio > 0:
            emotion.seal_score = 40.0
        else:
            emotion.seal_score = 20.0
            emotion.warnings.append("主力净流出")

        if emotion.break_count >= 3:
            emotion.seal_score = min(emotion.seal_score, 20.0)
            emotion.warnings.append(f"炸板偏多({emotion.break_count})")
        elif emotion.break_count >= 1:
            emotion.seal_score = min(emotion.seal_score, 45.0)
            emotion.warnings.append(f"存在炸板({emotion.break_count})")

        emotion.sector_score = float(np.clip(emotion.sector_strength * 100.0, 0.0, 100.0))
        if emotion.sector_score >= 70:
            emotion.signals.append(f"主线概念:{emotion.concept_name}")
        elif emotion.sector_score <= 30 and emotion.concept_name:
            emotion.warnings.append(f"概念偏弱:{emotion.concept_name}")

        if emotion.continuous_limit_days >= 2 and emotion.sector_score >= 70:
            emotion.is_leader = True
            emotion.leader_score = 90.0
        elif emotion.is_limit_up and emotion.sector_score >= 60:
            emotion.leader_score = 65.0
        else:
            emotion.leader_score = 20.0
        
        emotion.total_score = (
            0.30 * emotion.limit_score +
            0.20 * emotion.turnover_score +
            0.20 * emotion.seal_score +
            0.20 * emotion.sector_score +
            0.10 * emotion.leader_score
        )
        
        emotion.total_score = max(0, min(100, emotion.total_score))
    
    def batch_analyze(self, symbols: List[str], names: Dict[str, str] = None, date: str = None) -> List[StockEmotion]:
        """批量分析股票"""
        results = []
        for symbol in symbols:
            name = names.get(symbol, "") if names else ""
            result = self.analyze_stock(symbol, name, date)
            if result.success:
                emotion = StockEmotion(**result.raw_data)
                results.append(emotion)
        
        results.sort(key=lambda x: -x.total_score)
        return results
    
    def get_top_stocks(self, symbols: List[str], names: Dict[str, str] = None, 
                       date: str = None, top_n: int = 10) -> List[StockEmotion]:
        """获取情绪评分最高的股票"""
        all_stocks = self.batch_analyze(symbols, names, date)
        return all_stocks[:top_n]


from datetime import timedelta
