# -*- coding: utf-8 -*-
"""
大盘情绪分析器

基于A股特色的情绪指标体系:
1. 涨停/跌停数量 (Emotion_Core)
2. 连板高度 (Leader_Height)
3. 成交额情绪 (Liquidity)
4. 赚钱效应 (Strong_Return)
5. 分歧度 (Divergence)

参考: 弱转强体系 + 短线交易情绪
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import math

from strategy.analysis.base_analyzer import BaseAnalyzer, ScoreResult
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class MarketEmotion:
    """大盘情绪数据"""
    date: str
    zt_count: int = 0
    dt_count: int = 0
    lb_max: int = 0
    lb_count: int = 0
    zt_stocks: List[str] = field(default_factory=list)
    dt_stocks: List[str] = field(default_factory=list)
    lb_stocks: Dict[int, List[str]] = field(default_factory=dict)
    
    total_turnover: float = 0.0
    avg_turnover_20d: float = 0.0
    liquidity_ratio: float = 1.0
    
    up_count: int = 0
    down_count: int = 0
    strong_return: float = 0.0
    
    divergence: float = 0.0
    
    emotion_core: float = 0.0
    leader_score: float = 0.0
    liquidity_score: float = 50.0
    strong_score: float = 50.0
    divergence_score: float = 50.0
    
    total_score: float = 50.0
    normalized_score: float = 50.0
    
    cycle: str = "未知"
    cycle_description: str = ""
    
    hot_sectors: List[str] = field(default_factory=list)
    cold_sectors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "zt_count": self.zt_count,
            "dt_count": self.dt_count,
            "lb_max": self.lb_max,
            "lb_count": self.lb_count,
            "emotion_core": self.emotion_core,
            "total_score": self.total_score,
            "normalized_score": self.normalized_score,
            "cycle": self.cycle,
            "cycle_description": self.cycle_description,
            "hot_sectors": self.hot_sectors[:5],
            "cold_sectors": self.cold_sectors[:5],
        }
    
    def summary(self) -> str:
        return (
            f"【大盘情绪 {self.date}】\n"
            f"涨停: {self.zt_count} | 跌停: {self.dt_count} | 连板: {self.lb_count}只(最高{self.lb_max}板)\n"
            f"周期: {self.cycle} | 情绪分: {self.normalized_score:.1f}\n"
            f"热度板块: {', '.join(self.hot_sectors[:3]) if self.hot_sectors else '暂无'}"
        )


class MarketEmotionAnalyzer(BaseAnalyzer):
    """大盘情绪分析器"""
    
    def __init__(self):
        super().__init__("MarketEmotion")
        self._cache_ttl = 3600
    
    def analyze(self, date: str = None) -> ScoreResult:
        """分析大盘情绪"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        cache_key = f"market_emotion_{date}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        result = self._analyze_impl(date)
        self._set_cache(cache_key, result)
        return result
    
    def _analyze_impl(self, date: str) -> ScoreResult:
        """实际分析实现"""
        result = ScoreResult()
        emotion = MarketEmotion(date=date)
        
        try:
            self._load_limit_pools(emotion, date)
            
            self._load_market_breadth(emotion)
            
            self._calc_emotion_score(emotion)
            
            self._detect_cycle(emotion)
            
            self._load_sector_flow(emotion)
            
            result.score = emotion.normalized_score
            result.raw_data = emotion.to_dict()
            result.signals = [emotion.cycle_description]
            result.success = True
            
            self._set_cache(f"market_emotion_{date}", result)
            
            logger.info(f"大盘情绪分析完成: {emotion.cycle} ({emotion.normalized_score:.1f})")
            
        except Exception as e:
            result.success = False
            result.error_msg = str(e)
            logger.error(f"大盘情绪分析失败: {e}")
        
        return result
    
    def _load_limit_pools(self, emotion: MarketEmotion, date: str):
        """加载涨跌停池"""
        try:
            from data.data_source import DataSource

            data_source = DataSource()
            try:
                df_zt, df_dt, _ = data_source.get_limit_pool()
            finally:
                data_source.close()

            if df_zt is not None and not df_zt.empty:
                emotion.zt_count = len(df_zt)
                emotion.zt_stocks = df_zt["代码"].tolist() if "代码" in df_zt.columns else []
                
                if "连板数" in df_zt.columns:
                    emotion.lb_count = (df_zt["连板数"] > 1).sum()
                    emotion.lb_max = int(df_zt["连板数"].max()) if df_zt["连板数"].max() > 0 else 0
                    
                    for idx, row in df_zt.iterrows():
                        if "连板数" in row and row["连板数"] > 1:
                            lb = int(row["连板数"])
                            if lb not in emotion.lb_stocks:
                                emotion.lb_stocks[lb] = []
                            if "代码" in row:
                                emotion.lb_stocks[lb].append(row["代码"])
            
            if df_dt is not None and not df_dt.empty:
                emotion.dt_count = len(df_dt)
                emotion.dt_stocks = df_dt["代码"].tolist() if "代码" in df_dt.columns else []
                
        except Exception as e:
            logger.warning(f"涨跌停池加载失败: {e}")
    
    def _load_market_breadth(self, emotion: MarketEmotion):
        """加载市场广度数据"""
        try:
            from data.data_source import DataSource

            data_source = DataSource()
            try:
                df = data_source.get_a_share_market_snapshot()
            finally:
                data_source.close()

            if df is not None and not df.empty:
                change_col = "change_rate" if "change_rate" in df.columns else "涨跌幅"
                amount_col = "turnover" if "turnover" in df.columns else "成交额"
                if change_col in df.columns:
                    emotion.up_count = (df[change_col] > 0).sum()
                    emotion.down_count = (df[change_col] < 0).sum()
                    
                    emotion.divergence = float(pd.to_numeric(df[change_col], errors="coerce").std() or 0)
                    
                    total_amount = df[amount_col].sum() if amount_col in df.columns else 0
                    emotion.total_turnover = total_amount
                    
        except Exception as e:
            logger.warning(f"市场广度加载失败: {e}")
    
    def _calc_emotion_score(self, emotion: MarketEmotion):
        """计算情绪评分"""
        emotion.emotion_core = emotion.zt_count - emotion.dt_count
        
        emotion.leader_score = math.log(emotion.lb_max + 1) * 20 if emotion.lb_max > 0 else 0
        
        if emotion.avg_turnover_20d > 0:
            emotion.liquidity_ratio = emotion.total_turnover / emotion.avg_turnover_20d
            if emotion.liquidity_ratio > 1.5:
                emotion.liquidity_score = 80
            elif emotion.liquidity_ratio > 1.2:
                emotion.liquidity_score = 65
            elif emotion.liquidity_ratio > 1.0:
                emotion.liquidity_score = 50
            elif emotion.liquidity_ratio > 0.8:
                emotion.liquidity_score = 35
            else:
                emotion.liquidity_score = 20
        
        emotion.total_score = (
            0.30 * emotion.emotion_core +
            0.25 * emotion.leader_score +
            0.20 * emotion.liquidity_score +
            0.15 * emotion.strong_score -
            0.10 * (100 - emotion.divergence * 10)
        )
        
        emotion.normalized_score = max(0, min(100, 50 + emotion.total_score))
    
    def _detect_cycle(self, emotion: MarketEmotion):
        """判断情绪周期"""
        cycle_weight = emotion.zt_count - emotion.dt_count + emotion.lb_count
        
        if cycle_weight <= -10:
            emotion.cycle = "冰点"
            emotion.cycle_description = "市场极度悲观，建议观望或轻仓"
        elif cycle_weight <= 0:
            emotion.cycle = "冰点-修复"
            emotion.cycle_description = "市场情绪低迷，等待信号"
        elif cycle_weight <= 10:
            emotion.cycle = "修复"
            emotion.cycle_description = "情绪开始回暖，可试探性布局"
        elif cycle_weight <= 30:
            emotion.cycle = "主升"
            emotion.cycle_description = "赚钱效应显现，积极参与"
        elif cycle_weight <= 50:
            emotion.cycle = "高潮"
            emotion.cycle_description = "市场过热，注意高低切换"
        else:
            emotion.cycle = "崩溃预警"
            emotion.cycle_description = "极端行情，随时准备撤退"
    
    def _load_sector_flow(self, emotion: MarketEmotion):
        """加载板块资金流"""
        try:
            from strategy.analysis.emotion.sector_emotion import SectorEmotionAnalyzer

            result = SectorEmotionAnalyzer().analyze_sectors()
            if result.success and result.raw_data:
                sectors = result.raw_data.get("sectors", [])
                if sectors:
                    emotion.hot_sectors = [str(item.get("sector", "")) for item in sectors[:5] if str(item.get("sector", "")).strip()]
                    emotion.cold_sectors = [str(item.get("sector", "")) for item in sectors[-5:] if str(item.get("sector", "")).strip()]
        except Exception as e:
            logger.warning(f"板块资金流加载失败: {e}")
    
    def get_market_emotion(self, date: str = None) -> Optional[MarketEmotion]:
        """获取大盘情绪对象"""
        result = self.analyze(date)
        if result.success:
            return MarketEmotion(**result.raw_data)
        return None
