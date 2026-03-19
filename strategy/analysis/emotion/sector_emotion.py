# -*- coding: utf-8 -*-
"""
板块情绪分析器

分析板块热度:
1. 板块资金流入排名
2. 板块内涨停数量
3. 板块轮动检测
4. 板块联动性
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
class SectorEmotion:
    """板块情绪数据"""
    sector_name: str
    main_net_inflow: float = 0.0
    main_net_ratio: float = 0.0
    
    zt_count: int = 0
    zt_ratio: float = 0.0
    
    turnover: float = 0.0
    change_pct: float = 0.0
    
    score: float = 50.0
    rank: int = 0
    
    signals: List[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "sector": self.sector_name,
            "main_net_inflow": self.main_net_inflow,
            "zt_count": self.zt_count,
            "score": self.score,
            "rank": self.rank,
            "signals": self.signals,
        }


class SectorEmotionAnalyzer(BaseAnalyzer):
    """板块情绪分析器"""
    
    def __init__(self):
        super().__init__("SectorEmotion")
        self._sector_zt_cache: Dict[str, int] = {}
    
    def analyze(self, **kwargs) -> ScoreResult:
        """执行分析（实现基类抽象方法）"""
        date = kwargs.get("date")
        return self.analyze_sectors(date)
    
    def analyze_sectors(self, date: str = None) -> ScoreResult:
        """分析所有板块情绪"""
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        cache_key = f"sector_emotion_{date}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached
        
        result = self._analyze_impl(date)
        self._set_cache(cache_key, result)
        return result
    
    def _analyze_impl(self, date: str) -> ScoreResult:
        """实际分析实现"""
        result = ScoreResult()
        sectors = []
        
        try:
            import akshare as ak
            import akshare_proxy_patch
            akshare_proxy_patch.install_patch(
                "101.201.173.125", auth_token="", retry=30,
                hook_domains=["fund.eastmoney.com", "push2.eastmoney.com"]
            )
            
            df = ak.stock_sector_fund_flow_rank(indicator="今日")
            if df is not None and not df.empty:
                df.columns = [c.strip() for c in df.columns]
                
                inflow_col = None
                name_col = None
                for col in df.columns:
                    if "主力净流入" in col and "净额" in col:
                        inflow_col = col
                    if "名称" in col or "板块名称" in col:
                        name_col = col
                
                if inflow_col and name_col:
                    df_sorted = df.sort_values(inflow_col, ascending=False)
                    
                    max_inflow = abs(df_sorted[inflow_col].max()) if abs(df_sorted[inflow_col].max()) > 0 else 1
                    
                    for idx, row in df_sorted.iterrows():
                        sector = SectorEmotion(
                            sector_name=row.get(name_col, ""),
                            main_net_inflow=float(row.get(inflow_col, 0)),
                        )
                        
                        if max_inflow > 0:
                            sector.main_net_ratio = sector.main_net_inflow / max_inflow * 100
                        
                        sector.score = 50 + sector.main_net_ratio * 0.5
                        sector.score = max(0, min(100, sector.score))
                        
                        sectors.append(sector)
            
            for i, s in enumerate(sectors):
                s.rank = i + 1
            
            hot_sectors = [s for s in sectors if s.rank <= 5]
            if hot_sectors:
                result.signals = [f"热门板块: {', '.join([s.sector_name for s in hot_sectors])}"]
            
            result.score = sectors[0].score if sectors else 50
            result.raw_data = {
                "sectors": [s.to_dict() for s in sectors[:20]],
                "hot_sectors": [s.sector_name for s in hot_sectors],
            }
            result.success = True
            
        except Exception as e:
            result.success = False
            result.error_msg = str(e)
            logger.error(f"板块情绪分析失败: {e}")
        
        return result
    
    def get_hot_sectors(self, date: str = None, top_n: int = 5) -> List[SectorEmotion]:
        """获取最热门的板块"""
        result = self.analyze_sectors(date)
        if result.success and "sectors" in result.raw_data:
            return [
                SectorEmotion(**s) 
                for s in result.raw_data["sectors"][:top_n]
            ]
        return []
    
    def get_sector_by_name(self, sector_name: str, date: str = None) -> Optional[SectorEmotion]:
        """根据板块名称获取情绪数据"""
        result = self.analyze_sectors(date)
        if result.success and "sectors" in result.raw_data:
            for s in result.raw_data["sectors"]:
                if s.get("sector") == sector_name:
                    return SectorEmotion(**s)
        return None
