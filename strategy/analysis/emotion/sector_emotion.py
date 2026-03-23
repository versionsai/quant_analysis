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
            from data.data_source import DataSource

            data_source = DataSource()
            try:
                snapshot = data_source.get_a_share_market_snapshot()
                plate_df = data_source.get_plate_list("INDUSTRY")
                zt_df, _, _ = data_source.get_limit_pool()
            finally:
                data_source.close()

            if snapshot is not None and not snapshot.empty and plate_df is not None and not plate_df.empty:
                snapshot = snapshot.copy()
                snapshot["code"] = snapshot["code"].astype(str)
                snapshot["change_rate"] = pd.to_numeric(snapshot.get("change_rate"), errors="coerce").fillna(0.0)
                snapshot["turnover"] = pd.to_numeric(snapshot.get("turnover"), errors="coerce").fillna(0.0)
                zt_codes = set(zt_df["代码"].astype(str).tolist()) if zt_df is not None and not zt_df.empty and "代码" in zt_df.columns else set()

                data_source = DataSource()
                try:
                    for _, plate in plate_df.iterrows():
                        plate_name = str(plate.get("plate_name", "") or "").strip()
                        if not plate_name:
                            continue
                        members = data_source.get_plate_stocks(plate_name, "INDUSTRY")
                        if members is None or members.empty or "code" not in members.columns:
                            continue

                        member_codes = set(members["code"].astype(str).tolist())
                        sector_df = snapshot[snapshot["code"].isin(member_codes)]
                        if sector_df.empty:
                            continue

                        change_pct = float(sector_df["change_rate"].mean())
                        turnover = float(sector_df["turnover"].sum())
                        zt_count = int(len(member_codes & zt_codes))

                        sector = SectorEmotion(
                            sector_name=plate_name,
                            main_net_inflow=turnover * max(change_pct, 0.0) / 100.0,
                            zt_count=zt_count,
                            turnover=turnover,
                            change_pct=change_pct,
                        )
                        sector.zt_ratio = zt_count / max(len(member_codes), 1)
                        sectors.append(sector)
                finally:
                    data_source.close()

                if sectors:
                    max_turnover = max(abs(s.turnover) for s in sectors) or 1.0
                    max_change = max(abs(s.change_pct) for s in sectors) or 1.0
                    max_zt_ratio = max(s.zt_ratio for s in sectors) or 1.0
                    for sector in sectors:
                        sector.main_net_ratio = sector.main_net_inflow / max_turnover * 100
                        sector.score = (
                            50
                            + (sector.change_pct / max_change) * 20
                            + (sector.turnover / max_turnover) * 15
                            + (sector.zt_ratio / max_zt_ratio) * 15
                        )
                        sector.score = max(0, min(100, sector.score))

                    sectors.sort(key=lambda item: (-item.score, -item.turnover, -item.change_pct))
            
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
