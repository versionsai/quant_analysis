# -*- coding: utf-8 -*-
"""
短线情绪 Space_Score 模块

核心公式:
space_score = 0.4 * cycle + 0.3 * sector + 0.3 * intraday

示例数据结构:
{
    "trade_date": "20260327",
    "cycle_score": 0.72,
    "sector_score": 0.66,
    "intraday_score": 0.58,
    "space_score": 0.66,
    "space_level": "active"
}
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

from strategy.analysis.emotion.intraday_flow import IntradayFlowAnalyzer, IntradayFlowSnapshot
from strategy.analysis.emotion.market_cycle import MarketCycleAnalyzer, MarketCycleSnapshot
from strategy.analysis.emotion.sector_strength import SectorStrengthAnalyzer, SectorStrengthSnapshot


@dataclass
class EmotionSpaceScore:
    """情绪 Space_Score 快照。"""

    trade_date: str
    cycle_score: float
    sector_score: float
    intraday_score: float
    space_score: float
    space_level: str

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "trade_date": self.trade_date,
            "cycle_score": round(float(self.cycle_score or 0.0), 4),
            "sector_score": round(float(self.sector_score or 0.0), 4),
            "intraday_score": round(float(self.intraday_score or 0.0), 4),
            "space_score": round(float(self.space_score or 0.0), 4),
            "space_level": self.space_level,
        }


class EmotionSpaceScoreAnalyzer:
    """情绪 Space_Score 分析器。"""

    def __init__(self):
        self.market_cycle_analyzer = MarketCycleAnalyzer()
        self.sector_strength_analyzer = SectorStrengthAnalyzer()
        self.intraday_flow_analyzer = IntradayFlowAnalyzer()

    def analyze(
        self,
        trade_date: Optional[str] = None,
        as_of: Optional[datetime] = None,
    ) -> EmotionSpaceScore:
        """计算 Space_Score。"""
        market_cycle: MarketCycleSnapshot = self.market_cycle_analyzer.analyze(trade_date=trade_date)
        sector_strength: SectorStrengthSnapshot = self.sector_strength_analyzer.analyze(trade_date=trade_date)
        intraday_flow: IntradayFlowSnapshot = self.intraday_flow_analyzer.analyze(as_of=as_of)

        score = (
            0.4 * float(market_cycle.cycle_score or 0.0)
            + 0.3 * float(sector_strength.sector_score or 0.0)
            + 0.3 * float(intraday_flow.intraday_score or 0.0)
        )
        level = self._to_level(score)
        return EmotionSpaceScore(
            trade_date=str(trade_date or market_cycle.trade_date),
            cycle_score=float(market_cycle.cycle_score or 0.0),
            sector_score=float(sector_strength.sector_score or 0.0),
            intraday_score=float(intraday_flow.intraday_score or 0.0),
            space_score=score,
            space_level=level,
        )

    @staticmethod
    def _to_level(score: float) -> str:
        """将 Space_Score 映射为情绪档位。"""
        if score >= 0.80:
            return "hot"
        if score >= 0.60:
            return "active"
        if score >= 0.40:
            return "neutral"
        return "cold"


def build_emotion_space_score(trade_date: Optional[str] = None, as_of: Optional[datetime] = None) -> Dict[str, object]:
    """构建 Space_Score 字典。"""
    return EmotionSpaceScoreAnalyzer().analyze(trade_date=trade_date, as_of=as_of).to_dict()

