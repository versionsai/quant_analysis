# -*- coding: utf-8 -*-
"""
市场情绪周期模块

对外提供统一的 A 股短线市场周期快照，便于被策略、ML、看板和回测复用。

示例数据结构:
{
    "trade_date": "20260327",
    "cycle": "主升",
    "cycle_score": 0.78,
    "normalized_score": 78.2,
    "zt_count": 46,
    "dt_count": 4,
    "lb_max": 5,
    "lb_count": 12,
    "hot_sectors": ["算力", "机器人", "黄金"],
}
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from strategy.analysis.emotion.market_emotion import MarketEmotionAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class MarketCycleSnapshot:
    """市场情绪周期快照。"""

    trade_date: str
    cycle: str
    cycle_score: float
    normalized_score: float
    cycle_description: str = ""
    zt_count: int = 0
    dt_count: int = 0
    lb_max: int = 0
    lb_count: int = 0
    hot_sectors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "trade_date": self.trade_date,
            "cycle": self.cycle,
            "cycle_score": round(float(self.cycle_score or 0.0), 4),
            "normalized_score": round(float(self.normalized_score or 0.0), 2),
            "cycle_description": self.cycle_description,
            "zt_count": int(self.zt_count or 0),
            "dt_count": int(self.dt_count or 0),
            "lb_max": int(self.lb_max or 0),
            "lb_count": int(self.lb_count or 0),
            "hot_sectors": list(self.hot_sectors),
        }


class MarketCycleAnalyzer:
    """统一市场周期分析器。"""

    def __init__(self):
        self._analyzer = MarketEmotionAnalyzer()

    def analyze(self, trade_date: Optional[str] = None) -> MarketCycleSnapshot:
        """分析指定交易日的大盘周期。"""
        date_text = str(trade_date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
        emotion = self._analyzer.get_market_emotion(date_text)
        if emotion is None:
            logger.warning(f"市场周期分析失败，返回默认快照: {date_text}")
            return MarketCycleSnapshot(
                trade_date=date_text,
                cycle="未知",
                cycle_score=0.5,
                normalized_score=50.0,
                cycle_description="暂无可用市场情绪数据",
            )

        normalized_score = float(emotion.normalized_score or 50.0)
        return MarketCycleSnapshot(
            trade_date=date_text,
            cycle=str(emotion.cycle or "未知"),
            cycle_score=max(0.0, min(1.0, normalized_score / 100.0)),
            normalized_score=normalized_score,
            cycle_description=str(emotion.cycle_description or ""),
            zt_count=int(emotion.zt_count or 0),
            dt_count=int(emotion.dt_count or 0),
            lb_max=int(emotion.lb_max or 0),
            lb_count=int(emotion.lb_count or 0),
            hot_sectors=list(emotion.hot_sectors or [])[:5],
        )


def build_market_cycle_snapshot(trade_date: Optional[str] = None) -> Dict[str, object]:
    """构建市场周期快照字典。"""
    return MarketCycleAnalyzer().analyze(trade_date=trade_date).to_dict()

