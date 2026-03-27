# -*- coding: utf-8 -*-
"""
板块强度模块

将板块热度整理为统一的可复用结构，供 Space_Score、龙头识别和回测共用。

示例数据结构:
{
    "trade_date": "20260327",
    "sector_score": 0.66,
    "top_sector": "机器人",
    "top_sectors": [
        {"sector": "机器人", "score": 82.1, "rank": 1, "zt_count": 5},
        {"sector": "算力", "score": 79.4, "rank": 2, "zt_count": 4}
    ]
}
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from strategy.analysis.emotion.sector_emotion import SectorEmotionAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SectorStrengthSnapshot:
    """板块强度快照。"""

    trade_date: str
    sector_score: float
    top_sector: str = ""
    top_sectors: List[Dict[str, object]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "trade_date": self.trade_date,
            "sector_score": round(float(self.sector_score or 0.0), 4),
            "top_sector": self.top_sector,
            "top_sectors": list(self.top_sectors),
        }


class SectorStrengthAnalyzer:
    """统一板块强度分析器。"""

    def __init__(self):
        self._analyzer = SectorEmotionAnalyzer()

    def analyze(self, trade_date: Optional[str] = None, top_n: int = 10) -> SectorStrengthSnapshot:
        """分析指定交易日的板块强度。"""
        date_text = str(trade_date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
        result = self._analyzer.analyze_sectors(date_text)
        if not result.success:
            logger.warning(f"板块强度分析失败，返回默认快照: {date_text}")
            return SectorStrengthSnapshot(trade_date=date_text, sector_score=0.5)

        sector_rows = list((result.raw_data or {}).get("sectors", []) or [])
        normalized_rows: List[Dict[str, object]] = []
        for item in sector_rows[:max(int(top_n), 1)]:
            normalized_rows.append(
                {
                    "sector": str(item.get("sector", "") or ""),
                    "score": round(float(item.get("score", 0.0) or 0.0), 2),
                    "rank": int(item.get("rank", 0) or 0),
                    "zt_count": int(item.get("zt_count", 0) or 0),
                    "change_pct": round(float(item.get("change_pct", 0.0) or 0.0), 2),
                    "turnover": round(float(item.get("turnover", 0.0) or 0.0), 2),
                }
            )
        top_score = float(normalized_rows[0]["score"]) / 100.0 if normalized_rows else 0.5
        top_sector = str(normalized_rows[0]["sector"]) if normalized_rows else ""
        return SectorStrengthSnapshot(
            trade_date=date_text,
            sector_score=max(0.0, min(1.0, top_score)),
            top_sector=top_sector,
            top_sectors=normalized_rows,
        )


def build_sector_strength_snapshot(trade_date: Optional[str] = None, top_n: int = 10) -> Dict[str, object]:
    """构建板块强度快照字典。"""
    return SectorStrengthAnalyzer().analyze(trade_date=trade_date, top_n=top_n).to_dict()

