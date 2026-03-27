# -*- coding: utf-8 -*-
"""
龙头识别模型

面向 A 股短线情绪交易系统的统一龙头识别接口。
设计目标：
1. 可扩展：后续可继续加入盘口、公告、AI 研判等特征
2. 可复用：可被 realtime_monitor / ml / backtest / dashboard 直接引用
3. 可解释：输出龙头分数与分项理由

示例输入:
symbols = [
    {"code": "600580", "name": "卧龙电驱"},
    {"code": "000625", "name": "长安汽车"},
]

示例输出:
{
    "trade_date": "20260327",
    "market_cycle": "主升",
    "top_leader": {
        "code": "600580",
        "name": "卧龙电驱",
        "leader_score": 0.78,
        "concept_name": "机器人",
        "signals": ["连板2天", "主线概念:机器人"]
    },
    "leaders": [...]
}
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

import pandas as pd

from data.data_source import DataSource
from strategy.analysis.emotion.market_cycle import MarketCycleAnalyzer, MarketCycleSnapshot
from strategy.analysis.emotion.sector_strength import SectorStrengthAnalyzer, SectorStrengthSnapshot
from strategy.analysis.emotion.stock_emotion import StockEmotionAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LeaderCandidate:
    """单只候选股的龙头识别结果。"""

    code: str
    name: str
    leader_score: float
    rank: int = 0
    concept_name: str = ""
    stock_emotion_score: float = 0.0
    sector_score: float = 0.0
    price_strength: float = 0.0
    liquidity_score: float = 0.0
    limit_bonus: float = 0.0
    is_core_leader: bool = False
    last_price: float = 0.0
    change_pct: float = 0.0
    continuous_limit_days: int = 0
    signals: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "code": self.code,
            "name": self.name,
            "leader_score": round(float(self.leader_score or 0.0), 4),
            "rank": int(self.rank or 0),
            "concept_name": self.concept_name,
            "stock_emotion_score": round(float(self.stock_emotion_score or 0.0), 2),
            "sector_score": round(float(self.sector_score or 0.0), 4),
            "price_strength": round(float(self.price_strength or 0.0), 4),
            "liquidity_score": round(float(self.liquidity_score or 0.0), 4),
            "limit_bonus": round(float(self.limit_bonus or 0.0), 4),
            "is_core_leader": bool(self.is_core_leader),
            "last_price": round(float(self.last_price or 0.0), 4),
            "change_pct": round(float(self.change_pct or 0.0), 2),
            "continuous_limit_days": int(self.continuous_limit_days or 0),
            "signals": list(self.signals),
            "reasons": list(self.reasons),
        }


@dataclass
class LeaderSnapshot:
    """龙头快照。"""

    trade_date: str
    market_cycle: str
    market_cycle_score: float
    sector_top: str = ""
    leaders: List[LeaderCandidate] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        top_leader = self.leaders[0].to_dict() if self.leaders else {}
        return {
            "trade_date": self.trade_date,
            "market_cycle": self.market_cycle,
            "market_cycle_score": round(float(self.market_cycle_score or 0.0), 4),
            "sector_top": self.sector_top,
            "top_leader": top_leader,
            "leaders": [item.to_dict() for item in self.leaders],
        }


class LeaderModel:
    """统一龙头识别模型。"""

    def __init__(self):
        self.market_cycle_analyzer = MarketCycleAnalyzer()
        self.sector_strength_analyzer = SectorStrengthAnalyzer()
        self.stock_emotion_analyzer = StockEmotionAnalyzer()

    def analyze(
        self,
        symbols: Iterable[Dict[str, str]],
        trade_date: Optional[str] = None,
        top_n: int = 5,
    ) -> LeaderSnapshot:
        """分析给定候选池中的龙头股。"""
        date_text = str(trade_date or (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"))
        market_cycle: MarketCycleSnapshot = self.market_cycle_analyzer.analyze(trade_date=date_text)
        sector_strength: SectorStrengthSnapshot = self.sector_strength_analyzer.analyze(trade_date=date_text)

        normalized_symbols = self._normalize_symbols(symbols)
        quote_map = self._load_quote_map([item["code"] for item in normalized_symbols])
        candidates: List[LeaderCandidate] = []
        for item in normalized_symbols:
            candidate = self._score_symbol(
                code=item["code"],
                name=item["name"],
                trade_date=date_text,
                market_cycle=market_cycle,
                sector_strength=sector_strength,
                quote_map=quote_map,
            )
            if candidate is not None:
                candidates.append(candidate)

        candidates.sort(key=lambda row: (-row.leader_score, -row.stock_emotion_score, row.code))
        for index, row in enumerate(candidates[:max(int(top_n), 1)], 1):
            row.rank = index

        selected = candidates[:max(int(top_n), 1)]
        return LeaderSnapshot(
            trade_date=date_text,
            market_cycle=str(market_cycle.cycle or ""),
            market_cycle_score=float(market_cycle.cycle_score or 0.0),
            sector_top=str(sector_strength.top_sector or ""),
            leaders=selected,
        )

    def _score_symbol(
        self,
        code: str,
        name: str,
        trade_date: str,
        market_cycle: MarketCycleSnapshot,
        sector_strength: SectorStrengthSnapshot,
        quote_map: Dict[str, Dict[str, float]],
    ) -> Optional[LeaderCandidate]:
        """计算单只标的的龙头分数。"""
        try:
            result = self.stock_emotion_analyzer.analyze_stock(symbol=code, name=name, date=trade_date)
            if not result.success:
                return None

            stock_data = dict(result.raw_data or {})
            stock_score = float(result.score or 0.0) / 100.0
            concept_name = str(stock_data.get("concept_name", "") or "")
            concept_score = float(stock_data.get("sector_strength", 0.0) or 0.0)
            limit_days = int(stock_data.get("continuous_limit_days", 0) or 0)
            signals = list(result.signals or [])
            warnings = list(result.warnings or [])

            quote = quote_map.get(code, {})
            change_pct = float(quote.get("change_pct", 0.0) or 0.0)
            turnover_rate = float(stock_data.get("turnover_rate", 0.0) or 0.0)

            price_strength = max(0.0, min(1.0, (change_pct + 2.0) / 12.0))
            liquidity_score = max(0.0, min(1.0, turnover_rate / 15.0))
            limit_bonus = 0.0
            if limit_days >= 3:
                limit_bonus = 1.0
            elif limit_days == 2:
                limit_bonus = 0.75
            elif bool(stock_data.get("is_limit_up", False)):
                limit_bonus = 0.45

            leader_score = (
                0.35 * stock_score
                + 0.20 * concept_score
                + 0.15 * price_strength
                + 0.10 * liquidity_score
                + 0.15 * limit_bonus
                + 0.05 * float(market_cycle.cycle_score or 0.0)
            )
            leader_score = max(0.0, min(1.0, leader_score))

            reasons = [
                f"个股情绪{stock_score:.2f}",
                f"板块强度{concept_score:.2f}",
                f"价格强度{price_strength:.2f}",
                f"流动性{liquidity_score:.2f}",
            ]
            if limit_bonus > 0:
                reasons.append(f"连板加分{limit_bonus:.2f}")
            if concept_name:
                reasons.append(f"概念:{concept_name}")
            if warnings:
                reasons.extend([f"风险:{item}" for item in warnings[:2]])

            is_core_leader = bool(
                leader_score >= 0.68
                and stock_score >= 0.65
                and (concept_score >= 0.55 or limit_bonus >= 0.75)
            )

            return LeaderCandidate(
                code=code,
                name=name,
                leader_score=leader_score,
                concept_name=concept_name or str(sector_strength.top_sector or ""),
                stock_emotion_score=float(result.score or 0.0),
                sector_score=concept_score,
                price_strength=price_strength,
                liquidity_score=liquidity_score,
                limit_bonus=limit_bonus,
                is_core_leader=is_core_leader,
                last_price=float(quote.get("last_price", 0.0) or 0.0),
                change_pct=change_pct,
                continuous_limit_days=limit_days,
                signals=signals[:6],
                reasons=reasons,
            )
        except Exception as e:
            logger.debug(f"龙头识别失败 {code}: {e}")
            return None

    @staticmethod
    def _normalize_symbols(symbols: Iterable[Dict[str, str]]) -> List[Dict[str, str]]:
        """标准化候选池输入。"""
        normalized: List[Dict[str, str]] = []
        seen = set()
        for item in symbols:
            code = str((item or {}).get("code", "") or "").strip().zfill(6)
            name = str((item or {}).get("name", "") or "").strip()
            if not code or code in seen:
                continue
            seen.add(code)
            normalized.append({"code": code, "name": name})
        return normalized

    @staticmethod
    def _load_quote_map(codes: List[str]) -> Dict[str, Dict[str, float]]:
        """批量获取快照行情。"""
        if not codes:
            return {}
        data_source = DataSource()
        try:
            df = data_source.get_market_snapshots(codes)
        finally:
            data_source.close()
        if df is None or df.empty:
            return {}
        df = df.copy()
        df.columns = [str(col).lower() for col in df.columns]
        quote_map: Dict[str, Dict[str, float]] = {}
        for _, row in df.iterrows():
            code = str(row.get("code", "") or "").replace("SH.", "").replace("SZ.", "").strip().zfill(6)
            if not code:
                continue
            quote_map[code] = {
                "last_price": float(pd.to_numeric(row.get("last_price", 0.0), errors="coerce") or 0.0),
                "change_pct": float(pd.to_numeric(row.get("change_rate", 0.0), errors="coerce") or 0.0),
            }
        return quote_map


def build_leader_snapshot(
    symbols: Iterable[Dict[str, str]],
    trade_date: Optional[str] = None,
    top_n: int = 5,
) -> Dict[str, object]:
    """构建龙头识别快照字典。"""
    return LeaderModel().analyze(symbols=symbols, trade_date=trade_date, top_n=top_n).to_dict()
