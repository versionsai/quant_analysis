# -*- coding: utf-8 -*-
"""
情绪组合分析器

将市场周期、板块强度、盘中承接、过热度、龙头识别与 Top 风险统一成一套可复用输出，
供 realtime_monitor / emotion-scan / dashboard / backtest 共用。
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import numpy as np

from data.data_source import DataSource
from strategy.alpha.leader_model import LeaderModel
from strategy.analysis.emotion.market_cycle import MarketCycleAnalyzer, MarketCycleSnapshot
from strategy.analysis.emotion.overheat import OverheatAnalyzer, OverheatSnapshot
from strategy.analysis.emotion.sector_strength import SectorStrengthAnalyzer, SectorStrengthSnapshot
from strategy.analysis.emotion.space_score import EmotionSpaceScoreAnalyzer, EmotionSpaceScore
from strategy.analysis.emotion.stock_emotion import StockEmotionAnalyzer
from strategy.ml import TopFeatureRow
from strategy.ml.top_model import TopPredictModel, load_or_build_top_model


@dataclass
class EmotionMarketContext:
    """统一市场情绪上下文。"""

    trade_date: str
    market_cycle: str
    market_cycle_score: float
    sector_top: str
    space_score: float
    space_score_100: float
    space_level: str
    overheat: float
    overheat_risk: str
    recommended_exposure: float
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "trade_date": self.trade_date,
            "market_cycle": self.market_cycle,
            "market_cycle_score": round(float(self.market_cycle_score or 0.0), 4),
            "sector_top": self.sector_top,
            "space_score": round(float(self.space_score or 0.0), 4),
            "space_score_100": round(float(self.space_score_100 or 0.0), 2),
            "space_level": self.space_level,
            "overheat": round(float(self.overheat or 0.0), 4),
            "overheat_risk": self.overheat_risk,
            "recommended_exposure": round(float(self.recommended_exposure or 0.0), 4),
            "reasons": list(self.reasons),
        }


@dataclass
class EmotionStockProfile:
    """候选股情绪画像。"""

    symbol: str
    name: str
    trade_date: str
    stock_emotion_score: float
    concept_strength_score: float
    concept_name: str
    leader_score: float
    leader_rank: int
    is_core_leader: bool
    price_strength: float
    top_prob: float
    top_decision: str
    composite_score: float
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        """转换为标准字典。"""
        return {
            "symbol": self.symbol,
            "name": self.name,
            "trade_date": self.trade_date,
            "stock_emotion_score": round(float(self.stock_emotion_score or 0.0), 2),
            "concept_strength_score": round(float(self.concept_strength_score or 0.0), 4),
            "concept_name": self.concept_name,
            "leader_score": round(float(self.leader_score or 0.0), 4),
            "leader_rank": int(self.leader_rank or 0),
            "is_core_leader": bool(self.is_core_leader),
            "price_strength": round(float(self.price_strength or 0.0), 4),
            "top_prob": round(float(self.top_prob or 0.0), 4),
            "top_decision": self.top_decision,
            "composite_score": round(float(self.composite_score or 0.0), 4),
            "reasons": list(self.reasons),
        }


class EmotionEnsembleAnalyzer:
    """组合情绪分析器。"""

    def __init__(self, top_model: Optional[TopPredictModel] = None):
        self.market_cycle_analyzer = MarketCycleAnalyzer()
        self.sector_strength_analyzer = SectorStrengthAnalyzer()
        self.space_score_analyzer = EmotionSpaceScoreAnalyzer()
        self.overheat_analyzer = OverheatAnalyzer()
        self.stock_emotion_analyzer = StockEmotionAnalyzer()
        self.leader_model = LeaderModel()
        self.top_model = top_model or load_or_build_top_model()

    def build_market_context(
        self,
        trade_date: Optional[str] = None,
        as_of: Optional[datetime] = None,
    ) -> EmotionMarketContext:
        """构建市场级情绪上下文。"""
        market_cycle: MarketCycleSnapshot = self.market_cycle_analyzer.analyze(trade_date=trade_date)
        sector_strength: SectorStrengthSnapshot = self.sector_strength_analyzer.analyze(trade_date=trade_date)
        space_snapshot: EmotionSpaceScore = self.space_score_analyzer.analyze(trade_date=trade_date, as_of=as_of)
        overheat_snapshot: OverheatSnapshot = self.overheat_analyzer.analyze(trade_date=trade_date)

        recommended_exposure = float(np.clip(
            0.55 * float(space_snapshot.space_score or 0.0)
            + 0.25 * float(market_cycle.cycle_score or 0.0)
            + 0.20 * (1.0 - float(overheat_snapshot.overheat or 0.0)),
            0.0,
            1.0,
        ))
        reasons = [
            f"周期{market_cycle.cycle or '未知'}",
            f"空间{space_snapshot.space_level}",
            f"过热{overheat_snapshot.risk_level}",
        ]
        if sector_strength.top_sector:
            reasons.append(f"主线{sector_strength.top_sector}")

        return EmotionMarketContext(
            trade_date=str(space_snapshot.trade_date or trade_date or datetime.now().strftime("%Y%m%d")),
            market_cycle=str(market_cycle.cycle or ""),
            market_cycle_score=float(market_cycle.cycle_score or 0.0),
            sector_top=str(sector_strength.top_sector or ""),
            space_score=float(space_snapshot.space_score or 0.0),
            space_score_100=float(space_snapshot.space_score or 0.0) * 100.0,
            space_level=str(space_snapshot.space_level or ""),
            overheat=float(overheat_snapshot.overheat or 0.0),
            overheat_risk=str(overheat_snapshot.risk_level or ""),
            recommended_exposure=recommended_exposure,
            reasons=reasons + list(overheat_snapshot.reasons or [])[:2],
        )

    def build_stock_profiles(
        self,
        symbols: Iterable[Dict[str, str]],
        trade_date: Optional[str] = None,
        as_of: Optional[datetime] = None,
    ) -> Dict[str, EmotionStockProfile]:
        """构建候选股画像。"""
        normalized = self._normalize_symbols(symbols)
        if not normalized:
            return {}

        date_text = str(trade_date or datetime.now().strftime("%Y%m%d"))
        market_context = self.build_market_context(trade_date=date_text, as_of=as_of)
        leader_snapshot = self.leader_model.analyze(symbols=normalized, trade_date=date_text, top_n=max(len(normalized), 1))
        leader_map = {item.code: item for item in leader_snapshot.leaders}
        quote_map = self._load_quote_map([item["code"] for item in normalized])

        profiles: Dict[str, EmotionStockProfile] = {}
        zt_diff = float(np.clip((leader_snapshot.market_cycle_score - 0.5) * 1.4, -1.0, 1.0))
        eff_diff = float(np.clip(market_context.space_score - market_context.overheat, -1.0, 1.0))
        for item in normalized:
            code = item["code"]
            name = item["name"]
            stock_res = self.stock_emotion_analyzer.analyze_stock(symbol=code, name=name, date=date_text)
            stock_score = float(stock_res.score or 50.0) / 100.0 if stock_res and stock_res.success else 0.5
            stock_raw = dict(stock_res.raw_data or {}) if stock_res and stock_res.success else {}
            concept_score = float(stock_raw.get("sector_strength", 0.0) or 0.0)
            concept_name = str(stock_raw.get("concept_name", "") or "")
            leader = leader_map.get(code)
            leader_score = float(leader.leader_score or 0.0) if leader else 0.0
            leader_rank = int(leader.rank or 0) if leader else 0
            is_core_leader = bool(leader.is_core_leader) if leader else False
            quote = quote_map.get(code, {})
            price_change = float(quote.get("change_pct", 0.0) or 0.0)
            price_strength = float(np.clip((price_change + 2.0) / 12.0, 0.0, 1.0))

            feature_row = TopFeatureRow(
                trade_date=date_text,
                symbol=code,
                space_score=float(market_context.space_score or 0.0),
                overheat=float(market_context.overheat or 0.0),
                acc=float(np.clip(0.50 * stock_score + 0.25 * concept_score + 0.25 * leader_score, 0.0, 1.0)),
                zt_diff=zt_diff,
                eff_diff=eff_diff,
                leader_ret=float(np.clip(price_change / 10.0, -1.0, 1.0)),
                label=None,
            )
            prediction = self.top_model.predict_one(feature_row)
            top_prob = float(prediction.top_prob or 0.0)
            composite_score = float(np.clip(
                0.34 * stock_score
                + 0.24 * concept_score
                + 0.22 * leader_score
                + 0.12 * market_context.space_score
                + 0.08 * (1.0 - top_prob),
                0.0,
                1.0,
            ))
            reasons = []
            if leader is not None:
                reasons.extend(list(leader.reasons or [])[:3])
            if stock_res and stock_res.success:
                reasons.extend(list(stock_res.signals or [])[:2])
                reasons.extend([f"风险:{item}" for item in list(stock_res.warnings or [])[:1]])
            reasons.append(f"Top风险{top_prob:.2f}")

            profiles[code] = EmotionStockProfile(
                symbol=code,
                name=name,
                trade_date=date_text,
                stock_emotion_score=stock_score * 100.0,
                concept_strength_score=concept_score,
                concept_name=concept_name,
                leader_score=leader_score,
                leader_rank=leader_rank,
                is_core_leader=is_core_leader,
                price_strength=price_strength,
                top_prob=top_prob,
                top_decision=str(prediction.decision or ""),
                composite_score=composite_score,
                reasons=reasons,
            )
        return profiles

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
        """批量获取行情快照。"""
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
        result: Dict[str, Dict[str, float]] = {}
        for _, row in df.iterrows():
            code = str(row.get("code", "") or "").replace("SH.", "").replace("SZ.", "").strip().zfill(6)
            if not code:
                continue
            result[code] = {
                "last_price": float(row.get("last_price", 0.0) or 0.0),
                "change_pct": float(row.get("change_rate", 0.0) or 0.0),
            }
        return result


def build_emotion_market_context(
    trade_date: Optional[str] = None,
    as_of: Optional[datetime] = None,
) -> Dict[str, object]:
    """构建统一市场情绪上下文字典。"""
    return EmotionEnsembleAnalyzer().build_market_context(trade_date=trade_date, as_of=as_of).to_dict()


def build_emotion_stock_profiles(
    symbols: Iterable[Dict[str, str]],
    trade_date: Optional[str] = None,
    as_of: Optional[datetime] = None,
) -> Dict[str, Dict[str, object]]:
    """构建候选股画像字典。"""
    profiles = EmotionEnsembleAnalyzer().build_stock_profiles(symbols=symbols, trade_date=trade_date, as_of=as_of)
    return {code: item.to_dict() for code, item in profiles.items()}
