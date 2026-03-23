# -*- coding: utf-8 -*-
"""
实时选股监控系统
双策略运行: PriceAction+MACD + 弱转强
双重信号标注
"""
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from data import DataSource, get_dynamic_pool
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    WeakToStrongTimingStrategy,
    WeakToStrongParams,
)
from config.config import STRATEGY_CONFIG
from strategy.analysis.fund.fund_consistency import compute_fcf
from trading.report_formatter import SignalRecommendRow
from trading.runtime_config import get_runtime_settings
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class StockSignal:
    """股票信号"""
    code: str
    name: str
    price: float
    change_pct: float
    volume: float
    signal_type: str
    target_price: Optional[float] = None
    stop_loss: Optional[float] = None
    reason: str = ""
    score: float = 0.0
    dual_signal: bool = False
    ws_stage: int = 0
    ws_reason: str = ""
    ws_score: float = 0.0
    market_emotion_score: float = 0.0
    stock_emotion_score: float = 0.0
    concept_strength_score: float = 0.0
    concept_name: str = ""
    fcf: float = 0.0
    order_book_bias: str = ""
    order_book_ratio: float = 0.0
    bid_volume_sum: float = 0.0
    ask_volume_sum: float = 0.0


class RealtimeMonitor:
    """实时选股监控"""
    
    def __init__(
        self,
        data_source: DataSource = None,
        etf_count: int = 5,
        stock_count: int = 5,
        db_path: str = None,
        strategy_overrides: Optional[Dict[str, float]] = None,
        risk_overrides: Optional[Dict[str, float]] = None,
    ):
        self.data_source = data_source or DataSource()
        self.etf_count = etf_count
        self.stock_count = stock_count
        self.db_path = db_path or os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
        self.strategy_overrides = strategy_overrides or {}
        self.risk_cfg = dict(STRATEGY_CONFIG)
        if risk_overrides:
            self.risk_cfg.update(risk_overrides)

        self.pa_macd_strategy = PriceActionMACDStrategy(
            lookback=int(self.strategy_overrides.get("lookback", 20)),
            macd_fast=int(self.strategy_overrides.get("macd_fast", 12)),
            macd_slow=int(self.strategy_overrides.get("macd_slow", 26)),
            macd_signal=int(self.strategy_overrides.get("macd_signal", 9)),
        )
        
        self.ws_strategy = WeakToStrongTimingStrategy(
            params=WeakToStrongParams()
        )
        
        self.etf_pool = []
        self.stock_pool = []
        self._load_dynamic_pool()

        # 情绪（全市场辅助 + 个股优先）
        self._market_emotion_score: Optional[float] = None
        self._market_emotion_cycle: str = ""
        self._market_emotion_ts: Optional[datetime] = None
        self._stock_emotion_cache: Dict[str, float] = {}
        self._space_score: Optional[float] = None
        self._space_level: str = ""
        self._space_ts: Optional[datetime] = None
        self._concept_strength_cache: Dict[str, tuple] = {}
        self._analysis_cache: Dict[str, tuple] = {}
        self._analysis_cache_ttl_sec = int(os.environ.get("ANALYZE_STOCK_CACHE_SEC", "120") or "120")
        self._precheck_timeout_sec = float(os.environ.get("SCAN_PRECHECK_TIMEOUT_SEC", "8") or "8")
        self._runtime_mode: str = "auto"
        self._runtime_mode_label: str = "自动"
        self._runtime_mode_description: str = ""
        self._runtime_mode_ts: Optional[datetime] = None
        self._runtime_mode_cache_ttl_sec = int(os.environ.get("RUNTIME_MODE_CACHE_SEC", "30") or "30")
        self._index_change_map: Dict[str, float] = {}
        self._index_context_ts: Optional[datetime] = None
        self._index_context_cache_ttl_sec = int(os.environ.get("INDEX_CONTEXT_CACHE_SEC", "120") or "120")
        self._effective_market_regime: str = "normal"
        self._effective_market_regime_reason: str = ""
        self._ai_regime_decision_ts: Optional[datetime] = None
        self._ai_regime_decision_cache_sec = int(os.environ.get("AI_REGIME_CACHE_SEC", "300") or "300")
        self._ai_regime_decision: Dict[str, object] = {}
        self._ai_signal_review_cache: Dict[str, tuple] = {}
        self._ai_signal_review_cache_sec = int(os.environ.get("AI_SIGNAL_REVIEW_CACHE_SEC", "180") or "180")
        self._ai_agent = None
        self._ai_agent_ready = False
        self._ai_enabled = str(os.environ.get("ENABLE_AI_AGENT", "false")).lower() == "true"

    def clear_runtime_cache(self) -> None:
        """
        清理运行期分析缓存
        """
        self._analysis_cache = {}
        self._ai_signal_review_cache = {}

    def _get_ai_agent(self):
        """
        懒初始化 AI Agent。
        """
        if not self._ai_enabled:
            return None
        if self._ai_agent_ready:
            return self._ai_agent
        self._ai_agent_ready = True
        try:
            from agents import get_quant_agent

            self._ai_agent = get_quant_agent()
        except Exception as e:
            logger.warning(f"AI Agent 初始化失败，模式判断回退规则: {e}")
            self._ai_agent = None
        return self._ai_agent

    def _refresh_runtime_mode(self) -> None:
        """
        刷新运行时市场模式。
        """
        if self._runtime_mode_ts and (datetime.now() - self._runtime_mode_ts).total_seconds() < self._runtime_mode_cache_ttl_sec:
            return
        try:
            settings = get_runtime_settings(self.db_path)
            self._runtime_mode = str(settings.get("market_regime_mode", "auto") or "auto")
            self._runtime_mode_label = str(settings.get("market_regime_label", "自动") or "自动")
            self._runtime_mode_description = str(settings.get("market_regime_description", "") or "")
        except Exception as e:
            logger.debug(f"运行时模式读取失败，回退自动模式: {e}")
            self._runtime_mode = "auto"
            self._runtime_mode_label = "自动"
            self._runtime_mode_description = ""
        self._runtime_mode_ts = datetime.now()

    def _refresh_index_context(self) -> None:
        """
        刷新指数涨跌上下文，用于极端环境判断。
        """
        if self._index_context_ts and (datetime.now() - self._index_context_ts).total_seconds() < self._index_context_cache_ttl_sec:
            return
        try:
            df = self.data_source.get_market_snapshots(["000001", "399001", "399006"])
            change_map: Dict[str, float] = {}
            if df is not None and not df.empty:
                df.columns = [str(col).lower() for col in df.columns]
                for _, row in df.iterrows():
                    code = str(row.get("code", "") or "").strip()[-6:]
                    if not code:
                        continue
                    change_map[code] = float(row.get("change_rate", 0.0) or 0.0)
            self._index_change_map = change_map
            self._index_context_ts = datetime.now()
        except Exception as e:
            logger.debug(f"指数上下文刷新失败: {e}")
            self._index_change_map = {}
            self._index_context_ts = datetime.now()

    def _resolve_effective_market_regime(self) -> None:
        """
        解析当前实际生效的市场模式。
        """
        self._refresh_runtime_mode()
        self._refresh_index_context()

        if self._runtime_mode != "auto":
            self._effective_market_regime = self._runtime_mode
            self._effective_market_regime_reason = f"看板手动切换为{self._runtime_mode_label}"
            self._apply_ai_regime_overlay()
            return

        market_score = float(self._market_emotion_score) if self._market_emotion_score is not None else 50.0
        space_score = float(self._space_score) if self._space_score is not None else 50.0
        index_values = list(self._index_change_map.values())
        avg_change = float(np.mean(index_values)) if index_values else 0.0
        worst_change = float(min(index_values)) if index_values else 0.0

        if (market_score <= 30.0 and avg_change >= 1.2 and worst_change > -1.0 and space_score >= 55.0):
            self._effective_market_regime = "golden_pit"
            self._effective_market_regime_reason = (
                f"自动识别为黄金坑修复: 情绪{market_score:.0f}, 指数均值{avg_change:+.2f}%"
            )
        elif market_score <= 35.0 or avg_change <= -1.5 or worst_change <= -2.5:
            self._effective_market_regime = "defense"
            self._effective_market_regime_reason = (
                f"自动识别为防守: 情绪{market_score:.0f}, 指数均值{avg_change:+.2f}%"
            )
        else:
            self._effective_market_regime = "normal"
            self._effective_market_regime_reason = (
                f"自动识别为正常: 情绪{market_score:.0f}, 指数均值{avg_change:+.2f}%"
            )

        self._apply_ai_regime_overlay()

    def _apply_ai_regime_overlay(self) -> None:
        """
        使用 AI 对当前市场模式做二次判断。
        """
        agent = self._get_ai_agent()
        if agent is None:
            return

        if self._ai_regime_decision_ts and (datetime.now() - self._ai_regime_decision_ts).total_seconds() < self._ai_regime_decision_cache_sec:
            payload = dict(self._ai_regime_decision or {})
        else:
            market_snapshot = {
                "runtime_mode": self._runtime_mode,
                "rule_mode": self._effective_market_regime,
                "rule_reason": self._effective_market_regime_reason,
                "market_emotion_score": float(self._market_emotion_score) if self._market_emotion_score is not None else None,
                "market_emotion_cycle": self._market_emotion_cycle,
                "space_score": float(self._space_score) if self._space_score is not None else None,
                "space_level": self._space_level,
                "index_change_map": self._index_change_map,
            }
            try:
                payload = agent.judge_market_regime(
                    market_snapshot=market_snapshot,
                    candidate_mode=self._effective_market_regime,
                )
            except Exception as e:
                logger.warning(f"AI 市场模式判断失败，回退规则模式: {e}")
                payload = {}
            self._ai_regime_decision = payload
            self._ai_regime_decision_ts = datetime.now()

        ai_mode = str(payload.get("mode", "") or "").strip().lower()
        ai_reason = str(payload.get("reason", "") or "").strip()
        ai_confidence = float(payload.get("confidence", 0.0) or 0.0)
        if ai_mode in {"normal", "defense", "golden_pit"} and ai_confidence >= 0.55:
            self._effective_market_regime = ai_mode
            base_reason = self._effective_market_regime_reason
            self._effective_market_regime_reason = f"{base_reason} | AI:{ai_reason or ai_mode}({ai_confidence:.2f})"

    def _review_signal_with_ai(
        self,
        regime_mode: str,
        symbol: str,
        name: str,
        signal_type: str,
        reason: str,
        score: float,
        change_pct: float,
        stock_emotion_score: float,
        concept_strength_score: float,
        concept_name: str,
        order_book_bias: str,
        order_book_ratio: float,
        ws_stage: int,
        ws_score: float,
        fcf_score: float,
    ) -> Dict[str, object]:
        """
        使用 AI 对候选信号做二次审核。
        """
        agent = self._get_ai_agent()
        if agent is None:
            return {}

        cache_key = "|".join(
            [
                str(symbol).zfill(6),
                regime_mode,
                str(signal_type),
                f"{float(score or 0.0):.2f}",
                f"{float(change_pct or 0.0):.2f}",
                f"{float(stock_emotion_score or 0.0):.1f}",
                f"{float(concept_strength_score or 0.0):.2f}",
                str(ws_stage),
                f"{float(ws_score or 0.0):.1f}",
                f"{float(fcf_score or 0.0):.2f}",
            ]
        )
        cached = self._ai_signal_review_cache.get(cache_key)
        if cached is not None:
            cached_at, payload = cached
            if (datetime.now() - cached_at).total_seconds() < self._ai_signal_review_cache_sec:
                return dict(payload or {})

        signal_payload = {
            "code": str(symbol).zfill(6),
            "name": name,
            "regime_mode": regime_mode,
            "signal_type": signal_type,
            "reason": reason,
            "score": float(score or 0.0),
            "change_pct": float(change_pct or 0.0),
            "stock_emotion_score": float(stock_emotion_score or 0.0),
            "concept_strength_score": float(concept_strength_score or 0.0),
            "concept_name": concept_name,
            "order_book_bias": order_book_bias,
            "order_book_ratio": float(order_book_ratio or 0.0),
            "weak_to_strong_stage": int(ws_stage or 0),
            "weak_to_strong_score": float(ws_score or 0.0),
            "fcf": float(fcf_score or 0.0),
        }
        try:
            payload = agent.review_signal_with_regime(
                regime_mode=regime_mode,
                signal_payload=signal_payload,
            )
        except Exception as e:
            logger.warning(f"AI 模式信号审核失败 {symbol}: {e}")
            payload = {}
        self._ai_signal_review_cache[cache_key] = (datetime.now(), dict(payload or {}))
        return dict(payload or {})

    def _ensure_concept_context(self, symbol: str, name: str, stock_emotion_score: float, concept_strength_score: float, concept_name: str) -> tuple:
        """
        懒加载个股抱团上下文。
        """
        current_stock_score = float(stock_emotion_score or 0.0)
        current_concept_score = float(concept_strength_score or 0.0)
        current_concept_name = str(concept_name or "")
        if current_stock_score <= 0:
            current_stock_score = self._get_stock_emotion_score(symbol, name)
        if current_concept_score <= 0:
            current_concept_score, current_concept_name = self._get_concept_strength(symbol)
        return current_stock_score, current_concept_score, current_concept_name

    def _is_gold_pit_setup(self, df: pd.DataFrame, is_stock: bool, change_pct: float) -> bool:
        """
        判断是否符合恐慌后放量修复的黄金坑形态。
        """
        if df is None or df.empty or len(df) < 12:
            return False

        close = df["close"].astype(float).values
        open_ = df["open"].astype(float).values
        high = df["high"].astype(float).values
        volume = df["volume"].astype(float).values
        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]

        lookback_peak = float(np.max(high[-12:-1])) if len(high) >= 12 else float(np.max(high[:-1]))
        drawdown_pct = (close[-1] / lookback_peak - 1.0) * 100 if lookback_peak > 0 else 0.0
        recent_returns = (close[-3:] / np.where(prev_close[-3:] > 0, prev_close[-3:], close[-3:]) - 1.0) * 100
        latest_return = float((close[-1] / prev_close[-1] - 1.0) * 100) if prev_close[-1] > 0 else 0.0
        latest_bull = close[-1] > open_[-1] and close[-1] > prev_close[-1]
        vol_base = float(np.mean(volume[-6:-1])) if len(volume) >= 6 else float(np.mean(volume[:-1])) if len(volume) > 1 else 0.0
        vol_multiple = volume[-1] / vol_base if vol_base > 0 else 0.0
        panic_drop = float(np.min(recent_returns)) <= (-3.0 if is_stock else -2.0)
        rebound_strength = latest_return >= (1.5 if is_stock else 0.8) or float(change_pct or 0.0) >= (1.5 if is_stock else 0.8)
        return bool(drawdown_pct <= -6.0 and panic_drop and latest_bull and vol_multiple >= 1.2 and rebound_strength)

    def _apply_market_regime(
        self,
        signal_type: str,
        reason: str,
        score: float,
        target_price: Optional[float],
        stop_loss: Optional[float],
        symbol: str,
        name: str,
        is_stock: bool,
        df: pd.DataFrame,
        change_pct: float,
        stock_emotion_score: float,
        concept_strength_score: float,
        concept_name: str,
    ) -> tuple:
        """
        按运行模式对信号做二次过滤。
        """
        effective_mode = str(self._effective_market_regime or "normal")
        if signal_type != "买入":
            if effective_mode == "defense" and is_stock and reason != "无明确信号":
                reason = f"{reason},防守模式下仅观察"
            elif effective_mode == "golden_pit" and is_stock and reason != "无明确信号":
                reason = f"{reason},黄金坑模式等待修复确认"
            return signal_type, reason, score, target_price, stop_loss, stock_emotion_score, concept_strength_score, concept_name

        if effective_mode == "defense" and is_stock:
            stock_emotion_score, concept_strength_score, concept_name = self._ensure_concept_context(
                symbol, name, stock_emotion_score, concept_strength_score, concept_name
            )
            override = float(self.risk_cfg.get("stock_emotion_override_score", 75.0))
            concept_override = float(self.risk_cfg.get("concept_override_score", 0.70))
            if stock_emotion_score < override or concept_strength_score < concept_override:
                return (
                    "观望",
                    f"{reason},防守模式仅保留抱团核心({stock_emotion_score:.0f}/{concept_strength_score:.2f})",
                    0.0,
                    None,
                    None,
                    stock_emotion_score,
                    concept_strength_score,
                    concept_name,
                )
            return (
                signal_type,
                f"{reason},防守模式抱团豁免({stock_emotion_score:.0f}/{concept_strength_score:.2f})",
                min(score + 0.05, 1.0),
                target_price,
                stop_loss,
                stock_emotion_score,
                concept_strength_score,
                concept_name,
            )

        if effective_mode == "golden_pit":
            if not self._is_gold_pit_setup(df, is_stock=is_stock, change_pct=change_pct):
                return (
                    "观望",
                    f"{reason},黄金坑模式下未见放量修复确认",
                    0.0,
                    None,
                    None,
                    stock_emotion_score,
                    concept_strength_score,
                    concept_name,
                )
            return (
                signal_type,
                f"{reason},黄金坑修复确认",
                min(score + 0.08, 1.0),
                target_price,
                stop_loss,
                stock_emotion_score,
                concept_strength_score,
                concept_name,
            )

        return signal_type, reason, score, target_price, stop_loss, stock_emotion_score, concept_strength_score, concept_name

    def _apply_ai_signal_review(
        self,
        signal_type: str,
        reason: str,
        score: float,
        target_price: Optional[float],
        stop_loss: Optional[float],
        symbol: str,
        name: str,
        change_pct: float,
        stock_emotion_score: float,
        concept_strength_score: float,
        concept_name: str,
        order_book_bias: str,
        order_book_ratio: float,
        ws_stage: int,
        ws_score: float,
        fcf_score: float,
    ) -> tuple:
        """
        让 AI 对规则筛出的候选信号做最后一道审核。
        """
        effective_mode = str(self._effective_market_regime or "normal")
        if signal_type != "买入" or effective_mode not in {"normal", "defense", "golden_pit"}:
            return signal_type, reason, score, target_price, stop_loss

        payload = self._review_signal_with_ai(
            regime_mode=effective_mode,
            symbol=symbol,
            name=name,
            signal_type=signal_type,
            reason=reason,
            score=score,
            change_pct=change_pct,
            stock_emotion_score=stock_emotion_score,
            concept_strength_score=concept_strength_score,
            concept_name=concept_name,
            order_book_bias=order_book_bias,
            order_book_ratio=order_book_ratio,
            ws_stage=ws_stage,
            ws_score=ws_score,
            fcf_score=fcf_score,
        )
        decision = str(payload.get("decision", "") or "").strip().lower()
        ai_reason = str(payload.get("reason", "") or "").strip()
        ai_confidence = float(payload.get("confidence", 0.0) or 0.0)
        if decision == "buy" and ai_confidence >= 0.55:
            return signal_type, f"{reason},AI确认({ai_reason or effective_mode})", min(score + 0.05, 1.0), target_price, stop_loss
        if decision in {"watch", "skip"} and ai_confidence >= 0.55:
            return "观望", f"{reason},AI否决({ai_reason or effective_mode})", 0.0, None, None
        return signal_type, reason, score, target_price, stop_loss

    def _run_precheck_with_timeout(self, func, name: str) -> None:
        """
        为扫描前置分析增加超时降级，避免整条链路被阻塞。
        """
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func)
                future.result(timeout=self._precheck_timeout_sec)
        except TimeoutError:
            logger.warning(f"{name} 执行超时，已降级继续扫描")
        except Exception as e:
            logger.warning(f"{name} 执行失败，已降级继续扫描: {e}")

    def _refresh_market_emotion(self):
        """刷新大盘情绪缓存（避免每只票重复拉取）"""
        if not bool(self.risk_cfg.get("emotion_enabled", False)):
            self._market_emotion_score = None
            self._market_emotion_cycle = ""
            return

        if self._market_emotion_ts and (datetime.now() - self._market_emotion_ts).total_seconds() < 600:
            return

        try:
            from strategy.analysis.emotion.market_emotion import MarketEmotionAnalyzer
            analyzer = MarketEmotionAnalyzer()
            date_ymd = datetime.now().strftime("%Y%m%d")
            market = analyzer.get_market_emotion(date_ymd)
            if market:
                self._market_emotion_score = float(market.normalized_score)
                self._market_emotion_cycle = str(market.cycle)
            else:
                self._market_emotion_score = None
                self._market_emotion_cycle = ""
            self._market_emotion_ts = datetime.now()
        except Exception as e:
            logger.debug(f"大盘情绪获取失败: {e}")
            self._market_emotion_score = None
            self._market_emotion_cycle = ""

    def _get_stock_emotion_score(self, symbol: str, name: str) -> float:
        """获取个股情绪分（0-100），用于弱市抱团/强势豁免"""
        cached = self._stock_emotion_cache.get(symbol)
        if cached is not None:
            return float(cached)

        try:
            from strategy.analysis.emotion.stock_emotion import StockEmotionAnalyzer
            analyzer = StockEmotionAnalyzer()
            date_ymd = datetime.now().strftime("%Y%m%d")
            res = analyzer.analyze_stock(symbol=symbol, name=name, date=date_ymd)
            score = float(res.score) if res and res.success else 50.0
            self._stock_emotion_cache[symbol] = score
            return score
        except Exception as e:
            logger.debug(f"个股情绪获取失败 {symbol}: {e}")
            return 50.0

    def _refresh_space_score(self):
        """刷新 Space_Score 缓存（概念板块版本）"""
        if self._space_ts and (datetime.now() - self._space_ts).total_seconds() < 600:
            return
        try:
            from strategy.analysis.space.space_score import SpaceScoreAnalyzer

            analyzer = SpaceScoreAnalyzer()
            date_ymd = datetime.now().strftime("%Y%m%d")
            res = analyzer.analyze_space(date=date_ymd, top_concepts=30)
            if res and res.success and res.raw_data and "space" in res.raw_data:
                space = res.raw_data["space"]
                self._space_score = float(space.get("space_score", 0.0))
                self._space_level = str(space.get("level", ""))
            else:
                self._space_score = None
                self._space_level = ""
            self._space_ts = datetime.now()
        except Exception as e:
            logger.debug(f"SpaceScore 获取失败: {e}")
            self._space_score = None
            self._space_level = ""

    def _get_concept_strength(self, symbol: str) -> tuple:
        """获取个股所属主线概念强度"""
        cached = self._concept_strength_cache.get(symbol)
        if cached is not None:
            return cached

        try:
            from strategy.analysis.space.space_score import SpaceScoreAnalyzer

            analyzer = SpaceScoreAnalyzer()
            date_ymd = datetime.now().strftime("%Y%m%d")
            score, concept_name = analyzer.get_symbol_concept_strength(symbol=symbol, date=date_ymd, top_concepts=30)
            result = (float(score), str(concept_name or ""))
            self._concept_strength_cache[symbol] = result
            return result
        except Exception as e:
            logger.debug(f"概念强度获取失败 {symbol}: {e}")
            return 0.0, ""
    
    def _load_dynamic_pool(self):
        """从数据库加载动态股票池"""
        try:
            pool = get_dynamic_pool(pool_type="all", limit=100, db_path=self.db_path)
            
            etf_products = []
            stock_products = []
            
            for code, meta in pool._metadata.items():
                item = {"code": code, "name": meta.get("name", "")}
                ptype = meta.get("pool_type", "")
                if ptype in ("etf", "lof"):
                    etf_products.append(item)
                elif ptype == "stock":
                    stock_products.append(item)
            
            self.etf_pool = etf_products
            self.stock_pool = stock_products
            
            logger.info(f"动态股票池加载: ETF/LOF {len(self.etf_pool)} 只, 股票 {len(self.stock_pool)} 只")
        except Exception as e:
            logger.warning(f"动态股票池加载失败，使用空池: {e}")
            self.etf_pool = []
            self.stock_pool = []
    
    def reload_pool(self):
        """重新加载股票池"""
        self._load_dynamic_pool()
    
    def get_latest_price(self, symbol: str) -> Optional[dict]:
        """获取最新价格（优先实时行情，失败再回退最近交易日数据）"""
        try:
            quote_df = pd.DataFrame()
            if hasattr(self.data_source, "get_realtime_quotes"):
                try:
                    quote_df = self.data_source.get_realtime_quotes([symbol])
                except Exception as e:
                    logger.debug(f"实时行情获取失败 {symbol}: {e}")

            if quote_df is not None and not quote_df.empty:
                row = quote_df.iloc[0]

                code = str(row.get("code", row.get("代码", ""))).replace("SH.", "").replace("SZ.", "").zfill(6)
                if code == str(symbol).zfill(6):
                    price = row.get("last_price", row.get("最新价", row.get("最新", 0)))
                    change_pct = row.get("change_rate", row.get("涨跌幅", 0))
                    volume = row.get("volume", row.get("成交量", 0))
                    quote_time = row.get("update_time", row.get("时间", row.get("日期", "")))

                    price_val = float(pd.to_numeric(price, errors="coerce") or 0.0)
                    if price_val > 0:
                        return {
                            "price": price_val,
                            "change_pct": float(pd.to_numeric(change_pct, errors="coerce") or 0.0),
                            "volume": float(pd.to_numeric(volume, errors="coerce") or 0.0),
                            "date": str(quote_time or ""),
                            "source": "realtime",
                        }

            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            df = self.data_source.get_kline(
                symbol, 
                start_date.strftime("%Y%m%d"), 
                end_date.strftime("%Y%m%d")
            )
            
            if df is None or df.empty:
                return None
            
            latest = df.iloc[-1]
            
            return {
                "price": float(latest.get("close", 0)),
                "change_pct": float(latest.get("pct_change", 0)) if "pct_change" in latest else 0.0,
                "volume": float(latest.get("volume", 0)),
                "date": str(latest.get("date", "")),
                "source": "daily_kline",
            }
            
        except Exception as e:
            logger.warning(f"获取最新价格失败 {symbol}: {e}")
            return None
    
    def _get_order_book_metrics(self, symbol: str) -> Dict[str, float]:
        """
        汇总五档盘口强弱
        """
        try:
            if not hasattr(self.data_source, "get_order_book"):
                return {
                    "bias": "",
                    "ratio": 0.0,
                    "bid_volume_sum": 0.0,
                    "ask_volume_sum": 0.0,
                }

            order_book = self.data_source.get_order_book(symbol, depth=5)
            if not order_book:
                return {
                    "bias": "",
                    "ratio": 0.0,
                    "bid_volume_sum": 0.0,
                    "ask_volume_sum": 0.0,
                }

            bid_rows = order_book.get("bid", []) or []
            ask_rows = order_book.get("ask", []) or []
            bid_volume_sum = float(sum(float(item.get("volume", 0.0) or 0.0) for item in bid_rows))
            ask_volume_sum = float(sum(float(item.get("volume", 0.0) or 0.0) for item in ask_rows))
            total_volume = bid_volume_sum + ask_volume_sum
            ratio = 0.0 if total_volume <= 0 else (bid_volume_sum - ask_volume_sum) / total_volume

            if ratio >= 0.20:
                bias = "买盘强"
            elif ratio <= -0.20:
                bias = "卖盘强"
            else:
                bias = "均衡"

            return {
                "bias": bias,
                "ratio": ratio,
                "bid_volume_sum": bid_volume_sum,
                "ask_volume_sum": ask_volume_sum,
            }
        except Exception as e:
            logger.debug(f"盘口数据获取失败 {symbol}: {e}")
            return {
                "bias": "",
                "ratio": 0.0,
                "bid_volume_sum": 0.0,
                "ask_volume_sum": 0.0,
            }

    def analyze_stock(self, symbol: str, name: str, is_stock: bool = True) -> Optional[StockSignal]:
        """分析单只股票（双策略）"""
        try:
            self._resolve_effective_market_regime()
            cache_key = f"{str(symbol).zfill(6)}|{int(bool(is_stock))}|{self._effective_market_regime}"
            cached = self._analysis_cache.get(cache_key)
            if cached is not None:
                cached_at, cached_signal = cached
                if (datetime.now() - cached_at).total_seconds() < self._analysis_cache_ttl_sec:
                    return cached_signal

            market_emotion_score = float(self._market_emotion_score) if self._market_emotion_score is not None else 0.0
            stock_emotion_score = 0.0
            concept_strength_score = 0.0
            concept_name = ""
            fcf_score = 0.0
            order_book_metrics = self._get_order_book_metrics(symbol)

            latest_price = self.get_latest_price(symbol)
            
            if latest_price is None or latest_price["price"] <= 0:
                logger.warning(f"无法获取 {symbol} 价格数据")
                return None
            
            price = latest_price["price"]
            change_pct = latest_price["change_pct"]
            volume = latest_price["volume"]
            
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
            
            df = self.data_source.get_kline(symbol, start_date, end_date)
            if df is None or df.empty or len(df) < 30:
                logger.warning(f"历史数据不足 {symbol}")
                return None

            # 资金一致性因子（FCF）: 用 OHLCV 低延迟计算
            if bool(self.risk_cfg.get("fcf_enabled", False)):
                try:
                    death_turnover = float(self.risk_cfg.get("fcf_death_turnover", 50.0))
                    fcf_score = float(compute_fcf(df, turnover_rate=None, death_turnover=death_turnover).fcf)
                except Exception:
                    fcf_score = 0.0
            
            pa_signal = self.pa_macd_strategy.on_bar(symbol, df)
            
            ws_signal = None
            ws_stage = 0
            ws_reason = ""
            ws_score = 0.0
            dual_signal = False
            
            if is_stock:
                self.ws_strategy.load_data(symbol, df)
                ws_signal = self.ws_strategy.on_bar(symbol, df)
                ws_info = self.ws_strategy.get_stage_info()
                if ws_info is not None:
                    ws_stage = ws_info.stage
                    ws_reason = ws_info.details
                    ws_score = ws_info.score
            
            pa_signal_val = pa_signal.signal if pa_signal else 0
            ws_signal_val = ws_signal.signal if ws_signal else 0
            
            if pa_signal_val > 0 and ws_signal_val > 0:
                dual_signal = True
            
            if pa_signal_val > 0:
                take_profit = float(self.risk_cfg.get("take_profit", 0.15))
                stop_loss_pct = float(self.risk_cfg.get("stop_loss", -0.05))
                target_price = price * (1 + take_profit)
                stop_loss = price * (1 + stop_loss_pct)
                signal_type = "买入"
                reason = self._generate_pa_reason(df, pa_signal)
                score = min(pa_signal.weight + (ws_signal_val * 0.3 if ws_signal_val > 0 else 0), 1.0)
            elif pa_signal_val < 0:
                target_price = None
                stop_loss = None
                signal_type = "卖出"
                reason = self._generate_pa_reason(df, pa_signal)
                score = 1.0
            elif ws_signal_val > 0 and ws_stage >= 3:
                take_profit = float(self.risk_cfg.get("take_profit", 0.15))
                stop_loss_pct = float(self.risk_cfg.get("stop_loss", -0.05))
                target_price = price * (1 + take_profit)
                stop_loss = price * (1 + stop_loss_pct)
                signal_type = "买入"
                reason = f"弱转强({ws_reason})"
                score = min(ws_score / 100 + 0.3, 1.0)
            else:
                target_price = None
                stop_loss = None
                signal_type = "观望"
                if ws_stage > 0:
                    reason = f"弱转强{ws_stage}/4阶段"
                else:
                    reason = "无明确信号"
                score = 0.0

            order_book_bias = str(order_book_metrics.get("bias", "") or "")
            order_book_ratio = float(order_book_metrics.get("ratio", 0.0) or 0.0)
            bid_volume_sum = float(order_book_metrics.get("bid_volume_sum", 0.0) or 0.0)
            ask_volume_sum = float(order_book_metrics.get("ask_volume_sum", 0.0) or 0.0)

            if order_book_bias:
                reason = f"{reason},盘口{order_book_bias}({order_book_ratio:+.2f})"
                if signal_type == "买入" and order_book_ratio > 0:
                    score = min(score + min(order_book_ratio * 0.2, 0.08), 1.0)
                elif signal_type == "卖出" and order_book_ratio < 0:
                    score = min(score + min(abs(order_book_ratio) * 0.2, 0.08), 1.0)
                elif signal_type == "观望" and abs(order_book_ratio) >= 0.25:
                    reason = f"{reason},盘口分歧较大"

            # 弱市确认：大盘极差时，非强势抱团股不主动出手；强势抱团则允许“逆势买/持”
            if signal_type == "买入" and is_stock and bool(self.risk_cfg.get("emotion_enabled", False)):
                market_stop = float(self.risk_cfg.get("market_emotion_stop_score", 40.0))
                override = float(self.risk_cfg.get("stock_emotion_override_score", 75.0))
                concept_override = float(self.risk_cfg.get("concept_override_score", 0.70))
                if self._market_emotion_score is not None and float(self._market_emotion_score) <= market_stop:
                    stock_emotion_score = self._get_stock_emotion_score(symbol, name)
                    concept_strength_score, concept_name = self._get_concept_strength(symbol)
                    if stock_emotion_score < override or concept_strength_score < concept_override:
                        signal_type = "观望"
                        target_price = None
                        stop_loss = None
                        reason = (
                            f"{reason},弱市退潮({self._market_emotion_cycle}:{market_emotion_score:.0f})"
                            f",个股/概念不足({stock_emotion_score:.0f}/{concept_strength_score:.2f})"
                        )
                        score = 0.0
                    else:
                        scale_out_levels = self.risk_cfg.get("scale_out_levels", [0.10, 0.20])
                        try:
                            lv2 = float(scale_out_levels[1]) if len(scale_out_levels) > 1 else float(scale_out_levels[0])
                        except Exception:
                            lv2 = 0.20
                        target_price = price * (1 + lv2)
                        tstop = float(self.risk_cfg.get("override_trailing_stop", self.risk_cfg.get("trailing_stop", 0.06)))
                        reason = (
                            f"{reason},弱市抱团({stock_emotion_score:.0f})"
                            f",概念:{concept_name or '主线'}({concept_strength_score:.2f})"
                            f",分批10/20,回撤{tstop*100:.0f}%"
                        )

            # FCF 买入过滤：资金一致性不足则不出手
            if signal_type == "买入" and bool(self.risk_cfg.get("fcf_enabled", False)):
                buy_th = float(self.risk_cfg.get("fcf_buy_threshold", 0.0))
                if fcf_score <= buy_th:
                    signal_type = "观望"
                    target_price = None
                    stop_loss = None
                    reason = f"{reason},FCF偏弱({fcf_score:+.2f})"
                    score = 0.0

            (
                signal_type,
                reason,
                score,
                target_price,
                stop_loss,
                stock_emotion_score,
                concept_strength_score,
                concept_name,
            ) = self._apply_market_regime(
                signal_type=signal_type,
                reason=reason,
                score=score,
                target_price=target_price,
                stop_loss=stop_loss,
                symbol=symbol,
                name=name,
                is_stock=is_stock,
                df=df,
                change_pct=change_pct,
                stock_emotion_score=stock_emotion_score,
                concept_strength_score=concept_strength_score,
                concept_name=concept_name,
            )

            (
                signal_type,
                reason,
                score,
                target_price,
                stop_loss,
            ) = self._apply_ai_signal_review(
                signal_type=signal_type,
                reason=reason,
                score=score,
                target_price=target_price,
                stop_loss=stop_loss,
                symbol=symbol,
                name=name,
                change_pct=change_pct,
                stock_emotion_score=stock_emotion_score,
                concept_strength_score=concept_strength_score,
                concept_name=concept_name,
                order_book_bias=order_book_bias,
                order_book_ratio=order_book_ratio,
                ws_stage=ws_stage,
                ws_score=ws_score,
                fcf_score=fcf_score,
            )

            signal_result = StockSignal(
                code=symbol,
                name=name,
                price=price,
                change_pct=change_pct,
                volume=volume,
                signal_type=signal_type,
                target_price=target_price,
                stop_loss=stop_loss,
                reason=reason,
                score=score,
                dual_signal=dual_signal,
                ws_stage=ws_stage,
                ws_reason=ws_reason,
                ws_score=ws_score,
                market_emotion_score=market_emotion_score,
                stock_emotion_score=stock_emotion_score,
                concept_strength_score=concept_strength_score,
                concept_name=concept_name,
                fcf=fcf_score,
                order_book_bias=order_book_bias,
                order_book_ratio=order_book_ratio,
                bid_volume_sum=bid_volume_sum,
                ask_volume_sum=ask_volume_sum,
            )
            self._analysis_cache[cache_key] = (datetime.now(), signal_result)
            return signal_result
            
        except Exception as e:
            logger.error(f"分析股票失败 {symbol}: {e}")
            return None
    
    def _generate_pa_reason(self, df, signal) -> str:
        """生成PA+MACD推荐理由"""
        try:
            latest = df.iloc[-1]
            reasons = []
            
            if "macd" in df.columns:
                macd = latest.get("macd", 0)
                macd_signal = latest.get("macd_signal", 0)
                if macd > macd_signal:
                    reasons.append("MACD金叉")
                elif macd < macd_signal:
                    reasons.append("MACD死叉")
            
            if "ema20" in df.columns and "close" in df.columns:
                if latest["close"] > latest["ema20"]:
                    reasons.append("站上20日线")
                else:
                    reasons.append("跌破20日线")
            
            if "volume" in df.columns:
                vol_ma = df["volume"].tail(20).mean()
                if latest["volume"] > vol_ma * 1.5:
                    reasons.append("量能放大")
            
            return ",".join(reasons[:2]) if reasons else "技术面信号"
            
        except Exception:
            return "技术面信号"
    
    def scan_market(self) -> Dict[str, List[StockSignal]]:
        """
        扫描市场 (双策略: PA+MACD + 弱转强)
        
        Returns:
            {
                "etf": [StockSignal, ...],
                "stock": [StockSignal, ...]
            }
        """
        logger.info("开始实时扫描市场 (双策略)...")

        self._run_precheck_with_timeout(self._refresh_runtime_mode, "运行模式刷新")
        self._run_precheck_with_timeout(self._refresh_market_emotion, "大盘情绪刷新")
        self._run_precheck_with_timeout(self._refresh_space_score, "SpaceScore 刷新")
        self._run_precheck_with_timeout(self._refresh_index_context, "指数环境刷新")
        self._resolve_effective_market_regime()
        logger.info(
            f"当前市场模式: {self._runtime_mode_label} -> {self._effective_market_regime} "
            f"({self._effective_market_regime_reason})"
        )
        
        etf_signals = []
        stock_signals = []
        dual_count = 0
        
        logger.info(f"扫描ETF池 ({len(self.etf_pool)}只)...")
        for etf in self.etf_pool:
            signal = self.analyze_stock(etf["code"], etf["name"], is_stock=False)
            if signal:
                etf_signals.append(signal)
        
        logger.info(f"扫描A股池 ({len(self.stock_pool)}只, PA+MACD + 弱转强)...")
        for stock in self.stock_pool:
            signal = self.analyze_stock(stock["code"], stock["name"], is_stock=True)
            if signal:
                if signal.dual_signal:
                    dual_count += 1
                stock_signals.append(signal)
        
        etf_signals = sorted(etf_signals, key=lambda x: -x.score)
        stock_signals = sorted(stock_signals, key=lambda x: (-x.score, -x.dual_signal))
        
        logger.info(f"扫描完成: ETF {len(etf_signals)}只, A股 {len(stock_signals)}只, 双重信号 {dual_count}只")
        
        return {
            "etf": etf_signals[:self.etf_count],
            "stock": stock_signals[:self.stock_count],
        }
    
    def get_top_recommends(self, signals: List[StockSignal], top_n: int = 5) -> List[SignalRecommendRow]:
        """获取推荐列表"""
        recommends: List[SignalRecommendRow] = []
        
        for s in signals[:top_n]:
            dual_tag = "⭐双重信号" if s.dual_signal else ""
            recommends.append(
                SignalRecommendRow(
                    code=s.code,
                    name=s.name,
                    price=s.price,
                    change_pct=s.change_pct,
                    signal=s.signal_type,
                    target=s.target_price,
                    stop_loss=s.stop_loss,
                    reason=f"{s.reason} {dual_tag}".strip(),
                    order_book_text=f"{s.order_book_bias or '暂无'}({s.order_book_ratio:+.2f})",
                    dual_signal=s.dual_signal,
                    ws_stage=s.ws_stage,
                )
            )
        
        return recommends


def run_realtime_scan():
    """运行实时扫描"""
    from trading.push_service import get_pusher
    
    monitor = RealtimeMonitor(etf_count=5, stock_count=5)
    
    # 扫描市场
    results = monitor.scan_market()
    
    # 推送结果
    pusher = get_pusher()
    
    etf_recs = monitor.get_top_recommends(results["etf"])
    stock_recs = monitor.get_top_recommends(results["stock"])
    
    success = pusher.push_daily_recommend(etf_recs, stock_recs)
    
    if success:
        print("推送成功!")
    else:
        print("推送失败!")
    
    # 打印结果
    print("\n" + "="*50)
    print("ETF推荐:")
    for r in etf_recs:
        print(f"  {r.code} {r.name} - {r.signal} @ {r.price:.4f}")
    
    print("\nA股推荐:")
    for r in stock_recs:
        print(f"  {r.code} {r.name} - {r.signal} @ {r.price:.4f}")
    
    return results
