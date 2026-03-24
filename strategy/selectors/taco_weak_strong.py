# -*- coding: utf-8 -*-
"""
TACO + 弱转强组合策略

设计思路:
1. TACO 负责判断主题/事件窗口是否值得参与
2. 弱转强负责确认个股是否出现短线转强买点
3. 只有主题事件与短线结构同时满足，才允许进入候选或触发买入
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from strategy.base import BaseStrategy, Signal
from strategy.examples.taco_strategy import TACOStrategy, build_taco_params
from strategy.selectors.base_selector import BaseSelector, SelectResult, StockScore
from strategy.selectors.weak_to_strong import (
    WeakToStrongParams,
    WeakToStrongSelector,
    WeakToStrongStage,
    WeakToStrongTimingStrategy,
)


@dataclass
class TacoWeakStrongParams:
    """TACO + 弱转强组合参数。"""

    weak_params: WeakToStrongParams
    taco_variant: str = "taco"
    taco_candidate_threshold: float = 0.18
    taco_buy_score_threshold: float = 0.42
    combined_score_threshold: float = 0.46
    selector_stage_floor: int = 3
    selector_score_floor: float = 28.0
    stage4_buy_score_floor: float = 0.52
    stage3_buy_score_floor: float = 0.58


class TacoWeakStrongSelector(BaseSelector):
    """先做 TACO 主题门控，再做弱转强筛选。"""

    def __init__(self, params: TacoWeakStrongParams):
        super().__init__("TacoWeakStrongSelector")
        self.params = params
        self.weak_selector = WeakToStrongSelector(params.weak_params)
        self.taco_strategy = TACOStrategy(build_taco_params(params.taco_variant))

    def load_data(self, symbol: str, df: pd.DataFrame):
        """同步给内部子策略加载数据。"""
        super().load_data(symbol, df)
        self.weak_selector.load_data(symbol, df)
        self.taco_strategy.load_data(symbol, df)

    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """选择同时满足主题事件和弱转强结构的候选。"""
        selected_rows: List[StockScore] = []
        candidate_rows: List[StockScore] = []
        cutoff = pd.to_datetime(end_date)

        for symbol in symbols:
            df = self.get_data(symbol)
            hist = self._slice_history(df, cutoff)
            if hist is None or hist.empty or len(hist) < self.params.weak_params.total_window:
                continue

            taco_signal = self.taco_strategy.on_bar(symbol, hist)
            weak_stage = self.weak_selector.detect_stage(hist)
            taco_score = float(taco_signal.candidate_score if taco_signal else 0.0)
            weak_score = min(max(float(weak_stage.score or 0.0) / 100.0, 0.0), 1.0)
            combined_score = weak_score * 0.68 + taco_score * 0.32
            taco_ready = self._is_taco_ready(taco_signal)
            weak_ready = (
                weak_stage.stage >= self.params.selector_stage_floor
                and float(weak_stage.score or 0.0) >= self.params.selector_score_floor
            )

            reason = (
                f"TACO={taco_score:.2f}"
                f" | WS阶段={weak_stage.stage}"
                f" | WS评分={float(weak_stage.score or 0.0):.1f}"
            )
            candidate = StockScore(
                symbol=symbol,
                score=combined_score * 100.0,
                reason=f"{reason} | {weak_stage.details}",
                candidate_score=combined_score,
                gate_passed=bool(taco_ready and weak_ready and combined_score >= self.params.combined_score_threshold),
                gate_reason=str(taco_signal.gate_reason if taco_signal else weak_stage.details or ""),
            )
            candidate_rows.append(candidate)
            if candidate.gate_passed:
                selected_rows.append(candidate)

        selected_rows.sort(key=lambda item: (-item.score, item.symbol))
        candidate_rows.sort(key=lambda item: (-item.score, item.symbol))
        for index, row in enumerate(selected_rows, 1):
            row.rank = index

        return SelectResult(
            stocks=selected_rows[:top_n],
            pool_size=len(symbols),
            selected_count=len(selected_rows),
            candidates=candidate_rows,
        )

    @staticmethod
    def _slice_history(df: Optional[pd.DataFrame], cutoff: pd.Timestamp) -> pd.DataFrame:
        """截取截至当前调仓日的历史。"""
        if df is None or df.empty:
            return pd.DataFrame()
        try:
            return df[df.index <= cutoff].copy()
        except Exception:
            return pd.DataFrame()

    def _is_taco_ready(self, taco_signal: Optional[Signal]) -> bool:
        """判断 TACO 门控是否允许参与。"""
        if taco_signal is None:
            return False
        taco_score = float(taco_signal.candidate_score or 0.0)
        if taco_signal.signal > 0 and bool(taco_signal.gate_passed):
            return True
        if taco_score >= self.params.taco_buy_score_threshold:
            return True
        return bool(
            taco_score >= self.params.taco_candidate_threshold
            and bool(taco_signal.gate_passed)
        )


class TacoWeakStrongTimingStrategy(WeakToStrongTimingStrategy):
    """TACO 事件门控 + 弱转强买卖确认。"""

    def __init__(self, params: TacoWeakStrongParams):
        super().__init__(params=params.weak_params)
        self.name = "TacoWeakStrongTiming"
        self.combo_params = params
        self.taco_strategy = TACOStrategy(build_taco_params(params.taco_variant))

    def load_data(self, symbol: str, df: pd.DataFrame):
        """同步给内部 TACO 子策略加载数据。"""
        super().load_data(symbol, df)
        self.taco_strategy.load_data(symbol, df)

    def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        """仅在 TACO 窗口有效时，允许弱转强信号转成实际买卖。"""
        if df is None or df.empty or len(df) < self.params.total_window:
            return None

        self.selector.load_data(symbol, df)
        self._last_stage = self.selector.detect_stage(df)
        market_context = self.get_market_context()
        self.taco_strategy.set_market_context(market_context)
        taco_signal = self.taco_strategy.on_bar(symbol, df)
        taco_score = float(taco_signal.candidate_score if taco_signal else 0.0)
        weak_score = min(max(float(self._last_stage.score if self._last_stage else 0.0) / 100.0, 0.0), 1.0)
        combined_score = weak_score * 0.65 + taco_score * 0.35

        exit_signal = self._build_exit_signal(symbol, df, market_context)
        if exit_signal is not None:
            if taco_signal is not None and taco_signal.signal < 0:
                exit_signal.weight = max(float(exit_signal.weight or 0.0), float(taco_signal.weight or 0.0), 0.7)
                exit_signal.gate_reason = f"{exit_signal.gate_reason}; TACO转弱".strip("; ")
            exit_signal.candidate_score = min(max(combined_score, 0.0), 1.0)
            return exit_signal

        if taco_signal is not None and taco_signal.signal < 0 and taco_score < self.combo_params.taco_candidate_threshold:
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=-1,
                weight=max(float(taco_signal.weight or 0.0), 0.5),
                candidate_score=min(max(combined_score, 0.0), 1.0),
                gate_passed=True,
                gate_reason=f"TACO事件转弱: {taco_signal.gate_reason}",
            )

        taco_ready = self._is_taco_ready(taco_signal)
        reason = self._build_gate_reason(taco_signal, self._last_stage)

        if self._last_stage.stage >= 4 and combined_score >= self.combo_params.stage4_buy_score_floor and taco_ready:
            buy_weight = min(0.62, 0.26 + weak_score * 0.32 + min(taco_score, 0.8) * 0.18)
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=1,
                weight=max(buy_weight, 0.22),
                candidate_score=min(max(combined_score, 0.0), 1.0),
                gate_passed=True,
                gate_reason=reason,
            )

        if self._last_stage.stage >= 3 and combined_score >= self.combo_params.stage3_buy_score_floor and taco_ready:
            buy_weight = min(0.45, 0.18 + weak_score * 0.20 + min(taco_score, 0.8) * 0.12)
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=1,
                weight=max(buy_weight, 0.18),
                candidate_score=min(max(combined_score, 0.0), 1.0),
                gate_passed=True,
                gate_reason=reason,
            )

        return Signal(
            symbol=symbol,
            date=datetime.now(),
            signal=0,
            weight=0.0,
            candidate_score=min(max(combined_score, 0.0), 1.0),
            gate_passed=False,
            gate_reason=reason,
        )

    def _is_taco_ready(self, taco_signal: Optional[Signal]) -> bool:
        """判断 TACO 窗口是否有效。"""
        if taco_signal is None:
            return False
        taco_score = float(taco_signal.candidate_score or 0.0)
        if taco_signal.signal > 0 and bool(taco_signal.gate_passed):
            return True
        if taco_score >= self.combo_params.taco_buy_score_threshold:
            return True
        return bool(
            taco_score >= self.combo_params.taco_candidate_threshold
            and bool(taco_signal.gate_passed)
        )

    @staticmethod
    def _build_gate_reason(taco_signal: Optional[Signal], weak_stage: Optional[WeakToStrongStage]) -> str:
        """拼接组合策略原因。"""
        taco_reason = str(taco_signal.gate_reason if taco_signal else "TACO未激活")
        weak_reason = str(weak_stage.details if weak_stage else "弱转强未满足")
        return f"TACO: {taco_reason} | 弱转强: {weak_reason}"
