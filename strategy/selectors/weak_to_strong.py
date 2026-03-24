# -*- coding: utf-8 -*-
"""
弱转强选股策略

四阶段形态检测:
1. 强势启动期 (曾涨停): 股性活跃，有主力资金介入
2. 缩量回调期 (连续缩量下跌): 主力未出逃，浮筹清洗
3. 企稳转折期 (最近2-3天): 下跌动能耗尽，出现止跌信号
4. 确认反转期 (放量上涨): 新资金入场，趋势由弱转强

参考: https://www.itsoku.com/article/2344
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

from strategy.base import BaseStrategy, Signal
from strategy.selectors.base_selector import BaseSelector, StockScore, SelectResult
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class WeakToStrongParams:
    """弱转强参数"""
    limit_up_window: int = 15
    shrink_ratio: float = 0.5
    pullback_days: int = 7
    volume_multiple: float = 2.0
    min_rally_pct: float = 2.0
    total_window: int = 30
    breakdown_drop_pct: float = -3.0
    breakdown_volume_multiple: float = 1.8
    breakdown_lookback: int = 2
    max_pullback_pct: float = 12.0
    require_confirm_open_above_prev_close: bool = True
    max_confirm_gap_pct: float = 6.0
    min_close_position_ratio: float = 0.6
    prior_weak_upper_shadow_ratio: float = 0.35
    prior_weak_volume_multiple: float = 1.2


@dataclass
class WeakToStrongStage:
    """弱转强检测结果"""
    stage: int  # 0-4
    limit_up_idx: int = -1
    limit_up_price: float = 0.0
    limit_up_date: str = ""
    shrink_vol_avg: float = 0.0
    limit_up_vol: float = 0.0
    reversal_days: int = 0
    reversal_vol_avg: float = 0.0
    reversal_return: float = 0.0
    score: float = 0.0
    details: str = ""


class WeakToStrongSelector(BaseSelector):
    """弱转强选股器"""

    def __init__(self, params: WeakToStrongParams = None):
        super().__init__("WeakToStrong")
        self.params = params or WeakToStrongParams()

    def select(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        top_n: int = 10,
    ) -> SelectResult:
        """从候选池中筛选弱转强形态股票"""
        results = []
        for symbol in symbols:
            try:
                df = self.get_data(symbol)
                if df is None or df.empty or len(df) < self.params.total_window:
                    continue
                stage = self.detect_stage(df)
                if stage.stage >= 3 and stage.score > 0:
                    results.append(StockScore(
                        symbol=symbol,
                        score=stage.score,
                        reason=stage.details,
                    ))
            except Exception as e:
                logger.debug(f"检测 {symbol} 弱转强失败: {e}")

        results.sort(key=lambda x: -x.score)
        for i, r in enumerate(results):
            r.rank = i + 1

        return SelectResult(
            stocks=results[:top_n],
            pool_size=len(symbols),
            selected_count=len(results),
        )

    def detect_stage(self, df: pd.DataFrame) -> WeakToStrongStage:
        """检测弱转强四阶段"""
        df = df.tail(self.params.total_window).copy()
        if len(df) < self.params.total_window:
            return WeakToStrongStage(stage=0, score=0.0)

        close = df["close"].values
        open_ = df["open"].values
        high = df["high"].values
        low = df["low"].values
        volume = df["volume"].values
        n = len(df)

        stage = WeakToStrongStage(stage=0, score=0.0)
        score = 0.0
        details = []

        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]

        limit_up = close >= prev_close * 1.099
        bull_candle = close > open_

        stage1_end = n - 2
        candidate_limit_indices = [
            idx for idx in range(max(1, n - self.params.limit_up_window), stage1_end) if limit_up[idx]
        ]
        if not candidate_limit_indices:
            return WeakToStrongStage(stage=1, score=0.0, details="近期无涨停")
        best_stage = WeakToStrongStage(stage=0, score=0.0, details="未满足条件")
        for limit_idx in candidate_limit_indices[-3:]:
            candidate_stage = self._evaluate_limit_up_window(
                df=df,
                close=close,
                open_=open_,
                high=high,
                low=low,
                volume=volume,
                prev_close=prev_close,
                bull_candle=bull_candle,
                limit_idx=limit_idx,
            )
            if candidate_stage.score > best_stage.score:
                best_stage = candidate_stage
        return best_stage

    def _evaluate_limit_up_window(
        self,
        df: pd.DataFrame,
        close: np.ndarray,
        open_: np.ndarray,
        high: np.ndarray,
        low: np.ndarray,
        volume: np.ndarray,
        prev_close: np.ndarray,
        bull_candle: np.ndarray,
        limit_idx: int,
    ) -> WeakToStrongStage:
        """评估单个涨停触发点对应的弱转强形态。"""
        n = len(df)
        stage = WeakToStrongStage(stage=1, limit_up_idx=limit_idx, limit_up_price=close[limit_idx], limit_up_vol=volume[limit_idx])
        score = self._calc_limit_up_quality(df, limit_idx, close[limit_idx])
        details = [f"涨停日{limit_idx}(+{score:.0f})"]

        pullback_start = limit_idx + 1
        pullback_end = max(pullback_start, n - 3)
        if pullback_start >= n - 1:
            stage.score = min(score, 100)
            stage.details = "; ".join(details)
            return stage

        if pullback_end > pullback_start:
            pullback_vol = volume[pullback_start:pullback_end]
            if len(pullback_vol) > 0:
                stage.shrink_vol_avg = float(np.mean(pullback_vol))

            pullback_days_count = int(np.sum(close[pullback_start:pullback_end] < prev_close[pullback_start:pullback_end]))
            if stage.shrink_vol_avg > 0 and stage.limit_up_vol > 0:
                shrink_ratio = stage.shrink_vol_avg / stage.limit_up_vol
                if shrink_ratio <= self.params.shrink_ratio:
                    score += 20
                    details.append(f"缩量洗盘{shrink_ratio:.1%}(+20)")
                    stage.stage = max(stage.stage, 2)
                elif shrink_ratio <= min(self.params.shrink_ratio + 0.18, 0.95):
                    score += 10
                    details.append(f"温和缩量{shrink_ratio:.1%}(+10)")
                    stage.stage = max(stage.stage, 2)

            if pullback_days_count > self.params.pullback_days:
                score -= 8
                details.append("回调过久-8")

            pullback_low = float(np.min(low[pullback_start:pullback_end])) if pullback_end > pullback_start else float(low[pullback_start])
            if stage.limit_up_price > 0:
                pullback_pct = (pullback_low / stage.limit_up_price - 1) * 100
                if pullback_pct <= -self.params.max_pullback_pct:
                    stage.score = 0.0
                    stage.details = f"{'; '.join(details)}; 回撤过深{pullback_pct:.1f}%"
                    stage.stage = 0
                    return stage

        failure_reason = self._detect_breakdown_failure(
            close=close,
            open_=open_,
            low=low,
            volume=volume,
            prev_close=prev_close,
            pullback_start=pullback_start,
            shrink_vol_avg=stage.shrink_vol_avg,
        )
        if failure_reason:
            stage.score = 0.0
            stage.details = f"{'; '.join(details)}; {failure_reason}"
            stage.stage = 0
            return stage

        reversal_start = max(pullback_end, n - 4)
        reversal_end = n
        if reversal_end > reversal_start and reversal_end - reversal_start >= 2:
            reversal_vol = volume[reversal_start:reversal_end]
            reversal_close = close[reversal_start:reversal_end]
            reversal_open = open_[reversal_start:reversal_end]
            reversal_prev_close = prev_close[reversal_start:reversal_end]

            stage.reversal_days = reversal_end - reversal_start
            stage.reversal_vol_avg = float(np.mean(reversal_vol))
            stage.reversal_return = float((reversal_close[-1] - reversal_close[0]) / max(reversal_close[0], 1e-6) * 100)

            rally_count = 0
            strong_rally_count = 0
            for j in range(reversal_start, reversal_end):
                if bull_candle[j]:
                    rally_count += 1
                if bull_candle[j] and close[j] > prev_close[j]:
                    strong_rally_count += 1

            reversal_score = 0.0
            latest_confirm_valid = False
            confirm_reason = ""
            latest_idx = reversal_end - 1
            latest_gap_pct = ((reversal_open[-1] / reversal_prev_close[-1] - 1) * 100) if reversal_prev_close[-1] > 0 else 0.0
            latest_range = max(high[latest_idx] - low[latest_idx], 1e-6)
            latest_close_position = (close[latest_idx] - low[latest_idx]) / latest_range
            latest_confirm_valid = (
                reversal_close[-1] > reversal_prev_close[-1]
                and latest_gap_pct <= self.params.max_confirm_gap_pct
                and latest_close_position >= max(0.35, self.params.min_close_position_ratio - 0.1)
            )
            if reversal_close[-1] <= reversal_prev_close[-1]:
                confirm_reason = "确认日收盘未翻红"
            elif latest_gap_pct > self.params.max_confirm_gap_pct:
                confirm_reason = f"确认日高开{latest_gap_pct:.1f}%过猛"
            elif latest_close_position < max(0.35, self.params.min_close_position_ratio - 0.1):
                confirm_reason = "确认日收盘位置偏低"

            prior_weak_valid = True
            prior_weak_reason = ""
            if reversal_end >= 2:
                weak_idx = reversal_end - 2
                weak_prev_close = prev_close[weak_idx]
                weak_day_return = ((close[weak_idx] / weak_prev_close - 1) * 100) if weak_prev_close > 0 else 0.0
                weak_range = max(high[weak_idx] - low[weak_idx], 1e-6)
                weak_upper_shadow_ratio = (high[weak_idx] - max(open_[weak_idx], close[weak_idx])) / weak_range
                weak_vol_base = float(np.mean(volume[max(pullback_start, weak_idx - 3):weak_idx])) if weak_idx > pullback_start else stage.shrink_vol_avg
                weak_volume_multiple = (volume[weak_idx] / weak_vol_base) if weak_vol_base and weak_vol_base > 0 else 0.0
                prior_weak_valid = bool(
                    weak_day_return < 1.0
                    or weak_upper_shadow_ratio >= max(0.25, self.params.prior_weak_upper_shadow_ratio - 0.10)
                    or weak_volume_multiple >= max(1.1, self.params.prior_weak_volume_multiple - 0.25)
                )
                if not prior_weak_valid:
                    prior_weak_reason = "前一日分歧不明显"

            if rally_count >= 2:
                reversal_score += 10
                details.append(f"连续阳线{rally_count}天(+10)")
            elif rally_count == 1:
                reversal_score += 5
                details.append("单日转强(+5)")

            vol_multiple = (stage.reversal_vol_avg / stage.shrink_vol_avg) if stage.shrink_vol_avg > 0 else 0.0
            if strong_rally_count >= 1 and vol_multiple >= self.params.volume_multiple:
                reversal_score += 10
                details.append(f"放量{vol_multiple:.1f}倍(+10)")
            elif strong_rally_count >= 1 and vol_multiple >= max(1.1, self.params.volume_multiple - 0.3):
                reversal_score += 6
                details.append(f"温和放量{vol_multiple:.1f}倍(+6)")

            if stage.reversal_return >= self.params.min_rally_pct:
                reversal_score += 10
                details.append(f"反弹{stage.reversal_return:.1f}%(+10)")
            elif stage.reversal_return >= max(0.8, self.params.min_rally_pct - 0.6):
                reversal_score += 5
                details.append(f"弱修复{stage.reversal_return:.1f}%(+5)")

            if not prior_weak_valid:
                details.append(prior_weak_reason or "前一日弱势不明显")
            elif self.params.require_confirm_open_above_prev_close and not latest_confirm_valid:
                details.append(confirm_reason or "确认日未满足弱转强条件")
            elif stage.stage >= 2 and reversal_score >= 14:
                score += reversal_score
                stage.stage = 4
            elif stage.stage >= 2 and reversal_score >= 8 and strong_rally_count >= 1:
                score += max(reversal_score, 12)
                stage.stage = max(stage.stage, 3)
                details.append("初步企稳(+4)")

        stage.score = min(score, 100)
        stage.details = "; ".join(details) if details else "未满足条件"
        return stage

    def _calc_limit_up_quality(self, df: pd.DataFrame, idx: int, price: float) -> float:
        """计算涨停质量分 (0-30)"""
        if idx < 0 or idx >= len(df):
            return 0.0
        score = 15.0

        high_60 = df["high"].tail(60).max()
        if high_60 > 0 and price < high_60 * 0.90:
            score += 15
        elif price < high_60 * 0.95:
            score += 8

        vol_ratio = df["volume"].iloc[idx] / df["volume"].tail(20).mean()
        if vol_ratio > 2:
            score += 5

        return min(score, 30)

    def _detect_breakdown_failure(
        self,
        close: np.ndarray,
        open_: np.ndarray,
        low: np.ndarray,
        volume: np.ndarray,
        prev_close: np.ndarray,
        pullback_start: int,
        shrink_vol_avg: float,
    ) -> str:
        """识别放量大阴和破位补跌，直接判定弱转强失效"""
        if len(close) == 0 or pullback_start >= len(close):
            return ""

        lookback_start = max(pullback_start, len(close) - self.params.breakdown_lookback)
        for j in range(lookback_start, len(close)):
            if prev_close[j] <= 0:
                continue
            day_return = (close[j] / prev_close[j] - 1) * 100
            vol_multiple = volume[j] / shrink_vol_avg if shrink_vol_avg > 0 else 0.0
            if (
                close[j] < open_[j]
                and day_return <= self.params.breakdown_drop_pct
                and vol_multiple >= self.params.breakdown_volume_multiple
            ):
                return f"放量大跌{day_return:.1f}%(量比{vol_multiple:.1f})，弱转强失效"

        prior_low = float(np.min(low[pullback_start:-1])) if len(low[pullback_start:-1]) > 0 else 0.0
        if prior_low > 0 and close[-1] < prior_low:
            return f"跌破回调低点{prior_low:.2f}，弱转强失效"

        return ""


class WeakToStrongTimingStrategy(BaseStrategy):
    """弱转强择时策略"""

    def __init__(self, params: WeakToStrongParams = None):
        super().__init__("WeakToStrongTiming")
        self.params = params or WeakToStrongParams()
        self.selector = WeakToStrongSelector(params)
        self._last_stage: WeakToStrongStage = None

    def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        """逐K线回调，生成弱转强交易信号"""
        if df is None or df.empty or len(df) < self.params.total_window:
            return None

        self.selector.load_data(symbol, df)
        self._last_stage = self.selector.detect_stage(df)
        market_context = self.get_market_context()
        exit_signal = self._build_exit_signal(symbol, df, market_context)
        if exit_signal is not None:
            return exit_signal

        if self._last_stage.stage >= 4 and self._last_stage.score >= 40:
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=1,
                weight=min(self._last_stage.score / 100, 1.0),
                candidate_score=min(self._last_stage.score / 100, 1.0),
                gate_passed=True,
                gate_reason=self._last_stage.details,
            )
        elif self._last_stage.stage == 2 and self._last_stage.score >= 30:
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=0,
                weight=0.3,
                candidate_score=min(self._last_stage.score / 100, 1.0),
                gate_passed=False,
                gate_reason=f"阶段{self._last_stage.stage}，继续观察",
            )
        elif self._last_stage.score >= 50 and self._last_stage.stage == 3:
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=1,
                weight=0.5,
                candidate_score=min(self._last_stage.score / 100, 1.0),
                gate_passed=True,
                gate_reason=self._last_stage.details,
            )

        return Signal(
            symbol=symbol,
            date=datetime.now(),
            signal=0,
            weight=0.0,
            candidate_score=min((self._last_stage.score if self._last_stage else 0.0) / 100, 1.0),
            gate_passed=False,
            gate_reason=self._last_stage.details if self._last_stage else "",
        )

    def get_stage_info(self) -> Optional[WeakToStrongStage]:
        """获取当前检测阶段信息"""
        return self._last_stage

    def _build_exit_signal(
        self,
        symbol: str,
        df: pd.DataFrame,
        market_context: Dict[str, object],
    ) -> Optional[Signal]:
        """根据形态失效和短线转弱生成卖出信号。"""
        hist = df.tail(max(self.params.total_window, 15)).copy()
        if hist.empty or len(hist) < 5:
            return None

        hist["ema5"] = hist["close"].ewm(span=5).mean()
        hist["ema10"] = hist["close"].ewm(span=10).mean()
        hist["ema20"] = hist["close"].ewm(span=20).mean()
        latest = hist.iloc[-1]
        prev = hist.iloc[-2]
        stage_score = float(self._last_stage.score if self._last_stage else 0.0)
        stage_level = int(self._last_stage.stage if self._last_stage else 0)
        regime = str(market_context.get("regime", "normal") or "normal")

        latest_close = float(latest.get("close", 0.0) or 0.0)
        prev_close = float(prev.get("close", latest_close) or latest_close)
        day_return = (latest_close / prev_close - 1.0) if prev_close > 0 else 0.0
        recent_low = float(hist["low"].tail(5).min()) if "low" in hist.columns else latest_close
        ema10 = float(latest.get("ema10", latest_close) or latest_close)
        ema20 = float(latest.get("ema20", latest_close) or latest_close)
        close_below_short_ma = latest_close < ema10 and latest_close < ema20
        breakdown_recent_low = latest_close < recent_low * 1.005 if recent_low > 0 else False
        failure_text = str(self._last_stage.details if self._last_stage else "")
        close_strength = 0.5
        if "high" in hist.columns and "low" in hist.columns:
            latest_range = max(float(latest.get("high", latest_close) or latest_close) - float(latest.get("low", latest_close) or latest_close), 1e-6)
            close_strength = (latest_close - float(latest.get("low", latest_close) or latest_close)) / latest_range

        sell_score = 0.0
        reasons = []
        hard_failure = False
        if "失效" in failure_text or "跌破" in failure_text:
            sell_score += 0.55
            reasons.append("形态失效")
            hard_failure = True
        if close_below_short_ma and day_return < -0.015:
            sell_score += 0.20
            reasons.append("跌破短期均线")
        if breakdown_recent_low and day_return < -0.01:
            sell_score += 0.15
            reasons.append("跌破近5日低点")
        if stage_level <= 1 and stage_score < 30:
            sell_score += 0.10
            reasons.append("弱转强评分回落")
        if regime == "defense" and day_return < 0 and close_strength < 0.35:
            sell_score += 0.10
            reasons.append("弱市转弱")

        if sell_score >= 0.35:
            sell_weight = 1.0 if hard_failure or sell_score >= 0.75 else (0.7 if sell_score >= 0.5 else 0.5)
            return Signal(
                symbol=symbol,
                date=datetime.now(),
                signal=-1,
                weight=sell_weight,
                candidate_score=min(sell_score, 1.0),
                gate_passed=True,
                gate_reason="; ".join(reasons) if reasons else "弱转强卖出",
            )
        return None
