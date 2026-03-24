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

        for i in range(n - self.params.limit_up_window, stage1_end):
            if limit_up[i]:
                stage.limit_up_idx = i
                stage.limit_up_price = close[i]
                stage.limit_up_vol = volume[i]
                break

        if stage.limit_up_idx < 0:
            return WeakToStrongStage(stage=1, score=0.0, details="近期无涨停")

        stage1_score = self._calc_limit_up_quality(
            df, stage.limit_up_idx, stage.limit_up_price
        )
        score += stage1_score
        details.append(f"涨停日{i}(+{stage1_score:.0f})")
        stage.stage = 1

        pullback_start = stage.limit_up_idx + 1
        pullback_end = max(pullback_start, n - 3)

        if pullback_end > pullback_start:
            pullback_vol = volume[pullback_start:pullback_end]
            pullback_close = close[pullback_start:pullback_end]
            pullback_prev_close = prev_close[pullback_start:pullback_end]

            if len(pullback_vol) > 0:
                stage.shrink_vol_avg = float(np.mean(pullback_vol))

            pullback_valid = True
            pullback_days_count = 0
            for j in range(pullback_start, pullback_end):
                if close[j] > open_[j] and volume[j] > stage.limit_up_vol * 0.8:
                    pullback_valid = False
                    break
                if close[j] < prev_close[j]:
                    pullback_days_count += 1

            if stage.shrink_vol_avg > 0 and stage.limit_up_vol > 0:
                shrink_ratio = stage.shrink_vol_avg / stage.limit_up_vol
                if shrink_ratio <= self.params.shrink_ratio:
                    score += 20
                    details.append(f"缩量洗盘{shrink_ratio:.1%}(+20)")
                    stage.stage = max(stage.stage, 2)
                elif shrink_ratio <= 0.7:
                    score += 10
                    details.append(f"温和缩量{shrink_ratio:.1%}(+10)")
                    stage.stage = max(stage.stage, 2)

            if pullback_days_count > self.params.pullback_days:
                score -= 10
                details.append(f"回调过久-{10}")

            pullback_low = float(np.min(low[pullback_start:pullback_end]))
            if stage.limit_up_price > 0:
                pullback_pct = (pullback_low / stage.limit_up_price - 1) * 100
                if pullback_pct <= -self.params.max_pullback_pct:
                    return WeakToStrongStage(
                        stage=0,
                        limit_up_idx=stage.limit_up_idx,
                        limit_up_price=stage.limit_up_price,
                        limit_up_vol=stage.limit_up_vol,
                        shrink_vol_avg=stage.shrink_vol_avg,
                        score=0.0,
                        details=(
                            f"{'; '.join(details)}; 回撤过深{pullback_pct:.1f}%"
                            if details else f"回撤过深{pullback_pct:.1f}%"
                        ),
                    )

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
            return WeakToStrongStage(
                stage=0,
                limit_up_idx=stage.limit_up_idx,
                limit_up_price=stage.limit_up_price,
                limit_up_vol=stage.limit_up_vol,
                shrink_vol_avg=stage.shrink_vol_avg,
                score=0.0,
                details=f"{'; '.join(details)}; {failure_reason}" if details else failure_reason,
            )

        reversal_start = max(pullback_end, n - 3)
        reversal_end = n

        if reversal_end > reversal_start and reversal_end - reversal_start >= 2:
            reversal_vol = volume[reversal_start:reversal_end]
            reversal_close = close[reversal_start:reversal_end]
            reversal_open = open_[reversal_start:reversal_end]
            reversal_prev_close = prev_close[reversal_start:reversal_end]

            stage.reversal_days = reversal_end - reversal_start
            stage.reversal_vol_avg = float(np.mean(reversal_vol))
            stage.reversal_return = float(
                (reversal_close[-1] - reversal_close[0]) / reversal_close[0] * 100
            )

            rally_count = 0
            strong_rally_count = 0
            for j in range(reversal_start, reversal_end):
                if bull_candle[j]:
                    rally_count += 1
                if bull_candle[j] and close[j] > prev_close[j]:
                    strong_rally_count += 1

            reversal_score = 0
            latest_confirm_valid = False
            confirm_reason = ""
            if len(reversal_open) > 0 and len(reversal_prev_close) > 0:
                latest_idx = reversal_end - 1
                latest_gap_pct = (
                    (reversal_open[-1] / reversal_prev_close[-1] - 1) * 100
                    if reversal_prev_close[-1] > 0
                    else 0.0
                )
                latest_range = max(high[latest_idx] - low[latest_idx], 1e-6)
                latest_close_position = (close[latest_idx] - low[latest_idx]) / latest_range
                latest_confirm_valid = (
                    reversal_open[-1] > reversal_prev_close[-1]
                    and reversal_close[-1] > reversal_prev_close[-1]
                    and latest_gap_pct <= self.params.max_confirm_gap_pct
                    and latest_close_position >= self.params.min_close_position_ratio
                )
                if reversal_open[-1] <= reversal_prev_close[-1] or reversal_close[-1] <= reversal_prev_close[-1]:
                    confirm_reason = "确认日未水上开盘/收盘"
                elif latest_gap_pct > self.params.max_confirm_gap_pct:
                    confirm_reason = f"确认日高开{latest_gap_pct:.1f}%过猛"
                elif latest_close_position < self.params.min_close_position_ratio:
                    confirm_reason = "确认日收盘位置偏低"

            prior_weak_valid = True
            prior_weak_reason = ""
            if reversal_end >= 2:
                weak_idx = reversal_end - 2
                weak_prev_close = prev_close[weak_idx]
                weak_day_return = (
                    (close[weak_idx] / weak_prev_close - 1) * 100
                    if weak_prev_close > 0
                    else 0.0
                )
                weak_range = max(high[weak_idx] - low[weak_idx], 1e-6)
                weak_upper_shadow_ratio = (
                    (high[weak_idx] - max(open_[weak_idx], close[weak_idx])) / weak_range
                )
                weak_vol_base = (
                    float(np.mean(volume[max(pullback_start, weak_idx - 3):weak_idx]))
                    if weak_idx > pullback_start
                    else stage.shrink_vol_avg
                )
                weak_volume_multiple = (
                    volume[weak_idx] / weak_vol_base
                    if weak_vol_base and weak_vol_base > 0
                    else 0.0
                )
                prior_weak_valid = bool(
                    weak_day_return < 0
                    or weak_upper_shadow_ratio >= self.params.prior_weak_upper_shadow_ratio
                    or weak_volume_multiple >= self.params.prior_weak_volume_multiple
                )
                if not prior_weak_valid:
                    prior_weak_reason = "前一日分歧不明显，非典型弱转强"

            if rally_count >= 2:
                reversal_score += 10
                details.append(f"连续阳线{rally_count}天(+10)")

            vol_multiple = (
                stage.reversal_vol_avg / stage.shrink_vol_avg
                if stage.shrink_vol_avg > 0
                else 0
            )
            if strong_rally_count >= 1 and vol_multiple >= self.params.volume_multiple:
                reversal_score += 10
                details.append(f"放量{vol_multiple:.1f}倍(+10)")
            elif strong_rally_count >= 1 and vol_multiple >= 1.5:
                reversal_score += 5
                details.append(f"温和放量{vol_multiple:.1f}倍(+5)")

            if stage.reversal_return >= self.params.min_rally_pct:
                reversal_score += 10
                details.append(f"反弹{stage.reversal_return:.1f}%(+10)")

            if not prior_weak_valid:
                details.append(prior_weak_reason or "前一日弱势不明显")
            elif (
                self.params.require_confirm_open_above_prev_close
                and not latest_confirm_valid
            ):
                details.append(confirm_reason or "确认日未满足弱转强条件")
            elif stage.stage >= 2 and reversal_score >= 15:
                score += reversal_score
                stage.stage = 4
            elif stage.stage >= 2 and reversal_score >= 10 and strong_rally_count >= 1:
                score = max(score, 25)
                stage.stage = max(stage.stage, 3)
                details.append("初步企稳(+5)")

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
