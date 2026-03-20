# -*- coding: utf-8 -*-
"""
资金一致性因子（Fund_Consistency_Factor, FCF）

目标：用低延迟、可回测的数据（OHLCV + 可选换手率）识别“吸筹/拉升/出货”一致性。

核心结构：
FCF =
  0.4 * 筹码集中趋势(SCR_Trend) +
  0.3 * 换手结构(Turnover_Structure) +
  0.3 * 量价一致性(VP_Consistency)

说明：
- 真实 SCR 在数据上通常滞后且不可稳定获取；此处用“波动收敛+量能配合”的代理 SCR。
- 换手结构优先使用实时 spot 换手率；不可用时用成交量相对均量作为替代。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from strategy.analysis.base_analyzer import BaseAnalyzer, ScoreResult
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FundConsistencyResult:
    """FCF 结果"""

    fcf: float  # -1~1
    scr_trend: float
    turnover_structure: float
    vp_consistency: float
    raw: Dict

    def to_dict(self) -> dict:
        return {
            "fcf": self.fcf,
            "scr_trend": self.scr_trend,
            "turnover_structure": self.turnover_structure,
            "vp_consistency": self.vp_consistency,
            "raw": self.raw,
        }


def _tanh_norm(x: float, scale: float = 1.0) -> float:
    try:
        return float(np.tanh(float(x) * float(scale)))
    except Exception:
        return 0.0


def compute_fcf(
    df: pd.DataFrame,
    turnover_rate: Optional[float] = None,
    death_turnover: float = 50.0,
) -> FundConsistencyResult:
    """
    计算 FCF（资金一致性因子）

    Args:
        df: K线DataFrame，至少包含 close/high/low/volume（缺失则降级）
        turnover_rate: 换手率(%)，可选（实盘 spot 里可取）
        death_turnover: “死亡换手率”阈值（>该值直接负分）
    """
    if df is None or df.empty:
        return FundConsistencyResult(0.0, 0.0, 0.0, 0.0, {"reason": "empty_df"})

    hist = df.copy()
    # 统一列名为小写
    hist.columns = [str(c).lower() for c in hist.columns]

    for c in ("close", "high", "low"):
        if c not in hist.columns:
            hist[c] = hist.get("close", np.nan)
    if "volume" not in hist.columns:
        hist["volume"] = np.nan

    hist = hist.dropna(subset=["close"]).copy()
    if hist.empty or len(hist) < 10:
        return FundConsistencyResult(0.0, 0.0, 0.0, 0.0, {"reason": "insufficient_bars"})

    close = pd.to_numeric(hist["close"], errors="coerce")
    high = pd.to_numeric(hist["high"], errors="coerce")
    low = pd.to_numeric(hist["low"], errors="coerce")
    vol = pd.to_numeric(hist["volume"], errors="coerce").fillna(0.0)

    # --- (1) SCR 代理：区间收敛度 ---
    # range_ratio 越小，集中度越高；再乘以量能相对强度体现“吸筹”
    roll_high = high.rolling(20, min_periods=10).max()
    roll_low = low.rolling(20, min_periods=10).min()
    range_ratio = (roll_high - roll_low) / close.replace(0, np.nan)
    range_ratio = range_ratio.replace([np.inf, -np.inf], np.nan).fillna(range_ratio.median())

    vol_ma20 = vol.rolling(20, min_periods=10).mean().replace(0, np.nan)
    vol_ratio = (vol / vol_ma20).replace([np.inf, -np.inf], np.nan).fillna(1.0)

    scr_proxy = (1.0 - np.clip(range_ratio / 0.30, 0.0, 1.0)) * np.clip(vol_ratio, 0.0, 3.0)
    scr_proxy = scr_proxy.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    scr_trend = float(scr_proxy.iloc[-1] - scr_proxy.shift(5).iloc[-1]) if len(scr_proxy) >= 6 else 0.0
    scr_trend_n = _tanh_norm(scr_trend, scale=2.0)

    # --- (2) 换手结构 ---
    # 优先用 turnover_rate；无则用 vol_ratio 近似（结构：当前相对均量）
    if turnover_rate is not None:
        try:
            tr = float(turnover_rate)
        except Exception:
            tr = None
    else:
        tr = None

    if tr is not None and tr > death_turnover:
        turnover_n = -1.0
        turnover_struct = -1.0
    else:
        turnover_struct = float(np.log(max(float(vol_ratio.iloc[-1]), 1e-6)))
        turnover_n = _tanh_norm(turnover_struct, scale=1.2)

    # --- (3) 量价一致性 ---
    ret_1d = float(close.pct_change().iloc[-1]) if len(close) >= 2 else 0.0
    vp = float(np.sign(ret_1d) * np.log(max(float(vol_ratio.iloc[-1]), 1e-6)))
    vp_n = _tanh_norm(vp, scale=1.2)

    fcf = 0.4 * scr_trend_n + 0.3 * turnover_n + 0.3 * vp_n
    fcf = float(np.clip(fcf, -1.0, 1.0))

    raw = {
        "ret_1d": ret_1d,
        "vol_ratio": float(vol_ratio.iloc[-1]),
        "range_ratio": float(range_ratio.iloc[-1]) if len(range_ratio) > 0 else None,
        "turnover_rate": tr,
        "scr_proxy": float(scr_proxy.iloc[-1]) if len(scr_proxy) > 0 else None,
    }

    return FundConsistencyResult(
        fcf=fcf,
        scr_trend=float(scr_trend_n),
        turnover_structure=float(turnover_n),
        vp_consistency=float(vp_n),
        raw=raw,
    )


def compute_recent_fcf_series(
    df: pd.DataFrame,
    lookback_days: int = 3,
    turnover_rate: Optional[float] = None,
    death_turnover: float = 50.0,
) -> List[float]:
    """
    计算最近若干日的 FCF 序列（从旧到新）。

    Args:
        df: K线数据
        lookback_days: 返回最近 N 个交易日的 FCF
        turnover_rate: 可选换手率
        death_turnover: 死亡换手率阈值

    Returns:
        List[float]: FCF 序列，按时间正序排列
    """
    if df is None or df.empty or lookback_days <= 0:
        return []

    series: List[float] = []
    total = len(df)
    start = max(20, total - lookback_days + 1)
    for end_idx in range(start, total + 1):
        sub = df.iloc[:end_idx]
        if sub is None or sub.empty or len(sub) < 10:
            continue
        try:
            series.append(
                float(
                    compute_fcf(
                        sub,
                        turnover_rate=turnover_rate,
                        death_turnover=death_turnover,
                    ).fcf
                )
            )
        except Exception:
            continue
    return series


class FundConsistencyAnalyzer(BaseAnalyzer):
    """FCF 分析器（包装 compute_fcf，提供缓存与结果结构）"""

    def __init__(self):
        super().__init__("FundConsistency")
        self._cache_ttl = 300

    def analyze(self, **kwargs) -> ScoreResult:
        symbol = kwargs.get("symbol", "")
        df = kwargs.get("df")
        turnover_rate = kwargs.get("turnover_rate")
        return self.analyze_fcf(symbol=symbol, df=df, turnover_rate=turnover_rate)

    def analyze_fcf(self, symbol: str, df: pd.DataFrame, turnover_rate: Optional[float] = None) -> ScoreResult:
        cache_key = f"fcf_{symbol}_{len(df) if df is not None else 0}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        res = ScoreResult()
        try:
            f = compute_fcf(df, turnover_rate=turnover_rate)
            res.score = float((f.fcf + 1) * 50)  # -1~1 映射到 0~100
            res.raw_data = f.to_dict()
            res.signals = [f"FCF={f.fcf:.2f}"]
            res.success = True
        except Exception as e:
            res.success = False
            res.error_msg = str(e)
            logger.warning(f"FCF计算失败 {symbol}: {e}")

        self._set_cache(cache_key, res)
        return res
