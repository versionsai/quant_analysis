# -*- coding: utf-8 -*-
"""
回测引擎
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from config.config import BACKTEST_CONFIG, STRATEGY_CONFIG, TRADING_CONFIG
from strategy.base import BaseStrategy, Portfolio, Position
from strategy.analysis.fund.fund_consistency import compute_fcf, compute_recent_fcf_series
from strategy.selectors import BaseSelector, SelectResult
from utils.logger import get_logger

logger = get_logger(__name__)


def _calc_concept_proxy_score(symbol: str, date: datetime, price_data: Dict[str, pd.DataFrame]) -> float:
    """
    回测里的“概念主线强度”代理。

    说明：
    - 历史概念成分与强度数据在本地回测链路里不可稳定获得；
    - 这里用“个股相对强度 + 市场强势群体密度”近似主线承接度；
    - 返回 0~1，供弱市抱团豁免与买入过滤使用。
    """
    symbol_df = price_data.get(symbol)
    if symbol_df is None or symbol_df.empty or date not in symbol_df.index:
        return 0.0

    def _single_score(df: pd.DataFrame) -> Optional[float]:
        if df is None or df.empty or date not in df.index:
            return None
        hist = df[df.index <= date].tail(20)
        if hist.empty:
            return None
        latest = hist.iloc[-1]
        close = float(latest.get("close", 0.0))
        high = float(latest.get("high", close))
        low = float(latest.get("low", close))
        volume = float(latest.get("volume", 0.0))
        if close <= 0:
            return None

        if len(hist) >= 4:
            prev_close_3d = float(hist.iloc[-4].get("close", close))
            ret_3d = (close - prev_close_3d) / prev_close_3d if prev_close_3d > 0 else 0.0
        else:
            ret_3d = 0.0

        vol_ma10 = float(hist["volume"].tail(10).mean()) if "volume" in hist.columns and len(hist) >= 10 else volume
        vol_ratio = (volume / vol_ma10) if vol_ma10 > 0 else 1.0
        close_strength = (close - low) / max(high - low, 1e-6)
        score = 0.5
        score += float(np.clip(ret_3d / 0.10, -1.0, 1.0) * 0.30)
        score += float(np.clip((vol_ratio - 1.0) / 1.0, -1.0, 1.0) * 0.10)
        score += float(np.clip((close_strength - 0.5) / 0.5, -1.0, 1.0) * 0.10)
        return float(np.clip(score, 0.0, 1.0))

    symbol_score = _single_score(symbol_df)
    if symbol_score is None:
        return 0.0

    universe_scores: List[float] = []
    for _, df in price_data.items():
        score = _single_score(df)
        if score is not None:
            universe_scores.append(score)

    if not universe_scores:
        return float(symbol_score)

    symbol_rank = float(np.mean([1.0 if s <= symbol_score else 0.0 for s in universe_scores]))
    strong_ratio = float(np.mean([1.0 if s >= 0.65 else 0.0 for s in universe_scores]))
    proxy = 0.7 * symbol_rank + 0.3 * strong_ratio
    return float(np.clip(proxy, 0.0, 1.0))


def _calc_space_proxy_score(date: datetime, price_data: Dict[str, pd.DataFrame]) -> float:
    """
    计算市场空间强度代理分（0-100）。
    """
    above_ma20_flags: List[float] = []
    near_high_flags: List[float] = []
    positive_flags: List[float] = []

    for _, df in price_data.items():
        if df is None or df.empty or date not in df.index:
            continue
        hist = df[df.index <= date].tail(60).copy()
        if hist.empty:
            continue

        latest = hist.iloc[-1]
        close = float(latest.get("close", 0.0) or 0.0)
        if close <= 0:
            continue

        ma20 = float(hist["close"].tail(20).mean()) if "close" in hist.columns and len(hist) >= 20 else close
        high60 = float(hist["high"].max()) if "high" in hist.columns else close
        if len(hist) >= 2:
            prev_close = float(hist.iloc[-2].get("close", close) or close)
            day_ret = (close - prev_close) / prev_close if prev_close > 0 else 0.0
        else:
            day_ret = 0.0

        above_ma20_flags.append(1.0 if close >= ma20 else 0.0)
        near_high_flags.append(1.0 if close >= high60 * 0.97 else 0.0)
        positive_flags.append(1.0 if day_ret > 0 else 0.0)

    if not above_ma20_flags:
        return 50.0

    above_ma20_ratio = float(np.mean(above_ma20_flags))
    near_high_ratio = float(np.mean(near_high_flags))
    positive_ratio = float(np.mean(positive_flags))

    score = 50.0
    score += float(np.clip((above_ma20_ratio - 0.5) / 0.25, -1.0, 1.0) * 18.0)
    score += float(np.clip((near_high_ratio - 0.15) / 0.15, -1.0, 1.0) * 18.0)
    score += float(np.clip((positive_ratio - 0.5) / 0.25, -1.0, 1.0) * 14.0)
    return float(np.clip(score, 0.0, 100.0))


def _calc_index_change_context(date: datetime, benchmark_frames: Dict[str, dict], price_data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
    """
    计算指数环境代理。

    优先基于深证成指、中证500、中证1000构建综合强度，
    避免单一指数主导市场状态判断。
    """
    weighted_changes: List[tuple] = []
    raw_changes: List[float] = []
    component_names: List[str] = []
    for code, item in benchmark_frames.items():
        df = item.get("data")
        if df is None or df.empty or date not in df.index:
            continue
        try:
            row = df.loc[date]
            if "pct_change" in df.columns and not pd.isna(row.get("pct_change", np.nan)):
                change = float(row.get("pct_change", 0.0))
            else:
                loc = df.index.get_loc(date)
                if isinstance(loc, slice) or loc == 0:
                    continue
                prev_close = float(df.iloc[loc - 1].get("close", row.get("close", 0.0)) or 0.0)
                close = float(row.get("close", prev_close) or prev_close)
                change = ((close - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
            raw_changes.append(change)
            weight = float(BENCHMARK_REGIME_WEIGHTS.get(str(code), 0.0) or 0.0)
            if weight > 0:
                weighted_changes.append((change, weight))
                component_names.append(str(item.get("name", code)))
        except Exception:
            continue

    if weighted_changes:
        total_weight = sum(weight for _, weight in weighted_changes)
        composite_change = (
            sum(change * weight for change, weight in weighted_changes) / total_weight
            if total_weight > 0
            else 0.0
        )
        return {
            "avg_change": float(composite_change),
            "worst_change": float(np.min(raw_changes or [composite_change])),
            "composite_change": float(composite_change),
            "composite_name": COMPOSITE_BENCHMARK_NAME,
            "component_names": component_names,
        }

    universe_changes: List[float] = []
    for _, df in price_data.items():
        if df is None or df.empty or date not in df.index:
            continue
        try:
            row = df.loc[date]
            if "pct_change" in df.columns and not pd.isna(row.get("pct_change", np.nan)):
                universe_changes.append(float(row.get("pct_change", 0.0)))
        except Exception:
            continue
    if not universe_changes:
        return {
            "avg_change": 0.0,
            "worst_change": 0.0,
            "composite_change": 0.0,
            "composite_name": COMPOSITE_BENCHMARK_NAME,
            "component_names": [],
        }
    return {
        "avg_change": float(np.mean(universe_changes)),
        "worst_change": float(np.min(universe_changes)),
        "composite_change": float(np.mean(universe_changes)),
        "composite_name": COMPOSITE_BENCHMARK_NAME,
        "component_names": [],
    }


def _resolve_market_regime(market_score: float, space_score: float, avg_change: float, worst_change: float) -> str:
    """
    解析市场状态。
    """
    if market_score <= 30.0 and avg_change >= 1.2 and worst_change > -1.0 and space_score >= 55.0:
        return "golden_pit"
    if market_score <= 35.0 or avg_change <= -1.5 or worst_change <= -2.5:
        return "defense"
    return "normal"


def _build_market_context_payload(
    date: datetime,
    price_data: Dict[str, pd.DataFrame],
    benchmark_frames: Dict[str, dict],
    market_score: float,
) -> Dict[str, object]:
    """
    构建供策略使用的市场上下文。
    """
    space_score = _calc_space_proxy_score(date, price_data)
    index_context = _calc_index_change_context(date, benchmark_frames, price_data)
    avg_change = float(index_context.get("avg_change", 0.0) or 0.0)
    worst_change = float(index_context.get("worst_change", 0.0) or 0.0)
    regime = _resolve_market_regime(market_score, space_score, avg_change, worst_change)
    return {
        "date": date.strftime("%Y-%m-%d"),
        "market_score": float(market_score),
        "space_score": float(space_score),
        "avg_change": avg_change,
        "worst_change": worst_change,
        "composite_change": float(index_context.get("composite_change", avg_change) or avg_change),
        "composite_name": str(index_context.get("composite_name", COMPOSITE_BENCHMARK_NAME) or COMPOSITE_BENCHMARK_NAME),
        "component_names": list(index_context.get("component_names", []) or []),
        "regime": regime,
    }


@dataclass
class Trade:
    """成交记录"""
    date: datetime
    symbol: str
    direction: str  # buy/sell
    price: float
    quantity: int
    commission: float
    reason: str = ""


@dataclass
class BacktestResult:
    """回测结果"""
    trades: List[Trade] = field(default_factory=list)
    daily_values: pd.DataFrame = field(default_factory=pd.DataFrame)
    
    total_return: float = 0.0
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    benchmark_metrics: Dict[str, dict] = field(default_factory=dict)
    phase_metrics: List[dict] = field(default_factory=list)
    signal_summary: Dict[str, float] = field(default_factory=dict)


@dataclass
class PendingOrder:
    """待执行订单（用于回测中的 T+1 成交近似）"""
    symbol: str
    action: str
    decision_date: datetime
    execution_date: datetime
    weight: float = 0.0
    sell_ratio: float = 0.0
    reason: str = ""


DEFAULT_BENCHMARKS = [
    {"code": "399001", "name": "深证成指", "kind": "index"},
    {"code": "000905", "name": "中证500", "kind": "index"},
    {"code": "000852", "name": "中证1000", "kind": "index"},
]
BENCHMARK_REGIME_WEIGHTS = {
    "399001": 0.40,
    "000905": 0.35,
    "000852": 0.25,
}
COMPOSITE_BENCHMARK_NAME = "综合强度"


def _calc_basic_metrics(daily_values: pd.DataFrame, initial_capital: float) -> Dict[str, float]:
    """
    统一计算收益、年化、夏普、最大回撤。
    """
    if daily_values is None or daily_values.empty:
        return {
            "total_return": 0.0,
            "annual_return": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
        }

    df = daily_values.copy()
    df["return"] = df["total_value"].pct_change()
    total_return = (df["total_value"].iloc[-1] / initial_capital) - 1
    days = max((df["date"].iloc[-1] - df["date"].iloc[0]).days, 1)
    annual_return = (1 + total_return) ** (365 / days) - 1
    sharpe_ratio = df["return"].mean() / df["return"].std() * np.sqrt(252) if df["return"].std() > 0 else 0.0
    cummax = df["total_value"].cummax()
    drawdown = (df["total_value"] - cummax) / cummax
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
    return {
        "total_return": float(total_return),
        "annual_return": float(annual_return),
        "sharpe_ratio": float(sharpe_ratio),
        "max_drawdown": max_drawdown,
    }


def _calc_win_rate(trades: List[Trade]) -> float:
    """
    基于买卖配对计算胜率。
    """
    win_trades = 0
    total_round_trips = 0
    open_buys: Dict[str, List[Trade]] = {}
    for item in trades:
        if item.direction == "buy":
            open_buys.setdefault(item.symbol, []).append(item)
        elif item.direction == "sell":
            queue = open_buys.get(item.symbol, [])
            if not queue:
                continue
            buy_t = queue.pop(0)
            total_round_trips += 1
            if item.price > buy_t.price:
                win_trades += 1
    return win_trades / total_round_trips if total_round_trips > 0 else 0.0


def _build_benchmark_metrics(
    daily_values: pd.DataFrame,
    benchmark_frames: Dict[str, dict],
    initial_capital: float,
) -> Dict[str, dict]:
    """
    生成基准收益对比。
    """
    if daily_values is None or daily_values.empty:
        return {}

    result: Dict[str, dict] = {}
    strategy_series = daily_values.set_index("date")["total_value"].astype(float)
    strategy_return = float(strategy_series.iloc[-1] / initial_capital - 1)

    for code, item in benchmark_frames.items():
        df = item.get("data")
        if df is None or df.empty:
            continue
        benchmark_close = df["close"].astype(float)
        aligned = pd.concat([strategy_series, benchmark_close], axis=1, join="inner").dropna()
        if aligned.empty:
            continue
        benchmark_total_return = float(aligned.iloc[-1, 1] / aligned.iloc[0, 1] - 1)
        benchmark_curve = initial_capital * (aligned.iloc[:, 1] / aligned.iloc[0, 1])
        benchmark_daily = pd.DataFrame({"date": aligned.index, "total_value": benchmark_curve.values})
        metrics = _calc_basic_metrics(benchmark_daily, initial_capital)
        metrics.update({
            "code": code,
            "name": str(item.get("name", code)),
            "excess_return": float(strategy_return - benchmark_total_return),
        })
        result[code] = metrics
    return result


def _build_phase_metrics(
    daily_values: pd.DataFrame,
    benchmark_frames: Dict[str, dict],
    initial_capital: float,
) -> List[dict]:
    """
    基于深证成指、中证500、中证1000综合强度的 20 日动量，
    拆分上涨/震荡/下跌阶段。
    """
    if daily_values is None or daily_values.empty or not benchmark_frames:
        return []

    strategy_series = daily_values.set_index("date")["total_value"].astype(float)
    benchmark_parts: List[pd.Series] = []
    used_names: List[str] = []
    total_weight = 0.0
    for code, weight in BENCHMARK_REGIME_WEIGHTS.items():
        frame = benchmark_frames.get(code, {})
        df = frame.get("data")
        if df is None or df.empty or "close" not in df.columns:
            continue
        benchmark_parts.append(df["close"].astype(float).rename(code) * float(weight))
        used_names.append(str(frame.get("name", code)))
        total_weight += float(weight)

    if not benchmark_parts or total_weight <= 0:
        return []

    benchmark_df = pd.concat(benchmark_parts, axis=1, join="inner").dropna()
    if benchmark_df.empty:
        return []
    benchmark_close = benchmark_df.sum(axis=1) / total_weight
    aligned = pd.concat([strategy_series, benchmark_close.rename("benchmark_close")], axis=1, join="inner").dropna()
    if len(aligned) < 25:
        return []

    aligned.columns = ["strategy_value", "benchmark_close"]
    aligned["benchmark_20d_ret"] = aligned["benchmark_close"].pct_change(20)

    def _label(row: pd.Series) -> str:
        value = float(row.get("benchmark_20d_ret", 0.0) or 0.0)
        if value >= 0.05:
            return "上涨段"
        if value <= -0.05:
            return "下跌段"
        return "震荡段"

    aligned["phase"] = aligned.apply(_label, axis=1)
    rows: List[dict] = []
    for phase_name, group in aligned.groupby("phase"):
        if group.empty:
            continue
        strategy_daily = pd.DataFrame({
            "date": group.index,
            "total_value": group["strategy_value"].values,
        })
        phase_initial = float(group["strategy_value"].iloc[0])
        metrics = _calc_basic_metrics(strategy_daily, phase_initial)
        benchmark_return = float(group["benchmark_close"].iloc[-1] / group["benchmark_close"].iloc[0] - 1)
        rows.append({
            "phase": phase_name,
            "days": int(len(group)),
            "start": group.index[0].strftime("%Y-%m-%d"),
            "end": group.index[-1].strftime("%Y-%m-%d"),
            "benchmark_name": COMPOSITE_BENCHMARK_NAME,
            "benchmark_components": list(used_names),
            "total_return": float(metrics["total_return"]),
            "annual_return": float(metrics["annual_return"]),
            "max_drawdown": float(metrics["max_drawdown"]),
            "benchmark_return": benchmark_return,
            "excess_return": float(metrics["total_return"] - benchmark_return),
        })
    rows.sort(key=lambda item: item["phase"])
    return rows


def _summarize_signal_journal(signal_journal: List[dict]) -> Dict[str, float]:
    """
    汇总候选信号与最终放行情况。
    """
    if not signal_journal:
        return {}

    candidate_count = len(signal_journal)
    gated_count = sum(1 for item in signal_journal if bool(item.get("gate_passed", True)))
    buy_count = sum(1 for item in signal_journal if float(item.get("final_signal", 0.0)) > 0)
    sell_count = sum(1 for item in signal_journal if float(item.get("final_signal", 0.0)) < 0)
    avg_candidate_score = float(np.mean([float(item.get("candidate_score", 0.0) or 0.0) for item in signal_journal]))
    return {
        "candidate_count": float(candidate_count),
        "gated_count": float(gated_count),
        "gate_pass_rate": float(gated_count / candidate_count) if candidate_count > 0 else 0.0,
        "buy_signal_count": float(buy_count),
        "sell_signal_count": float(sell_count),
        "avg_candidate_score": avg_candidate_score,
    }


class BacktestEngine:
    """回测引擎"""
    
    def __init__(
        self,
        strategy: BaseStrategy,
        initial_capital: float = None,
        commission_rate: float = None,
        stamp_tax: float = None,
        slippage: float = None,
        execution_mode: Optional[str] = None,
        risk_overrides: Optional[Dict[str, object]] = None,
        candidate_gate_threshold: Optional[float] = None,
    ):
        self.strategy = strategy
        self.initial_capital = initial_capital or BACKTEST_CONFIG["initial_capital"]
        self.commission_rate = commission_rate or BACKTEST_CONFIG["commission_rate"]
        self.stamp_tax = stamp_tax or BACKTEST_CONFIG["stamp_tax"]
        self.slippage = slippage or BACKTEST_CONFIG["slippage"]
        self.min_commission = BACKTEST_CONFIG["min_commission"]
        self.execution_mode = str(execution_mode or BACKTEST_CONFIG.get("execution_mode", "next_open"))
        self.max_position = float(TRADING_CONFIG.get("max_position", 0.2))
        self.max_stocks = int(TRADING_CONFIG.get("max_stocks", 10))
        
        self.portfolio = Portfolio(cash=self.initial_capital)
        self.trades: List[Trade] = []
        self.daily_records: List[dict] = []
        self.pending_orders: List[PendingOrder] = []
        self._trade_dates: List[datetime] = []
        self._benchmark_frames: Dict[str, dict] = {}
        self.signal_journal: List[dict] = []
        self._risk_cfg = dict(STRATEGY_CONFIG)
        if risk_overrides:
            self._risk_cfg.update(dict(risk_overrides))
        self.candidate_gate_threshold = float(candidate_gate_threshold) if candidate_gate_threshold is not None else None
    
    def run(
        self,
        symbols: List[str],
        start_date: str,
        end_date: str,
        data_source,
    ) -> BacktestResult:
        """运行回测"""
        logger.info(f"开始回测: {start_date} ~ {end_date}")
        
        dates = pd.date_range(start_date, end_date, freq="D")
        dates = [d for d in dates if d.weekday() < 5]
        self._trade_dates = list(dates)
        self.pending_orders = []
        self.signal_journal = []
        self._benchmark_frames = {}
        
        price_data: Dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            df = data_source.get_kline(symbol, start_date.replace("-", ""), 
                                        end_date.replace("-", ""))
            if not df.empty:
                if "日期" in df.columns:
                    df = df.rename(columns={"日期": "date"})
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.set_index("date")
                price_data[symbol] = df
                self.strategy.load_data(symbol, df)

        for benchmark in DEFAULT_BENCHMARKS:
            code = str(benchmark.get("code", "")).zfill(6)
            if str(benchmark.get("kind", "")) == "index":
                df = data_source.get_index_daily(code)
                if df is not None and not df.empty:
                    df = df[
                        (df["date"] >= pd.to_datetime(start_date.replace("-", "")))
                        & (df["date"] <= pd.to_datetime(end_date.replace("-", "")))
                    ].copy()
            else:
                df = data_source.get_kline(code, start_date.replace("-", ""), end_date.replace("-", ""))
            if df is None or df.empty:
                continue
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date")
            self._benchmark_frames[code] = {
                "name": str(benchmark.get("name", code)),
                "data": df,
            }
        
        for date in dates:
            self._on_date(date, symbols, price_data)
        
        return self._calc_result()

    def _get_next_trade_date(self, date: datetime) -> Optional[datetime]:
        """
        获取下一个交易日，用于避免“信号与成交同K线”。
        """
        for trade_date in self._trade_dates:
            if trade_date > date:
                return trade_date
        return None

    def _get_execution_price(self, df: pd.DataFrame, date: datetime, direction: str) -> Optional[float]:
        """
        获取执行价格。
        """
        if df is None or df.empty or date not in df.index:
            return None

        row = df.loc[date]
        if self.execution_mode == "next_open":
            raw_price = row.get("open", row.get("close"))
        else:
            raw_price = row.get("close")

        try:
            price = float(raw_price)
        except Exception:
            return None

        if price <= 0:
            return None
        if direction == "buy":
            return price * (1 + self.slippage)
        return price * (1 - self.slippage)

    def _normalize_weight(self, weight: float) -> float:
        """
        将策略权重收敛到组合允许的单票上限内。
        """
        try:
            target_weight = float(weight or 0.0)
        except Exception:
            target_weight = 0.0
        if target_weight <= 0:
            target_weight = self.max_position
        return float(np.clip(target_weight, 0.0, self.max_position))

    def _queue_order(
        self,
        symbol: str,
        action: str,
        decision_date: datetime,
        weight: float = 0.0,
        sell_ratio: float = 0.0,
        reason: str = "",
    ) -> None:
        """
        按执行模式登记待执行订单。
        """
        if action == "buy" and symbol in self.portfolio.positions:
            return

        if self.execution_mode == "next_open":
            execution_date = self._get_next_trade_date(decision_date)
            if execution_date is None:
                return
        else:
            execution_date = decision_date

        if action == "buy":
            for item in self.pending_orders:
                if item.symbol == symbol and item.action == "buy":
                    return
        elif action == "sell":
            self.pending_orders = [
                item for item in self.pending_orders
                if not (item.symbol == symbol and item.action in {"buy", "sell", "sell_partial"})
            ]
        elif action == "sell_partial":
            for item in self.pending_orders:
                if item.symbol == symbol and item.action in {"sell", "sell_partial"}:
                    return

        self.pending_orders.append(
            PendingOrder(
                symbol=symbol,
                action=action,
                decision_date=decision_date,
                execution_date=execution_date,
                weight=weight,
                sell_ratio=sell_ratio,
                reason=reason,
            )
        )

    def _execute_pending_orders(self, date: datetime, price_data: Dict[str, pd.DataFrame]) -> None:
        """
        执行当日生效的待执行订单。
        """
        if not self.pending_orders:
            return

        action_priority = {"sell": 0, "sell_partial": 1, "buy": 2}
        remaining: List[PendingOrder] = []

        for order in sorted(self.pending_orders, key=lambda item: (item.execution_date, action_priority.get(item.action, 9))):
            if order.execution_date > date:
                remaining.append(order)
                continue

            df = price_data.get(order.symbol)
            if df is None or df.empty or date not in df.index:
                remaining.append(order)
                continue

            if order.action == "buy":
                self._execute_buy(order.symbol, order.decision_date, date, order.weight, price_data)
                continue
            if order.action == "sell":
                self._execute_sell(order.symbol, date, price_data, reason=order.reason)
                continue
            if order.action == "sell_partial":
                self._execute_sell_partial(order.symbol, date, price_data, sell_ratio=order.sell_ratio, reason=order.reason)
                continue

            remaining.append(order)

        self.pending_orders = remaining

    def _held_trading_days(self, entry_date: Optional[datetime], current_date: datetime) -> int:
        """计算持仓交易日天数（用于T+1/时间止损）"""
        if entry_date is None:
            return 0
        if current_date <= entry_date:
            return 0
        try:
            return max(len(pd.bdate_range(entry_date, current_date)) - 1, 0)
        except Exception:
            return (current_date.date() - entry_date.date()).days

    def _calc_market_emotion_score(
        self,
        date: datetime,
        symbols: List[str],
        price_data: Dict[str, pd.DataFrame],
    ) -> float:
        """
        计算大盘情绪分（回测简化版，0-100）
        用市场广度 + 平均涨跌幅 + 极端下跌占比近似。
        """
        rets: List[float] = []
        down_big = 0
        total = 0

        for symbol in symbols:
            df = price_data.get(symbol)
            if df is None or df.empty or date not in df.index:
                continue

            total += 1
            try:
                if "pct_change" in df.columns and not pd.isna(df.loc[date, "pct_change"]):
                    r = float(df.loc[date, "pct_change"]) / 100.0
                else:
                    loc = df.index.get_loc(date)
                    if isinstance(loc, slice) or loc == 0:
                        continue
                    prev_close = float(df.iloc[loc - 1]["close"])
                    close = float(df.loc[date, "close"])
                    r = (close - prev_close) / prev_close if prev_close > 0 else 0.0
                rets.append(r)
                if r <= -0.095:
                    down_big += 1
            except Exception:
                continue

        if total <= 0 or not rets:
            return 50.0

        avg_ret = float(np.mean(rets))
        up_ratio = float(np.mean([1.0 if r > 0 else 0.0 for r in rets]))
        down_big_ratio = down_big / total

        # 经验映射：平均涨跌幅(±2%) → ±20 分，广度(0.5±0.25) → ±20 分，极端下跌占比惩罚
        score = 50.0
        score += float(np.clip(avg_ret / 0.02, -1, 1) * 20.0)
        score += float(np.clip((up_ratio - 0.5) / 0.25, -1, 1) * 20.0)
        score -= float(np.clip(down_big_ratio / 0.05, 0, 1) * 25.0)
        return float(np.clip(score, 0.0, 100.0))

    def _calc_stock_emotion_score(self, df: pd.DataFrame, date: datetime) -> float:
        """
        计算个股强势/抱团分（回测简化版，0-100）
        以动量 + 放量 + 收盘强度 + 趋势为主。
        """
        if df is None or df.empty or date not in df.index:
            return 50.0

        try:
            hist = df[df.index <= date].tail(80).copy()
            if hist.empty:
                return 50.0
            latest = hist.iloc[-1]

            close = float(latest.get("close", 0.0))
            high = float(latest.get("high", close))
            low = float(latest.get("low", close))
            volume = float(latest.get("volume", 0.0))

            if "pct_change" in hist.columns and not pd.isna(latest.get("pct_change", np.nan)):
                ret = float(latest.get("pct_change", 0.0)) / 100.0
            else:
                if len(hist) < 2:
                    ret = 0.0
                else:
                    prev_close = float(hist.iloc[-2].get("close", close))
                    ret = (close - prev_close) / prev_close if prev_close > 0 else 0.0

            vol_ma20 = float(hist["volume"].tail(20).mean()) if "volume" in hist.columns and len(hist) >= 20 else 0.0
            vol_ratio = (volume / vol_ma20) if vol_ma20 > 0 else 1.0

            rng = max(high - low, 1e-6)
            close_strength = (close - low) / rng  # 0~1

            ma20 = float(hist["close"].tail(20).mean()) if "close" in hist.columns and len(hist) >= 20 else close
            trend_bonus = 10.0 if close >= ma20 else -10.0

            score = 50.0
            score += float(np.clip(ret / 0.05, -1, 1) * 30.0)
            score += float(np.clip((vol_ratio - 1.0) / 1.0, -1, 1) * 15.0)
            score += float(np.clip((close_strength - 0.5) / 0.5, -1, 1) * 20.0)
            score += trend_bonus
            return float(np.clip(score, 0.0, 100.0))
        except Exception:
            return 50.0

    def _check_risk_exits(self, date: datetime, symbols: List[str], price_data: Dict[str, pd.DataFrame]):
        """风控退出（止损/止盈/跟踪止盈/时间止损/情绪退出）"""
        stop_loss = float(self._risk_cfg.get("stop_loss", -0.07))
        take_profit = float(self._risk_cfg.get("take_profit", 0.15))
        trailing_stop = float(self._risk_cfg.get("trailing_stop", 0.0))
        max_hold_days = int(self._risk_cfg.get("max_hold_days", 0))
        time_stop_days = int(self._risk_cfg.get("time_stop_days", 0))
        time_stop_min_return = float(self._risk_cfg.get("time_stop_min_return", 0.0))
        min_hold_days_before_sell = int(self._risk_cfg.get("min_hold_days_before_sell", 0))
        emotion_enabled = bool(self._risk_cfg.get("emotion_enabled", False))
        market_emotion_stop_score = float(self._risk_cfg.get("market_emotion_stop_score", 0.0))
        stock_emotion_override_score = float(self._risk_cfg.get("stock_emotion_override_score", 100.0))
        concept_override_score = float(self._risk_cfg.get("concept_override_score", 1.0))
        override_trailing_stop = float(self._risk_cfg.get("override_trailing_stop", trailing_stop))
        scale_out_enabled = bool(self._risk_cfg.get("scale_out_enabled", False))
        scale_out_levels = self._risk_cfg.get("scale_out_levels", [0.10, 0.20])
        scale_out_ratios = self._risk_cfg.get("scale_out_ratios", [0.50, 1.00])
        entry_low_stop_enabled = bool(self._risk_cfg.get("entry_low_stop_enabled", False))
        entry_low_stop_buffer = float(self._risk_cfg.get("entry_low_stop_buffer", 0.0))
        limit_up_close_strength_sell_all = float(self._risk_cfg.get("limit_up_close_strength_sell_all", 0.10))
        limit_up_close_strength_sell_half = float(self._risk_cfg.get("limit_up_close_strength_sell_half", 0.20))
        fcf_enabled = bool(self._risk_cfg.get("fcf_enabled", False))
        fcf_sell_threshold = float(self._risk_cfg.get("fcf_sell_threshold", 0.0))
        fcf_down_days = int(self._risk_cfg.get("fcf_down_days", 2))
        fcf_death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))

        market_score = self._calc_market_emotion_score(date, symbols, price_data) if emotion_enabled else 50.0

        for symbol, pos in list(self.portfolio.positions.items()):
            if pos.cost <= 0:
                continue

            held_days = self._held_trading_days(pos.entry_date, date)
            if held_days < min_hold_days_before_sell:
                continue

            pnl_pct = (pos.current_price - pos.cost) / pos.cost
            stock_score = self._calc_stock_emotion_score(price_data.get(symbol, pd.DataFrame()), date) if emotion_enabled else 50.0
            concept_score = _calc_concept_proxy_score(symbol, date, price_data) if emotion_enabled else 0.0
            override_hold = (
                emotion_enabled
                and stock_score >= stock_emotion_override_score
                and concept_score >= concept_override_score
            )
            trailing_stop_eff = override_trailing_stop if override_hold else trailing_stop

            # 不及预期：跌破买入日低点（硬止损，优先级仅次于固定止损）
            if entry_low_stop_enabled and pos.entry_low > 0:
                if pos.current_price <= pos.entry_low * (1 - entry_low_stop_buffer):
                    self._sell(symbol, date, reason="break_entry_low")
                    continue

            if fcf_enabled:
                df_hist = price_data.get(symbol)
                if df_hist is not None and not df_hist.empty:
                    hist = df_hist[df_hist.index <= date]
                    if hist is not None and not hist.empty and len(hist) >= 20:
                        fcf_val = float(compute_fcf(hist, turnover_rate=None, death_turnover=fcf_death_turnover).fcf)
                        if fcf_val < fcf_sell_threshold:
                            self._sell(symbol, date, reason="fcf_negative")
                            continue
                        if fcf_down_days >= 2:
                            fcf_series = compute_recent_fcf_series(
                                hist,
                                lookback_days=fcf_down_days + 1,
                                turnover_rate=None,
                                death_turnover=fcf_death_turnover,
                            )
                            if len(fcf_series) >= fcf_down_days + 1:
                                is_down = all(
                                    fcf_series[idx] < fcf_series[idx - 1]
                                    for idx in range(1, len(fcf_series))
                                )
                                if is_down:
                                    self._sell(symbol, date, reason="fcf_down")
                                    continue

            if pnl_pct <= stop_loss:
                self._sell(symbol, date, reason="stop_loss")
                continue

            # 情绪退潮：大盘弱且个股不强势时，优先撤退；强势抱团则“坚定持有”
            if emotion_enabled and (market_score <= market_emotion_stop_score) and (not override_hold):
                self._sell(symbol, date, reason="emotion_stop")
                continue

            # 涨停封板强度/炸板风险（回测近似：收盘强度 close_strength）
            try:
                df_today = price_data.get(symbol)
                if df_today is not None and (date in df_today.index):
                    row = df_today.loc[date]
                    day_ret = None
                    if "pct_change" in df_today.columns and not pd.isna(row.get("pct_change", np.nan)):
                        day_ret = float(row.get("pct_change", 0.0)) / 100.0
                    # 无 pct_change 时用 close/prev_close 估算
                    if day_ret is None:
                        loc = df_today.index.get_loc(date)
                        if not isinstance(loc, slice) and loc > 0:
                            prev_close = float(df_today.iloc[loc - 1]["close"])
                            close = float(row.get("close", pos.current_price))
                            day_ret = (close - prev_close) / prev_close if prev_close > 0 else 0.0
                        else:
                            day_ret = 0.0

                    if day_ret is not None and day_ret >= 0.095:
                        high = float(row.get("high", pos.current_price))
                        low = float(row.get("low", pos.current_price))
                        close = float(row.get("close", pos.current_price))
                        rng = max(high - low, 1e-6)
                        close_strength = (close - low) / rng

                        if close_strength < limit_up_close_strength_sell_all:
                            self._sell(symbol, date, reason="limit_up_weak_seal_all")
                            continue
                        if close_strength < limit_up_close_strength_sell_half:
                            self._sell_partial(symbol, date, sell_ratio=0.5, reason="limit_up_weak_seal_half")
                            continue
            except Exception:
                pass

            # 抱团股增强：分批止盈（10%卖半、20%清仓）
            if override_hold and scale_out_enabled:
                try:
                    lv1 = float(scale_out_levels[0]) if len(scale_out_levels) > 0 else take_profit
                    lv2 = float(scale_out_levels[1]) if len(scale_out_levels) > 1 else max(take_profit, lv1 + 0.05)
                    r1 = float(scale_out_ratios[0]) if len(scale_out_ratios) > 0 else 0.5
                except Exception:
                    lv1, lv2, r1 = 0.10, 0.20, 0.5

                if pos.take_profit_stage <= 0 and pnl_pct >= lv1:
                    self._sell_partial(symbol, date, sell_ratio=r1, reason="scale_out_1")
                    if symbol in self.portfolio.positions:
                        self.portfolio.positions[symbol].take_profit_stage = 1
                    continue
                if pos.take_profit_stage >= 1 and pnl_pct >= lv2:
                    self._sell(symbol, date, reason="scale_out_2")
                    continue
            else:
                if take_profit > 0 and pnl_pct >= take_profit:
                    self._sell(symbol, date, reason="take_profit")
                    continue

            if trailing_stop_eff > 0 and pos.highest_price > 0:
                if pos.current_price <= pos.highest_price * (1 - trailing_stop_eff):
                    self._sell(symbol, date, reason="trailing_stop")
                    continue

            if (not override_hold) and max_hold_days > 0 and held_days >= max_hold_days:
                self._sell(symbol, date, reason="max_hold_days")
                continue

            if (not override_hold) and time_stop_days > 0 and held_days >= time_stop_days and pnl_pct <= time_stop_min_return:
                self._sell(symbol, date, reason="time_stop")
                continue
    
    def _on_date(self, date, symbols: List[str], price_data: Dict[str, pd.DataFrame]):
        """每日处理"""
        self._execute_pending_orders(date, price_data)

        for symbol in symbols:
            if symbol not in price_data:
                continue
            
            df = price_data[symbol]
            if date not in df.index:
                continue
            
            current_price = df.loc[date, "close"]
            
            if symbol in self.portfolio.positions:
                pos = self.portfolio.positions[symbol]
                pos.current_price = float(current_price)
                if pos.highest_price <= 0:
                    pos.highest_price = float(pos.current_price)
                else:
                    pos.highest_price = max(float(pos.highest_price), float(pos.current_price))
            
            self.strategy.load_data(symbol, df[df.index <= date])

        context_market_score = (
            self._calc_market_emotion_score(date, symbols, price_data)
            if bool(self._risk_cfg.get("emotion_enabled", False))
            else 50.0
        )
        self.strategy.set_market_context(
            _build_market_context_payload(date, price_data, self._benchmark_frames, context_market_score)
        )

        # 风控退出优先：短线策略先活下来，再谈信号
        self._check_risk_exits(date, symbols, price_data)
        
        signals = []
        for symbol in symbols:
            if symbol in price_data:
                df = price_data[symbol][price_data[symbol].index <= date]
                signal = self.strategy.on_bar(symbol, df)
                if signal:
                    signal.date = date
                    effective_gate_passed = bool(signal.gate_passed)
                    gate_reason = str(signal.gate_reason or "")
                    if (
                        signal.signal != 0
                        and self.candidate_gate_threshold is not None
                        and float(signal.candidate_score or 0.0) < float(self.candidate_gate_threshold)
                    ):
                        effective_gate_passed = False
                        threshold_reason = (
                            f"candidate_score={float(signal.candidate_score or 0.0):.2f} "
                            f"< threshold={float(self.candidate_gate_threshold):.2f}"
                        )
                        gate_reason = f"{gate_reason}; {threshold_reason}".strip("; ")
                    signals.append(signal)
                    self.signal_journal.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "symbol": signal.symbol,
                        "raw_signal": float(signal.signal),
                        "final_signal": float(signal.signal if effective_gate_passed else 0.0),
                        "weight": float(signal.weight or 0.0),
                        "candidate_score": float(signal.candidate_score or 0.0),
                        "gate_passed": effective_gate_passed,
                        "gate_reason": gate_reason,
                    })
        
        for signal_item, journal_item in zip(signals, self.signal_journal[-len(signals):] if signals else []):
            final_signal = float(journal_item.get("final_signal", 0.0))
            if final_signal > 0:
                self._buy(signal_item.symbol, date, signal_item.weight)
            elif final_signal < 0:
                self._sell(signal_item.symbol, date)
        
        total_value = self.portfolio.get_total_value()
        self.daily_records.append({
            "date": date,
            "cash": self.portfolio.cash,
            "position_value": self.portfolio.get_position_value(),
            "total_value": total_value,
        })
    
    def _buy(self, symbol: str, date, weight: float = 1.0):
        """登记买入订单"""
        self._queue_order(symbol, "buy", date, weight=weight, reason="signal")

    def _execute_buy(self, symbol: str, decision_date: datetime, execution_date: datetime, weight: float, price_data: Dict[str, pd.DataFrame]) -> bool:
        """按执行日价格完成买入"""
        if symbol in self.portfolio.positions:
            return False
        if len(self.portfolio.positions) >= self.max_stocks:
            return False

        df = price_data.get(symbol)
        if df is None or df.empty:
            return False

        hist = df[df.index <= decision_date]
        if hist is None or hist.empty:
            return False

        if bool(self._risk_cfg.get("fcf_enabled", False)) and len(hist) >= 20:
            fcf_buy_threshold = float(self._risk_cfg.get("fcf_buy_threshold", 0.0))
            fcf_death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))
            fcf_val = float(compute_fcf(hist, turnover_rate=None, death_turnover=fcf_death_turnover).fcf)
            if fcf_val <= fcf_buy_threshold:
                return False

        if bool(self._risk_cfg.get("emotion_enabled", False)):
            market_symbols = list(self.strategy.data.keys())
            market_score = self._calc_market_emotion_score(decision_date, market_symbols, self.strategy.data)
            market_stop = float(self._risk_cfg.get("market_emotion_stop_score", 40.0))
            stock_score = self._calc_stock_emotion_score(hist, decision_date)
            concept_score = _calc_concept_proxy_score(symbol, decision_date, self.strategy.data)
            stock_override = float(self._risk_cfg.get("stock_emotion_override_score", 75.0))
            concept_override = float(self._risk_cfg.get("concept_override_score", 0.70))
            if market_score <= market_stop and not (
                stock_score >= stock_override and concept_score >= concept_override
            ):
                return False

        if execution_date not in df.index:
            return False
        price = self._get_execution_price(df, execution_date, "buy")
        if price is None:
            return False

        available_cash = self.portfolio.cash
        max_value = min(available_cash, self.portfolio.get_total_value() * self._normalize_weight(weight))
        quantity = int(max_value / price / 100) * 100

        if quantity < 100:
            return False

        cost = quantity * price
        commission = max(cost * self.commission_rate, self.min_commission)

        if cost + commission <= self.portfolio.cash:
            self.portfolio.cash -= (cost + commission)
            self.portfolio.positions[symbol] = Position(
                symbol=symbol,
                quantity=quantity,
                cost=price,
                current_price=price,
                entry_date=execution_date,
                entry_low=float(df.loc[execution_date].get("low", price) if "low" in df.columns else price),
                highest_price=price,
                take_profit_stage=0,
            )
            self.trades.append(Trade(execution_date, symbol, "buy", price, quantity, commission, reason="signal"))
            return True
        return False

    def _sell_partial(self, symbol: str, date, sell_ratio: float, reason: str):
        """登记部分卖出订单"""
        self._queue_order(symbol, "sell_partial", date, sell_ratio=sell_ratio, reason=reason)

    def _execute_sell_partial(self, symbol: str, date, price_data: Dict[str, pd.DataFrame], sell_ratio: float, reason: str):
        """部分卖出（用于分批止盈）"""
        if symbol not in self.portfolio.positions:
            return

        pos = self.portfolio.positions[symbol]
        if pos.quantity <= 0:
            return

        df = price_data.get(symbol)
        if df is None or df.empty:
            return

        if date not in df.index:
            return
        price = self._get_execution_price(df, date, "sell")
        if price is None:
            return
        ratio = max(0.0, min(1.0, float(sell_ratio)))
        sell_qty = int(pos.quantity * ratio / 100) * 100
        if sell_qty < 100:
            return
        if sell_qty >= pos.quantity:
            self._execute_sell(symbol, date, price_data, reason=reason)
            return

        commission = max(sell_qty * price * self.commission_rate, self.min_commission)
        stamp_tax_pos = sell_qty * price * self.stamp_tax

        self.portfolio.cash -= (commission + stamp_tax_pos)
        self.portfolio.cash += sell_qty * price

        pos.quantity -= sell_qty
        self.trades.append(Trade(date, symbol, "sell", price, sell_qty, commission + stamp_tax_pos, reason=reason))
    
    def _sell(self, symbol: str, date, reason: str = "signal"):
        """登记卖出订单"""
        self._queue_order(symbol, "sell", date, reason=reason)

    def _execute_sell(self, symbol: str, date, price_data: Dict[str, pd.DataFrame], reason: str = "signal"):
        """按执行日价格完成卖出"""
        if symbol not in self.portfolio.positions:
            return
        
        pos = self.portfolio.positions[symbol]
        df = price_data.get(symbol)
        if df is None or df.empty:
            return
        if date not in df.index:
            return

        price = self._get_execution_price(df, date, "sell")
        if price is None:
            return
        
        commission = max(pos.quantity * price * self.commission_rate, self.min_commission)
        stamp_tax_pos = pos.quantity * price * self.stamp_tax
        
        self.portfolio.cash -= (commission + stamp_tax_pos)
        self.portfolio.cash += pos.quantity * price
        
        self.trades.append(Trade(date, symbol, "sell", price, pos.quantity,
                                  commission + stamp_tax_pos, reason=reason))
        del self.portfolio.positions[symbol]
    
    def _calc_result(self) -> BacktestResult:
        """计算回测结果"""
        df = pd.DataFrame(self.daily_records)
        if df.empty:
            return BacktestResult()

        metrics = _calc_basic_metrics(df, self.initial_capital)
        win_rate = _calc_win_rate(self.trades)
        benchmark_metrics = _build_benchmark_metrics(df, self._benchmark_frames, self.initial_capital)
        phase_metrics = _build_phase_metrics(df, self._benchmark_frames, self.initial_capital)
        signal_summary = _summarize_signal_journal(self.signal_journal)

        result = BacktestResult(
            trades=self.trades,
            daily_values=df,
            total_return=float(metrics["total_return"]),
            annual_return=float(metrics["annual_return"]),
            sharpe_ratio=float(metrics["sharpe_ratio"]),
            max_drawdown=float(metrics["max_drawdown"]),
            win_rate=win_rate,
            benchmark_metrics=benchmark_metrics,
            phase_metrics=phase_metrics,
            signal_summary=signal_summary,
        )
        
        logger.info(
            f"回测完成: 总收益={result.total_return:.2%}, 年化={result.annual_return:.2%}, "
            f"夏普={result.sharpe_ratio:.2f}, 最大回撤={result.max_drawdown:.2%}"
        )
        
        return result


class SelectorBacktestEngine:
    """选股+择时组合回测引擎"""
    
    def __init__(
        self,
        selector: BaseSelector,
        timing_strategy: BaseStrategy,
        initial_capital: float = None,
        commission_rate: float = None,
        stamp_tax: float = None,
        slippage: float = None,
        execution_mode: Optional[str] = None,
        risk_overrides: Optional[Dict[str, object]] = None,
        candidate_gate_threshold: Optional[float] = None,
    ):
        self.selector = selector
        self.timing_strategy = timing_strategy
        self.initial_capital = initial_capital or BACKTEST_CONFIG["initial_capital"]
        self.commission_rate = commission_rate or BACKTEST_CONFIG["commission_rate"]
        self.stamp_tax = stamp_tax or BACKTEST_CONFIG["stamp_tax"]
        self.slippage = slippage or BACKTEST_CONFIG["slippage"]
        self.min_commission = BACKTEST_CONFIG["min_commission"]
        self.execution_mode = str(execution_mode or BACKTEST_CONFIG.get("execution_mode", "next_open"))
        self.max_position = float(TRADING_CONFIG.get("max_position", 0.2))
        self.max_stocks = int(TRADING_CONFIG.get("max_stocks", 10))
        
        self.portfolio = Portfolio(cash=self.initial_capital)
        self.trades: List[Trade] = []
        self.daily_records: List[dict] = []
        self.selected_stocks: List[str] = []
        self.pending_orders: List[PendingOrder] = []
        self._trade_dates: List[datetime] = []
        self._benchmark_frames: Dict[str, dict] = {}
        self.signal_journal: List[dict] = []
        self._risk_cfg = dict(STRATEGY_CONFIG)
        if risk_overrides:
            self._risk_cfg.update(dict(risk_overrides))
        self.candidate_gate_threshold = float(candidate_gate_threshold) if candidate_gate_threshold is not None else None
    
    def run(
        self,
        pool_symbols: List[str],
        start_date: str,
        end_date: str,
        data_source,
        select_top_n: int = 10,
        rebalance_freq: int = 20,
    ) -> BacktestResult:
        """运行选股+择时回测
        
        Args:
            pool_symbols: 股票池
            start_date: 开始日期
            end_date: 结束日期
            data_source: 数据源
            select_top_n: 选股数量
            rebalance_freq: 调仓频率(天)
        """
        logger.info(f"开始选股+择时回测: {start_date} ~ {end_date}")
        
        dates = pd.date_range(start_date, end_date, freq="D")
        dates = [d for d in dates if d.weekday() < 5]
        self._trade_dates = list(dates)
        self.pending_orders = []
        self.signal_journal = []
        self._benchmark_frames = {}
        
        price_data: Dict[str, pd.DataFrame] = {}
        for symbol in pool_symbols:
            df = data_source.get_kline(symbol, start_date.replace("-", ""), 
                                        end_date.replace("-", ""))
            if df is not None and not df.empty:
                if "日期" in df.columns:
                    df = df.rename(columns={"日期": "date"})
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.set_index("date")
                price_data[symbol] = df
                self.selector.load_data(symbol, df)
                self.timing_strategy.load_data(symbol, df)

        for benchmark in DEFAULT_BENCHMARKS:
            code = str(benchmark.get("code", "")).zfill(6)
            if str(benchmark.get("kind", "")) == "index":
                df = data_source.get_index_daily(code)
                if df is not None and not df.empty:
                    df = df[
                        (df["date"] >= pd.to_datetime(start_date.replace("-", "")))
                        & (df["date"] <= pd.to_datetime(end_date.replace("-", "")))
                    ].copy()
            else:
                df = data_source.get_kline(code, start_date.replace("-", ""), end_date.replace("-", ""))
            if df is None or df.empty:
                continue
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date")
            self._benchmark_frames[code] = {
                "name": str(benchmark.get("name", code)),
                "data": df,
            }
        
        logger.info(f"成功加载 {len(price_data)} 只股票数据")
        
        next_rebalance_date = dates[0]
        
        for i, date in enumerate(dates):
            if date >= next_rebalance_date:
                select_result = self.selector.select(
                    symbols=pool_symbols,
                    start_date=start_date,
                    end_date=date.strftime("%Y%m%d"),
                    top_n=select_top_n,
                )
                
                self.selected_stocks = select_result.get_symbols()
                logger.info(f"{date.strftime('%Y-%m-%d')} 选股结果: {self.selected_stocks[:5]}...")
                
                self._rebalance(date, price_data)
                
                next_rebalance_date = dates[min(i + rebalance_freq, len(dates) - 1)]
            
            self._on_date(date, price_data)
        
        return self._calc_result()

    def _get_next_trade_date(self, date: datetime) -> Optional[datetime]:
        """
        获取下一个交易日。
        """
        for trade_date in self._trade_dates:
            if trade_date > date:
                return trade_date
        return None

    def _get_execution_price(self, df: pd.DataFrame, date: datetime, direction: str) -> Optional[float]:
        """
        获取执行价格。
        """
        if df is None or df.empty or date not in df.index:
            return None

        row = df.loc[date]
        if self.execution_mode == "next_open":
            raw_price = row.get("open", row.get("close"))
        else:
            raw_price = row.get("close")

        try:
            price = float(raw_price)
        except Exception:
            return None

        if price <= 0:
            return None
        if direction == "buy":
            return price * (1 + self.slippage)
        return price * (1 - self.slippage)

    def _normalize_weight(self, weight: float) -> float:
        """
        收敛到单票上限。
        """
        try:
            target_weight = float(weight or 0.0)
        except Exception:
            target_weight = 0.0
        if target_weight <= 0:
            target_weight = self.max_position
        return float(np.clip(target_weight, 0.0, self.max_position))

    def _queue_order(
        self,
        symbol: str,
        action: str,
        decision_date: datetime,
        weight: float = 0.0,
        sell_ratio: float = 0.0,
        reason: str = "",
    ) -> None:
        """
        登记待执行订单。
        """
        if action == "buy" and symbol in self.portfolio.positions:
            return

        if self.execution_mode == "next_open":
            execution_date = self._get_next_trade_date(decision_date)
            if execution_date is None:
                return
        else:
            execution_date = decision_date

        if action == "buy":
            for item in self.pending_orders:
                if item.symbol == symbol and item.action == "buy":
                    return
        elif action == "sell":
            self.pending_orders = [
                item for item in self.pending_orders
                if not (item.symbol == symbol and item.action in {"buy", "sell", "sell_partial"})
            ]
        elif action == "sell_partial":
            for item in self.pending_orders:
                if item.symbol == symbol and item.action in {"sell", "sell_partial"}:
                    return

        self.pending_orders.append(
            PendingOrder(
                symbol=symbol,
                action=action,
                decision_date=decision_date,
                execution_date=execution_date,
                weight=weight,
                sell_ratio=sell_ratio,
                reason=reason,
            )
        )

    def _execute_pending_orders(self, date: datetime, price_data: Dict[str, pd.DataFrame]) -> None:
        """
        执行当日待执行订单。
        """
        if not self.pending_orders:
            return

        action_priority = {"sell": 0, "sell_partial": 1, "buy": 2}
        remaining: List[PendingOrder] = []
        for order in sorted(self.pending_orders, key=lambda item: (item.execution_date, action_priority.get(item.action, 9))):
            if order.execution_date > date:
                remaining.append(order)
                continue

            df = price_data.get(order.symbol)
            if df is None or df.empty or date not in df.index:
                remaining.append(order)
                continue

            if order.action == "buy":
                self._execute_buy(order.symbol, order.decision_date, date, order.weight, price_data)
                continue
            if order.action == "sell":
                self._execute_sell(order.symbol, date, price_data, reason=order.reason)
                continue
            if order.action == "sell_partial":
                self._execute_sell_partial(order.symbol, date, price_data, sell_ratio=order.sell_ratio, reason=order.reason)
                continue

            remaining.append(order)

        self.pending_orders = remaining

    def _held_trading_days(self, entry_date: Optional[datetime], current_date: datetime) -> int:
        """计算持仓交易日天数（用于T+1/时间止损）"""
        if entry_date is None:
            return 0
        if current_date <= entry_date:
            return 0
        try:
            return max(len(pd.bdate_range(entry_date, current_date)) - 1, 0)
        except Exception:
            return (current_date.date() - entry_date.date()).days

    def _calc_market_emotion_score(self, date: datetime, price_data: Dict[str, pd.DataFrame]) -> float:
        """
        计算大盘情绪分（回测简化版，0-100）
        以当前股票池的广度近似。
        """
        rets: List[float] = []
        down_big = 0
        total = 0

        for symbol, df in price_data.items():
            if df is None or df.empty or date not in df.index:
                continue

            total += 1
            try:
                if "pct_change" in df.columns and not pd.isna(df.loc[date, "pct_change"]):
                    r = float(df.loc[date, "pct_change"]) / 100.0
                else:
                    loc = df.index.get_loc(date)
                    if isinstance(loc, slice) or loc == 0:
                        continue
                    prev_close = float(df.iloc[loc - 1]["close"])
                    close = float(df.loc[date, "close"])
                    r = (close - prev_close) / prev_close if prev_close > 0 else 0.0
                rets.append(r)
                if r <= -0.095:
                    down_big += 1
            except Exception:
                continue

        if total <= 0 or not rets:
            return 50.0

        avg_ret = float(np.mean(rets))
        up_ratio = float(np.mean([1.0 if r > 0 else 0.0 for r in rets]))
        down_big_ratio = down_big / total

        score = 50.0
        score += float(np.clip(avg_ret / 0.02, -1, 1) * 20.0)
        score += float(np.clip((up_ratio - 0.5) / 0.25, -1, 1) * 20.0)
        score -= float(np.clip(down_big_ratio / 0.05, 0, 1) * 25.0)
        return float(np.clip(score, 0.0, 100.0))

    def _calc_stock_emotion_score(self, df: pd.DataFrame, date: datetime) -> float:
        """
        计算个股强势/抱团分（回测简化版，0-100）
        """
        if df is None or df.empty or date not in df.index:
            return 50.0

        try:
            hist = df[df.index <= date].tail(80).copy()
            if hist.empty:
                return 50.0
            latest = hist.iloc[-1]

            close = float(latest.get("close", 0.0))
            high = float(latest.get("high", close))
            low = float(latest.get("low", close))
            volume = float(latest.get("volume", 0.0))

            if "pct_change" in hist.columns and not pd.isna(latest.get("pct_change", np.nan)):
                ret = float(latest.get("pct_change", 0.0)) / 100.0
            else:
                if len(hist) < 2:
                    ret = 0.0
                else:
                    prev_close = float(hist.iloc[-2].get("close", close))
                    ret = (close - prev_close) / prev_close if prev_close > 0 else 0.0

            vol_ma20 = float(hist["volume"].tail(20).mean()) if "volume" in hist.columns and len(hist) >= 20 else 0.0
            vol_ratio = (volume / vol_ma20) if vol_ma20 > 0 else 1.0

            rng = max(high - low, 1e-6)
            close_strength = (close - low) / rng

            ma20 = float(hist["close"].tail(20).mean()) if "close" in hist.columns and len(hist) >= 20 else close
            trend_bonus = 10.0 if close >= ma20 else -10.0

            score = 50.0
            score += float(np.clip(ret / 0.05, -1, 1) * 30.0)
            score += float(np.clip((vol_ratio - 1.0) / 1.0, -1, 1) * 15.0)
            score += float(np.clip((close_strength - 0.5) / 0.5, -1, 1) * 20.0)
            score += trend_bonus
            return float(np.clip(score, 0.0, 100.0))
        except Exception:
            return 50.0

    def _check_risk_exits(self, date: datetime, price_data: Dict[str, pd.DataFrame]):
        """风控退出（止损/止盈/跟踪止盈/时间止损/情绪退出）"""
        stop_loss = float(self._risk_cfg.get("stop_loss", -0.07))
        take_profit = float(self._risk_cfg.get("take_profit", 0.15))
        trailing_stop = float(self._risk_cfg.get("trailing_stop", 0.0))
        max_hold_days = int(self._risk_cfg.get("max_hold_days", 0))
        time_stop_days = int(self._risk_cfg.get("time_stop_days", 0))
        time_stop_min_return = float(self._risk_cfg.get("time_stop_min_return", 0.0))
        min_hold_days_before_sell = int(self._risk_cfg.get("min_hold_days_before_sell", 0))
        emotion_enabled = bool(self._risk_cfg.get("emotion_enabled", False))
        market_emotion_stop_score = float(self._risk_cfg.get("market_emotion_stop_score", 0.0))
        stock_emotion_override_score = float(self._risk_cfg.get("stock_emotion_override_score", 100.0))
        concept_override_score = float(self._risk_cfg.get("concept_override_score", 1.0))
        override_trailing_stop = float(self._risk_cfg.get("override_trailing_stop", trailing_stop))
        scale_out_enabled = bool(self._risk_cfg.get("scale_out_enabled", False))
        scale_out_levels = self._risk_cfg.get("scale_out_levels", [0.10, 0.20])
        scale_out_ratios = self._risk_cfg.get("scale_out_ratios", [0.50, 1.00])
        entry_low_stop_enabled = bool(self._risk_cfg.get("entry_low_stop_enabled", False))
        entry_low_stop_buffer = float(self._risk_cfg.get("entry_low_stop_buffer", 0.0))
        limit_up_close_strength_sell_all = float(self._risk_cfg.get("limit_up_close_strength_sell_all", 0.10))
        limit_up_close_strength_sell_half = float(self._risk_cfg.get("limit_up_close_strength_sell_half", 0.20))
        fcf_enabled = bool(self._risk_cfg.get("fcf_enabled", False))
        fcf_sell_threshold = float(self._risk_cfg.get("fcf_sell_threshold", 0.0))
        fcf_down_days = int(self._risk_cfg.get("fcf_down_days", 2))
        fcf_death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))

        market_score = self._calc_market_emotion_score(date, price_data) if emotion_enabled else 50.0

        for symbol, pos in list(self.portfolio.positions.items()):
            if symbol not in price_data:
                continue
            df = price_data[symbol]
            if date not in df.index:
                continue

            pos.current_price = float(df.loc[date, "close"])
            if pos.highest_price <= 0:
                pos.highest_price = float(pos.current_price)
            else:
                pos.highest_price = max(float(pos.highest_price), float(pos.current_price))

            if pos.cost <= 0:
                continue

            held_days = self._held_trading_days(pos.entry_date, date)
            if held_days < min_hold_days_before_sell:
                continue

            pnl_pct = (pos.current_price - pos.cost) / pos.cost
            stock_score = self._calc_stock_emotion_score(df, date) if emotion_enabled else 50.0
            concept_score = _calc_concept_proxy_score(symbol, date, price_data) if emotion_enabled else 0.0
            override_hold = (
                emotion_enabled
                and stock_score >= stock_emotion_override_score
                and concept_score >= concept_override_score
            )
            trailing_stop_eff = override_trailing_stop if override_hold else trailing_stop

            if entry_low_stop_enabled and pos.entry_low > 0:
                if pos.current_price <= pos.entry_low * (1 - entry_low_stop_buffer):
                    self._sell(symbol, date, price_data, reason="break_entry_low")
                    continue

            if fcf_enabled:
                hist = df[df.index <= date]
                if hist is not None and not hist.empty and len(hist) >= 20:
                    fcf_val = float(compute_fcf(hist, turnover_rate=None, death_turnover=fcf_death_turnover).fcf)
                    if fcf_val < fcf_sell_threshold:
                        self._sell(symbol, date, price_data, reason="fcf_negative")
                        continue
                    if fcf_down_days >= 2:
                        fcf_series = compute_recent_fcf_series(
                            hist,
                            lookback_days=fcf_down_days + 1,
                            turnover_rate=None,
                            death_turnover=fcf_death_turnover,
                        )
                        if len(fcf_series) >= fcf_down_days + 1:
                            is_down = all(
                                fcf_series[idx] < fcf_series[idx - 1]
                                for idx in range(1, len(fcf_series))
                            )
                            if is_down:
                                self._sell(symbol, date, price_data, reason="fcf_down")
                                continue

            if pnl_pct <= stop_loss:
                self._sell(symbol, date, price_data, reason="stop_loss")
                continue

            if emotion_enabled and (market_score <= market_emotion_stop_score) and (not override_hold):
                self._sell(symbol, date, price_data, reason="emotion_stop")
                continue

            try:
                row = df.loc[date]
                day_ret = None
                if "pct_change" in df.columns and not pd.isna(row.get("pct_change", np.nan)):
                    day_ret = float(row.get("pct_change", 0.0)) / 100.0
                if day_ret is None:
                    loc = df.index.get_loc(date)
                    if not isinstance(loc, slice) and loc > 0:
                        prev_close = float(df.iloc[loc - 1]["close"])
                        close = float(row.get("close", pos.current_price))
                        day_ret = (close - prev_close) / prev_close if prev_close > 0 else 0.0
                    else:
                        day_ret = 0.0

                if day_ret is not None and day_ret >= 0.095:
                    high = float(row.get("high", pos.current_price))
                    low = float(row.get("low", pos.current_price))
                    close = float(row.get("close", pos.current_price))
                    rng = max(high - low, 1e-6)
                    close_strength = (close - low) / rng

                    if close_strength < limit_up_close_strength_sell_all:
                        self._sell(symbol, date, price_data, reason="limit_up_weak_seal_all")
                        continue
                    if close_strength < limit_up_close_strength_sell_half:
                        self._sell_partial(symbol, date, price_data, sell_ratio=0.5, reason="limit_up_weak_seal_half")
                        continue
            except Exception:
                pass

            if override_hold and scale_out_enabled:
                try:
                    lv1 = float(scale_out_levels[0]) if len(scale_out_levels) > 0 else take_profit
                    lv2 = float(scale_out_levels[1]) if len(scale_out_levels) > 1 else max(take_profit, lv1 + 0.05)
                    r1 = float(scale_out_ratios[0]) if len(scale_out_ratios) > 0 else 0.5
                except Exception:
                    lv1, lv2, r1 = 0.10, 0.20, 0.5

                if pos.take_profit_stage <= 0 and pnl_pct >= lv1:
                    self._sell_partial(symbol, date, price_data, sell_ratio=r1, reason="scale_out_1")
                    if symbol in self.portfolio.positions:
                        self.portfolio.positions[symbol].take_profit_stage = 1
                    continue
                if pos.take_profit_stage >= 1 and pnl_pct >= lv2:
                    self._sell(symbol, date, price_data, reason="scale_out_2")
                    continue
            else:
                if take_profit > 0 and pnl_pct >= take_profit:
                    self._sell(symbol, date, price_data, reason="take_profit")
                    continue

            if trailing_stop_eff > 0 and pos.highest_price > 0:
                if pos.current_price <= pos.highest_price * (1 - trailing_stop_eff):
                    self._sell(symbol, date, price_data, reason="trailing_stop")
                    continue

            if (not override_hold) and max_hold_days > 0 and held_days >= max_hold_days:
                self._sell(symbol, date, price_data, reason="max_hold_days")
                continue

            if (not override_hold) and time_stop_days > 0 and held_days >= time_stop_days and pnl_pct <= time_stop_min_return:
                self._sell(symbol, date, price_data, reason="time_stop")
                continue

    def _sell_partial(self, symbol: str, date, price_data: Dict[str, pd.DataFrame], sell_ratio: float, reason: str):
        """登记部分卖出订单"""
        self._queue_order(symbol, "sell_partial", date, sell_ratio=sell_ratio, reason=reason)

    def _execute_sell_partial(self, symbol: str, date, price_data: Dict[str, pd.DataFrame], sell_ratio: float, reason: str):
        """部分卖出（用于分批止盈）"""
        if symbol not in self.portfolio.positions:
            return
        if symbol not in price_data:
            return
        df = price_data[symbol]
        if df is None or df.empty or date not in df.index:
            return

        pos = self.portfolio.positions[symbol]
        if pos.quantity <= 0:
            return

        price = self._get_execution_price(df, date, "sell")
        if price is None:
            return
        ratio = max(0.0, min(1.0, float(sell_ratio)))
        sell_qty = int(pos.quantity * ratio / 100) * 100
        if sell_qty < 100:
            return
        if sell_qty >= pos.quantity:
            self._execute_sell(symbol, date, price_data, reason=reason)
            return

        commission = max(sell_qty * price * self.commission_rate, self.min_commission)
        stamp_tax_pos = sell_qty * price * self.stamp_tax

        self.portfolio.cash -= (commission + stamp_tax_pos)
        self.portfolio.cash += sell_qty * price

        pos.quantity -= sell_qty
        self.trades.append(Trade(date, symbol, "sell", price, sell_qty, commission + stamp_tax_pos, reason=reason))
    
    def _rebalance(self, date, price_data: Dict[str, pd.DataFrame]):
        """调仓"""
        for symbol in list(self.portfolio.positions.keys()):
            if symbol not in self.selected_stocks:
                self._sell(symbol, date, price_data, reason="rebalance")
        
        target_count = min(len(self.selected_stocks), self.max_stocks)
        if target_count == 0:
            return

        for symbol in self.selected_stocks[:target_count]:
            if symbol in self.portfolio.positions:
                continue
            self._queue_order(symbol, "buy", date, weight=self.max_position, reason="rebalance")
    
    def _on_date(self, date, price_data: Dict[str, pd.DataFrame]):
        """每日处理"""
        self._execute_pending_orders(date, price_data)

        for symbol in list(self.portfolio.positions.keys()):
            if symbol not in price_data:
                continue
            df = price_data[symbol]
            if date not in df.index:
                continue
            
            current_price = df.loc[date, "close"]
            pos = self.portfolio.positions[symbol]
            pos.current_price = float(current_price)
            if pos.highest_price <= 0:
                pos.highest_price = float(pos.current_price)
            else:
                pos.highest_price = max(float(pos.highest_price), float(pos.current_price))

        context_market_score = (
            self._calc_market_emotion_score(date, price_data)
            if bool(self._risk_cfg.get("emotion_enabled", False))
            else 50.0
        )
        self.timing_strategy.set_market_context(
            _build_market_context_payload(date, price_data, self._benchmark_frames, context_market_score)
        )

        # 风控退出优先
        self._check_risk_exits(date, price_data)
        
        signals = []
        for symbol in self.selected_stocks:
            if symbol not in price_data:
                continue
            df = price_data[symbol][price_data[symbol].index <= date]
            if len(df) < 5:
                continue
            signal = self.timing_strategy.on_bar(symbol, df)
            if signal:
                signal.date = date
                effective_gate_passed = bool(signal.gate_passed)
                gate_reason = str(signal.gate_reason or "")
                if (
                    signal.signal != 0
                    and self.candidate_gate_threshold is not None
                    and float(signal.candidate_score or 0.0) < float(self.candidate_gate_threshold)
                ):
                    effective_gate_passed = False
                    threshold_reason = (
                        f"candidate_score={float(signal.candidate_score or 0.0):.2f} "
                        f"< threshold={float(self.candidate_gate_threshold):.2f}"
                    )
                    gate_reason = f"{gate_reason}; {threshold_reason}".strip("; ")
                signals.append(signal)
                self.signal_journal.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "symbol": signal.symbol,
                    "raw_signal": float(signal.signal),
                    "final_signal": float(signal.signal if effective_gate_passed else 0.0),
                    "weight": float(signal.weight or 0.0),
                    "candidate_score": float(signal.candidate_score or 0.0),
                    "gate_passed": effective_gate_passed,
                    "gate_reason": gate_reason,
                })
        
        for signal_item, journal_item in zip(signals, self.signal_journal[-len(signals):] if signals else []):
            final_signal = float(journal_item.get("final_signal", 0.0))
            if final_signal > 0:
                self._buy(signal_item.symbol, date, signal_item.weight, price_data)
            elif final_signal < 0:
                sell_weight = float(signal_item.weight or 1.0)
                if sell_weight >= 0.95:
                    self._sell(signal_item.symbol, date, price_data, reason="signal")
                else:
                    self._sell_partial(
                        signal_item.symbol,
                        date,
                        price_data,
                        sell_ratio=max(0.3, min(sell_weight, 0.9)),
                        reason="signal_partial",
                    )
        
        total_value = self.portfolio.get_total_value()
        self.daily_records.append({
            "date": date,
            "cash": self.portfolio.cash,
            "position_value": self.portfolio.get_position_value(),
            "total_value": total_value,
            "selected_count": len(self.selected_stocks),
        })
    
    def _buy(self, symbol: str, date, weight: float, price_data: Dict[str, pd.DataFrame]):
        """登记买入订单"""
        self._queue_order(symbol, "buy", date, weight=weight, reason="signal")

    def _execute_buy(self, symbol: str, decision_date: datetime, execution_date: datetime, weight: float, price_data: Dict[str, pd.DataFrame]):
        """按执行日价格完成买入"""
        if symbol in self.portfolio.positions:
            return
        if len(self.portfolio.positions) >= self.max_stocks:
            return
        
        if symbol not in price_data:
            return
        
        df = price_data[symbol]
        if execution_date not in df.index:
            return

        hist = df[df.index <= decision_date]
        if hist is None or hist.empty:
            return

        if bool(self._risk_cfg.get("fcf_enabled", False)) and len(hist) >= 20:
            fcf_buy_threshold = float(self._risk_cfg.get("fcf_buy_threshold", 0.0))
            fcf_death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))
            fcf_val = float(compute_fcf(hist, turnover_rate=None, death_turnover=fcf_death_turnover).fcf)
            if fcf_val <= fcf_buy_threshold:
                return

        if bool(self._risk_cfg.get("emotion_enabled", False)):
            market_score = self._calc_market_emotion_score(decision_date, price_data)
            market_stop = float(self._risk_cfg.get("market_emotion_stop_score", 40.0))
            stock_score = self._calc_stock_emotion_score(hist, decision_date)
            concept_score = _calc_concept_proxy_score(symbol, decision_date, price_data)
            stock_override = float(self._risk_cfg.get("stock_emotion_override_score", 75.0))
            concept_override = float(self._risk_cfg.get("concept_override_score", 0.70))
            if market_score <= market_stop and not (
                stock_score >= stock_override and concept_score >= concept_override
            ):
                return

        price = self._get_execution_price(df, execution_date, "buy")
        if price is None:
            return

        available_cash = self.portfolio.cash
        max_value = min(available_cash, self.portfolio.get_total_value() * self._normalize_weight(weight))
        quantity = int(max_value / price / 100) * 100
        
        if quantity < 100:
            return
        
        cost = quantity * price
        commission = max(cost * self.commission_rate, self.min_commission)
        
        if cost + commission <= self.portfolio.cash:
            self.portfolio.cash -= (cost + commission)
            self.portfolio.positions[symbol] = Position(
                symbol=symbol,
                quantity=quantity,
                cost=price,
                current_price=price,
                entry_date=execution_date,
                entry_low=float(df.loc[execution_date].get("low", price) if "low" in df.columns else price),
                highest_price=price,
                take_profit_stage=0,
            )
            self.trades.append(Trade(execution_date, symbol, "buy", price, quantity, commission, reason="signal"))
    
    def _sell(self, symbol: str, date, price_data: Dict[str, pd.DataFrame], reason: str = "signal"):
        """登记卖出订单"""
        self._queue_order(symbol, "sell", date, reason=reason)

    def _execute_sell(self, symbol: str, date, price_data: Dict[str, pd.DataFrame], reason: str = "signal"):
        """按执行日价格完成卖出"""
        if symbol not in self.portfolio.positions:
            return
        
        if symbol not in price_data:
            return
        
        df = price_data[symbol]
        if date not in df.index:
            return
        
        pos = self.portfolio.positions[symbol]
        price = self._get_execution_price(df, date, "sell")
        if price is None:
            return
        
        commission = max(pos.quantity * price * self.commission_rate, self.min_commission)
        stamp_tax_pos = pos.quantity * price * self.stamp_tax
        
        self.portfolio.cash -= (commission + stamp_tax_pos)
        self.portfolio.cash += pos.quantity * price
        
        self.trades.append(Trade(date, symbol, "sell", price, pos.quantity,
                                  commission + stamp_tax_pos, reason=reason))
        del self.portfolio.positions[symbol]
    
    def _calc_result(self) -> BacktestResult:
        """计算回测结果"""
        df = pd.DataFrame(self.daily_records)
        if df.empty:
            return BacktestResult()

        metrics = _calc_basic_metrics(df, self.initial_capital)
        win_rate = _calc_win_rate(self.trades)
        benchmark_metrics = _build_benchmark_metrics(df, self._benchmark_frames, self.initial_capital)
        phase_metrics = _build_phase_metrics(df, self._benchmark_frames, self.initial_capital)
        signal_summary = _summarize_signal_journal(self.signal_journal)

        result = BacktestResult(
            trades=self.trades,
            daily_values=df,
            total_return=float(metrics["total_return"]),
            annual_return=float(metrics["annual_return"]),
            sharpe_ratio=float(metrics["sharpe_ratio"]),
            max_drawdown=float(metrics["max_drawdown"]),
            win_rate=win_rate,
            benchmark_metrics=benchmark_metrics,
            phase_metrics=phase_metrics,
            signal_summary=signal_summary,
        )
        
        logger.info(
            f"选股+择时回测完成: 总收益={result.total_return:.2%}, 年化={result.annual_return:.2%}, "
            f"夏普={result.sharpe_ratio:.2f}, 最大回撤={result.max_drawdown:.2%}"
        )
        
        return result
