# -*- coding: utf-8 -*-
"""
回测引擎
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from config.config import BACKTEST_CONFIG, STRATEGY_CONFIG
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


class BacktestEngine:
    """回测引擎"""
    
    def __init__(
        self,
        strategy: BaseStrategy,
        initial_capital: float = None,
        commission_rate: float = None,
        stamp_tax: float = None,
        slippage: float = None,
    ):
        self.strategy = strategy
        self.initial_capital = initial_capital or BACKTEST_CONFIG["initial_capital"]
        self.commission_rate = commission_rate or BACKTEST_CONFIG["commission_rate"]
        self.stamp_tax = stamp_tax or BACKTEST_CONFIG["stamp_tax"]
        self.slippage = slippage or BACKTEST_CONFIG["slippage"]
        self.min_commission = BACKTEST_CONFIG["min_commission"]
        
        self.portfolio = Portfolio(cash=self.initial_capital)
        self.trades: List[Trade] = []
        self.daily_records: List[dict] = []
        self._risk_cfg = STRATEGY_CONFIG
    
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
        
        for date in dates:
            self._on_date(date, symbols, price_data)
        
        return self._calc_result()

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

        # 风控退出优先：短线策略先活下来，再谈信号
        self._check_risk_exits(date, symbols, price_data)
        
        signals = []
        for symbol in symbols:
            if symbol in price_data:
                df = price_data[symbol][price_data[symbol].index <= date]
                signal = self.strategy.on_bar(symbol, df)
                if signal:
                    signal.date = date
                    signals.append(signal)
        
        for signal in signals:
            if signal.signal > 0:
                self._buy(signal.symbol, date, signal.weight)
            elif signal.signal < 0:
                self._sell(signal.symbol, date)
        
        total_value = self.portfolio.get_total_value()
        self.daily_records.append({
            "date": date,
            "cash": self.portfolio.cash,
            "position_value": self.portfolio.get_position_value(),
            "total_value": total_value,
        })
    
    def _buy(self, symbol: str, date, weight: float = 1.0):
        """买入"""
        if symbol in self.portfolio.positions:
            return
        
        df = self.strategy.get_data(symbol)
        if df is None or df.empty:
            return

        hist = df[df.index <= date] if date in df.index else df
        if hist is None or hist.empty:
            return

        if bool(self._risk_cfg.get("fcf_enabled", False)) and len(hist) >= 20:
            fcf_buy_threshold = float(self._risk_cfg.get("fcf_buy_threshold", 0.0))
            fcf_death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))
            fcf_val = float(compute_fcf(hist, turnover_rate=None, death_turnover=fcf_death_turnover).fcf)
            if fcf_val <= fcf_buy_threshold:
                return

        if bool(self._risk_cfg.get("emotion_enabled", False)):
            market_symbols = list(self.strategy.data.keys())
            market_score = self._calc_market_emotion_score(date, market_symbols, self.strategy.data)
            market_stop = float(self._risk_cfg.get("market_emotion_stop_score", 40.0))
            stock_score = self._calc_stock_emotion_score(hist, date)
            concept_score = _calc_concept_proxy_score(symbol, date, self.strategy.data)
            stock_override = float(self._risk_cfg.get("stock_emotion_override_score", 75.0))
            concept_override = float(self._risk_cfg.get("concept_override_score", 0.70))
            if market_score <= market_stop and not (
                stock_score >= stock_override and concept_score >= concept_override
            ):
                return
        
        price = hist.iloc[-1]["close"] * (1 + self.slippage)
        
        available_cash = self.portfolio.cash
        max_value = available_cash * weight
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
                entry_date=date,
                entry_low=float(df.iloc[-1].get("low", price) if "low" in df.columns else price),
                highest_price=price,
                take_profit_stage=0,
            )
            self.trades.append(Trade(date, symbol, "buy", price, quantity, commission, reason="signal"))

    def _sell_partial(self, symbol: str, date, sell_ratio: float, reason: str):
        """部分卖出（用于分批止盈）"""
        if symbol not in self.portfolio.positions:
            return

        pos = self.portfolio.positions[symbol]
        if pos.quantity <= 0:
            return

        df = self.strategy.get_data(symbol)
        if df is None or df.empty:
            return

        price = float(df.iloc[-1]["close"]) * (1 - self.slippage)
        ratio = max(0.0, min(1.0, float(sell_ratio)))
        sell_qty = int(pos.quantity * ratio / 100) * 100
        if sell_qty < 100:
            return
        if sell_qty >= pos.quantity:
            self._sell(symbol, date, reason=reason)
            return

        commission = max(sell_qty * price * self.commission_rate, self.min_commission)
        stamp_tax_pos = sell_qty * price * self.stamp_tax

        self.portfolio.cash -= (commission + stamp_tax_pos)
        self.portfolio.cash += sell_qty * price

        pos.quantity -= sell_qty
        self.trades.append(Trade(date, symbol, "sell", price, sell_qty, commission + stamp_tax_pos, reason=reason))
    
    def _sell(self, symbol: str, date, reason: str = "signal"):
        """卖出"""
        if symbol not in self.portfolio.positions:
            return
        
        pos = self.portfolio.positions[symbol]
        df = self.strategy.get_data(symbol)
        if df is None or df.empty:
            return
        
        price = df.iloc[-1]["close"] * (1 - self.slippage)
        
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
        
        df["return"] = df["total_value"].pct_change()
        
        total_return = (df["total_value"].iloc[-1] / self.initial_capital) - 1
        
        days = (df["date"].iloc[-1] - df["date"].iloc[0]).days
        annual_return = (1 + total_return) ** (365 / max(days, 1)) - 1
        
        sharpe_ratio = df["return"].mean() / df["return"].std() * np.sqrt(252) if df["return"].std() > 0 else 0
        
        cummax = df["total_value"].cummax()
        drawdown = (df["total_value"] - cummax) / cummax
        max_drawdown = drawdown.min()
        
        buy_trades = [t for t in self.trades if t.direction == "buy"]
        sell_trades = [t for t in self.trades if t.direction == "sell"]
        win_trades = 0
        total_round_trips = 0
        open_buys: Dict[str, List[Trade]] = {}
        for t in self.trades:
            if t.direction == "buy":
                open_buys.setdefault(t.symbol, []).append(t)
            elif t.direction == "sell":
                queue = open_buys.get(t.symbol, [])
                if not queue:
                    continue
                buy_t = queue.pop(0)
                total_round_trips += 1
                if t.price > buy_t.price:
                    win_trades += 1
        win_rate = win_trades / total_round_trips if total_round_trips > 0 else 0
        
        result = BacktestResult(
            trades=self.trades,
            daily_values=df,
            total_return=total_return,
            annual_return=annual_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate
        )
        
        logger.info(f"回测完成: 总收益={total_return:.2%}, 年化={annual_return:.2%}, "
                   f"夏普={sharpe_ratio:.2f}, 最大回撤={max_drawdown:.2%}")
        
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
    ):
        self.selector = selector
        self.timing_strategy = timing_strategy
        self.initial_capital = initial_capital or BACKTEST_CONFIG["initial_capital"]
        self.commission_rate = commission_rate or BACKTEST_CONFIG["commission_rate"]
        self.stamp_tax = stamp_tax or BACKTEST_CONFIG["stamp_tax"]
        self.slippage = slippage or BACKTEST_CONFIG["slippage"]
        self.min_commission = BACKTEST_CONFIG["min_commission"]
        
        self.portfolio = Portfolio(cash=self.initial_capital)
        self.trades: List[Trade] = []
        self.daily_records: List[dict] = []
        self.selected_stocks: List[str] = []
        self._risk_cfg = STRATEGY_CONFIG
    
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
        
        logger.info(f"成功加载 {len(price_data)} 只股票数据")
        
        next_rebalance_date = dates[0]
        
        for i, date in enumerate(dates):
            if date >= next_rebalance_date:
                select_end_date = dates[min(i + 5, len(dates) - 1)]
                select_result = self.selector.select(
                    symbols=pool_symbols,
                    start_date=start_date,
                    end_date=select_end_date.strftime("%Y%m%d"),
                    top_n=select_top_n,
                )
                
                self.selected_stocks = select_result.get_symbols()
                logger.info(f"{date.strftime('%Y-%m-%d')} 选股结果: {self.selected_stocks[:5]}...")
                
                self._rebalance(date, price_data)
                
                next_rebalance_date = dates[min(i + rebalance_freq, len(dates) - 1)]
            
            self._on_date(date, price_data)
        
        return self._calc_result()

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

        price = float(df.loc[date, "close"]) * (1 - self.slippage)
        ratio = max(0.0, min(1.0, float(sell_ratio)))
        sell_qty = int(pos.quantity * ratio / 100) * 100
        if sell_qty < 100:
            return
        if sell_qty >= pos.quantity:
            self._sell(symbol, date, price_data, reason=reason)
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
        
        target_count = len(self.selected_stocks)
        if target_count == 0:
            return
        
        per_stock_value = self.portfolio.cash / target_count
        
        for symbol in self.selected_stocks:
            if symbol in self.portfolio.positions:
                continue
            
            if symbol not in price_data:
                continue
            
            df = price_data[symbol]
            if date not in df.index:
                continue

            hist = df[df.index <= date]
            if hist is None or hist.empty:
                continue

            if bool(self._risk_cfg.get("fcf_enabled", False)) and len(hist) >= 20:
                fcf_buy_threshold = float(self._risk_cfg.get("fcf_buy_threshold", 0.0))
                fcf_death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))
                fcf_val = float(compute_fcf(hist, turnover_rate=None, death_turnover=fcf_death_turnover).fcf)
                if fcf_val <= fcf_buy_threshold:
                    continue

            if bool(self._risk_cfg.get("emotion_enabled", False)):
                market_score = self._calc_market_emotion_score(date, price_data)
                market_stop = float(self._risk_cfg.get("market_emotion_stop_score", 40.0))
                stock_score = self._calc_stock_emotion_score(hist, date)
                concept_score = _calc_concept_proxy_score(symbol, date, price_data)
                stock_override = float(self._risk_cfg.get("stock_emotion_override_score", 75.0))
                concept_override = float(self._risk_cfg.get("concept_override_score", 0.70))
                if market_score <= market_stop and not (
                    stock_score >= stock_override and concept_score >= concept_override
                ):
                    continue
            
            price = df.loc[date, "close"] * (1 + self.slippage)
            quantity = int(per_stock_value / price / 100) * 100
            
            if quantity < 100:
                continue
            
            cost = quantity * price
            commission = max(cost * self.commission_rate, self.min_commission)
            
            if cost + commission <= self.portfolio.cash:
                self.portfolio.cash -= (cost + commission)
                self.portfolio.positions[symbol] = Position(
                    symbol=symbol,
                    quantity=quantity,
                    cost=price,
                    current_price=price,
                    entry_date=date,
                    entry_low=float(df.loc[date].get("low", price) if "low" in df.columns else price),
                    highest_price=price,
                    take_profit_stage=0,
                )
                self.trades.append(Trade(date, symbol, "buy", price, quantity, commission, reason="rebalance"))
    
    def _on_date(self, date, price_data: Dict[str, pd.DataFrame]):
        """每日处理"""
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
                signals.append(signal)
        
        for signal in signals:
            if signal.signal > 0:
                self._buy(signal.symbol, date, signal.weight, price_data)
            elif signal.signal < 0:
                self._sell(signal.symbol, date, price_data, reason="signal")
        
        total_value = self.portfolio.get_total_value()
        self.daily_records.append({
            "date": date,
            "cash": self.portfolio.cash,
            "position_value": self.portfolio.get_position_value(),
            "total_value": total_value,
            "selected_count": len(self.selected_stocks),
        })
    
    def _buy(self, symbol: str, date, weight: float, price_data: Dict[str, pd.DataFrame]):
        """买入"""
        if symbol in self.portfolio.positions:
            return
        
        if symbol not in price_data:
            return
        
        df = price_data[symbol]
        if date not in df.index:
            return

        hist = df[df.index <= date]
        if hist is None or hist.empty:
            return

        if bool(self._risk_cfg.get("fcf_enabled", False)) and len(hist) >= 20:
            fcf_buy_threshold = float(self._risk_cfg.get("fcf_buy_threshold", 0.0))
            fcf_death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))
            fcf_val = float(compute_fcf(hist, turnover_rate=None, death_turnover=fcf_death_turnover).fcf)
            if fcf_val <= fcf_buy_threshold:
                return

        if bool(self._risk_cfg.get("emotion_enabled", False)):
            market_score = self._calc_market_emotion_score(date, price_data)
            market_stop = float(self._risk_cfg.get("market_emotion_stop_score", 40.0))
            stock_score = self._calc_stock_emotion_score(hist, date)
            concept_score = _calc_concept_proxy_score(symbol, date, price_data)
            stock_override = float(self._risk_cfg.get("stock_emotion_override_score", 75.0))
            concept_override = float(self._risk_cfg.get("concept_override_score", 0.70))
            if market_score <= market_stop and not (
                stock_score >= stock_override and concept_score >= concept_override
            ):
                return
        
        price = hist.iloc[-1]["close"] * (1 + self.slippage)
        
        available_cash = self.portfolio.cash
        max_value = available_cash * weight
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
                entry_date=date,
                entry_low=float(df.loc[date].get("low", price) if "low" in df.columns else price),
                highest_price=price,
                take_profit_stage=0,
            )
            self.trades.append(Trade(date, symbol, "buy", price, quantity, commission, reason="signal"))
    
    def _sell(self, symbol: str, date, price_data: Dict[str, pd.DataFrame], reason: str = "signal"):
        """卖出"""
        if symbol not in self.portfolio.positions:
            return
        
        if symbol not in price_data:
            return
        
        df = price_data[symbol]
        if date not in df.index:
            return
        
        pos = self.portfolio.positions[symbol]
        price = df.loc[date, "close"] * (1 - self.slippage)
        
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
        
        df["return"] = df["total_value"].pct_change()
        
        total_return = (df["total_value"].iloc[-1] / self.initial_capital) - 1
        
        days = (df["date"].iloc[-1] - df["date"].iloc[0]).days
        annual_return = (1 + total_return) ** (365 / max(days, 1)) - 1
        
        sharpe_ratio = df["return"].mean() / df["return"].std() * np.sqrt(252) if df["return"].std() > 0 else 0
        
        cummax = df["total_value"].cummax()
        drawdown = (df["total_value"] - cummax) / cummax
        max_drawdown = drawdown.min()
        
        buy_trades = [t for t in self.trades if t.direction == "buy"]
        sell_trades = [t for t in self.trades if t.direction == "sell"]
        win_trades = 0
        total_round_trips = 0
        open_buys: Dict[str, List[Trade]] = {}
        for t in self.trades:
            if t.direction == "buy":
                open_buys.setdefault(t.symbol, []).append(t)
            elif t.direction == "sell":
                queue = open_buys.get(t.symbol, [])
                if not queue:
                    continue
                buy_t = queue.pop(0)
                total_round_trips += 1
                if t.price > buy_t.price:
                    win_trades += 1
        win_rate = win_trades / total_round_trips if total_round_trips > 0 else 0
        
        result = BacktestResult(
            trades=self.trades,
            daily_values=df,
            total_return=total_return,
            annual_return=annual_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate
        )
        
        logger.info(f"选股+择时回测完成: 总收益={total_return:.2%}, 年化={annual_return:.2%}, "
                   f"夏普={sharpe_ratio:.2f}, 最大回撤={max_drawdown:.2%}")
        
        return result
