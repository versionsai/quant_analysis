# -*- coding: utf-8 -*-
"""
回测引擎
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from strategy.base import BaseStrategy, Signal, Portfolio, Position
from config.config import BACKTEST_CONFIG
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Trade:
    """成交记录"""
    date: datetime
    symbol: str
    direction: str  # buy/sell
    price: float
    quantity: int
    commission: float


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
                self.portfolio.positions[symbol].current_price = current_price
            
            self.strategy.load_data(symbol, df[df.index <= date])
        
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
        
        price = df.iloc[-1]["close"] * (1 + self.slippage)
        
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
                current_price=price
            )
            self.trades.append(Trade(date, symbol, "buy", price, quantity, commission))
    
    def _sell(self, symbol: str, date):
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
                                  commission + stamp_tax_pos))
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
        for i in range(0, len(sell_trades), 2):
            if i + 1 < len(buy_trades):
                if sell_trades[i].price > buy_trades[i].price:
                    win_trades += 1
        win_rate = win_trades / len(sell_trades) if sell_trades else 0
        
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
