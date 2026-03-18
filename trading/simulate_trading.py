# -*- coding: utf-8 -*-
"""
模拟交易模块
每日检查持仓，触发止盈/止损时自动卖出
"""
import os
from datetime import datetime
from typing import Dict, List

from data.recommend_db import RecommendDB, get_db
from data import DataSource
from utils.logger import get_logger

logger = get_logger(__name__)


class SimulateTrader:
    """模拟交易器"""
    
    def __init__(self, db_path: str = "./data/recommend.db"):
        self.db = get_db(db_path)
        self.data_source = DataSource()
        self.today = datetime.now().strftime("%Y-%m-%d")
    
    def check_and_trade(self) -> Dict:
        """
        检查持仓并执行交易
        
        Returns:
            执行结果
        """
        logger.info("开始检查持仓...")
        
        # 获取当前持仓
        holdings = self.db.get_holdings()
        
        if not holdings:
            logger.info("当前无持仓")
            return {"action": "hold", "trades": []}
        
        logger.info(f"当前持仓: {len(holdings)}只")
        
        trades = []
        
        for holding in holdings:
            code = holding["code"]
            name = holding["name"]
            buy_price = holding["buy_price"]
            target_price = holding["target_price"]
            stop_loss = holding["stop_loss"]
            
            # 获取最新价格
            try:
                latest = self._get_latest_price(code)
                if not latest:
                    logger.warning(f"无法获取{code}最新价格，跳过")
                    continue
                
                current_price = latest["price"]
                
                # 更新持仓现价
                self.db.update_position_price(code, current_price)
                
                # 计算涨跌幅
                change_pct = (current_price - buy_price) / buy_price * 100
                
                logger.info(f"{code} {name}: 买入{buy_price:.2f} 当前{current_price:.2f} ({change_pct:+.2f}%)")
                
                # 检查是否触发止盈
                if target_price and current_price >= target_price:
                    pnl = self._close_position(code, current_price, "止盈")
                    trades.append({"code": code, "action": "sell", "reason": "止盈", "pnl": pnl})
                    continue
                
                # 检查是否触发止损
                if stop_loss and current_price <= stop_loss:
                    pnl = self._close_position(code, current_price, "止损")
                    trades.append({"code": code, "action": "sell", "reason": "止损", "pnl": pnl})
                    continue
                
            except Exception as e:
                logger.error(f"检查{code}失败: {e}")
        
        # 获取统计信息
        stats = self.db.get_statistics()
        
        return {
            "action": "trade" if trades else "hold",
            "trades": trades,
            "holdings": self.db.get_holdings(),
            "statistics": stats
        }
    
    def _get_latest_price(self, symbol: str) -> Dict:
        """获取最新价格"""
        try:
            from datetime import timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5)
            
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
                "change_pct": float(latest.get("pct_change", 0))
            }
        except Exception as e:
            logger.error(f"获取价格失败 {symbol}: {e}")
            return None
    
    def _close_position(self, code: str, sell_price: float, reason: str) -> float:
        """平仓"""
        pnl = self.db.close_position(code, sell_price, self.today)
        
        if pnl is None:
            logger.warning(f"{code}平仓失败")
            return 0
        
        pnl_pct = pnl / (sell_price * self.db.get_holdings()[0]["quantity"] if self.db.get_holdings() else 1) * 100
        
        result = "盈利" if pnl > 0 else "亏损"
        logger.info(f"{code}触发{reason}，卖出@{sell_price:.2f}，{result}{pnl:.2f}元")
        
        return pnl
    
    def get_report(self) -> str:
        """生成交易报告"""
        stats = self.db.get_statistics()
        holdings = self.db.get_holdings()
        
        report = f"""
{'='*50}
          模拟交易报告 ({self.today})
{'='*50}

【持仓情况】
"""
        
        if holdings:
            for h in holdings:
                change_pct = h.get("pnl_pct", 0)
                report += f"  {h['code']} {h['name']}\n"
                report += f"    买入价: {h['buy_price']:.2f} 现价: {h['current_price']:.2f}\n"
                report += f"    盈亏: {change_pct:+.2f}%\n"
        else:
            report += "  空仓\n"
        
        report += f"""
【历史统计】
  总交易次数: {stats['total_trades']}
  盈利次数: {stats['win_trades']}
  亏损次数: {stats['loss_trades']}
  胜率: {stats['win_rate']:.1f}%
  总收益: {stats['total_pnl']:.2f}元
  平均收益: {stats['avg_pnl']:.2f}元
{'='*50}
"""
        return report


# 全局实例
_trader: SimulateTrader = None


def get_trader(db_path: str = "./data/recommend.db") -> SimulateTrader:
    """获取交易器实例"""
    global _trader
    if _trader is None:
        _trader = SimulateTrader(db_path)
    return _trader
