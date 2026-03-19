# -*- coding: utf-8 -*-
"""
荐股记录器
将荐股信息保存到数据库，并在推送时自动执行模拟买入
"""
import os
from datetime import datetime
from typing import List, Dict

from data.recommend_db import RecommendDB, RecommendRecord, get_db
from trading.realtime_monitor import StockSignal
from utils.logger import get_logger

logger = get_logger(__name__)


class RecommendRecorder:
    """荐股记录器"""
    
    def __init__(self, db_path: str = "./data/recommend.db"):
        self.db = get_db(db_path)
        self.today = datetime.now().strftime("%Y-%m-%d")
    
    def save_recommends(self, etf_signals: List[StockSignal], stock_signals: List[StockSignal]) -> List[int]:
        """
        保存荐股记录到数据库
        
        Args:
            etf_signals: ETF信号列表
            stock_signals: A股信号列表
        
        Returns:
            保存的荐股记录ID列表
        """
        saved_ids = []
        
        # 只保存买入信号
        all_signals = etf_signals + stock_signals
        buy_signals = [s for s in all_signals if s.signal_type == "买入" and s.target_price and s.stop_loss]
        
        logger.info(f"开始保存荐股记录，共{len(buy_signals)}只股票")
        
        for signal in buy_signals:
            try:
                record = RecommendRecord(
                    date=self.today,
                    code=signal.code,
                    name=signal.name,
                    price=signal.price,
                    target_price=signal.target_price,
                    stop_loss=signal.stop_loss,
                    reason=signal.reason,
                    signal_type="买入"
                )
                
                recommend_id = self.db.add_recommend(record)
                saved_ids.append(recommend_id)
                
                logger.info(f"保存荐股: {signal.code} {signal.name} @ {signal.price}")
                
            except Exception as e:
                logger.error(f"保存荐股失败 {signal.code}: {e}")
        
        logger.info(f"荐股记录保存完成，共{len(saved_ids)}条")
        return saved_ids
    
    def auto_buy(self, max_positions: int = 3, max_position_pct: float = 0.3) -> Dict:
        """
        自动执行模拟买入
        
        Args:
            max_positions: 最大持仓数量
            max_position_pct: 单只股票最大仓位比例
        
        Returns:
            执行结果
        """
        # 获取今日荐股
        recommends = self.db.get_recommends_by_date(self.today)
        
        if not recommends:
            logger.info("今日无荐股记录")
            return {"action": "skip", "reason": "no_recommends", "positions": []}
        
        # 获取当前持仓
        current_holdings = self.db.get_holdings()
        held_codes = {h["code"] for h in current_holdings if h.get("code")}
        
        if len(current_holdings) >= max_positions:
            logger.info(f"已达到最大持仓数 {max_positions}，跳过买入")
            return {"action": "skip", "reason": "max_positions", "positions": current_holdings}
        
        # 模拟买入（按仓位比例，过滤已持仓的代码）
        buy_positions = []
        for rec in recommends:
            if rec.code in held_codes:
                logger.info(f"已持有 {rec.code}，跳过")
                continue
            # 计算买入数量（按金额）
            # 假设初始资金100万，单只仓位30%
            position_value = 1000000 * max_position_pct
            quantity = int(position_value / rec.price / 100) * 100  # 整手
            
            if quantity < 100:
                continue
            
            # 添加持仓记录
            try:
                pos_id = self.db.add_position(
                    recommend_id=rec.id,
                    code=rec.code,
                    name=rec.name,
                    buy_date=self.today,
                    buy_price=rec.price,
                    quantity=quantity,
                    target_price=rec.target_price,
                    stop_loss=rec.stop_loss
                )
                
                buy_positions.append({
                    "code": rec.code,
                    "name": rec.name,
                    "price": rec.price,
                    "quantity": quantity,
                    "amount": quantity * rec.price
                })
                
                logger.info(f"模拟买入: {rec.code} {rec.name} @ {rec.price} x {quantity}")
                
            except Exception as e:
                logger.error(f"买入失败 {rec.code}: {e}")
        
        return {
            "action": "buy" if buy_positions else "skip",
            "recommends": len(recommends),
            "positions": buy_positions,
            "current_holdings": current_holdings
        }


# 全局实例
_recorder: RecommendRecorder = None


def get_recorder(db_path: str = "./data/recommend.db") -> RecommendRecorder:
    """获取荐股记录器实例"""
    global _recorder
    if _recorder is None:
        _recorder = RecommendRecorder(db_path)
    return _recorder
