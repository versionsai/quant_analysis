# -*- coding: utf-8 -*-
"""
荐股记录器
将荐股信息保存到数据库，并在推送时自动执行模拟买入
"""
import os
from datetime import datetime
from typing import Dict, List, Optional

from agents.skills import get_skills_manager, load_skills
from data.recommend_db import RecommendRecord, SignalPoolRecord, TradePointRecord, TradeRecord, get_db
from trading.realtime_monitor import StockSignal
from utils.logger import get_logger
from config.config import STRATEGY_CONFIG
from data import DataSource

logger = get_logger(__name__)


def _load_risk_rule_overrides() -> Dict:
    """读取 risk skill 配置，用于自动买入风控。"""
    try:
        load_skills()
        manager = get_skills_manager()
        rules = manager.get_risk_rules()
        return rules or {}
    except Exception as e:
        logger.warning(f"读取 risk skill 配置失败，使用默认风控: {e}")
        return {}


class RecommendRecorder:
    """荐股记录器"""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db = get_db(db_path)
        self.today = datetime.now().strftime("%Y-%m-%d")

    def refresh_signal_pool(self, etf_signals: List[StockSignal], stock_signals: List[StockSignal]) -> Dict[str, int]:
        """
        独立刷新信号池。

        Args:
            etf_signals: ETF信号列表
            stock_signals: A股信号列表

        Returns:
            刷新结果统计
        """
        all_signals = etf_signals + stock_signals
        cleared_count = self.db.clear_signal_pool_by_status(status="active", next_status="inactive")
        saved_count = 0

        for signal in all_signals:
            try:
                pool_type = "etf" if signal.code.startswith(("1", "5")) else "stock"
                self.db.upsert_signal_pool(SignalPoolRecord(
                    date=self.today,
                    code=signal.code,
                    name=signal.name,
                    pool_type=pool_type,
                    signal_type=signal.signal_type,
                    price=signal.price,
                    target_price=signal.target_price or 0.0,
                    stop_loss=signal.stop_loss or 0.0,
                    reason=signal.reason,
                    score=signal.score,
                    source="realtime_monitor",
                    status="active",
                    metadata="",
                ))
                saved_count += 1
            except Exception as e:
                logger.warning(f"写入信号池失败 {signal.code}: {e}")

        logger.info(
            f"信号池刷新完成: 清理旧记录 {cleared_count} 条, 写入新记录 {saved_count} 条, "
            f"ETF信号 {len(etf_signals)} 条, A股信号 {len(stock_signals)} 条"
        )
        return {
            "cleared_count": cleared_count,
            "saved_count": saved_count,
            "etf_count": len(etf_signals),
            "stock_count": len(stock_signals),
            "buy_count": len([signal for signal in all_signals if signal.signal_type == "买入"]),
        }
    
    def save_recommends(
        self,
        etf_signals: List[StockSignal],
        stock_signals: List[StockSignal],
        refresh_pool: bool = True,
    ) -> List[int]:
        """
        保存荐股记录到数据库
        
        Args:
            etf_signals: ETF信号列表
            stock_signals: A股信号列表
        
        Returns:
            保存的荐股记录ID列表
        """
        saved_ids = []
        all_signals = etf_signals + stock_signals
        if refresh_pool:
            self.refresh_signal_pool(etf_signals, stock_signals)

        existing_codes = {
            str(record.code).strip()
            for record in self.db.get_recommends_by_date(self.today)
            if str(record.code).strip()
        }
        
        # 只保存买入信号
        buy_signals = [s for s in all_signals if s.signal_type == "买入" and s.target_price and s.stop_loss]
        
        logger.info(f"开始保存荐股记录，共{len(buy_signals)}只股票")
        
        for signal in buy_signals:
            try:
                code = str(signal.code).strip()
                if code in existing_codes:
                    logger.info(f"荐股已存在，跳过重复写入: {signal.code} {signal.name}")
                    continue

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
                self.db.add_trade_point(TradePointRecord(
                    recommend_id=recommend_id,
                    date=self.today,
                    code=signal.code,
                    name=signal.name,
                    event_type="recommend",
                    signal_type=signal.signal_type,
                    price=signal.price,
                    target_price=signal.target_price or 0.0,
                    stop_loss=signal.stop_loss or 0.0,
                    quantity=0,
                    reason=signal.reason,
                    source="realtime_monitor",
                    status="pending",
                ))
                saved_ids.append(recommend_id)
                existing_codes.add(code)
                
                logger.info(f"保存荐股: {signal.code} {signal.name} @ {signal.price}")
                
            except Exception as e:
                logger.error(f"保存荐股失败 {signal.code}: {e}")
        
        logger.info(f"荐股记录保存完成，共{len(saved_ids)}条")
        return saved_ids
    
    def auto_buy(self, ai_decision: Dict = None, max_positions: int = 3, max_position_pct: float = 0.3) -> Dict:
        """
        自动执行模拟买入（基于 AI Agent 决策）
        
        Args:
            ai_decision: AI Agent 决策结果，包含 buy_list/add_list
            max_positions: 最大持仓数量
            max_position_pct: 单只股票最大仓位比例
        
        Returns:
            执行结果
        """
        risk_rules = _load_risk_rule_overrides()
        max_positions = int(risk_rules.get("max_positions", max_positions))
        max_position_pct = float(risk_rules.get("max_position_pct", max_position_pct))

        recommends = self.db.get_recommends_by_date(self.today)
        
        if not recommends:
            logger.info("今日无荐股记录")
            return {"action": "skip", "reason": "no_recommends", "positions": []}
        
        current_holdings = self.db.get_holdings_aggregated()
        held_codes = {h["code"] for h in current_holdings}
        
        if ai_decision:
            buy_codes = set(ai_decision.get("buy_list", []) + ai_decision.get("add_list", []))
            skip_codes = set(ai_decision.get("skip_list", []))
            reason = ai_decision.get("reason", "")
            logger.info(f"AI 决策: {reason}, 买入 {buy_codes}, 跳过 {skip_codes}")
        else:
            buy_codes = {rec.code for rec in recommends}
            skip_codes = set()
        
        position_value = 1000000 * max_position_pct
        default_target_mult = 1 + float(STRATEGY_CONFIG.get("take_profit", 0.15))
        default_stop_mult = 1 + float(STRATEGY_CONFIG.get("stop_loss", -0.05))
        buy_positions = []
        data_source = DataSource()
        
        for rec in recommends:
            if rec.code in held_codes:
                if rec.code in skip_codes:
                    logger.info(f"{rec.code} 已持仓但被 AI 跳过")
                    continue
                if rec.code in buy_codes:
                    logger.info(f"{rec.code} 浮盈加仓")
            else:
                if rec.code not in buy_codes:
                    continue
                if len(held_codes) >= max_positions:
                    logger.info(f"已达最大持仓数 {max_positions}，跳过 {rec.code}")
                    continue
            
            quantity = int(position_value / rec.price / 100) * 100
            if quantity < 100:
                continue
            
            try:
                entry_low = None
                try:
                    quote_df = data_source.get_market_snapshots([rec.code])
                    if quote_df is not None and (not quote_df.empty):
                        low_price = float(quote_df.iloc[0].get("low_price", 0) or 0)
                        if low_price > 0:
                            entry_low = low_price
                except Exception:
                    entry_low = None

                if entry_low is None:
                    try:
                        from datetime import timedelta

                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=5)
                        kdf = data_source.get_kline(
                            rec.code,
                            start_date.strftime("%Y%m%d"),
                            end_date.strftime("%Y%m%d"),
                        )
                        if kdf is not None and (not kdf.empty) and "low" in kdf.columns:
                            entry_low = float(kdf.iloc[-1].get("low", rec.price) or rec.price)
                    except Exception:
                        entry_low = None

                self.db.add_position_merged(
                    code=rec.code,
                    name=rec.name,
                    buy_price=rec.price,
                    quantity=quantity,
                    target_price=rec.target_price or (rec.price * default_target_mult),
                    stop_loss=rec.stop_loss or (rec.price * default_stop_mult),
                    buy_date=self.today,
                    entry_low=entry_low,
                )
                self.db.add_trade(TradeRecord(
                    recommend_id=rec.id or 0,
                    date=self.today,
                    code=rec.code,
                    name=rec.name,
                    direction="buy",
                    price=rec.price,
                    quantity=quantity,
                    amount=quantity * rec.price,
                    commission=0.0,
                    pnl=0.0,
                    pnl_pct=0.0,
                    status="holding",
                ))
                self.db.add_trade_point(TradePointRecord(
                    recommend_id=rec.id or 0,
                    date=self.today,
                    code=rec.code,
                    name=rec.name,
                    event_type="buy",
                    signal_type="买入",
                    price=rec.price,
                    target_price=rec.target_price or (rec.price * default_target_mult),
                    stop_loss=rec.stop_loss or (rec.price * default_stop_mult),
                    quantity=quantity,
                    reason="模拟买入",
                    source="recommend_recorder.auto_buy",
                    status="holding",
                    metadata='',
                ))
                self.db.update_signal_pool_status(rec.code, "holding")
                
                buy_positions.append({
                    "code": rec.code,
                    "name": rec.name,
                    "price": rec.price,
                    "quantity": quantity,
                    "amount": quantity * rec.price,
                    "action": "add" if rec.code in held_codes else "buy",
                })
                
                logger.info(f"模拟{'加仓' if rec.code in held_codes else '买入'}: {rec.code} {rec.name} @ {rec.price} x {quantity}")
                held_codes.add(rec.code)
                
            except Exception as e:
                logger.error(f"买入失败 {rec.code}: {e}")
        try:
            data_source.close()
        except Exception:
            pass
        
        return {
            "action": "buy" if buy_positions else "skip",
            "recommends": len(recommends),
            "positions": buy_positions,
            "ai_reason": ai_decision.get("reason") if ai_decision else None,
            "current_holdings": current_holdings,
            "risk_rules": {
                "max_positions": max_positions,
                "max_position_pct": max_position_pct,
            },
        }


# 全局实例
_recorder: RecommendRecorder = None


def get_recorder(db_path: Optional[str] = None) -> RecommendRecorder:
    """获取荐股记录器实例"""
    global _recorder
    if _recorder is None:
        _recorder = RecommendRecorder(db_path)
    return _recorder
