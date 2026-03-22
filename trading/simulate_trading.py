# -*- coding: utf-8 -*-
"""
模拟交易模块
每日检查持仓，触发止盈/止损时自动卖出
"""
import os
import json
from datetime import datetime
from typing import Dict, List, Optional

from data.recommend_db import RecommendDB, get_db
from data import DataSource
from config.config import STRATEGY_CONFIG
from strategy.analysis.fund.fund_consistency import compute_fcf, compute_recent_fcf_series
from trading.report_formatter import (
    TradeLifecycleRow,
    TradeTimelineRow,
    format_trade_lifecycle_section,
    format_trade_timeline_section,
)
from utils.logger import get_logger

logger = get_logger(__name__)


class SimulateTrader:
    """模拟交易器"""
    
    def __init__(self, db_path: str = "./data/recommend.db", risk_overrides: Optional[Dict] = None):
        self.db = get_db(db_path)
        self.data_source = DataSource()
        self.today = datetime.now().strftime("%Y-%m-%d")
        self._risk_cfg = dict(STRATEGY_CONFIG)
        if risk_overrides:
            self._risk_cfg.update(risk_overrides)
        self._market_emotion_cache: Dict[str, float] = {}
        self._stock_emotion_cache: Dict[str, float] = {}
        self._concept_strength_cache: Dict[str, tuple] = {}
        self._market_cycle: str = ""

    def _held_trading_days(self, buy_date: str) -> int:
        """计算持仓交易日天数（简化：工作日近似）"""
        try:
            import pandas as pd

            entry = pd.to_datetime(buy_date)
            now = pd.to_datetime(self.today)
            if now <= entry:
                return 0
            return max(len(pd.bdate_range(entry, now)) - 1, 0)
        except Exception:
            try:
                entry_dt = datetime.strptime(buy_date, "%Y-%m-%d")
                now_dt = datetime.strptime(self.today, "%Y-%m-%d")
                return max((now_dt.date() - entry_dt.date()).days, 0)
            except Exception:
                return 0

    def _get_market_emotion_score(self) -> float:
        """获取大盘情绪分（0-100）"""
        if not bool(self._risk_cfg.get("emotion_enabled", False)):
            self._market_cycle = ""
            return 50.0

        date_ymd = datetime.now().strftime("%Y%m%d")
        cached = self._market_emotion_cache.get(date_ymd)
        if cached is not None:
            return float(cached)

        try:
            from strategy.analysis.emotion.market_emotion import MarketEmotionAnalyzer

            analyzer = MarketEmotionAnalyzer()
            market = analyzer.get_market_emotion(date_ymd)
            if market:
                score = float(market.normalized_score)
                self._market_cycle = str(market.cycle)
            else:
                score = 50.0
                self._market_cycle = ""
            self._market_emotion_cache[date_ymd] = score
            return score
        except Exception as e:
            logger.debug(f"大盘情绪获取失败: {e}")
            self._market_cycle = ""
            return 50.0

    def _get_stock_emotion_score(self, code: str, name: str) -> float:
        """获取个股情绪分（0-100）"""
        if not bool(self._risk_cfg.get("emotion_enabled", False)):
            return 50.0

        date_ymd = datetime.now().strftime("%Y%m%d")
        key = f"{code}_{date_ymd}"
        cached = self._stock_emotion_cache.get(key)
        if cached is not None:
            return float(cached)

        try:
            from strategy.analysis.emotion.stock_emotion import StockEmotionAnalyzer

            analyzer = StockEmotionAnalyzer()
            res = analyzer.analyze_stock(symbol=code, name=name, date=date_ymd)
            score = float(res.score) if res and res.success else 50.0
            self._stock_emotion_cache[key] = score
            return score
        except Exception as e:
            logger.debug(f"个股情绪获取失败 {code}: {e}")
            return 50.0

    def _get_concept_strength(self, code: str) -> tuple:
        """获取个股所属概念强度"""
        date_ymd = datetime.now().strftime("%Y%m%d")
        key = f"{code}_{date_ymd}"
        cached = self._concept_strength_cache.get(key)
        if cached is not None:
            return cached

        try:
            from strategy.analysis.space.space_score import SpaceScoreAnalyzer

            analyzer = SpaceScoreAnalyzer()
            score, concept_name = analyzer.get_symbol_concept_strength(
                symbol=code,
                date=date_ymd,
                top_concepts=30,
            )
            result = (float(score), str(concept_name or ""))
            self._concept_strength_cache[key] = result
            return result
        except Exception as e:
            logger.debug(f"概念强度获取失败 {code}: {e}")
            return 0.0, ""
    
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
        market_emotion_score = self._get_market_emotion_score()
        market_stop = float(self._risk_cfg.get("market_emotion_stop_score", 40.0))
        override_score = float(self._risk_cfg.get("stock_emotion_override_score", 75.0))
        concept_override_score = float(self._risk_cfg.get("concept_override_score", 0.70))
        trailing_stop = float(self._risk_cfg.get("trailing_stop", 0.0))
        override_trailing_stop = float(self._risk_cfg.get("override_trailing_stop", trailing_stop))
        max_hold_days = int(self._risk_cfg.get("max_hold_days", 0))
        time_stop_days = int(self._risk_cfg.get("time_stop_days", 0))
        time_stop_min_return = float(self._risk_cfg.get("time_stop_min_return", 0.0))
        scale_out_enabled = bool(self._risk_cfg.get("scale_out_enabled", False))
        scale_out_levels = self._risk_cfg.get("scale_out_levels", [0.10, 0.20])
        scale_out_ratios = self._risk_cfg.get("scale_out_ratios", [0.50, 1.00])
        entry_low_stop_enabled = bool(self._risk_cfg.get("entry_low_stop_enabled", False))
        entry_low_stop_buffer = float(self._risk_cfg.get("entry_low_stop_buffer", 0.0))
        limit_up_seal_exit_enabled = bool(self._risk_cfg.get("limit_up_seal_exit_enabled", False))
        seal_ratio_sell_all = float(self._risk_cfg.get("seal_ratio_sell_all", 0.10))
        seal_ratio_sell_half = float(self._risk_cfg.get("seal_ratio_sell_half", 0.20))
        break_count_sell_all = int(self._risk_cfg.get("break_count_sell_all", 3))
        break_count_sell_half = int(self._risk_cfg.get("break_count_sell_half", 1))
        seal_weak_for_break_half = 0.30

        zt_map = None
        if limit_up_seal_exit_enabled:
            try:
                import akshare as ak
                try:
                    token = str(os.environ.get("AKSHARE_PROXY_TOKEN", "")).strip()
                    if token:
                        import akshare_proxy_patch

                        akshare_proxy_patch.install_patch(
                            "101.201.173.125",
                            auth_token=token,
                            retry=2,
                            hook_domains=["push2.eastmoney.com", "fund.eastmoney.com"],
                        )
                except Exception:
                    pass
                date_ymd = datetime.now().strftime("%Y%m%d")
                df_zt = ak.stock_zt_pool_em(date=date_ymd)
                if df_zt is not None and not df_zt.empty:
                    zt_map = {}
                    for _, row in df_zt.iterrows():
                        code = str(row.get("代码", "")).strip()
                        if code:
                            zt_map[code] = {
                                "seal_fund": float(row.get("封板资金", 0) or 0),
                                "turnover": float(row.get("成交额", 0) or 0),
                                "break_count": int(row.get("炸板次数", 0) or 0),
                            }
            except Exception as e:
                logger.debug(f"涨停封板数据获取失败: {e}")
        
        for holding in holdings:
            code = holding["code"]
            name = holding["name"]
            buy_price = holding["buy_price"]
            quantity = int(holding.get("quantity") or 0)
            target_price = holding["target_price"]
            stop_loss = holding["stop_loss"]
            highest_price = holding.get("highest_price") or buy_price
            tp_stage = int(holding.get("tp_stage") or 0)
            entry_low = float(holding.get("entry_low") or 0)
            buy_date = holding.get("buy_date") or self.today
            
            # 获取最新价格
            try:
                latest = self._get_latest_price(code)
                if not latest:
                    logger.warning(f"无法获取{code}最新价格，跳过")
                    continue
                
                current_price = latest["price"]
                
                # 更新持仓现价
                self.db.update_position_price(code, current_price)
                highest_price = max(float(highest_price or 0), float(current_price))
                
                # 计算涨跌幅
                change_pct = (current_price - buy_price) / buy_price * 100
                
                logger.info(f"{code} {name}: 买入{buy_price:.2f} 当前{current_price:.2f} ({change_pct:+.2f}%)")

                held_days = self._held_trading_days(str(buy_date))
                pnl_pct = (current_price - buy_price) / buy_price if buy_price > 0 else 0.0

                # 不及预期：跌破买入日低点（硬止损）
                if entry_low_stop_enabled and entry_low > 0:
                    if current_price <= entry_low * (1 - entry_low_stop_buffer):
                        pnl = self._close_position(code, current_price, "跌破买入日低点")
                        trades.append({"code": code, "action": "sell", "reason": "break_entry_low", "pnl": pnl})
                        continue

                # 资金一致性卖点：FCF 转负 或 连续下降
                if bool(self._risk_cfg.get("fcf_enabled", False)):
                    try:
                        from datetime import timedelta

                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=120)
                        kdf = self.data_source.get_kline(
                            code,
                            start_date.strftime("%Y%m%d"),
                            end_date.strftime("%Y%m%d"),
                        )
                        if kdf is not None and (not kdf.empty) and len(kdf) >= 20:
                            death_turnover = float(self._risk_cfg.get("fcf_death_turnover", 50.0))
                            f = compute_fcf(kdf, turnover_rate=None, death_turnover=death_turnover)
                            fcf_val = float(f.fcf)

                            sell_th = float(self._risk_cfg.get("fcf_sell_threshold", 0.0))
                            if fcf_val < sell_th:
                                pnl = self._close_position(code, current_price, f"FCF转负({fcf_val:+.2f})")
                                trades.append({"code": code, "action": "sell", "reason": "fcf_negative", "pnl": pnl})
                                continue

                            down_days = int(self._risk_cfg.get("fcf_down_days", 2))
                            if down_days >= 2:
                                fcf_series = compute_recent_fcf_series(
                                    kdf,
                                    lookback_days=down_days + 1,
                                    turnover_rate=None,
                                    death_turnover=death_turnover,
                                )
                                if len(fcf_series) >= down_days + 1:
                                    is_down = all(
                                        fcf_series[idx] < fcf_series[idx - 1]
                                        for idx in range(1, len(fcf_series))
                                    )
                                    if is_down:
                                        pnl = self._close_position(code, current_price, f"FCF连续下降({fcf_val:+.2f})")
                                        trades.append({"code": code, "action": "sell", "reason": "fcf_down", "pnl": pnl})
                                        continue
                    except Exception as e:
                        logger.debug(f"FCF检查失败 {code}: {e}")
                
                # 弱市退潮：大盘差时，非强势抱团股优先撤退；强势抱团则跳过情绪/时间退出
                override_hold = False
                if bool(self._risk_cfg.get("emotion_enabled", False)) and market_emotion_score <= market_stop:
                    stock_score = self._get_stock_emotion_score(code, name)
                    concept_score, concept_name = self._get_concept_strength(code)
                    if stock_score >= override_score and concept_score >= concept_override_score:
                        override_hold = True
                        logger.info(
                            f"{code} 弱市抱团豁免: stock_emotion={stock_score:.0f}, "
                            f"concept={concept_name or '-'}:{concept_score:.2f}, "
                            f"market={self._market_cycle}:{market_emotion_score:.0f}"
                        )
                    else:
                        pnl = self._close_position(code, current_price, f"情绪退潮({self._market_cycle}:{market_emotion_score:.0f})")
                        trades.append({"code": code, "action": "sell", "reason": "emotion_stop", "pnl": pnl})
                        continue

                trailing_stop_eff = override_trailing_stop if override_hold else trailing_stop

                # 涨停封板强度/炸板风险（仅对当天封板中的涨停股）
                if limit_up_seal_exit_enabled and zt_map and code in zt_map and quantity >= 100:
                    info = zt_map.get(code, {})
                    seal_fund = float(info.get("seal_fund", 0.0))
                    turnover = float(info.get("turnover", 0.0))
                    break_count = int(info.get("break_count", 0))
                    seal_ratio = (seal_fund / turnover) if turnover > 0 else 0.0

                    if break_count >= break_count_sell_all or seal_ratio < seal_ratio_sell_all:
                        pnl = self._close_position(code, current_price, f"封板弱/炸板({seal_ratio:.2f},{break_count})")
                        trades.append({"code": code, "action": "sell", "reason": "limit_up_weak_seal_all", "pnl": pnl})
                        continue

                    if seal_ratio < seal_ratio_sell_half or (break_count >= break_count_sell_half and seal_ratio < seal_weak_for_break_half):
                        sell_qty = int(quantity * 0.5 / 100) * 100
                        if sell_qty < 100:
                            sell_qty = quantity
                        pnl = self.db.sell_partial(code, current_price, self.today, sell_qty, tp_stage=tp_stage, reason="封板弱/炸板减仓")
                        if pnl is not None:
                            trades.append({"code": code, "action": "sell", "reason": "limit_up_weak_seal_half", "pnl": pnl})
                            continue

                # 分批止盈（抱团股）：10%卖半、20%清仓
                if override_hold and scale_out_enabled and quantity >= 100:
                    try:
                        lv1 = float(scale_out_levels[0]) if len(scale_out_levels) > 0 else 0.10
                        lv2 = float(scale_out_levels[1]) if len(scale_out_levels) > 1 else 0.20
                        r1 = float(scale_out_ratios[0]) if len(scale_out_ratios) > 0 else 0.5
                    except Exception:
                        lv1, lv2, r1 = 0.10, 0.20, 0.5

                    if tp_stage <= 0 and pnl_pct >= lv1:
                        sell_qty = int(quantity * r1 / 100) * 100
                        if sell_qty < 100:
                            sell_qty = quantity
                        pnl = self.db.sell_partial(code, current_price, self.today, sell_qty, tp_stage=1, reason="一段止盈")
                        if pnl is not None:
                            trades.append({"code": code, "action": "sell", "reason": "scale_out_1", "pnl": pnl})
                        continue

                    if tp_stage >= 1 and pnl_pct >= lv2:
                        pnl = self._close_position(code, current_price, "二段止盈")
                        trades.append({"code": code, "action": "sell", "reason": "scale_out_2", "pnl": pnl})
                        continue
                
                # 检查是否触发止盈
                if (not override_hold) and target_price and current_price >= target_price:
                    pnl = self._close_position(code, current_price, "止盈")
                    trades.append({"code": code, "action": "sell", "reason": "止盈", "pnl": pnl})
                    continue
                
                # 检查是否触发止损
                if stop_loss and current_price <= stop_loss:
                    pnl = self._close_position(code, current_price, "止损")
                    trades.append({"code": code, "action": "sell", "reason": "止损", "pnl": pnl})
                    continue

                # 跟踪止盈：从最高点回撤
                if trailing_stop_eff > 0 and highest_price and pnl_pct > 0:
                    if current_price <= float(highest_price) * (1 - trailing_stop_eff):
                        pnl = self._close_position(code, current_price, "跟踪止盈")
                        trades.append({"code": code, "action": "sell", "reason": "trailing_stop", "pnl": pnl})
                        continue

                # 时间止损/最长持仓（抱团股豁免）
                if (not override_hold) and max_hold_days > 0 and held_days >= max_hold_days:
                    pnl = self._close_position(code, current_price, f"超时({held_days}d)")
                    trades.append({"code": code, "action": "sell", "reason": "max_hold_days", "pnl": pnl})
                    continue

                if (not override_hold) and time_stop_days > 0 and held_days >= time_stop_days and pnl_pct <= time_stop_min_return:
                    pnl = self._close_position(code, current_price, f"时间止损({held_days}d)")
                    trades.append({"code": code, "action": "sell", "reason": "time_stop", "pnl": pnl})
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

    def preview_timing_decisions(self, limit: int = 20) -> Dict[str, object]:
        """
        预览当前择时参数下的持仓决策，不落库。
        """
        holdings = self.db.get_holdings()[:limit]
        decisions: List[Dict[str, object]] = []
        sell_reason_counts: Dict[str, int] = {}
        sell_count = 0
        hold_count = 0
        total_sell_pnl_pct = 0.0

        stop_loss_threshold = abs(float(self._risk_cfg.get("stop_loss", -0.05) or -0.05))
        trailing_stop = float(self._risk_cfg.get("trailing_stop", 0.0) or 0.0)
        time_stop_days = int(self._risk_cfg.get("time_stop_days", 0) or 0)
        time_stop_min_return = float(self._risk_cfg.get("time_stop_min_return", 0.0) or 0.0)
        entry_low_stop_enabled = bool(self._risk_cfg.get("entry_low_stop_enabled", False))
        entry_low_stop_buffer = float(self._risk_cfg.get("entry_low_stop_buffer", 0.0) or 0.0)

        for holding in holdings:
            code = str(holding.get("code", "") or "")
            name = str(holding.get("name", "") or "")
            buy_price = float(holding.get("buy_price", 0) or 0.0)
            target_price = float(holding.get("target_price", 0) or 0.0)
            stop_loss_price = float(holding.get("stop_loss", 0) or 0.0)
            highest_price = float(holding.get("highest_price", 0) or buy_price)
            entry_low = float(holding.get("entry_low", 0) or 0.0)
            buy_date = str(holding.get("buy_date", self.today) or self.today)

            latest = self._get_latest_price(code)
            if not latest:
                decisions.append({
                    "code": code,
                    "name": name,
                    "action": "skip",
                    "reason": "无法获取最新价格",
                    "current_price": 0.0,
                    "pnl_pct": 0.0,
                    "held_days": self._held_trading_days(buy_date),
                })
                continue

            current_price = float(latest.get("price", 0) or 0.0)
            held_days = self._held_trading_days(buy_date)
            pnl_pct = ((current_price - buy_price) / buy_price) if buy_price > 0 else 0.0
            reason = "继续持有"
            action = "hold"

            if entry_low_stop_enabled and entry_low > 0 and current_price <= entry_low * (1 - entry_low_stop_buffer):
                action = "sell"
                reason = "跌破买入日低点"
            elif target_price > 0 and current_price >= target_price:
                action = "sell"
                reason = "止盈"
            elif stop_loss_price > 0 and current_price <= stop_loss_price:
                action = "sell"
                reason = "止损"
            elif stop_loss_threshold > 0 and pnl_pct <= -stop_loss_threshold:
                action = "sell"
                reason = "固定止损"
            elif trailing_stop > 0 and highest_price > 0 and pnl_pct > 0 and current_price <= highest_price * (1 - trailing_stop):
                action = "sell"
                reason = "跟踪止盈"
            elif time_stop_days > 0 and held_days >= time_stop_days and pnl_pct <= time_stop_min_return:
                action = "sell"
                reason = "时间止损"

            decision = {
                "code": code,
                "name": name,
                "action": action,
                "reason": reason,
                "current_price": current_price,
                "buy_price": buy_price,
                "pnl_pct": pnl_pct * 100,
                "held_days": held_days,
            }
            decisions.append(decision)

            if action == "sell":
                sell_count += 1
                total_sell_pnl_pct += pnl_pct * 100
                sell_reason_counts[reason] = int(sell_reason_counts.get(reason, 0)) + 1
            elif action == "hold":
                hold_count += 1

        return {
            "summary": {
                "holding_count": len(holdings),
                "sell_count": sell_count,
                "hold_count": hold_count,
                "avg_sell_pnl_pct": (total_sell_pnl_pct / sell_count) if sell_count > 0 else 0.0,
            },
            "reason_counts": sell_reason_counts,
            "decisions": decisions,
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
        pnl = self.db.close_position(code, sell_price, self.today, reason=reason)
        
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
        holdings_aggregated = self.db.get_holdings_aggregated()
        trade_history = self.db.get_trade_history(days=500)
        timeline_rows = []
        for item in self.db.get_trade_timeline(limit=100):
            metadata = {}
            try:
                raw_metadata = item.get("metadata", "")
                if raw_metadata:
                    metadata = json.loads(str(raw_metadata))
            except Exception:
                metadata = {}
            timeline_rows.append(TradeTimelineRow(
                date=str(item.get("date", "")),
                code=str(item.get("code", "")),
                name=str(item.get("name", "")),
                event_type=str(item.get("event_type", "")),
                signal_type=str(item.get("signal_type", "")),
                price=float(item.get("price", 0) or 0),
                target_price=float(item.get("target_price", 0) or 0),
                stop_loss=float(item.get("stop_loss", 0) or 0),
                quantity=int(item.get("quantity", 0) or 0),
                reason=str(item.get("reason", "")),
                status=str(item.get("status", "")),
                pnl=float(metadata.get("pnl", 0) or 0),
                pnl_pct=float(metadata.get("pnl_pct", 0) or 0),
            ))

        realized_pnl_map: Dict[str, float] = {}
        latest_name_map: Dict[str, str] = {}
        for trade in trade_history:
            code = str(trade.get("code", ""))
            latest_name_map[code] = str(trade.get("name", latest_name_map.get(code, "")))
            if str(trade.get("direction", "")) == "sell":
                realized_pnl_map[code] = float(realized_pnl_map.get(code, 0.0)) + float(trade.get("pnl", 0) or 0)

        lifecycle_rows: List[TradeLifecycleRow] = []
        holding_codes = set()
        for holding in holdings_aggregated:
            code = str(holding.get("code", ""))
            name = str(holding.get("name", ""))
            holding_codes.add(code)
            open_cost = float(holding.get("total_quantity", 0) or 0) * float(holding.get("avg_buy_price", 0) or 0)
            floating_pnl = float(holding.get("total_pnl", 0) or 0)
            realized_pnl = float(realized_pnl_map.get(code, 0.0))
            total_pnl = floating_pnl + realized_pnl
            total_pnl_pct = (total_pnl / open_cost * 100) if open_cost > 0 else 0.0
            lifecycle_rows.append(TradeLifecycleRow(
                code=code,
                name=name,
                open_cost=open_cost,
                holding_quantity=int(holding.get("total_quantity", 0) or 0),
                latest_price=float(holding.get("avg_current_price", 0) or 0),
                floating_pnl=floating_pnl,
                floating_pnl_pct=float(holding.get("total_pnl_pct", 0) or 0),
                realized_pnl=realized_pnl,
                total_pnl=total_pnl,
                total_pnl_pct=total_pnl_pct,
            ))

        for code, realized_pnl in realized_pnl_map.items():
            if code in holding_codes:
                continue
            lifecycle_rows.append(TradeLifecycleRow(
                code=code,
                name=latest_name_map.get(code, ""),
                open_cost=0.0,
                holding_quantity=0,
                latest_price=0.0,
                floating_pnl=0.0,
                floating_pnl_pct=0.0,
                realized_pnl=float(realized_pnl),
                total_pnl=float(realized_pnl),
                total_pnl_pct=0.0,
            ))
        
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
        return (
            f"{report}\n"
            f"{format_trade_lifecycle_section(lifecycle_rows)}\n"
            f"{format_trade_timeline_section(timeline_rows)}"
        )


# 全局实例
_trader: SimulateTrader = None


def get_trader(db_path: str = "./data/recommend.db") -> SimulateTrader:
    """获取交易器实例"""
    global _trader
    if _trader is None:
        _trader = SimulateTrader(db_path)
    return _trader
