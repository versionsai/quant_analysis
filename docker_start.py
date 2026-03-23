# -*- coding: utf-8 -*-
"""
Docker启动脚本 - 定时推送
支持 AI Agent 增强分析
"""
from dotenv import load_dotenv

load_dotenv()
load_dotenv(".env.local", override=True)
import os
import sys
import time
import signal
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trading import RealtimeMonitor, get_pusher, set_pusher_key
from trading.push_service import format_mobile_trade_report
from trading.review_report import build_runtime_review_report_from_db, save_runtime_review_report
from trading.report_formatter import (
    DecisionReportRow,
    HoldingReportRow,
    NewsReportBlock,
    ProxyDiffRow,
    ReviewTradeRow,
    format_decision_section,
    format_holdings_section,
    format_news_section,
    format_review_section,
    format_signal_section,
)
from trading.recommend_recorder import get_recorder
from trading.simulate_trading import get_trader
from agents.tools.stock_analysis import get_stock_fundamental_summary
from data import DataSource, get_pool_generator
from utils.logger import get_logger

logger = get_logger(__name__)


def _safe_preview(value, max_len: int = 500) -> str:
    """安全截断输出，避免对非字符串对象做切片导致异常"""
    try:
        if value is None:
            text = ""
        elif isinstance(value, str):
            text = value
        else:
            import json

            try:
                text = json.dumps(value, ensure_ascii=False, default=str)
            except Exception:
                text = str(value)
        return text[:max_len]
    except Exception:
        return ""


def _summarize_news_with_agent(title: str, text: str) -> str:
    """
    使用 AI Agent 对资讯内容提炼重点。
    """
    content = str(text or "").strip()
    if not content:
        return ""

    if str(os.environ.get("ENABLE_AI_AGENT", "false")).lower() != "true":
        return content

    try:
        from agents import get_quant_agent

        agent = get_quant_agent()
        result = agent.run(
            task=(
                f"请把下面这段“{title}”资讯提炼成 4-6 条重点。"
                "保留利好、利空、风险提示、相关标的或行业、可执行结论。"
                "不要输出 JSON，不要使用省略号，不要照搬大段原文。"
                "输出使用中文项目符号，每条单独一行。\n\n"
                f"{content}"
            ),
            timeout_sec=45,
            operation_name=f"{title}资讯提炼",
        )
        summary = agent.extract_text(result).strip()
        if summary and "失败" not in summary and "超时" not in summary:
            return summary
    except Exception as e:
        logger.warning(f"{title} AI 提炼失败，回退原摘要: {e}")

    return content


class ScheduledPusher:
    """定时推送服务"""
    
    def __init__(self):
        bark_key = os.environ.get("BARK_KEY", "WnLnofnzPUAyzy9VsvyaCg")
        set_pusher_key(bark_key)
        
        db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
        
        self.recorder = get_recorder(db_path)
        self.trader = get_trader(db_path)
        
        self.enable_agent = os.environ.get("ENABLE_AI_AGENT", "false").lower() == "true"
        self.agent = None
        self.data_source = DataSource()
        self.monitor: Optional[RealtimeMonitor] = None
        self._monitor_signature: Optional[str] = None
        self._news_section_cache_text = ""
        self._news_section_cache_ts: Optional[datetime] = None
        self._news_section_cache_sec = max(
            60,
            int(os.environ.get("NEWS_SECTION_CACHE_SEC", "180") or "180"),
        )
        self._intraday_focus_news_cache_text = ""
        self._intraday_focus_news_cache_key = ""
        self._intraday_focus_news_cache_ts: Optional[datetime] = None
        self._intraday_focus_news_cache_sec = max(
            60,
            int(os.environ.get("INTRADAY_MX_CACHE_SEC", "180") or "180"),
        )
        
        market_brief_times = os.environ.get("MARKET_BRIEF_PUSH_TIMES", "09:00,15:00")
        trap_times = os.environ.get(
            "INTRADAY_TRAP_PUSH_TIMES",
            "09:45,10:00,10:30,10:45,11:30,13:15,13:45,14:15,14:30,14:45,15:00",
        )
        enable_news_report = str(os.environ.get("ENABLE_NEWS_REPORT", "false")).lower() == "true"
        news_time = os.environ.get("NEWS_REPORT_TIME", "")
        self.enable_cls_news_alerts = str(os.environ.get("ENABLE_CLS_NEWS_ALERTS", "false")).lower() == "true"
        self.enable_trade_check_push = str(os.environ.get("ENABLE_TRADE_CHECK_PUSH", "false")).lower() == "true"
        
        self.trade_check_times = []
        if self.enable_trade_check_push:
            for t in self._split_time_items(market_brief_times):
                if not str(t or "").strip():
                    continue
                try:
                    h, m = map(int, t.split(":"))
                    if m + 5 >= 60:
                        self.trade_check_times.append((h + 1, m + 5 - 60))
                    else:
                        self.trade_check_times.append((h, m + 5))
                except Exception:
                    pass

        self.push_times = []
        for t in self._split_time_items(market_brief_times):
            try:
                h, m = map(int, t.split(":"))
                self.push_times.append((h, m))
            except:
                logger.warning(f"无效的推送时间: {t}")

        self.news_report_time = None
        if enable_news_report and str(news_time or "").strip():
            try:
                h, m = map(int, news_time.split(":"))
                self.news_report_time = (h, m)
            except Exception:
                logger.warning(f"无效的新闻推送时间: {news_time}")
        
        self.pool_update_times = [
            (9, 20, False),
            (13, 0, True),
            (15, 20, True),
        ]
        self.intraday_trap_times = self._parse_time_list(trap_times)
        
        if not self.push_times:
            self.push_times = [(9, 0), (15, 0)]
        
        cache_dir = os.environ.get("QUANT_CACHE_DIR", "./runtime/data")
        os.makedirs(cache_dir, exist_ok=True)
        self._us_market_cache_path = os.path.join(cache_dir, "us_market_cache.json")
        self.cls_news_poll_interval_sec = max(
            30,
            int(os.environ.get("CLS_NEWS_POLL_INTERVAL_SEC", "30") or "30"),
        )
        self.cls_news_symbol = str(os.environ.get("CLS_NEWS_SYMBOL", "重点") or "重点").strip()
        self.cls_news_alert_level = str(os.environ.get("CLS_NEWS_ALERT_LEVEL", "important") or "important").strip()
        self.cls_news_last_poll_ts = 0.0
        
        self.running = True

    def _parse_time_list(self, text: str) -> List[tuple]:
        """解析时间列表"""
        result: List[tuple] = []
        for item in str(text or "").split(","):
            raw = item.strip()
            if not raw:
                continue
            try:
                hour, minute = map(int, raw.split(":"))
                result.append((hour, minute))
            except Exception:
                logger.warning(f"无效的盘中诱多诱空推送时间: {raw}")
        return result

    @staticmethod
    def _split_time_items(text: str) -> List[str]:
        """解析逗号分隔的时间文本。"""
        return [item.strip() for item in str(text or "").split(",") if item.strip()]
    
    def _init_agent(self):
        """初始化 AI Agent"""
        try:
            from agents import init_quant_agent
            
            api_key = os.environ.get("SILICONFLOW_API_KEY", "")
            if not api_key:
                logger.warning("未配置 SILICONFLOW_API_KEY，禁用 AI Agent")
                self.enable_agent = False
                return
            
            self.agent = init_quant_agent(api_key=api_key)
            logger.info("AI Agent 初始化成功")
            
        except Exception as e:
            logger.error(f"AI Agent 初始化失败: {e}")
            self.enable_agent = False

    def _get_agent(self):
        """??? AI Agent"""
        if not self.enable_agent:
            return None
        if self.agent is None:
            self._init_agent()
        return self.agent

    def _get_monitor(self, etf_count: int = 5, stock_count: int = 5, reload_pool: bool = False) -> RealtimeMonitor:
        """????????"""
        signature = f"{etf_count}|{stock_count}"
        if self.monitor is None or self._monitor_signature != signature:
            self.monitor = RealtimeMonitor(
                data_source=self.data_source,
                etf_count=etf_count,
                stock_count=stock_count,
                db_path=os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"),
            )
            self._monitor_signature = signature
        if reload_pool:
            self.monitor.reload_pool()
        self.monitor.clear_runtime_cache()
        return self.monitor

    def fetch_us_market(self):
        """抓取并缓存美股夜盘数据（美股收盘后约04:00执行）"""
        try:
            from agents.tools.tradingagents_tools import _fetch_us_realtime
            data = _fetch_us_realtime(["SPY", "QQQ", "IWM", "DIA"])
            if not data:
                logger.warning("美股数据获取失败")
                return False
            
            import json
            cache = {
                "fetch_time": datetime.now().isoformat(),
                "data": data,
            }
            with open(self._us_market_cache_path, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)
            logger.info(f"美股数据已缓存: {[d['code'] for d in data]}")
            return True
        except Exception as e:
            logger.error(f"美股数据缓存失败: {e}")
            return False

    def get_cached_us_market(self) -> Optional[Dict]:
        """读取美股缓存数据"""
        try:
            if not os.path.exists(self._us_market_cache_path):
                return None
            import json
            with open(self._us_market_cache_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    
    def signal_handler(self, sig, frame):
        """处理退出信号"""
        logger.info("收到停止信号，正在退出...")
        self.running = False
    
    def should_push(self, hour, minute):
        """检查是否应该推送"""
        for h, m in self.push_times:
            if hour == h and minute == m:
                return True
        return False
    
    def should_trade_check(self, hour, minute):
        """检查是否应该执行交易检查"""
        for h, m in self.trade_check_times:
            if hour == h and minute == m:
                return True
        return False
    
    def should_news_report(self, hour, minute):
        """检查是否应该执行新闻报告"""
        if self.news_report_time:
            h, m = self.news_report_time
            if hour == h and minute == m:
                return True
        return False

    def should_intraday_trap_push(self, hour, minute) -> bool:
        """检查是否应该执行盘中诱多/诱空推送"""
        for h, m in self.intraday_trap_times:
            if hour == h and minute == m:
                return True
        return False
    
    def get_pool_update_mode(self, hour, minute) -> Optional[bool]:
        """检查是否应该更新股票池，并返回是否合并模式"""
        for h, m, merge_existing in self.pool_update_times:
            if hour == h and minute == m:
                return merge_existing
        return None
    
    def update_stock_pool(self, merge_existing: bool = False):
        """更新每日股票池"""
        try:
            logger.info("开始更新每日股票池...")
            db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
            generator = get_pool_generator(db_path)
            result = generator.update_daily(merge_existing=merge_existing)
            
            etf_count = len(result.get("etf_lof", []))
            stock_count = len(result.get("stock", []))
            merged_count = len(result.get("merged", []))
            logger.info(
                f"股票池更新完成: ETF/LOF {etf_count} 只, 热点股票 {stock_count} 只, "
                f"合并后 {merged_count} 只"
            )
            
        except Exception as e:
            logger.error(f"股票池更新失败: {e}")

    def refresh_signal_pool(self, etf_count: int = 5, stock_count: int = 5, reload_pool: bool = True) -> Dict[str, int]:
        """独立刷新信号池，不依赖推送成功。"""
        logger.info("开始刷新信号池...")
        monitor = self._get_monitor(etf_count=etf_count, stock_count=stock_count, reload_pool=reload_pool)
        if reload_pool and self._should_refresh_pool(etf_count=etf_count, stock_count=stock_count, monitor=monitor):
            logger.info("动态股票池过旧或数量过少，先刷新股票池后重试信号扫描")
            self.update_stock_pool(merge_existing=False)
            monitor.reload_pool()
        results = monitor.scan_market()
        refresh_result = self.recorder.refresh_signal_pool(results["etf"], results["stock"])
        recommend_ids = self.recorder.save_recommends(results["etf"], results["stock"], refresh_pool=False)
        refresh_result["recommend_count"] = len(recommend_ids)
        self._refresh_symbol_news_contexts(signal_rows=(results.get("etf", []) + results.get("stock", [])))
        auto_buy_result = self._auto_buy_from_signals(results=results, monitor=monitor)
        self._refresh_symbol_news_contexts(signal_rows=(results.get("etf", []) + results.get("stock", [])))
        refresh_result["auto_buy_count"] = len(auto_buy_result.get("positions", []) or [])
        refresh_result["auto_buy_result"] = auto_buy_result
        logger.info(
            f"信号池刷新完成: ETF {refresh_result['etf_count']} 条, "
            f"A股 {refresh_result['stock_count']} 条, 买入 {refresh_result['buy_count']} 条, "
            f"荐股 {refresh_result.get('recommend_count', 0)} 条, "
            f"自动买入 {refresh_result.get('auto_buy_count', 0)} 条"
        )
        return refresh_result

    def _build_buy_decision_payload(self, results: Dict[str, List], monitor: RealtimeMonitor) -> Dict[str, object]:
        """构建 AI 自动买入决策所需上下文。"""
        buy_signals = [
            signal
            for signal in (results.get("etf", []) + results.get("stock", []))
            if str(getattr(signal, "signal_type", "") or "").strip() == "买入"
        ]
        signal_lines = []
        for signal in buy_signals[:10]:
            signal_lines.append(
                f"- {signal.code} {signal.name} | 现价{float(signal.price or 0.0):.3f} | "
                f"评分{float(signal.score or 0.0):.2f} | 理由:{str(signal.reason or '')}"
            )

        sentiment = (
            f"市场模式: {getattr(monitor, '_runtime_mode_label', '自动')} -> "
            f"{getattr(monitor, '_effective_market_regime', 'normal')} | "
            f"{getattr(monitor, '_effective_market_regime_reason', '')}\n"
            f"大盘情绪: {float(getattr(monitor, '_market_emotion_score', 0.0) or 0.0):.0f} "
            f"({str(getattr(monitor, '_market_emotion_cycle', '') or '')})\n"
            f"空间板: {float(getattr(monitor, '_space_score', 0.0) or 0.0):.0f} "
            f"({str(getattr(monitor, '_space_level', '') or '')})"
        )
        holdings = self.trader.db.get_holdings_aggregated()
        holding_lines = []
        for item in holdings:
            holding_lines.append(
                f"- {item.get('code', '')} {item.get('name', '')} | "
                f"成本{float(item.get('avg_buy_price', 0.0) or 0.0):.3f} | "
                f"收益率{float(item.get('total_pnl_pct', 0.0) or 0.0):+.2f}%"
            )

        us_analysis = ""
        try:
            cached_us_market = self.get_cached_us_market() or {}
            market_rows = cached_us_market.get("data", []) if isinstance(cached_us_market, dict) else []
            if market_rows:
                parts = []
                for row in market_rows[:4]:
                    parts.append(
                        f"{row.get('code', '')}:{float(row.get('change_pct', 0.0) or 0.0):+.2f}%"
                    )
                us_analysis = " | ".join(parts)
        except Exception:
            us_analysis = ""

        return {
            "signals": "\n".join(signal_lines) if signal_lines else "暂无买入候选",
            "sentiment": sentiment,
            "holdings": "\n".join(holding_lines) if holding_lines else "当前空仓",
            "us_analysis": us_analysis or "(暂无外围缓存)",
        }

    def _auto_buy_from_signals(self, results: Dict[str, List], monitor: RealtimeMonitor) -> Dict[str, object]:
        """基于信号池执行自动模拟买入，AI 仅做事后提示。"""
        buy_signals = [
            signal
            for signal in (results.get("etf", []) + results.get("stock", []))
            if str(getattr(signal, "signal_type", "") or "").strip() == "买入"
        ]
        if not buy_signals:
            logger.info("当前无买入信号，跳过自动模拟买入")
            return {"action": "skip", "reason": "no_buy_signals", "positions": []}

        result = self.recorder.auto_buy(ai_decision=None)
        logger.info(f"自动模拟买入结果(严格执行量化信号): {_safe_preview(result, max_len=800)}")
        self._refresh_position_ai_hints(monitor=monitor, latest_action="buy")
        return result

    def _refresh_symbol_news_contexts(self, signal_rows: Optional[List[object]] = None) -> Dict[str, object]:
        """刷新持仓与信号池标的的结构化资讯缓存。"""
        try:
            from agents.tools.news_router import build_watchlist_news_digest
        except Exception as e:
            logger.warning(f"加载资讯路由失败: {e}")
            return {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "items": {}}

        items: Dict[str, Dict[str, str]] = {}
        raw_entries: List[Dict[str, str]] = []

        for row in self.trader.db.get_holdings_aggregated()[:10]:
            code = str(row.get("code", "") or "").strip()
            name = str(row.get("name", "") or "").strip()
            if code:
                raw_entries.append({"code": code, "name": name, "source": "holding"})

        if signal_rows:
            for row in signal_rows[:10]:
                code = str(getattr(row, "code", "") or "").strip()
                name = str(getattr(row, "name", "") or "").strip()
                if code:
                    raw_entries.append({"code": code, "name": name, "source": "signal_pool"})

        seen = set()
        entries: List[Dict[str, str]] = []
        for item in raw_entries:
            cache_key = str(item.get("code", "") or "").strip()
            if not cache_key or cache_key in seen:
                continue
            entries.append(item)
            seen.add(cache_key)

        for item in entries:
            code = str(item.get("code", "") or "").strip()
            name = str(item.get("name", "") or "").strip()
            source = str(item.get("source", "") or "").strip()
            text = build_watchlist_news_digest([{"code": code, "name": name}], limit=6, title="标的资讯")
            lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
            conclusion_lines: List[str] = []
            in_conclusion = False
            for line in lines:
                if line == "【结论】":
                    in_conclusion = True
                    continue
                if in_conclusion and line.startswith("【"):
                    break
                if in_conclusion:
                    conclusion_lines.append(line.lstrip("- ").strip())

            summary = "；".join(conclusion_lines[:2]) if conclusion_lines else ""
            items[code] = {
                "code": code,
                "name": name,
                "source": source,
                "news_text": text,
                "news_summary": summary,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        payload = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "items": items,
        }
        self.trader.db.set_dashboard_cache("symbol_news_contexts", payload)
        return payload

    def _refresh_position_ai_hints(self, monitor: Optional[RealtimeMonitor] = None, latest_action: str = "") -> Dict[str, object]:
        """为当前持仓生成 AI 辅助提示，并缓存到看板数据库。"""
        holdings = self.trader.db.get_holdings_aggregated()
        if not holdings:
            payload = {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "items": {}}
            self.trader.db.set_dashboard_cache("position_ai_hints", payload)
            self._refresh_symbol_news_contexts(signal_rows=[])
            return payload

        monitor = monitor or self._get_monitor(etf_count=1, stock_count=1, reload_pool=False)
        hints: Dict[str, Dict[str, str]] = {}
        agent = self._get_agent() if self.enable_agent else None

        for item in holdings[:10]:
            code = str(item.get("code", "") or "").strip()
            name = str(item.get("name", "") or "").strip()
            if not code:
                continue

            signal = monitor.analyze_stock(code, name, is_stock=not code.startswith(("5", "1")))
            rule_hint = "暂无量化提示"
            if signal is not None:
                rule_hint = (
                    f"{signal.signal_type} | {signal.reason or '暂无原因'} | "
                    f"评分 {float(signal.score or 0.0):.2f}"
                )

            ai_hint = rule_hint
            if agent and signal is not None:
                try:
                    prompt = (
                        "你是A股持仓辅助决策助手。请根据给定的持仓、量化信号和市场环境，"
                        "用一句中文给出简洁提示，重点说明继续持有、观察加仓还是谨慎减仓。"
                        "不要输出 JSON，不要分点，控制在40字以内。\n\n"
                        f"最近动作: {latest_action or 'hold'}\n"
                        f"持仓: {code} {name}\n"
                        f"成本: {float(item.get('avg_buy_price', 0.0) or 0.0):.3f}\n"
                        f"当前收益率: {float(item.get('total_pnl_pct', 0.0) or 0.0):+.2f}%\n"
                        f"市场模式: {getattr(monitor, '_runtime_mode_label', '自动')} -> "
                        f"{getattr(monitor, '_effective_market_regime', 'normal')}\n"
                        f"量化信号: {signal.signal_type}\n"
                        f"量化原因: {signal.reason}\n"
                        f"量化评分: {float(signal.score or 0.0):.2f}"
                    )
                    result = agent.run(
                        task=prompt,
                        timeout_sec=30,
                        operation_name=f"持仓提示 {code}",
                    )
                    text = agent.extract_text(result).strip()
                    if text and "失败" not in text and "超时" not in text:
                        ai_hint = text
                except Exception as e:
                    logger.warning(f"持仓 AI 提示生成失败 {code}: {e}")

            hints[code] = {
                "code": code,
                "name": name,
                "ai_hint": ai_hint,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        payload = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "items": hints,
        }
        self.trader.db.set_dashboard_cache("position_ai_hints", payload)
        self._refresh_symbol_news_contexts(signal_rows=[])
        return payload

    def _should_refresh_pool(self, etf_count: int, stock_count: int, monitor: RealtimeMonitor) -> bool:
        """判断刷新信号池前是否需要先重建股票池。"""
        if not monitor.etf_pool and not monitor.stock_pool:
            return True

        try:
            db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
            generator = get_pool_generator(db_path)
            summary = generator.get_pool_summary()
            total = int(summary.get("total", 0) or 0)
            minimum_total = max(10, int(etf_count or 0) + int(stock_count or 0))
            if total < minimum_total:
                logger.info(f"股票池数量过少({total})，低于阈值 {minimum_total}")
                return True

            updated_text = str(summary.get("updated", "") or "").strip()
            if updated_text:
                updated_at = datetime.strptime(updated_text, "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - updated_at).total_seconds() > 12 * 3600:
                    logger.info(f"股票池更新时间过旧: {updated_text}")
                    return True
        except Exception as e:
            logger.warning(f"检查股票池状态失败，转为保守刷新: {e}")
            return True

        return False
    
    def news_report(self):
        """执行综合新闻报告"""
        if not self.enable_agent:
            logger.info("AI Agent 已在配置中禁用，跳过新闻报告")
            return

        agent = self._get_agent()
        if not agent:
            logger.warning("AI Agent 初始化失败，跳过新闻报告")
            return
        
        try:
            logger.info("开始执行综合新闻报告...")
            agent_result = agent.run_news_report()
            logger.info(f"新闻报告完成: {_safe_preview(agent_result)}...")
        except Exception as e:
            logger.error(f"新闻报告失败: {e}")
    
    def poll_cls_news(self):
        """轮询财联社快讯并推送高优先级提醒"""
        try:
            from agents.tools.cls_news import (
                filter_cls_news_by_level,
                format_cls_alert,
                poll_cls_telegraph,
            )
            from trading import get_pusher

            new_items = poll_cls_telegraph(symbol=self.cls_news_symbol, limit=20)
            if new_items:
                logger.info(f"财联社新增快讯 {len(new_items)} 条")
                alert_items = filter_cls_news_by_level(new_items, min_level=self.cls_news_alert_level)
                if alert_items:
                    first_item = alert_items[0]
                    category_label = str(first_item.get("category_label", "市场快讯")).strip() or "市场快讯"
                    title = f"盘中快讯·{category_label}"
                    alert_text = format_cls_alert(alert_items, limit=3)
                    get_pusher().push(title, alert_text, sound="minuet", level="active")
            return new_items
        except Exception as e:
            logger.warning(f"财联社快讯轮询失败: {e}")
            return []

    def _get_emotion_summary(self) -> str:
        """获取快速市场情绪摘要（大盘+板块，基于全市场扫描，无个股遍历）"""
        try:
            from data.data_source import DataSource
            import akshare as ak

            data_source = DataSource()
            try:
                df = data_source.get_a_share_market_snapshot()
            finally:
                data_source.close()

            if df is None or df.empty:
                return ""

            change_col = "change_rate" if "change_rate" in df.columns else "涨跌幅"
            amount_col = "turnover" if "turnover" in df.columns else "成交额"
            zt_count = int((df[change_col] >= 9.5).sum())
            dt_count = int((df[change_col] <= -9.5).sum())
            up_count = int((df[change_col] > 0).sum())
            down_count = int((df[change_col] < 0).sum())
            total_amount = float(df[amount_col].sum() / 1e8) if amount_col in df.columns else 0.0

            zt_dt_net = zt_count - dt_count
            if zt_dt_net >= 50:
                cycle = "崩溃预警"
                cycle_score = 95
            elif zt_dt_net >= 30:
                cycle = "高潮"
                cycle_score = 80
            elif zt_dt_net >= 10:
                cycle = "主升"
                cycle_score = 65
            elif zt_dt_net >= 0:
                cycle = "修复"
                cycle_score = 50
            elif zt_dt_net >= -10:
                cycle = "冰点-修复"
                cycle_score = 35
            else:
                cycle = "冰点"
                cycle_score = 20

            try:
                sector_df = ak.stock_sector_fund_flow_rank(indicator="今日")
                hot_sectors = []
                if sector_df is not None and not sector_df.empty:
                    if "名称" in sector_df.columns and "今日主力净流入-净额" in sector_df.columns:
                        hot_sectors = sector_df.sort_values(
                            "今日主力净流入-净额", ascending=False
                        )["名称"].head(3).tolist()
            except Exception:
                hot_sectors = []

            parts = [
                f"涨停{zt_count} | 跌停{dt_count} | 上涨{up_count} | 下跌{down_count} | "
                f"{cycle}({cycle_score}) | 成交{total_amount:.0f}亿"
            ]
            if hot_sectors:
                parts.append(f"热门: {', '.join(hot_sectors)}")

            return " | ".join(parts)

        except Exception as e:
            logger.warning(f"情绪摘要获取失败: {e}")
            return ""

    def _build_news_section(self) -> str:
        """构建资讯区块。"""
        if (
            self._news_section_cache_text
            and self._news_section_cache_ts is not None
            and (datetime.now() - self._news_section_cache_ts).total_seconds() < self._news_section_cache_sec
        ):
            return self._news_section_cache_text

        blocks: List[NewsReportBlock] = []

        market_news_text = self._build_market_news_section()
        if market_news_text:
            blocks.append(NewsReportBlock(title="市场资讯", content=market_news_text))

        watchlist_news_text = self._build_watchlist_news_section()
        if watchlist_news_text:
            blocks.append(NewsReportBlock(title="持仓/信号池", content=watchlist_news_text))

        cls_text = ""
        try:
            from agents.tools.cls_news import get_cls_telegraph_news

            cls_text = get_cls_telegraph_news.invoke({"symbol": self.cls_news_symbol, "limit": 6}) or ""
            if cls_text:
                blocks.append(NewsReportBlock(title="财联社快讯", content=str(cls_text)))
        except Exception as e:
            logger.warning(f"财联社快讯获取失败: {e}")

        if not market_news_text:
            try:
                from agents.tools.global_news import get_global_finance_news

                global_text = _safe_preview(get_global_finance_news.invoke({}) or "", max_len=1200)
                if global_text:
                    blocks.append(NewsReportBlock(title="全球市场", content=global_text))
            except Exception as e:
                logger.warning(f"全球市场资讯获取失败: {e}")

        emotion_summary = self._get_emotion_summary()
        if emotion_summary:
            blocks.append(NewsReportBlock(title="A股情绪", content=emotion_summary))

        section_text = format_news_section(blocks=blocks)
        self._news_section_cache_text = section_text
        self._news_section_cache_ts = datetime.now()
        return section_text

    def _build_market_brief_section(self) -> str:
        """构建早晚固定推送的外围简报。"""
        try:
            from agents.tools.global_news import get_global_finance_news

            global_text = str(get_global_finance_news.invoke({}) or "").strip()
            if not global_text:
                global_text = "暂无新的外围市场信息"
            return _summarize_news_with_agent("外围市场简报", global_text)
        except Exception as e:
            logger.warning(f"外围市场简报构建失败: {e}")
            return "暂无新的外围市场信息"

    def _build_close_review_section(self) -> str:
        """构建收盘复盘摘要区块。"""
        try:
            report = build_runtime_review_report_from_db(os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"))
            save_runtime_review_report(report)
            report_text = str(report.get("report_text", "") or "").strip()
            if not report_text:
                return ""
            summary = _summarize_news_with_agent(
                "收盘复盘",
                (
                    "请把下面的综合复盘报告压缩成 6-8 条中文要点，"
                    "重点保留：持仓变化、活跃信号、失效信号、交易事件、复盘结论。"
                    "不要输出 JSON。\n\n"
                    f"{report_text}"
                ),
            )
            return f"【收盘复盘摘要】\n{summary}".strip()
        except Exception as e:
            logger.warning(f"收盘复盘摘要构建失败: {e}")
            return ""

    def _build_market_news_section(self) -> str:
        """构建市场资讯补充区块。"""
        try:
            from agents.tools.news_router import build_market_news_digest

            query = "A股最新政策、宏观新闻、行业热点、海外市场影响、监管变化"
            market_text = build_market_news_digest(query=query, limit=6)
            return _summarize_news_with_agent("市场资讯", market_text) if market_text else ""
        except Exception as e:
            logger.warning(f"市场资讯获取失败: {e}")
            return ""

    def _get_intraday_watchlist(self) -> List[Dict]:
        """获取盘中需要重点跟踪的持仓与信号池标的。"""
        holdings = self.trader.db.get_holdings_aggregated()
        signal_pool = self.trader.db.get_signal_pool(limit=8)
        watchlist: List[Dict] = []
        seen_codes = set()

        for item in holdings[:6]:
            code = str(item.get("code", "")).strip()
            if not code or code in seen_codes:
                continue
            watchlist.append(
                {
                    "code": code,
                    "name": str(item.get("name", "")).strip(),
                    "source": "holding",
                }
            )
            seen_codes.add(code)

        for item in signal_pool[:6]:
            code = str(item.get("code", "")).strip()
            if not code or code in seen_codes:
                continue
            watchlist.append(
                {
                    "code": code,
                    "name": str(item.get("name", "")).strip(),
                    "source": "signal_pool",
                }
            )
            seen_codes.add(code)

        return watchlist

    def _build_watchlist_news_section(self) -> str:
        """构建持仓与信号池资讯补充区块。"""
        watchlist = self._get_intraday_watchlist()
        if not watchlist:
            return ""

        try:
            from agents.tools.news_router import build_watchlist_news_digest

            text = build_watchlist_news_digest(watchlist[:8], limit=6)
            return _summarize_news_with_agent("持仓/信号池", text) if text else ""
        except Exception as e:
            logger.warning(f"持仓/信号池资讯获取失败: {e}")
            return ""

    def _build_holdings_snapshot(self, monitor: RealtimeMonitor) -> str:
        """组装持仓分析区块"""
        holdings = self.trader.db.get_holdings_aggregated()
        rows: List[HoldingReportRow] = []
        for holding in holdings:
            code = str(holding.get("code", ""))
            name = str(holding.get("name", ""))
            latest_price = float(holding.get("avg_current_price") or holding.get("avg_buy_price") or 0.0)
            pnl_pct = float(holding.get("total_pnl_pct") or 0.0)
            target_price = float(holding.get("target_price") or 0.0)
            stop_loss = float(holding.get("stop_loss") or 0.0)
            signal = monitor.analyze_stock(code, name, is_stock=not code.startswith(("5", "1")))

            factor_text = "量化因子: 暂无"
            tech_text = "技术面: 暂无"
            fund_text = "资金面: 暂无"
            emotion_text = "情绪面: 暂无"
            fundamental_text = get_stock_fundamental_summary(code)
            if signal:
                factor_text = f"量化因子: {signal.reason or '无'}"
                tech_text = f"技术面: {signal.signal_type} | 双重信号={'是' if signal.dual_signal else '否'}"
                fund_text = (
                    f"资金面: FCF={signal.fcf:+.2f} | "
                    f"盘口{signal.order_book_bias or '暂无'}({signal.order_book_ratio:+.2f}) | "
                    f"买卖盘{signal.bid_volume_sum:.0f}/{signal.ask_volume_sum:.0f}"
                )
                emotion_text = f"情绪面: 市场{signal.market_emotion_score:.0f}/个股{signal.stock_emotion_score:.0f}"
                if signal.concept_name:
                    emotion_text += f"/概念{signal.concept_name}({signal.concept_strength_score:.2f})"

            rows.append(
                HoldingReportRow(
                    code=code,
                    name=name,
                    latest_price=latest_price,
                    pnl_pct=pnl_pct,
                    target_price=target_price,
                    stop_loss=stop_loss,
                    factor_text=factor_text,
                    fundamental_text=fundamental_text,
                    tech_text=tech_text,
                    fund_text=fund_text,
                    emotion_text=emotion_text,
                )
            )

        return format_holdings_section(rows)

    def _build_decision_section(self, monitor: RealtimeMonitor, ai_decision: Optional[Dict]) -> str:
        """组装决策分析区块"""
        holdings = self.trader.db.get_holdings_aggregated()
        buy_list = set(ai_decision.get("buy_list", [])) if ai_decision else set()
        add_list = set(ai_decision.get("add_list", [])) if ai_decision else set()
        skip_list = set(ai_decision.get("skip_list", [])) if ai_decision else set()

        rows: List[DecisionReportRow] = []
        for holding in holdings:
            code = str(holding.get("code", ""))
            name = str(holding.get("name", ""))
            pnl_pct = float(holding.get("total_pnl_pct") or 0.0)
            signal = monitor.analyze_stock(code, name, is_stock=not code.startswith(("5", "1")))

            action = "保持不变"
            reasons: List[str] = []
            if signal:
                reasons.append(signal.reason or "无明确信号")
                if signal.signal_type == "卖出":
                    action = "清仓"
                elif code in add_list or (signal.signal_type == "买入" and pnl_pct > 0):
                    action = "加仓"

            if code in skip_list:
                action = "保持不变"
                reasons.append("AI 决策跳过")
            if code in buy_list or code in add_list:
                reasons.append("AI 决策支持")

            if not reasons:
                reasons.append("暂无额外说明")
            rows.append(
                DecisionReportRow(
                    code=code,
                    name=name,
                    action=action,
                    reasons=reasons,
                )
            )

        return format_decision_section(rows)

    def _build_signal_section(self, etf_recs: List[Dict], stock_recs: List[Dict]) -> str:
        """组装信号推荐区块"""
        return format_signal_section(etf_recs, stock_recs)

    def _build_mobile_push_body(
        self,
        news_section: str,
        holdings_section: str,
        decision_section: str,
        signal_section: str,
        review_section: str,
    ) -> str:
        """组装移动端 Bark 推送正文（完整信息版）"""
        parts: List[str] = [f"时间 {datetime.now().strftime('%m-%d %H:%M')}"]
        for section in [
            news_section,
            holdings_section,
            decision_section,
            signal_section,
            review_section,
        ]:
            text = str(section or "").strip()
            if text:
                parts.append(text)
        return "\n\n".join(parts)

    def _build_push_outline(
        self,
        holdings_section: str,
        signal_section: str,
        review_section: str,
        trade_count: int = 0,
    ) -> str:
        """生成综合报告目录式开头"""
        holdings_count = max(sum(1 for line in str(holdings_section).splitlines() if "|" in line), 0)
        signal_count = sum(1 for line in str(signal_section).splitlines() if line.strip().startswith("| ") and not line.strip().startswith("| :"))
        signal_count = max(signal_count - 1, 0)

        lines = [
            "【目录】",
            f"持仓标的: {holdings_count}",
            f"信号条数: {signal_count}",
            f"历史卖出: {trade_count}",
            "以下为完整详细报告",
        ]
        return "\n".join(lines)

    def _get_push_title(self, now: datetime) -> str:
        """生成分层的推荐推送标题"""
        hour = int(now.hour)
        time_text = now.strftime("%m-%d %H:%M")
        if hour < 11:
            return f"盘前推荐 {time_text}"
        if hour < 15:
            return f"午盘跟踪 {time_text}"
        return f"收盘复盘 {time_text}"

    def _get_market_brief_title(self, now: datetime) -> str:
        """生成外围简报标题。"""
        return f"{'早盘' if now.hour < 12 else '收盘'}外围简报 {now.strftime('%m-%d %H:%M')}"

    def _get_trade_check_title(self, now: datetime) -> str:
        """生成分层的交易检查标题"""
        hour = int(now.hour)
        time_text = now.strftime("%m-%d %H:%M")
        if hour < 11:
            return f"盘中检查 {time_text}"
        if hour < 15:
            return f"午后检查 {time_text}"
        return f"收盘检查 {time_text}"

    def _get_intraday_alert_title(self, now: datetime, trap_type: str = "neutral") -> str:
        """生成统一风格的盘中预警标题"""
        label_map = {
            "fake_up": "诱多",
            "fake_down": "诱空",
            "chaotic": "震荡",
            "true_break": "真突破",
            "true_drop": "真走弱",
            "neutral": "观察",
            "no_data": "数据不足",
        }
        label = label_map.get(str(trap_type or "neutral"), "观察")
        return f"盘中预警·{label} {now.strftime('%m-%d %H:%M')}"

    def _build_review_section(self) -> str:
        """组装回测复盘区块"""
        stats = self.trader.db.get_statistics()
        raw_trades = self.trader.db.get_trade_history(days=5)
        trades = [
            ReviewTradeRow(
                date=str(t.get("date", "")),
                code=str(t.get("code", "")),
                direction=str(t.get("direction", "")),
                price=float(t.get("price", 0) or 0.0),
                pnl=float(t.get("pnl", 0) or 0.0),
            )
            for t in raw_trades
        ]
        proxy_diff = self._build_concept_proxy_diff()
        return format_review_section(stats=stats, trades=trades, proxy_diff_rows=proxy_diff)

    def _build_concept_proxy_diff(self) -> List[ProxyDiffRow]:
        """比较回测主线强度代理与实盘概念强度差异"""
        holdings = self.trader.db.get_holdings_aggregated()
        if not holdings:
            return []

        try:
            from backtest.engine import _calc_concept_proxy_score

            data_source = DataSource()
            today = datetime.now()
            start_date = (today - timedelta(days=120)).strftime("%Y%m%d")
            end_date = today.strftime("%Y%m%d")
            price_data: Dict[str, object] = {}
            rows: List[ProxyDiffRow] = []
            try:
                for holding in holdings:
                    code = str(holding.get("code", ""))
                    if not code:
                        continue
                    df = data_source.get_kline(code, start_date, end_date)
                    if df is None or df.empty:
                        continue
                    if "date" in df.columns:
                        df["date"] = df["date"].astype("datetime64[ns]")
                        df = df.set_index("date")
                    price_data[code] = df

                monitor = self._get_monitor(etf_count=1, stock_count=1, reload_pool=False)
                for holding in holdings:
                    code = str(holding.get("code", ""))
                    name = str(holding.get("name", ""))
                    if code not in price_data:
                        continue
                    signal = monitor.analyze_stock(code, name, is_stock=not code.startswith(("5", "1")))
                    real_score = float(signal.concept_strength_score) if signal else 0.0
                    real_name = signal.concept_name if signal else ""
                    proxy_score = float(_calc_concept_proxy_score(code, pd.to_datetime(end_date), price_data))
                    diff = real_score - proxy_score
                    rows.append(
                        ProxyDiffRow(
                            code=code,
                            name=name,
                            real_concept_name=real_name or "-",
                            real_score=real_score,
                            proxy_score=proxy_score,
                            diff_score=diff,
                        )
                    )
            finally:
                data_source.close()
            return rows[:5]
        except Exception as e:
            logger.debug(f"主线强度偏差构建失败: {e}")
            return []

    @staticmethod
    def _resolve_intraday_bias(signal) -> str:
        """根据实时信号给出盘中偏向描述"""
        if signal is None:
            return "观察"
        if signal.signal_type == "买入":
            return "偏多"
        if signal.signal_type == "卖出":
            return "偏空"
        if float(signal.score or 0.0) >= 0.6:
            return "偏强"
        return "中性"

    def _collect_intraday_focus_targets(self, monitor: RealtimeMonitor) -> Dict[str, List[Dict]]:
        """????????????????"""
        holdings = self.trader.db.get_holdings_aggregated()
        signal_pool = self.trader.db.get_signal_pool(limit=12)
        holding_codes = {str(item.get("code", "")) for item in holdings}

        def build_entry(item: Dict, source: str) -> Optional[Dict]:
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip()
            if not code:
                return None
            signal = monitor.analyze_stock(code, name, is_stock=not code.startswith(("5", "1")))
            if signal is None:
                return None
            return {
                "source": source,
                "code": code,
                "name": name,
                "signal_type": signal.signal_type,
                "bias": self._resolve_intraday_bias(signal),
                "price": float(signal.price or 0.0),
                "change_pct": float(signal.change_pct or 0.0),
                "score": float(signal.score or 0.0),
                "reason": str(signal.reason or ""),
                "fcf": float(signal.fcf or 0.0),
                "market_emotion_score": float(signal.market_emotion_score or 0.0),
                "stock_emotion_score": float(signal.stock_emotion_score or 0.0),
                "concept_strength_score": float(signal.concept_strength_score or 0.0),
                "concept_name": str(signal.concept_name or ""),
                "order_book_bias": str(signal.order_book_bias or ""),
                "order_book_ratio": float(signal.order_book_ratio or 0.0),
            }

        holding_rows = []
        for item in holdings[:8]:
            entry = build_entry(item, "holding")
            if entry:
                holding_rows.append(entry)

        signal_rows = []
        for item in signal_pool:
            if str(item.get("code", "")) in holding_codes:
                continue
            entry = build_entry(item, "signal_pool")
            if entry:
                signal_rows.append(entry)
            if len(signal_rows) >= 8:
                break

        return {"holdings": holding_rows, "signal_pool": signal_rows}

    def _format_intraday_focus_section(self, title: str, rows: List[Dict]) -> str:
        """格式化盘中关注标的区块"""
        lines = [f"【{title}】"]
        if not rows:
            lines.append("暂无数据")
            return "\n".join(lines)

        for row in rows:
            concept_text = row.get("concept_name") or "-"
            order_book_bias = row.get("order_book_bias") or "暂无"
            order_book_ratio = float(row.get("order_book_ratio", 0.0) or 0.0)
            lines.append(
                f"- {row['code']} {row['name']} | {row['bias']} | 盘口{order_book_bias}({order_book_ratio:+.2f}) | "
                f"信号{row['signal_type']} | 现价{row['price']:.2f} | 涨跌{row['change_pct']:+.2f}% | 评分{row['score']:.2f}"
            )
            lines.append(
                f"  理由: {row['reason']} | FCF {row['fcf']:+.2f} | "
                f"情绪 {row['market_emotion_score']:.0f}/{row['stock_emotion_score']:.0f} | "
                f"概念 {concept_text}({row['concept_strength_score']:.2f})"
            )
        return "\n".join(lines)

    def _build_intraday_news_section(self, target_data: Dict[str, List[Dict]]) -> str:
        """构建盘中预警里的重点标的资讯补充区块。"""
        rows = list(target_data.get("holdings", [])) + list(target_data.get("signal_pool", []))
        if not rows:
            return "【重点标的资讯】\n暂无重点标的"

        names: List[str] = []
        codes: List[str] = []
        for row in rows[:8]:
            code = str(row.get("code", "")).strip()
            name = str(row.get("name", "")).strip()
            if not code:
                continue
            names.append(f"{code} {name}".strip())
            codes.append(code)

        cache_key = "|".join(names)
        if (
            cache_key
            and cache_key == self._intraday_focus_news_cache_key
            and self._intraday_focus_news_cache_text
            and self._intraday_focus_news_cache_ts is not None
            and (datetime.now() - self._intraday_focus_news_cache_ts).total_seconds() < self._intraday_focus_news_cache_sec
        ):
            return self._intraday_focus_news_cache_text

        try:
            from agents.tools.news_router import build_intraday_news_digest

            news_text = build_intraday_news_digest(rows[:8], limit=6)
            news_text = _summarize_news_with_agent("重点标的资讯", news_text)

            lines = ["【重点标的资讯】"]
            if news_text:
                lines.append(_safe_preview(news_text, max_len=1400))
            lines.append("【盘中数据补充】")
            for row in rows[:6]:
                lines.append(
                    f"- {row.get('code', '')} {row.get('name', '')} | 现价{float(row.get('price', 0.0) or 0.0):.2f} | "
                    f"涨跌{float(row.get('change_pct', 0.0) or 0.0):+.2f}% | FCF {float(row.get('fcf', 0.0) or 0.0):+.2f} | "
                    f"信号{row.get('signal_type', '')} | 评分{float(row.get('score', 0.0) or 0.0):.2f}"
                )
            if len(lines) == 1:
                lines.append("暂无新增资讯")

            result = "\n".join(lines)
            self._intraday_focus_news_cache_key = cache_key
            self._intraday_focus_news_cache_text = result
            self._intraday_focus_news_cache_ts = datetime.now()
            return result
        except Exception as e:
            logger.warning(f"盘中重点标的资讯获取失败: {e}")
            return "【重点标的资讯】\n暂无新增资讯"

    def _build_intraday_ai_section(
        self,
        trap_signal,
        news_section: str,
        holdings_section: str,
        signal_pool_section: str,
        focus_news_section: str,
    ) -> str:
        """构建盘中预警 AI 综合研判区块"""
        agent = self._get_agent()
        if agent:
            try:
                prompt = (
                    "请结合盘中预警、新闻快讯、重点标的资讯、持仓股跟踪、信号池跟踪，"
                    "输出一份中文研判，重点说明：1）主要利好与利空；2）持仓风险；3）可执行动作。\n\n"
                    f"盘中预警\n类型: {getattr(trap_signal, 'trap_type', '')}\n"
                    f"诱多分: {getattr(trap_signal, 'fake_up_score', 0.0):.2f}\n"
                    f"诱空分: {getattr(trap_signal, 'fake_down_score', 0.0):.2f}\n"
                    f"结构: {getattr(trap_signal, 'regime_comment', '')}\n"
                    f"摘要: {getattr(trap_signal, 'summary', '')}\n\n"
                    f"{news_section}\n\n{focus_news_section}\n\n{holdings_section}\n\n{signal_pool_section}"
                )
                result = agent.run(
                    task=prompt,
                    timeout_sec=45,
                    operation_name="盘中预警综合研判",
                )
                text = agent.extract_text(result)
                if text:
                    return f"【AI综合研判】\n{text}"
            except Exception as e:
                logger.warning(f"盘中预警 AI 研判失败: {e}")

        lines = ["【AI综合研判】"]
        if "偏空" in holdings_section:
            lines.append("- 持仓股中出现偏空信号，建议优先检查止损与仓位控制。")
        else:
            lines.append("- 持仓股整体仍可跟踪，但需要结合盘口和量价继续确认。")
        if "偏多" in signal_pool_section:
            lines.append("- 信号池中存在偏多标的，可优先关注强势延续与放量确认。")
        else:
            lines.append("- 信号池暂未出现明显共振，盘中更适合等待进一步确认。")
        if getattr(trap_signal, "fake_down_score", 0.0) > getattr(trap_signal, "fake_up_score", 0.0):
            lines.append("- 当前诱空压力更大，注意指数回落对个股的拖累。")
        else:
            lines.append("- 当前情绪仍有修复空间，可结合盘口寻找强于指数的品种。")
        if "风险提示" in focus_news_section or "利空" in focus_news_section:
            lines.append("- 重点标的资讯中出现风险提示，盘中操作宜更保守。")
        if "公告" in focus_news_section or "利好" in focus_news_section:
            lines.append("- 重点标的资讯存在正向催化，可关注强势标的的二次确认。")
        return "\n".join(lines)

    def push_once(self):
        """执行一次外围简报推送。"""
        try:
            logger.info("开始执行外围简报推送...")
            body = self._build_market_brief_section()
            now = datetime.now()
            if now.hour >= 15:
                review_section = self._build_close_review_section()
                if review_section:
                    body = f"{body}\n\n{review_section}".strip()
            if not body:
                logger.info("外围简报为空，跳过推送")
                return False

            pusher = get_pusher()
            success = pusher.push(self._get_market_brief_title(now), body)
            if success:
                logger.info("外围简报推送成功")
                return True
            logger.warning("外围简报推送失败")
            return False

        except Exception as e:
            logger.error(f"推送异常: {e}")
            return False

    def push_intraday_trap_signal(self):
        """?????????/?????"""
        monitor = None
        try:
            from strategy.analysis.intraday.index_trap import IntradayTrapAnalyzer, to_trap_type_label

            analyzer = IntradayTrapAnalyzer()
            signal = analyzer.analyze_market_intraday()
            if not signal.data_ready or signal.trap_type == "no_data":
                logger.warning(f"????/???????????: {signal.summary}")
                return False

            monitor = self._get_monitor(etf_count=1, stock_count=1, reload_pool=False)
            news_section = self._build_news_section()
            target_data = self._collect_intraday_focus_targets(monitor)
            holdings_section = self._format_intraday_focus_section("持仓股盘中跟踪", target_data.get("holdings", []))
            signal_pool_section = self._format_intraday_focus_section("信号池盘中跟踪", target_data.get("signal_pool", []))
            focus_news_section = self._build_intraday_news_section(target_data)
            ai_section = self._build_intraday_ai_section(
                trap_signal=signal,
                news_section=news_section,
                holdings_section=holdings_section,
                signal_pool_section=signal_pool_section,
                focus_news_section=focus_news_section,
            )

            message = signal.to_message()
            pusher = get_pusher()
            title = self._get_intraday_alert_title(datetime.now(), signal.trap_type)
            full_message = (
                f"类型: {to_trap_type_label(signal.trap_type)}\n"
                f"{message}\n\n"
                f"{focus_news_section}\n\n"
                f"{holdings_section}\n\n"
                f"{signal_pool_section}\n\n"
                f"{ai_section}"
            )
            success = pusher.push(title, full_message, sound="minuet", level="active")
            if success:
                logger.info(f"盘中诱多/诱空推送成功: {to_trap_type_label(signal.trap_type)}")
            else:
                logger.warning("盘中诱多/诱空推送失败")
            return success
        except Exception as e:
            logger.error(f"????/??????: {e}")
            return False
    def trade_check(self):
        """执行交易检查和报告推送"""
        try:
            logger.info("开始执行交易检查...")
            
            trade_result = self.trader.check_and_trade()
            try:
                monitor = self._get_monitor(etf_count=1, stock_count=1, reload_pool=False)
                self._refresh_position_ai_hints(monitor=monitor, latest_action="sell")
            except Exception as e:
                logger.warning(f"刷新持仓 AI 提示失败: {e}")
            
            report = self.trader.get_report()
            mobile_report = format_mobile_trade_report(report)
            
            pusher = get_pusher()
            pusher.push(self._get_trade_check_title(datetime.now()), mobile_report)
            
            logger.info(f"交易检查完成: {trade_result}")
            
            agent = self._get_agent() if self.enable_agent else None
            if agent:
                try:
                    logger.info("AI Agent 交易分析中...")
                    agent_result = agent.run_trade_check()
                    logger.info(f"AI Agent 交易分析: {_safe_preview(agent_result)}...")
                except Exception as e:
                    logger.error(f"AI Agent 分析失败: {e}")
            elif self.enable_agent:
                logger.warning("AI Agent 初始化失败，本次交易检查跳过 AI 分析")
            
        except Exception as e:
            logger.error(f"交易检查异常: {e}")
    
    def run(self):
        """运行定时推送"""
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        logger.info("定时推送服务已启动")
        logger.info(f"股票池更新时间: {self.pool_update_times}")
        logger.info(f"推送时间: {self.push_times}")
        logger.info(f"盘中诱多/诱空推送时间: {self.intraday_trap_times}")
        logger.info(f"交易检查时间: {self.trade_check_times}")
        if self.news_report_time:
            logger.info(f"新闻报告时间: {self.news_report_time}")
        logger.info(f"财联社快讯轮询: {self.cls_news_symbol} / {self.cls_news_poll_interval_sec}s")
        logger.info(f"财联社快讯预警级别: {self.cls_news_alert_level}")
        
        executed_push_slots = set()
        executed_intraday_trap_slots = set()
        executed_trade_slots = set()
        executed_news_slots = set()
        executed_pool_slots = set()
        last_us_fetch_day = -1
        
        # 启动时立即尝试获取美股数据（如果今天还没获取）
        if last_us_fetch_day != datetime.now().day:
            self.fetch_us_market()
            last_us_fetch_day = datetime.now().day
        
        while self.running:
            try:
                now = datetime.now()
                current_hour = now.hour
                current_minute = now.minute
                current_day = now.day
                
                # 04:05 抓取美股夜盘数据（美股收盘后约04:00北京时间）
                if current_hour == 4 and current_minute == 5:
                    if current_day != last_us_fetch_day:
                        logger.info("时间到达 04:05，执行美股夜盘数据抓取")
                        self.fetch_us_market()
                        last_us_fetch_day = current_day
                
                is_trading_day = now.weekday() < 5

                now_ts = time.time()
                if self.enable_cls_news_alerts and is_trading_day and (now_ts - self.cls_news_last_poll_ts >= self.cls_news_poll_interval_sec):
                    self.poll_cls_news()
                    self.cls_news_last_poll_ts = now_ts

                day_prefix = now.strftime("%Y-%m-%d")

                # 检查是否需要更新股票池
                merge_existing = self.get_pool_update_mode(current_hour, current_minute)
                pool_slot = f"{day_prefix}-{current_hour:02d}:{current_minute:02d}"
                if is_trading_day and merge_existing is not None:
                    if pool_slot not in executed_pool_slots:
                        logger.info(
                            f"时间到达 {current_hour}:{current_minute}，执行股票池更新"
                            f"(merge_existing={merge_existing})"
                        )
                        self.update_stock_pool(merge_existing=bool(merge_existing))
                        executed_pool_slots.add(pool_slot)
                
                # 检查是否需要执行新闻报告
                news_slot = f"{day_prefix}-{current_hour:02d}:{current_minute:02d}"
                if is_trading_day and self.should_news_report(current_hour, current_minute):
                    if news_slot not in executed_news_slots:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行新闻报告")
                        self.news_report()
                        executed_news_slots.add(news_slot)

                trap_slot = f"{day_prefix}-{current_hour:02d}:{current_minute:02d}"
                if is_trading_day and self.should_intraday_trap_push(current_hour, current_minute):
                    if trap_slot not in executed_intraday_trap_slots:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行盘中诱多/诱空推送")
                        self.push_intraday_trap_signal()
                        executed_intraday_trap_slots.add(trap_slot)
                
                # 检查是否需要执行交易检查（默认关闭，按需启用）
                trade_slot = f"{day_prefix}-{current_hour:02d}:{current_minute:02d}"
                if self.enable_trade_check_push and is_trading_day and self.should_trade_check(current_hour, current_minute):
                    if trade_slot not in executed_trade_slots:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行交易检查")
                        self.trade_check()
                        executed_trade_slots.add(trade_slot)
                
                # 检查是否需要推送（交易日）
                push_slot = f"{day_prefix}-{current_hour:02d}:{current_minute:02d}"
                if is_trading_day and self.should_push(current_hour, current_minute):
                    if push_slot not in executed_push_slots:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行推送")
                        self.push_once()
                        executed_push_slots.add(push_slot)
                
                time.sleep(30)  # 每30秒检查一次
                
            except Exception as e:
                logger.error(f"主循环异常: {e}")
                time.sleep(60)
        
        logger.info("服务已停止")


def main():
    """主函数"""
    print("=" * 50)
    print("量化选股推送服务 (Docker)")
    print("=" * 50)
    print("功能:")
    print("  09:00  早盘外围简报 (PUSH_TIME_MORNING)")
    print("  09:20  更新每日股票池 (ETF/LOF + 热点股票)")
    print("  13:00  更新股票池并与上午结果合并")
    print("  15:20  更新股票池并与日内结果合并")
    print("  09:45/10:00/10:30/10:45/11:30")
    print("  13:15/13:45/14:15/14:30/14:45/15:00 盘中诱多/诱空独立推送")
    print("  15:00  收盘外围简报 (PUSH_TIME_CLOSE)")
    print("=" * 50)
    
    pusher = ScheduledPusher()
    pusher.run()


if __name__ == "__main__":
    main()
