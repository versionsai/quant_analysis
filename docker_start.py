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
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trading import RealtimeMonitor, get_pusher, set_pusher_key
from trading.push_service import format_mobile_trade_report
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


class ScheduledPusher:
    """定时推送服务"""
    
    def __init__(self):
        bark_key = os.environ.get("BARK_KEY", "WnLnofnzPUAyzy9VsvyaCg")
        set_pusher_key(bark_key)
        
        db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
        
        self.recorder = get_recorder(db_path)
        self.trader = get_trader(db_path)
        
        self.enable_agent = os.environ.get("ENABLE_AI_AGENT", "false").lower() == "true"
        self.agent = None
        
        if self.enable_agent:
            self._init_agent()
        
        morning_time = os.environ.get("PUSH_TIME_MORNING", "09:28")
        afternoon_time = os.environ.get("PUSH_TIME_AFTERNOON", "13:10")
        close_time = os.environ.get("PUSH_TIME_CLOSE", "15:30")
        trap_times = os.environ.get(
            "INTRADAY_TRAP_PUSH_TIMES",
            "09:45,10:00,10:30,10:45,11:30,13:15,13:45,14:15,14:30,14:45,15:00",
        )
        news_time = os.environ.get("NEWS_REPORT_TIME", "09:00")
        
        self.trade_check_times = []
        for t in [morning_time, afternoon_time, close_time]:
            try:
                h, m = map(int, t.split(":"))
                if m + 5 >= 60:
                    self.trade_check_times.append((h + 1, m + 5 - 60))
                else:
                    self.trade_check_times.append((h, m + 5))
            except:
                pass
        
        self.push_times = []
        for t in [morning_time, afternoon_time, close_time]:
            try:
                h, m = map(int, t.split(":"))
                self.push_times.append((h, m))
            except:
                logger.warning(f"无效的推送时间: {t}")
        
        self.news_report_time = None
        try:
            h, m = map(int, news_time.split(":"))
            self.news_report_time = (h, m)
        except:
            logger.warning(f"无效的新闻推送时间: {news_time}")
        
        self.pool_update_times = [
            (9, 20, False),
            (13, 0, True),
            (15, 20, True),
        ]
        self.intraday_trap_times = self._parse_time_list(trap_times)
        
        if not self.push_times:
            self.push_times = [(9, 28), (13, 10), (15, 30)]
        
        if not self.trade_check_times:
            self.trade_check_times = [(9, 33), (13, 15), (15, 35)]
        
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
            db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
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
    
    def news_report(self):
        """执行综合新闻报告"""
        if not self.enable_agent or not self.agent:
            logger.info("AI Agent 未启用，跳过新闻报告")
            return
        
        try:
            logger.info("开始执行综合新闻报告...")
            agent_result = self.agent.run_news_report()
            logger.info(f"新闻报告完成: {_safe_preview(agent_result)}...")
        except Exception as e:
            logger.error(f"新闻报告失败: {e}")
    
    def poll_cls_news(self):
        """?????????????????????"""
        try:
            from agents.tools.cls_news import (
                filter_cls_news_by_level,
                format_cls_alert,
                poll_cls_telegraph,
            )
            from trading import get_pusher

            new_items = poll_cls_telegraph(symbol=self.cls_news_symbol, limit=20)
            if new_items:
                logger.info(f"??????? {len(new_items)} ?")
                alert_items = filter_cls_news_by_level(new_items, min_level=self.cls_news_alert_level)
                if alert_items:
                    level_map = {
                        "critical": "????",
                        "important": "??",
                        "normal": "??",
                    }
                    title = f"?????{level_map.get(self.cls_news_alert_level, '??')}"
                    alert_text = format_cls_alert(alert_items, limit=3)
                    get_pusher().push(title, alert_text, sound="minuet", level="active")
            return new_items
        except Exception as e:
            logger.warning(f"?????????: {e}")
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
        """组装新闻分析区块"""
        blocks: List[NewsReportBlock] = []
        global_text = ""
        try:
            from agents.tools.global_news import get_global_finance_news

            global_text = _safe_preview(get_global_finance_news.invoke({}) or "", max_len=600)
            if global_text:
                blocks.append(NewsReportBlock(title="外盘表现", content=global_text))
        except Exception as e:
            logger.warning(f"外盘新闻获取失败: {e}")

        policy_text = ""
        try:
            from agents.tools.policy_news import get_policy_news

            policy_text = _safe_preview(get_policy_news.invoke({}) or "", max_len=600)
            if policy_text:
                blocks.append(NewsReportBlock(title="A???/????", content=policy_text))
        except Exception as e:
            logger.warning(f"????????: {e}")

        cls_text = ""
        try:
            from agents.tools.cls_news import get_cls_telegraph_news

            cls_text = _safe_preview(
                get_cls_telegraph_news.invoke({"symbol": self.cls_news_symbol, "limit": 6}) or "",
                max_len=1200,
            )
            if cls_text:
                blocks.append(NewsReportBlock(title="?????", content=cls_text))
        except Exception as e:
            logger.warning(f"?????????: {e}")

        emotion_summary = self._get_emotion_summary()
        if emotion_summary:
            blocks.append(NewsReportBlock(title="A股最新表现", content=emotion_summary))
        return format_news_section(blocks=blocks)

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
                fund_text = f"资金面: FCF={signal.fcf:+.2f}"
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

                monitor = RealtimeMonitor(etf_count=1, stock_count=1)
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

    def push_once(self):
        """执行一次推送（全部内容合并为一条，AI Agent 决策买入）"""
        try:
            logger.info("开始执行推送...")

            db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
            monitor = RealtimeMonitor(etf_count=5, stock_count=5, db_path=db_path)
            results = monitor.scan_market()

            etf_recs = monitor.get_top_recommends(results["etf"])
            stock_recs = monitor.get_top_recommends(results["stock"])

            news_section = self._build_news_section()
            sections = [news_section]

            ai_decision = None
            if self.enable_agent:
                try:
                    from agents.tools.sentiment import get_market_sentiment
                    from agents.tools.portfolio import analyze_portfolio
                    from agents.tools.signals import check_quant_signals

                    logger.info("获取市场情绪...")
                    sentiment_text = get_market_sentiment.invoke({}) or ""
                    logger.info("获取持仓分析...")
                    portfolio_text = analyze_portfolio.invoke({}) or ""
                    logger.info("获取量化信号...")
                    signals_text = check_quant_signals.invoke({}) or ""

                    logger.info("获取美股 TradingAgents 分析...")
                    try:
                        from agents.tools.tradingagents_tools import ta_analyze_us_market
                        us_analysis = ta_analyze_us_market.invoke({"symbols": "SPY,QQQ"}) or ""
                    except Exception as e:
                        logger.warning(f"美股分析失败: {e}")
                        us_analysis = ""

                    ai_decision = None
                    if self.agent:
                        logger.info("AI Agent 买入决策中...")
                        ai_decision = self.agent.run_buy_decision(
                            signals=signals_text,
                            sentiment=sentiment_text,
                            holdings=portfolio_text,
                            us_analysis=us_analysis,
                        )
                        logger.info(f"AI 决策结果: {ai_decision}")

                    if sentiment_text:
                        sections.append(f"【AI补充情绪】\n{_safe_preview(sentiment_text, max_len=500)}")
                    if us_analysis and us_analysis.startswith("【"):
                        sections.append(f"【AI外盘补充】\n{_safe_preview(us_analysis, max_len=500)}")
                except Exception as e:
                    logger.error(f"AI 分析失败: {e}")

            holdings_section = self._build_holdings_snapshot(monitor)
            decision_section = self._build_decision_section(monitor, ai_decision)
            signal_section = self._build_signal_section(etf_recs, stock_recs)
            review_section = self._build_review_section()

            sections.append(holdings_section)
            sections.append(decision_section)
            sections.append(signal_section)
            sections.append(review_section)

            if sections:
                stats = self.trader.db.get_statistics()
                outline_section = self._build_push_outline(
                    holdings_section=holdings_section,
                    signal_section=signal_section,
                    review_section=review_section,
                    trade_count=int(stats.get("total_trades", 0)),
                )
                body = self._build_mobile_push_body(
                    news_section=f"{outline_section}\n\n{news_section}",
                    holdings_section=holdings_section,
                    decision_section=decision_section,
                    signal_section=signal_section,
                    review_section=review_section,
                )
                pusher = get_pusher()
                now = datetime.now()
                success = pusher.push(self._get_push_title(now), body)

                if success:
                    logger.info("推送成功!")
                    self.recorder.save_recommends(results["etf"], results["stock"])
                    buy_result = self.recorder.auto_buy(ai_decision=ai_decision)
                    logger.info(f"自动买入结果: {buy_result}")
                    return True
                else:
                    logger.warning("推送失败")
                    return False
            else:
                logger.info("无内容推送")
                return False

        except Exception as e:
            logger.error(f"推送异常: {e}")
            return False

    def push_intraday_trap_signal(self):
        """推送独立的盘中诱多/诱空信号"""
        try:
            from strategy.analysis.intraday.index_trap import IntradayTrapAnalyzer, to_trap_type_label

            analyzer = IntradayTrapAnalyzer()
            signal = analyzer.analyze_market_intraday()
            if not signal.data_ready or signal.trap_type == "no_data":
                logger.warning(f"盘中诱多/诱空数据不足，跳过推送: {signal.summary}")
                return False
            message = signal.to_message()
            pusher = get_pusher()
            title = self._get_intraday_alert_title(datetime.now(), signal.trap_type)
            full_message = f"类型: {to_trap_type_label(signal.trap_type)}\n{message}"
            success = pusher.push(title, full_message, sound="minuet", level="active")
            if success:
                logger.info(f"盘中诱多/诱空推送成功: {to_trap_type_label(signal.trap_type)}")
            else:
                logger.warning("盘中诱多/诱空推送失败")
            return success
        except Exception as e:
            logger.error(f"盘中诱多/诱空推送异常: {e}")
            return False
    
    def trade_check(self):
        """执行交易检查和报告推送"""
        try:
            logger.info("开始执行交易检查...")
            
            trade_result = self.trader.check_and_trade()
            
            report = self.trader.get_report()
            mobile_report = format_mobile_trade_report(report)
            
            pusher = get_pusher()
            pusher.push(self._get_trade_check_title(datetime.now()), mobile_report)
            
            logger.info(f"交易检查完成: {trade_result}")
            
            if self.enable_agent and self.agent:
                try:
                    logger.info("AI Agent 交易分析中...")
                    agent_result = self.agent.run_trade_check()
                    logger.info(f"AI Agent 交易分析: {_safe_preview(agent_result)}...")
                except Exception as e:
                    logger.error(f"AI Agent 分析失败: {e}")
            
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
                if is_trading_day and (now_ts - self.cls_news_last_poll_ts >= self.cls_news_poll_interval_sec):
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
                if self.should_news_report(current_hour, current_minute):
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
                
                # 检查是否需要执行交易检查（交易日：推送后约5分钟）
                trade_slot = f"{day_prefix}-{current_hour:02d}:{current_minute:02d}"
                if is_trading_day and self.should_trade_check(current_hour, current_minute):
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
    print("  09:00  综合新闻报告 (AI Agent，可配置 NEWS_REPORT_TIME)")
    print("  09:20  更新每日股票池 (ETF/LOF + 热点股票)")
    print("  13:00  更新股票池并与上午结果合并")
    print("  15:20  更新股票池并与日内结果合并")
    print("  09:45/10:00/10:30/10:45/11:30")
    print("  13:15/13:45/14:15/14:30/14:45/15:00 盘中诱多/诱空独立推送")
    print("  09:28  推送买入信号 + 自动买入 (PUSH_TIME_MORNING)")
    print("  09:33  检查持仓 + 止盈/止损 (推送后约5分钟)")
    print("  13:10  推送买入信号 + 自动买入 (PUSH_TIME_AFTERNOON)")
    print("  13:15  检查持仓 + 止盈/止损 (推送后约5分钟)")
    print("  15:30  盘后再执行一遍推送与复盘 (PUSH_TIME_CLOSE)")
    print("  15:35  盘后检查持仓 + 止盈/止损")
    print("=" * 50)
    
    pusher = ScheduledPusher()
    pusher.run()


if __name__ == "__main__":
    main()
