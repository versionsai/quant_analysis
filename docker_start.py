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
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trading import RealtimeMonitor, get_pusher, set_pusher_key
from trading.recommend_recorder import get_recorder
from trading.simulate_trading import get_trader
from data import get_pool_generator
from utils.logger import get_logger

logger = get_logger(__name__)


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
        
        morning_time = os.environ.get("PUSH_TIME_MORNING", "09:25")
        afternoon_time = os.environ.get("PUSH_TIME_AFTERNOON", "13:25")
        news_time = os.environ.get("NEWS_REPORT_TIME", "09:00")
        
        self.trade_check_times = []
        for t in [morning_time, afternoon_time]:
            try:
                h, m = map(int, t.split(":"))
                if m + 5 >= 60:
                    self.trade_check_times.append((h + 1, m + 5 - 60))
                else:
                    self.trade_check_times.append((h, m + 5))
            except:
                pass
        
        self.push_times = []
        for t in [morning_time, afternoon_time]:
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
        
        self.pool_update_time = (9, 20)
        
        self.push_times = []
        for t in [morning_time, afternoon_time]:
            try:
                h, m = map(int, t.split(":"))
                self.push_times.append((h, m))
            except:
                logger.warning(f"无效的推送时间: {t}")
        
        if not self.push_times:
            self.push_times = [(9, 25), (13, 25)]
        
        if not self.trade_check_times:
            self.trade_check_times = [(9, 30), (13, 30)]
        
        self.running = True
    
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
    
    def should_pool_update(self, hour, minute):
        """检查是否应该更新股票池"""
        h, m = self.pool_update_time
        return hour == h and minute == m
    
    def update_stock_pool(self):
        """更新每日股票池"""
        try:
            logger.info("开始更新每日股票池...")
            db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
            generator = get_pool_generator(db_path)
            result = generator.update_daily()
            
            etf_count = len(result.get("etf_lof", []))
            stock_count = len(result.get("stock", []))
            logger.info(f"股票池更新完成: ETF/LOF {etf_count} 只, 热点股票 {stock_count} 只")
            
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
            logger.info(f"新闻报告完成: {agent_result[:500]}...")
        except Exception as e:
            logger.error(f"新闻报告失败: {e}")
    
    def push_once(self):
        """执行一次推送"""
        try:
            logger.info("开始执行推送...")
            
            db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
            monitor = RealtimeMonitor(etf_count=5, stock_count=5, db_path=db_path)
            results = monitor.scan_market()
            
            pusher = get_pusher()
            etf_recs = monitor.get_top_recommends(results["etf"])
            stock_recs = monitor.get_top_recommends(results["stock"])
            
            success = pusher.push_daily_recommend(etf_recs, stock_recs)
            
            if success:
                logger.info("推送成功!")
                
                self.recorder.save_recommends(results["etf"], results["stock"])
                buy_result = self.recorder.auto_buy()
                logger.info(f"自动买入结果: {buy_result}")
                
                if self.enable_agent and self.agent:
                    try:
                        logger.info("AI Agent 分析中...")
                        agent_result = self.agent.run_daily_analysis()
                        logger.info(f"AI Agent 分析结果: {agent_result[:500]}...")
                    except Exception as e:
                        logger.error(f"AI Agent 分析失败: {e}")
            else:
                logger.warning("推送失败")
                
            return success
            
        except Exception as e:
            logger.error(f"推送异常: {e}")
            return False
    
    def trade_check(self):
        """执行交易检查和报告推送"""
        try:
            logger.info("开始执行交易检查...")
            
            trade_result = self.trader.check_and_trade()
            
            report = self.trader.get_report()
            
            pusher = get_pusher()
            pusher.push("交易检查报告", report)
            
            logger.info(f"交易检查完成: {trade_result}")
            
            if self.enable_agent and self.agent:
                try:
                    logger.info("AI Agent 交易分析中...")
                    agent_result = self.agent.run_trade_check()
                    logger.info(f"AI Agent 交易分析: {agent_result[:500]}...")
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
        logger.info(f"股票池更新时间: {self.pool_update_time}")
        logger.info(f"推送时间: {self.push_times}")
        logger.info(f"交易检查时间: {self.trade_check_times}")
        if self.news_report_time:
            logger.info(f"新闻报告时间: {self.news_report_time}")
        
        last_pushed_hour = -1
        last_traded_hour = -1
        last_news_hour = -1
        last_pool_update_day = -1
        
        while self.running:
            try:
                now = datetime.now()
                current_hour = now.hour
                current_minute = now.minute
                current_day = now.day
                
                # 检查是否需要更新股票池 (每天只在 9:20 执行一次)
                if self.should_pool_update(current_hour, current_minute):
                    if current_day != last_pool_update_day:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行股票池更新")
                        self.update_stock_pool()
                        last_pool_update_day = current_day
                
                # 检查是否需要执行新闻报告
                if self.should_news_report(current_hour, current_minute):
                    if current_hour != last_news_hour:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行新闻报告")
                        self.news_report()
                        last_news_hour = current_hour
                
                # 检查是否需要执行交易检查 (9:30, 13:30)
                if self.should_trade_check(current_hour, current_minute):
                    if current_hour != last_traded_hour:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行交易检查")
                        self.trade_check()
                        last_traded_hour = current_hour
                
                # 检查是否需要推送 (9:25, 13:25)
                if self.should_push(current_hour, current_minute):
                    # 避免同一分钟重复推送
                    if current_hour != last_pushed_hour:
                        logger.info(f"时间到达 {current_hour}:{current_minute}，执行推送")
                        self.push_once()
                        last_pushed_hour = current_hour
                
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
    print("  9:00  综合新闻报告 (AI Agent)")
    print("  9:20  更新每日股票池 (ETF/LOF + 热点股票)")
    print("  9:25  推送买入信号 + 自动买入")
    print("  9:30  检查持仓 + 止盈/止损")
    print("  13:15 综合新闻报告 (AI Agent)")
    print("  13:25 推送买入信号 + 自动买入")
    print("  13:30 检查持仓 + 止盈/止损")
    print("=" * 50)
    
    pusher = ScheduledPusher()
    pusher.run()


if __name__ == "__main__":
    main()
