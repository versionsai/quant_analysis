# -*- coding: utf-8 -*-
"""
Docker启动脚本 - 定时推送
"""
import os
import sys
import time
import signal
from datetime import datetime

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from trading import RealtimeMonitor, get_pusher, set_pusher_key
from trading.recommend_recorder import get_recorder
from trading.simulate_trading import get_trader
from utils.logger import get_logger

logger = get_logger(__name__)


class ScheduledPusher:
    """定时推送服务"""
    
    def __init__(self):
        # 获取Bark Key
        bark_key = os.environ.get("BARK_KEY", "WnLnofnzPUAyzy9VsvyaCg")
        set_pusher_key(bark_key)
        
        # 获取数据库路径
        db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
        
        # 初始化荐股记录器和交易器
        self.recorder = get_recorder(db_path)
        self.trader = get_trader(db_path)
        
        # 从环境变量获取推送时间
        morning_time = os.environ.get("PUSH_TIME_MORNING", "09:25")
        afternoon_time = os.environ.get("PUSH_TIME_AFTERNOON", "13:25")
        
        # 交易检查时间 (推送后5分钟)
        self.trade_check_times = []
        for t in [morning_time, afternoon_time]:
            try:
                h, m = map(int, t.split(":"))
                # 交易检查时间为推送后5分钟
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
        
        # 如果没有有效时间，使用默认
        if not self.push_times:
            self.push_times = [(9, 25), (13, 25)]
        
        if not self.trade_check_times:
            self.trade_check_times = [(9, 30), (13, 30)]
        
        self.running = True
    
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
    
    def push_once(self):
        """执行一次推送"""
        try:
            logger.info("开始执行推送...")
            
            monitor = RealtimeMonitor(etf_count=5, stock_count=5)
            results = monitor.scan_market()
            
            pusher = get_pusher()
            etf_recs = monitor.get_top_recommends(results["etf"])
            stock_recs = monitor.get_top_recommends(results["stock"])
            
            success = pusher.push_daily_recommend(etf_recs, stock_recs)
            
            if success:
                logger.info("推送成功!")
                
                # 保存荐股记录到数据库
                self.recorder.save_recommends(results["etf"], results["stock"])
                
                # 自动买入
                buy_result = self.recorder.auto_buy()
                logger.info(f"自动买入结果: {buy_result}")
                
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
            
            # 检查持仓并执行交易
            trade_result = self.trader.check_and_trade()
            
            # 生成报告并推送
            report = self.trader.get_report()
            
            pusher = get_pusher()
            pusher.push("交易检查报告", report)
            
            logger.info(f"交易检查完成: {trade_result}")
            logger.info(f"报告:\n{report}")
            
        except Exception as e:
            logger.error(f"交易检查异常: {e}")
    
    def run(self):
        """运行定时推送"""
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        logger.info("定时推送服务已启动")
        logger.info(f"推送时间: {self.push_times}")
        logger.info(f"交易检查时间: {self.trade_check_times}")
        
        last_pushed_hour = -1
        last_traded_hour = -1
        
        while self.running:
            try:
                now = datetime.now()
                current_hour = now.hour
                current_minute = now.minute
                
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
    print("功能: 每天9:25和13:25自动推送买入信号")
    print("      9:30和13:30检查持仓并执行止盈/止损")
    print("=" * 50)
    
    pusher = ScheduledPusher()
    pusher.run()


if __name__ == "__main__":
    main()
