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
from utils.logger import get_logger

logger = get_logger(__name__)


class ScheduledPusher:
    """定时推送服务"""
    
    def __init__(self):
        # 获取Bark Key
        bark_key = os.environ.get("BARK_KEY", "WnLnofnzPUAyzy9VsvyaCg")
        set_pusher_key(bark_key)
        
        # 从环境变量获取推送时间
        morning_time = os.environ.get("PUSH_TIME_MORNING", "09:30")
        afternoon_time = os.environ.get("PUSH_TIME_AFTERNOON", "14:30")
        
        self.push_times = []
        for t in [morning_time, afternoon_time]:
            try:
                h, m = map(int, t.split(":"))
                self.push_times.append((h, m))
            except:
                logger.warning(f"无效的推送时间: {t}")
        
        # 如果没有有效时间，使用默认
        if not self.push_times:
            self.push_times = [(9, 30), (14, 30)]
        
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
            else:
                logger.warning("推送失败")
                
            return success
            
        except Exception as e:
            logger.error(f"推送异常: {e}")
            return False
    
    def run(self):
        """运行定时推送"""
        # 注册信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        logger.info("定时推送服务已启动")
        logger.info(f"推送时间: {self.push_times}")
        
        last_pushed_hour = -1
        
        while self.running:
            try:
                now = datetime.now()
                current_hour = now.hour
                current_minute = now.minute
                
                # 检查是否需要推送 (每分钟的第一次扫描)
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
    print("功能: 每天9:30和14:30自动推送买入信号")
    print("=" * 50)
    
    pusher = ScheduledPusher()
    pusher.run()


if __name__ == "__main__":
    main()
