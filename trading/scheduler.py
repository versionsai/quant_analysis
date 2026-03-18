# -*- coding: utf-8 -*-
"""
定时任务调度器
"""
import time
import threading
from datetime import datetime, time as dt_time
from typing import List, Callable, Optional
from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ScheduleTask:
    """定时任务"""
    name: str
    time: dt_time  # 执行时间 (HH:MM:SS)
    callback: Callable
    enabled: bool = True


class TaskScheduler:
    """任务调度器"""
    
    def __init__(self):
        self.tasks: List[ScheduleTask] = []
        self.running = False
        self.thread: Optional[threading.Thread] = None
    
    def add_task(self, name: str, time_str: str, callback: Callable):
        """
        添加定时任务
        
        Args:
            name: 任务名称
            time_str: 执行时间 (HH:MM:SS)
            callback: 回调函数
        """
        hour, minute, second = map(int, time_str.split(":"))
        task_time = dt_time(hour, minute, second)
        
        task = ScheduleTask(
            name=name,
            time=task_time,
            callback=callback,
        )
        
        self.tasks.append(task)
        logger.info(f"添加定时任务: {name} @ {time_str}")
    
    def remove_task(self, name: str):
        """移除任务"""
        self.tasks = [t for t in self.tasks if t.name != name]
    
    def start(self):
        """启动调度器"""
        if self.running:
            logger.warning("调度器已在运行中")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()
        logger.info("任务调度器已启动")
    
    def stop(self):
        """停止调度器"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("任务调度器已停止")
    
    def _run_loop(self):
        """运行循环"""
        while self.running:
            now = datetime.now()
            current_time = now.time()
            
            for task in self.tasks:
                if not task.enabled:
                    continue
                
                # 检查是否到达执行时间
                if self._should_run(task.time, current_time):
                    try:
                        logger.info(f"执行任务: {task.name}")
                        task.callback()
                    except Exception as e:
                        logger.error(f"任务执行失败 {task.name}: {e}")
            
            # 每30秒检查一次
            time.sleep(30)
    
    def _should_run(self, target_time: dt_time, current_time: dt_time) -> bool:
        """检查是否应该执行任务"""
        # 简单实现：每分钟检查一次
        if current_time.hour == target_time.hour:
            if current_time.minute == target_time.minute:
                # 确保每分钟只执行一次
                return True
        return False
    
    def get_status(self) -> dict:
        """获取调度器状态"""
        return {
            "running": self.running,
            "task_count": len(self.tasks),
            "tasks": [
                {
                    "name": t.name,
                    "time": t.time.strftime("%H:%M:%S"),
                    "enabled": t.enabled,
                }
                for t in self.tasks
            ],
        }


# 全局调度器实例
_scheduler: Optional[TaskScheduler] = None


def get_scheduler() -> TaskScheduler:
    """获取全局调度器实例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = TaskScheduler()
    return _scheduler


def setup_schedule():
    """设置定时任务"""
    from trading.realtime_monitor import run_realtime_scan
    
    scheduler = get_scheduler()
    
    # 早盘推荐: 9:30 (开盘后)
    scheduler.add_task("早盘推荐", "09:31", run_realtime_scan)
    
    # 午盘推荐: 13:00 (下午开盘)
    scheduler.add_task("午盘推荐", "13:01", run_realtime_scan)
    
    # 收盘总结: 15:05
    scheduler.add_task("收盘总结", "15:05", run_realtime_scan)
    
    return scheduler


def run_scheduler():
    """运行调度器"""
    scheduler = setup_schedule()
    scheduler.start()
    
    logger.info("调度器运行中，按 Ctrl+C 停止")
    
    try:
        while True:
            time.sleep(60)
            status = scheduler.get_status()
            logger.info(f"调度器状态: {status['task_count']}个任务, 运行中: {status['running']}")
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    finally:
        scheduler.stop()


if __name__ == "__main__":
    run_scheduler()
