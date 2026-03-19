# -*- coding: utf-8 -*-
"""
复盘报告生成器

生成每日复盘报告，支持Bark推送
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import json
import os

from strategy.analysis.review.portfolio_tracker import PortfolioTracker, Position
from strategy.analysis.review.pnl_analyzer import PnLAnalyzer
from strategy.analysis.emotion.market_emotion import MarketEmotion, MarketEmotionAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DailyReport:
    """每日复盘报告"""
    date: str
    
    portfolio_summary: Dict = field(default_factory=dict)
    emotion_summary: Dict = field(default_factory=dict)
    position_changes: List[Dict] = field(default_factory=list)
    winning_positions: List[Dict] = field(default_factory=list)
    losing_positions: List[Dict] = field(default_factory=list)
    
    market_comment: str = ""
    trade_summary: str = ""
    
    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "portfolio_summary": self.portfolio_summary,
            "emotion_summary": self.emotion_summary,
            "position_changes": self.position_changes,
            "winning_positions": self.winning_positions,
            "losing_positions": self.losing_positions,
            "market_comment": self.market_comment,
            "trade_summary": self.trade_summary,
        }


class ReportGenerator:
    """复盘报告生成器"""
    
    def __init__(self, reports_dir: str = "./runtime/reports"):
        self.reports_dir = reports_dir
        self.pnl_analyzer = PnLAnalyzer()
        self.market_analyzer = MarketEmotionAnalyzer()
        os.makedirs(reports_dir, exist_ok=True)
    
    def generate_daily_report(
        self,
        tracker: PortfolioTracker,
        market_emotion: MarketEmotion = None,
        current_date: str = None,
    ) -> DailyReport:
        """生成每日复盘报告"""
        if current_date is None:
            current_date = datetime.now().strftime("%Y%m%d")
        
        report = DailyReport(date=current_date)
        
        report.portfolio_summary = self.pnl_analyzer.analyze_portfolio(tracker, current_date)
        
        if market_emotion:
            report.emotion_summary = market_emotion.to_dict()
            report.market_comment = self._generate_market_comment(market_emotion)
        
        positions = list(tracker.positions.values())
        report.winning_positions = [
            p.to_dict() for p in sorted(positions, key=lambda x: -x.unrealized_pnl_pct)[:5]
        ]
        report.losing_positions = [
            p.to_dict() for p in sorted(positions, key=lambda x: x.unrealized_pnl_pct)[:5]
        ]
        
        return report
    
    def _generate_market_comment(self, emotion: MarketEmotion) -> str:
        """生成市场点评"""
        comments = []
        
        comments.append(f"大盘情绪: {emotion.cycle}")
        
        if emotion.zt_count > 30:
            comments.append(f"涨停家数{emotion.zt_count}家，市场活跃")
        elif emotion.zt_count > 10:
            comments.append(f"涨停家数{emotion.zt_count}家，赚钱效应一般")
        else:
            comments.append(f"涨停家数{emotion.zt_count}家，市场清淡")
        
        if emotion.lb_count > 0:
            comments.append(f"连板股{emotion.lb_count}只，最高{emotion.lb_max}板")
        
        if emotion.hot_sectors:
            comments.append(f"热门板块: {', '.join(emotion.hot_sectors[:3])}")
        
        return " | ".join(comments)
    
    def format_report_for_push(self, report: DailyReport) -> tuple:
        """格式化报告用于Bark推送"""
        title = f"📊 每日复盘 {report.date}"
        
        body_parts = []
        
        summary = report.portfolio_summary
        if summary:
            value = summary.get("total_value", 0)
            pnl = summary.get("unrealized_pnl", 0)
            ret = summary.get("total_return", 0)
            body_parts.append(
                f"总资产: {value:,.0f}\n"
                f"浮盈亏: {pnl:,.0f} ({ret:+.2f}%)\n"
                f"胜率: {summary.get('win_rate', 0):.0f}% "
                f"({summary.get('win_count', 0)}/{summary.get('positions_count', 0)})"
            )
        
        if report.winning_positions:
            top_winners = report.winning_positions[:3]
            body_parts.append("🔥 涨幅前三:")
            for p in top_winners:
                body_parts.append(
                    f"  {p['symbol']} {p['name']}: {p['unrealized_pnl_pct']:+.2f}%"
                )
        
        if report.losing_positions:
            top_losers = report.losing_positions[:3]
            body_parts.append("❄️ 跌幅前三:")
            for p in top_losers:
                body_parts.append(
                    f"  {p['symbol']} {p['name']}: {p['unrealized_pnl_pct']:+.2f}%"
                )
        
        if report.market_comment:
            body_parts.append(f"\n📈 市场: {report.market_comment}")
        
        body = "\n".join(body_parts)
        
        return title, body
    
    def save_report(self, report: DailyReport) -> str:
        """保存报告到文件"""
        filename = f"report_{report.date}.json"
        filepath = os.path.join(self.reports_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        
        logger.info(f"报告已保存: {filepath}")
        return filepath
    
    def load_report(self, date: str) -> Optional[DailyReport]:
        """加载指定日期的报告"""
        filename = f"report_{date}.json"
        filepath = os.path.join(self.reports_dir, filename)
        
        if not os.path.exists(filepath):
            return None
        
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        return DailyReport(**data)
    
    def generate_comparison_report(
        self,
        tracker: PortfolioTracker,
        days: int = 7,
    ) -> str:
        """生成对比报告（最近N天）"""
        lines = [f"【近{days}日复盘对比】"]
        
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            report = self.load_report(date)
            
            if report:
                summary = report.portfolio_summary
                lines.append(
                    f"{date}: "
                    f"总资产{summary.get('total_value', 0):,.0f} "
                    f"盈亏{summary.get('unrealized_pnl', 0):+,.0f} "
                    f"({summary.get('total_return', 0):+.2f}%)"
                )
            else:
                lines.append(f"{date}: 无数据")
        
        return "\n".join(lines)
