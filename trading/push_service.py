# -*- coding: utf-8 -*-
"""
Bark 推送服务
"""
from datetime import datetime
from typing import Optional
import urllib.parse

import requests

from trading.report_formatter import to_status_label
from utils.logger import get_logger

logger = get_logger(__name__)


def _rec_value(rec, key: str, default=None):
    """兼容 dict / dataclass 的字段读取"""
    if isinstance(rec, dict):
        return rec.get(key, default)
    return getattr(rec, key, default)


def _is_missing_key(key: Optional[str]) -> bool:
    """判断 Bark Key 是否为空或占位值"""
    if key is None:
        return True
    raw = str(key).strip()
    return raw == "" or raw.lower() == "your_bark_key_here" or raw.lower() == "changeme"


def _compact_reason(reason: str, limit: int = 18) -> str:
    """压缩推荐理由，适配移动端阅读"""
    text = str(reason or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}…"


def _format_mobile_recommend_lines(group_name: str, recs: list, limit: int = 3) -> list:
    """格式化移动端推荐列表"""
    lines = [group_name]
    for rec in (recs or [])[:limit]:
        code = str(_rec_value(rec, "code", ""))
        name = str(_rec_value(rec, "name", ""))
        price = float(_rec_value(rec, "price", 0) or 0)
        target = _rec_value(rec, "target")
        stop_loss = _rec_value(rec, "stop_loss")
        signal = str(_rec_value(rec, "signal", "观望"))
        reason = _compact_reason(_rec_value(rec, "reason", ""))

        price_text = f"{price:.2f}" if price > 0 else "-"
        target_pct = "-"
        stop_pct = "-"
        if price > 0 and target:
            target_pct = f"+{((float(target) / price) - 1) * 100:.1f}%"
        if price > 0 and stop_loss:
            stop_pct = f"{((float(stop_loss) / price) - 1) * 100:.1f}%"

        line = f"- {code} {name} {signal} @{price_text}"
        if target is not None or stop_loss is not None:
            line += f" | 止盈{target_pct} 止损{stop_pct}"
        if reason:
            line += f"\n  {reason}"
        lines.append(line)
    return lines


def format_mobile_daily_recommend(etf_recommends: list, stock_recommends: list) -> str:
    """移动端每日推荐文案"""
    buy_etf = [
        rec for rec in (etf_recommends or [])
        if _rec_value(rec, "signal") == "买入" and _rec_value(rec, "target") and _rec_value(rec, "stop_loss")
    ]
    buy_stock = [
        rec for rec in (stock_recommends or [])
        if _rec_value(rec, "signal") == "买入" and _rec_value(rec, "target") and _rec_value(rec, "stop_loss")
    ]

    total_count = len(buy_etf) + len(buy_stock)
    lines = [f"日期 {datetime.now().strftime('%m-%d')}", f"可执行信号 {total_count} 条"]
    if buy_stock:
        lines.extend(_format_mobile_recommend_lines("A股", buy_stock, limit=3))
    if buy_etf:
        lines.extend(_format_mobile_recommend_lines("ETF/LOF", buy_etf, limit=2))
    if not buy_stock and not buy_etf:
        lines.append("今日无明确买入信号")
        lines.append("建议以观望和持仓管理为主")
    return "\n".join(lines)


def format_mobile_trade_report(report: str) -> str:
    """整理交易报告，适配 Bark 移动端阅读但不省略内容"""
    if not report:
        return "暂无交易报告"

    lines = [f"时间 {datetime.now().strftime('%m-%d %H:%M')}"]
    for raw_line in str(report).splitlines():
        line = raw_line.rstrip()
        if not line or line.startswith("="):
            continue
        normalized = (
            line.replace("状态holding", f"状态{to_status_label('holding')}")
            .replace("状态pending", f"状态{to_status_label('pending')}")
            .replace("状态sold", f"状态{to_status_label('sold')}")
        )
        lines.append(normalized)
    return "\n".join(lines)


class NoopPusher:
    """无推送器：BARK_KEY 缺失时只记录日志，不请求外部接口。"""

    def push(self, title: str, body: str, sound: str = "alarm", level: str = "timeSensitive") -> bool:
        logger.info(f"[NOOP PUSH] {title}\n{body}")
        return True

    def push_simple(self, message: str) -> bool:
        logger.info(f"[NOOP PUSH] {message}")
        return True

    def push_stock_signal(
        self,
        symbol: str,
        name: str,
        signal_type: str,
        price: float,
        target_price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        reason: str = "",
    ) -> bool:
        title = f"{signal_type}信号 - {symbol}"
        body = f"{name} 现价:{price:.4f} 目标:{target_price} 止损:{stop_loss} 理由:{reason}"
        return self.push(title, body)

    def push_daily_recommend(self, etf_recommends: list, stock_recommends: list) -> bool:
        title = f"今日推荐(未配置BARK_KEY) {datetime.now().strftime('%Y-%m-%d')}"
        body = format_mobile_daily_recommend(etf_recommends, stock_recommends)
        return self.push(title, body)


class BarkPusher:
    """Bark 推送服务"""

    def __init__(self, key: str):
        self.key = key
        self.base_url = f"https://api.day.app/{key}"

    def push(
        self,
        title: str,
        body: str,
        sound: str = "alarm",
        level: str = "timeSensitive",
    ) -> bool:
        """推送消息，优先使用 POST JSON，避免中文编码问题"""
        try:
            data = {
                "title": str(title),
                "body": str(body),
                "sound": sound,
                "level": level,
            }
            headers = {
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
            }
            response = requests.post(self.base_url + "/", json=data, headers=headers, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 200:
                    logger.info(f"Bark推送成功: {title}")
                    return True

            content = f"{title}\n{body}"
            encoded_content = urllib.parse.quote(content, safe="")
            simple_url = f"{self.base_url}/{encoded_content}"
            response = requests.get(simple_url, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 200:
                    logger.info(f"Bark推送成功: {title}")
                    return True

            logger.error(f"Bark推送失败: {response.text}")
            return False
        except Exception as e:
            logger.error(f"Bark推送异常: {e}")
            return False

    def push_simple(self, message: str) -> bool:
        """简单推送"""
        return self.push("通知", message, sound="minuet", level="active")

    def push_stock_signal(
        self,
        symbol: str,
        name: str,
        signal_type: str,
        price: float,
        target_price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        reason: str = "",
    ) -> bool:
        """推送股票买卖信号"""
        if signal_type == "买入":
            title = f"买入信号 - {symbol}"
            body = f"{name}\n现价: {price:.4f}\n"
            if target_price:
                body += f"目标: {target_price:.4f} (+{(target_price / price - 1) * 100:.1f}%)\n"
            if stop_loss:
                body += f"止损: {stop_loss:.4f} ({(stop_loss / price - 1) * 100:.1f}%)\n"
            body += f"理由: {reason}"
        elif signal_type == "卖出":
            title = f"卖出信号 - {symbol}"
            body = f"{name}\n现价: {price:.4f}\n理由: {reason}"
        else:
            title = f"观望信号 - {symbol}"
            body = f"{name}\n现价: {price:.4f}\n理由: {reason}"
        return self.push(title, body, sound="alarm" if signal_type != "观望" else "static")

    def push_daily_recommend(self, etf_recommends: list, stock_recommends: list) -> bool:
        """推送每日股票推荐"""
        title = f"今日买入推荐 ({datetime.now().strftime('%Y-%m-%d')})"
        body = format_mobile_daily_recommend(etf_recommends, stock_recommends)
        return self.push(title, body)


_bark_pusher: Optional[BarkPusher] = None


def get_pusher() -> BarkPusher:
    """获取全局推送实例"""
    global _bark_pusher
    if _bark_pusher is None:
        import os

        key = os.environ.get("BARK_KEY", "")
        if _is_missing_key(key):
            logger.warning("BARK_KEY 未配置，推送将仅记录日志（不会调用 Bark API）")
            _bark_pusher = NoopPusher()
        else:
            _bark_pusher = BarkPusher(key)
    return _bark_pusher


def set_pusher_key(key: str):
    """设置 Bark Key"""
    global _bark_pusher
    if _is_missing_key(key):
        _bark_pusher = NoopPusher()
        logger.warning("BARK_KEY 未配置，推送将仅记录日志（不会调用 Bark API）")
    else:
        _bark_pusher = BarkPusher(key)
        logger.info(f"Bark Key已设置: {str(key)[:10]}...")
