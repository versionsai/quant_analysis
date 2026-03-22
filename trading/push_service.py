# -*- coding: utf-8 -*-
"""
Bark 推送服务
"""
from datetime import datetime
from typing import List, Optional
import urllib.parse

import requests

from trading.report_formatter import to_status_label
from utils.logger import get_logger

logger = get_logger(__name__)
_BARK_MAX_BODY_LEN = 900


def _rec_value(rec, key: str, default=None):
    """兼容 dict / dataclass 字段读取"""
    if isinstance(rec, dict):
        return rec.get(key, default)
    return getattr(rec, key, default)


def _is_missing_key(key: Optional[str]) -> bool:
    """判断 Bark Key 是否为空或占位值"""
    if key is None:
        return True
    raw = str(key).strip()
    return raw == "" or raw.lower() in {"your_bark_key_here", "changeme"}


def _split_body_for_bark(body: str, max_len: int = _BARK_MAX_BODY_LEN) -> List[str]:
    """将超长正文按段拆分，避免 Bark 长度限制"""
    text = str(body or "")
    if len(text) <= max_len:
        return [text]

    blocks = text.split("\n\n")
    parts: List[str] = []
    current = ""
    for block in blocks:
        block = str(block)
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= max_len:
            current = candidate
            continue

        if current:
            parts.append(current)
            current = ""

        if len(block) <= max_len:
            current = block
            continue

        lines = block.splitlines()
        chunk = ""
        for line in lines:
            line_candidate = line if not chunk else f"{chunk}\n{line}"
            if len(line_candidate) <= max_len:
                chunk = line_candidate
            else:
                if chunk:
                    parts.append(chunk)
                chunk = line
        if chunk:
            current = chunk

    if current:
        parts.append(current)
    return parts or [text]


def _compact_reason(reason: str, limit: int = 18) -> str:
    """压缩推荐理由，适配移动端阅读"""
    text = str(reason or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}…"


def _format_mobile_recommend_lines(group_name: str, recs: list) -> list:
    """格式化移动端推荐列表"""
    lines = [group_name]
    for rec in recs or []:
        code = str(_rec_value(rec, "code", ""))
        name = str(_rec_value(rec, "name", ""))
        price = float(_rec_value(rec, "price", 0) or 0)
        target = _rec_value(rec, "target")
        stop_loss = _rec_value(rec, "stop_loss")
        signal = str(_rec_value(rec, "signal", "观望"))
        reason = _compact_reason(_rec_value(rec, "reason", ""))

        line = f"- {code} {name} | {signal} | 现价 {price:.2f}" if price > 0 else f"- {code} {name} | {signal}"
        if target is not None and stop_loss is not None and price > 0:
            line += f"\n  止盈止损: {float(target):.2f} / {float(stop_loss):.2f}"
        if reason:
            line += f"\n  原因: {reason}"
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
        lines.extend(_format_mobile_recommend_lines("A股", buy_stock))
    if buy_etf:
        lines.extend(_format_mobile_recommend_lines("ETF/LOF", buy_etf))
    if not buy_stock and not buy_etf:
        lines.append("今日暂无明确买入信号")
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
    """无推送器，BARK_KEY 缺失时只记录日志，不请求外部接口。"""

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

    def _push_single(
        self,
        title: str,
        body: str,
        sound: str = "alarm",
        level: str = "timeSensitive",
    ) -> bool:
        """推送单条消息，优先使用 POST JSON"""
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

        logger.warning(f"Bark POST推送失败: status={response.status_code}, body={response.text[:200]}")

        if len(str(title)) + len(str(body)) > 1200:
            return False

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

    def push(
        self,
        title: str,
        body: str,
        sound: str = "alarm",
        level: str = "timeSensitive",
    ) -> bool:
        """推送消息，超长正文自动拆分，避免 Bark 长度限制"""
        try:
            parts = _split_body_for_bark(body)
            if len(parts) == 1:
                return self._push_single(title=title, body=parts[0], sound=sound, level=level)

            success = True
            total = len(parts)
            for index, part in enumerate(parts, 1):
                part_title = f"{title} ({index}/{total})"
                if not self._push_single(title=part_title, body=part, sound=sound, level=level):
                    success = False
                    break
            return success
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
