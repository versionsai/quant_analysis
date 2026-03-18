# -*- coding: utf-8 -*-
"""
Bark 推送服务
"""
import requests
import urllib.parse
from typing import Optional
from datetime import datetime
from utils.logger import get_logger

logger = get_logger(__name__)


class BarkPusher:
    """Bark推送服务"""
    
    def __init__(self, key: str = "WnLnofnzPUAyzy9VsvyaCg"):
        self.key = key
        self.base_url = f"https://api.day.app/{key}"
    
    def push(
        self,
        title: str,
        body: str,
        sound: str = "alarm",
        level: str = "timeSensitive",
    ) -> bool:
        """
        推送消息
        """
        try:
            # 方式1: URL带参数 (简单模式)
            # https://api.day.app/{key}/{内容}
            content = f"{title}\n{body}"
            encoded_content = urllib.parse.quote(content)
            simple_url = f"{self.base_url}/{encoded_content}"
            
            response = requests.get(simple_url, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 200:
                    logger.info(f"Bark推送成功: {title}")
                    return True
            
            # 方式2: POST JSON (备用)
            data = {
                "title": title,
                "body": body,
                "sound": sound,
                "level": level,
            }
            
            response = requests.post(self.base_url + "/", json=data, timeout=10)
            
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
        """
        简单推送 - 直接在URL中传递消息
        """
        try:
            encoded_msg = urllib.parse.quote(message)
            url = f"{self.base_url}/{encoded_msg}"
            
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 200:
                    logger.info(f"Bark推送成功")
                    return True
            
            logger.warning(f"简单推送失败，尝试POST: {response.text}")
            return False
            
        except Exception as e:
            logger.error(f"Bark推送异常: {e}")
            return False
    
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
            title = f"📈 买入信号 - {symbol}"
            body = f"{name}\n"
            body += f"现价: {price:.4f}\n"
            if target_price:
                body += f"目标: {target_price:.4f} (+{(target_price/price-1)*100:.1f}%)\n"
            if stop_loss:
                body += f"止损: {stop_loss:.4f} ({(stop_loss/price-1)*100:.1f}%)\n"
            body += f"理由: {reason}"
        elif signal_type == "卖出":
            title = f"📉 卖出信号 - {symbol}"
            body = f"{name}\n"
            body += f"现价: {price:.4f}\n"
            body += f"理由: {reason}"
        else:
            title = f"⏸️ 观望信号 - {symbol}"
            body = f"{name}\n"
            body += f"现价: {price:.4f}\n"
            body += f"理由: {reason}"
        
        return self.push(title, body, sound="alarm" if signal_type != "观望" else "static")
    
    def push_daily_recommend(
        self,
        etf_recommends: list,
        stock_recommends: list,
    ) -> bool:
        """推送每日股票推荐"""
        title = f"📈 今日买入推荐 ({datetime.now().strftime('%Y-%m-%d')})"
        
        body = ""
        
        buy_etf = [r for r in etf_recommends if r.get('signal') == '买入' and r.get('target') and r.get('stop_loss')]
        buy_stock = [r for r in stock_recommends if r.get('signal') == '买入' and r.get('target') and r.get('stop_loss')]
        
        if buy_etf:
            body += "【ETF/LOF 买入】\n"
            for r in buy_etf[:5]:
                price = r.get('price', 0)
                target = r.get('target')
                stop = r.get('stop_loss')
                
                profit_pct = (target/price - 1) * 100 if price else 0
                loss_pct = (stop/price - 1) * 100 if price else 0
                body += f"✅ {r['code']} {r['name']}\n"
                body += f"   买入:{price:.2f} 止盈:{target:.2f}(+{profit_pct:.1f}%) 止损:{stop:.2f}({loss_pct:.1f}%)\n"
                body += f"   依据:{r.get('reason', '')[:12]}\n"
            body += "\n"
        
        if buy_stock:
            body += "【A股 买入】\n"
            for r in buy_stock[:5]:
                price = r.get('price', 0)
                target = r.get('target')
                stop = r.get('stop_loss')
                
                profit_pct = (target/price - 1) * 100 if price else 0
                loss_pct = (stop/price - 1) * 100 if price else 0
                body += f"✅ {r['code']} {r['name']}\n"
                body += f"   买入:{price:.2f} 止盈:{target:.2f}(+{profit_pct:.1f}%) 止损:{stop:.2f}({loss_pct:.1f}%)\n"
                body += f"   依据:{r.get('reason', '')[:12]}\n"
        
        if not buy_etf and not buy_stock:
            body = "今日无买入信号\n"
            body += "市场处于震荡或下跌趋势，建议观望\n"
        
        return self.push(title, body)


_bark_pusher: Optional[BarkPusher] = None


def get_pusher() -> BarkPusher:
    """获取全局推送实例"""
    global _bark_pusher
    if _bark_pusher is None:
        _bark_pusher = BarkPusher()
    return _bark_pusher


def set_pusher_key(key: str):
    """设置Bark Key"""
    global _bark_pusher
    _bark_pusher = BarkPusher(key)
    logger.info(f"Bark Key已设置: {key[:10]}...")
