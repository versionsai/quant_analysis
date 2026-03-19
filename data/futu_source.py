# -*- coding: utf-8 -*-
"""
富途数据源 - 统一入口
自动检测并连接本地或远程 OpenD
"""
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


class FutuDataSource:
    """富途数据源"""

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 11111,
    ):
        self.port = port
        self.host = host or self._auto_detect_host()
        self._context = None
        self._connected = False
        self._subscribed = set()

    def _auto_detect_host(self) -> str:
        """自动检测可用主机"""
        import socket

        env_host = os.environ.get("FUTU_HOST", "")
        if env_host:
            logger.info(f"使用环境变量 FUTU_HOST: {env_host}")
            return env_host

        local_host = "127.0.0.1"
        prod_host = "192.168.5.6"

        if self._check_port(local_host, self.port):
            logger.info("检测到本地 OpenD")
            return local_host

        if self._check_port(prod_host, self.port):
            logger.info("检测到生产环境 OpenD (NAS)")
            return prod_host

        logger.warning("未检测到 Futu OpenD，将使用本地地址")
        return local_host

    def _check_port(self, host: str, port: int) -> bool:
        """检查端口"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
        except:
            return False

    def connect(self) -> bool:
        """连接富途 OpenD"""
        if self._connected:
            return True

        try:
            from futuquant import OpenQuoteContext

            logger.info(f"连接富途: {self.host}:{self.port}")
            self._context = OpenQuoteContext(host=self.host, port=self.port)
            self._connected = True
            logger.info("富途连接成功!")
            return True

        except Exception as e:
            logger.error(f"富途连接失败: {e}")
            self._connected = False
            return False

    def close(self):
        """关闭连接"""
        if self._context:
            try:
                self._context.close()
            except:
                pass
            self._context = None
            self._connected = False

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._connected

    def _ensure_subscription(self, codes: List[str]):
        """确保已订阅"""
        from futuquant import SubType

        need_sub = [c for c in codes if c not in self._subscribed]
        if need_sub:
            try:
                self._context.subscribe(need_sub, [SubType.QUOTE])
                self._subscribed.update(need_sub)
            except Exception as e:
                logger.warning(f"订阅失败: {e}")

    def get_quote(self, symbol: str) -> Optional[Dict]:
        """获取单只股票报价"""
        if not self._connected:
            self.connect()

        if not self._connected:
            return None

        try:
            code = self._normalize_code(symbol)
            self._ensure_subscription([code])

            ret, data = self._context.get_stock_quote(code)
            if ret == 0 and data is not None and len(data) > 0:
                row = data.iloc[0]
                return {
                    "symbol": row.get("code", ""),
                    "price": row.get("last_price", 0),
                    "open": row.get("open_price", 0),
                    "high": row.get("high_price", 0),
                    "low": row.get("low_price", 0),
                    "prev_close": row.get("prev_close_price", 0),
                    "volume": row.get("volume", 0),
                    "turnover": row.get("turnover", 0),
                }
            return None

        except Exception as e:
            logger.error(f"获取报价失败 {symbol}: {e}")
            return None

    def get_batch_quote(self, symbols: List[str]) -> List[Dict]:
        """批量获取报价"""
        if not self._connected:
            self.connect()

        if not self._connected:
            return []

        try:
            codes = [self._normalize_code(s) for s in symbols]
            self._ensure_subscription(codes)

            ret, data = self._context.get_stock_quote(codes)
            if ret == 0 and data is not None and len(data) > 0:
                results = []
                for _, row in data.iterrows():
                    results.append({
                        "symbol": row.get("code", ""),
                        "price": row.get("last_price", 0),
                        "open": row.get("open_price", 0),
                        "high": row.get("high_price", 0),
                        "low": row.get("low_price", 0),
                        "prev_close": row.get("prev_close_price", 0),
                        "volume": row.get("volume", 0),
                        "turnover": row.get("turnover", 0),
                    })
                return results
            return []

        except Exception as e:
            logger.error(f"批量获取报价失败: {e}")
            return []

    def get_history_kline(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        ktype: str = "K_DAY",
        max_count: int = 1000,
    ) -> pd.DataFrame:
        """
        获取历史K线数据 (在线请求，无需下载)

        Args:
            symbol: 股票代码
            start_date: 开始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            ktype: K线类型 K_DAY, K_WEEK, K_60M, K_30M, K_15M, K_5M, K_1M
            max_count: 最大返回数量

        Returns:
            K线 DataFrame
        """
        if not self._connected:
            self.connect()

        if not self._connected:
            return pd.DataFrame()

        try:
            from futuquant import KLType

            code = self._normalize_code(symbol)

            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

            ktype_map = {
                "K_DAY": KLType.K_DAY,
                "K_WEEK": KLType.K_WEEK,
                "K_MON": KLType.K_MON,
                "K_60M": KLType.K_60M,
                "K_30M": KLType.K_30M,
                "K_15M": KLType.K_15M,
                "K_5M": KLType.K_5M,
                "K_1M": KLType.K_1M,
            }
            kl_type = ktype_map.get(ktype, KLType.K_DAY)

            logger.info(f"请求K线: {code} {start_date} ~ {end_date}")

            ret, data, page_key = self._context.request_history_kline(
                code,
                start=start_date,
                end=end_date,
                ktype=kl_type,
                max_count=max_count,
            )

            if ret == 0 and data is not None and len(data) > 0:
                df = data.copy()
                df.columns = [c.lower() for c in df.columns]
                if "time_key" in df.columns:
                    df = df.rename(columns={"time_key": "date"})
                logger.info(f"K线获取成功: {len(df)} 条")
                return df

            logger.warning(f"获取K线失败 {symbol}: ret={ret}, data={data}")
            return pd.DataFrame()

        except Exception as e:
            logger.error(f"获取K线异常 {symbol}: {e}")
            return pd.DataFrame()

    def get_kline(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        count: int = 100,
    ) -> pd.DataFrame:
        """获取K线数据 (别名)"""
        return self.get_history_kline(symbol, start_date, end_date, max_count=count)

    def _normalize_code(self, symbol: str) -> str:
        """标准化代码"""
        symbol = symbol.strip().upper()
        if symbol.startswith("SH.") or symbol.startswith("SZ."):
            return symbol
        if symbol.startswith("6"):
            return f"SH.{symbol}"
        return f"SZ.{symbol}"


_futu_source: Optional["FutuDataSource"] = None


def get_futu_source() -> "FutuDataSource":
    """获取富途数据源"""
    global _futu_source
    if _futu_source is None:
        _futu_source = FutuDataSource()
        _futu_source.connect()
    return _futu_source


def init_futu_source(host: Optional[str] = None, port: int = 11111) -> "FutuDataSource":
    """初始化富途数据源"""
    global _futu_source
    _futu_source = FutuDataSource(host=host, port=port)
    _futu_source.connect()
    return _futu_source
