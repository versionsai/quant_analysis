# -*- coding: utf-8 -*-
"""
富途 HTTP API 数据源
通过 HTTP 调用富途数据服务
"""
import os
import time
from typing import Optional, List, Dict, Any
import requests
import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


class FutuHTTPDataSource:
    """富途 HTTP API 数据源"""

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30):
        self.base_url = base_url or os.environ.get("FUTU_API_URL", "http://192.168.5.6:8080")
        self.timeout = timeout
        self.session = requests.Session()
        self._connected = False

    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """GET 请求"""
        url = f"{self.base_url}/{endpoint}"
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"HTTP GET 失败 {url}: {e}")
            return {"code": 1, "error": str(e)}

    def _post(self, endpoint: str, json_data: Optional[Dict] = None) -> Dict:
        """POST 请求"""
        url = f"{self.base_url}/{endpoint}"
        try:
            resp = self.session.post(url, json=json_data, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"HTTP POST 失败 {url}: {e}")
            return {"code": 1, "error": str(e)}

    def health_check(self) -> bool:
        """健康检查"""
        result = self._get("health")
        self._connected = result.get("code") == 0 and result.get("status") == "ok"
        return self._connected

    def get_quote(self, symbol: str) -> Optional[Dict]:
        """
        获取股票报价

        Args:
            symbol: 股票代码，如 "600036" 或 "SH.600036"

        Returns:
            股票报价信息
        """
        result = self._get(f"quote/{symbol}")
        if result.get("code") == 0:
            return result.get("data")
        logger.warning(f"获取报价失败 {symbol}: {result.get('error')}")
        return None

    def get_kline(
        self,
        symbol: str,
        start_date: str = "20260101",
        end_date: Optional[str] = None,
        ktype: str = "K_DAY"
    ) -> pd.DataFrame:
        """
        获取K线数据

        Args:
            symbol: 股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            ktype: K线类型 K_DAY/K_WEEK/K_15M/K_30M/K_60M

        Returns:
            K线 DataFrame
        """
        if end_date is None:
            end_date = time.strftime("%Y%m%d")

        params = {"start": start_date, "end": end_date, "ktype": ktype}
        result = self._get(f"kline/{symbol}", params)

        if result.get("code") == 0:
            data = result.get("data", [])
            if data:
                df = pd.DataFrame(data)
                df.columns = [c.lower() for c in df.columns]
                return df

        logger.warning(f"获取K线失败 {symbol}: {result.get('error')}")
        return pd.DataFrame()

    def get_batch_quote(self, symbols: List[str]) -> List[Dict]:
        """
        批量获取报价

        Args:
            symbols: 股票代码列表

        Returns:
            报价列表
        """
        result = self._post("batch_quote", {"symbols": symbols})
        if result.get("code") == 0:
            return result.get("data", [])
        logger.warning(f"批量获取报价失败: {result.get('error')}")
        return []

    def get_rt_kline(self, symbol: str, num: int = 100) -> pd.DataFrame:
        """
        获取实时K线

        Args:
            symbol: 股票代码
            num: K线数量

        Returns:
            K线 DataFrame
        """
        result = self._get(f"rt_kline/{symbol}", {"num": num})

        if result.get("code") == 0:
            data = result.get("data", [])
            if data:
                df = pd.DataFrame(data)
                df.columns = [c.lower() for c in df.columns]
                return df

        logger.warning(f"获取实时K线失败 {symbol}: {result.get('error')}")
        return pd.DataFrame()

    def get_trade_days(self, start_date: str, end_date: str) -> List[str]:
        """
        获取交易日历

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日列表
        """
        params = {"start": start_date, "end": end_date}
        result = self._get("trade_days", params)

        if result.get("code") == 0:
            return result.get("data", [])
        logger.warning(f"获取交易日历失败: {result.get('error')}")
        return []


_futu_source: Optional[FutuHTTPDataSource] = None


def get_futu_source() -> FutuHTTPDataSource:
    """获取富途数据源实例"""
    global _futu_source
    if _futu_source is None:
        _futu_source = FutuHTTPDataSource()
    return _futu_source


def init_futu_source(base_url: str) -> FutuHTTPDataSource:
    """初始化富途数据源"""
    global _futu_source
    _futu_source = FutuHTTPDataSource(base_url=base_url)
    return _futu_source
