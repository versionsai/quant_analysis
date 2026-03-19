# -*- coding: utf-8 -*-
"""
聚宽数据源
使用聚宽 API 获取 A 股数据
官网: https://www.joinquant.com
"""
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import pandas as pd

try:
    import jqdatasdk as jq
    JQ_AVAILABLE = True
except ImportError:
    JQ_AVAILABLE = False
    jq = None

from utils.logger import get_logger

logger = get_logger(__name__)


class JoinQuantDataSource:
    """聚宽数据源"""

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.username = username or os.environ.get("JQ_USERNAME", "")
        self.password = password or os.environ.get("JQ_PASSWORD", "")
        self._connected = False

    def connect(self) -> bool:
        """连接聚宽"""
        if not JQ_AVAILABLE:
            logger.warning("聚宽 SDK 未安装，请运行: pip install jqdatasdk")
            return False

        if not self.username or not self.password:
            logger.warning("未配置聚宽账号密码 (JQ_USERNAME, JQ_PASSWORD)")
            return False

        try:
            jq.auth(self.username, self.password)
            self._connected = True
            logger.info(f"聚宽连接成功: {self.username}")
            return True
        except Exception as e:
            logger.error(f"聚宽连接失败: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        if JQ_AVAILABLE and self._connected:
            try:
                jq.logout()
                self._connected = False
                logger.info("聚宽已断开连接")
            except:
                pass

    def is_connected(self) -> bool:
        """检查连接状态"""
        return self._connected

    def get_price(
        self,
        security: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: str = "daily",
        count: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        获取股票价格

        Args:
            security: 股票代码，如 "600036.XSHG" 或 "000001.XSHE"
            start_date: 开始日期
            end_date: 结束日期
            frequency: "daily" / "minute" / "hour"
            count: 获取数量

        Returns:
            价格 DataFrame
        """
        if not self._connected:
            if not self.connect():
                return pd.DataFrame()

        try:
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")
            if start_date is None and count is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

            freq_map = {"daily": "d", "minute": "m", "hour": "h"}
            freq = freq_map.get(frequency, "d")

            df = jq.get_price(
                security,
                start_date=start_date,
                end_date=end_date,
                frequency=freq,
                count=count,
                panel=False,
            )

            if df is not None and not df.empty:
                df = df.reset_index()
                df.columns = [c.lower() for c in df.columns]
                if "time" in df.columns:
                    df["date"] = df["time"]
                if "code" in df.columns:
                    df["symbol"] = df["code"].str.replace(".XSHG", "").str.replace(".XSHE", "")

            return df

        except Exception as e:
            logger.error(f"获取价格失败 {security}: {e}")
            return pd.DataFrame()

    def get_bars(
        self,
        security: str,
        count: int = 100,
        unit: str = "1d",
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取 K 线数据

        Args:
            security: 股票代码
            count: 数量
            unit: "1d" / "1m" / "5m" / "15m" / "30m" / "60m"
            end_date: 结束日期

        Returns:
            K线 DataFrame
        """
        if not self._connected:
            if not self.connect():
                return pd.DataFrame()

        try:
            if end_date is None:
                end_date = datetime.now()

            df = jq.get_bars(
                security,
                count=count,
                unit=unit,
                end_date=end_date,
                fields=["date", "open", "high", "low", "close", "volume"],
            )

            if df is not None and not df.empty:
                df = df.reset_index()
                df.columns = [c.lower() for c in df.columns]
                if "code" in df.columns:
                    df["symbol"] = df["code"].str.replace(".XSHG", "").str.replace(".XSHE", "")

            return df

        except Exception as e:
            logger.error(f"获取K线失败 {security}: {e}")
            return pd.DataFrame()

    def get_fundamentals(
        self,
        security: str,
        date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        获取基本面数据

        Args:
            security: 股票代码
            date: 查询日期

        Returns:
            基本面 DataFrame
        """
        if not self._connected:
            if not self.connect():
                return pd.DataFrame()

        try:
            if date is None:
                date = datetime.now().strftime("%Y-%m-%d")

            q = jq.query(jq因子).filter(jq股票代码 == security)
            df = jq.get_fundamentals(q, date=date)

            return df

        except Exception as e:
            logger.error(f"获取基本面失败 {security}: {e}")
            return pd.DataFrame()

    def get_instruments(self, code: Optional[str] = None) -> List[Dict]:
        """
        获取股票信息

        Args:
            code: 股票代码，不传则获取所有

        Returns:
            股票信息列表
        """
        if not self._connected:
            if not self.connect():
                return []

        try:
            if code:
                info = jq.get_instrument(code)
                if info:
                    return [{
                        "symbol": info.code,
                        "name": info.name,
                        "type": info.type,
                        "market": info.market,
                    }]
            else:
                df = jq.get_all_securities()
                if df is not None and not df.empty:
                    return df.to_dict("records")

            return []

        except Exception as e:
            logger.error(f"获取股票信息失败: {e}")
            return []

    def get_index_stocks(self, index_code: str) -> List[str]:
        """
        获取指数成分股

        Args:
            index_code: 指数代码，如 "000300.XSHG" (沪深300)

        Returns:
            成分股列表
        """
        if not self._connected:
            if not self.connect():
                return []

        try:
            stocks = jq.get_index_stocks(index_code)
            return [s.replace(".XSHG", "").replace(".XSHE", "") for s in stocks]
        except Exception as e:
            logger.error(f"获取指数成分股失败: {e}")
            return []

    def get_industry(self, code: str) -> Dict:
        """
        获取行业信息

        Args:
            code: 股票代码

        Returns:
            行业信息
        """
        if not self._connected:
            if not self.connect():
                return {}

        try:
            industry = jq.get_industry(code)
            return industry
        except Exception as e:
            logger.error(f"获取行业信息失败: {e}")
            return {}

    def normalize_code(self, code: str) -> str:
        """
        标准化股票代码

        Args:
            code: 原始代码，如 "600036" 或 "000001"

        Returns:
            聚宽格式代码
        """
        code = code.strip()

        if code.startswith("6"):
            return f"{code}.XSHG"
        elif code.startswith(("0", "3")):
            return f"{code}.XSHE"
        elif "." in code:
            return code

        return f"{code}.XSHG"


_jq_source: Optional[JoinQuantDataSource] = None


def get_jq_source() -> JoinQuantDataSource:
    """获取聚宽数据源实例"""
    global _jq_source
    if _jq_source is None:
        _jq_source = JoinQuantDataSource()
    return _jq_source


def init_jq_source(username: str, password: str) -> JoinQuantDataSource:
    """初始化聚宽数据源"""
    global _jq_source
    _jq_source = JoinQuantDataSource(username=username, password=password)
    _jq_source.connect()
    return _jq_source
