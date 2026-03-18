# -*- coding: utf-8 -*-
"""
数据源适配器 - 支持多种数据源
- Baostock (优先)
- Tushare Pro (备用)
- akshare (备用)
"""
import baostock as bs
import pandas as pd
import time
from typing import Optional, List
from utils.logger import get_logger

logger = get_logger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 3

# 常用ETF代码列表
DEFAULT_ETF_LIST = [
    {"code": "511880", "name": "银华日利ETF", "type": "ETF"},
    {"code": "511010", "name": "易方达上证50ETF", "type": "ETF"},
    {"code": "510300", "name": "华夏沪深300ETF", "type": "ETF"},
    {"code": "510500", "name": "南方中证500ETF", "type": "ETF"},
    {"code": "512880", "name": "证券ETF", "type": "ETF"},
    {"code": "512690", "name": "消费ETF", "type": "ETF"},
    {"code": "513050", "name": "中概互联网ETF", "type": "ETF"},
    {"code": "513100", "name": "纳指ETF", "type": "ETF"},
    {"code": "515790", "name": "光伏ETF", "type": "ETF"},
    {"code": "515000", "name": "智能制造ETF", "type": "ETF"},
    {"code": "159919", "name": "券商ETF", "type": "LOF"},
    {"code": "159995", "name": "券商ETF(LOF)", "type": "LOF"},
    {"code": "161039", "name": "富国中证1000指数增强(LOF)", "type": "LOF"},
    {"code": "160625", "name": "中证500指数增强(LOF)", "type": "LOF"},
    {"code": "501025", "name": "银行指数分级(LOF)", "type": "LOF"},
    {"code": "162411", "name": "华宝油气(LOF)", "type": "LOF"},
    {"code": "160216", "name": "国泰房地产指数(LOF)", "type": "LOF"},
    {"code": "512480", "name": "半导体ETF", "type": "ETF"},
    {"code": "515220", "name": "煤炭ETF", "type": "ETF"},
]

# 启用akshare代理补丁（解决东方财富接口连接问题）
# 如遇到 RemoteDisconnected 错误，请启用此功能
ENABLE_PROXY_PATCH = True

def _init_proxy_patch():
    """初始化akshare代理补丁"""
    global _proxy_patch_enabled
    if ENABLE_PROXY_PATCH and not _proxy_patch_enabled:
        try:
            import akshare_proxy_patch
            akshare_proxy_patch.install_patch(
                "101.201.173.125",
                auth_token="",
                retry=30,
                hook_domains=[
                    "fund.eastmoney.com",
                    "push2.eastmoney.com",
                    "push2his.eastmoney.com",
                    "emweb.securities.eastmoney.com",
                ],
            )
            _proxy_patch_enabled = True
            logger.info("akshare代理补丁已启用")
        except ImportError:
            logger.warning("akshare-proxy-patch未安装，跳过代理补丁")
        except Exception as e:
            logger.warning(f"代理补丁启用失败: {e}")

_proxy_patch_enabled = False


# Tushare Pro Token (用户提供的)
DEFAULT_TUSHARE_TOKEN = "e146e608b30ea9e1050f8312269e269da2606ad9344537b6e275e38a"


class DataSource:
    """多数据源适配器"""
    
    def __init__(self, cache_dir: str = None, tushare_token: str = None):
        if cache_dir is None:
            import os
            cache_dir = os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")
        self.cache_dir = cache_dir
        self.tushare_token = tushare_token or DEFAULT_TUSHARE_TOKEN
        import os
        os.makedirs(cache_dir, exist_ok=True)
        
        _init_proxy_patch()
        self._init_baostock()
        self._init_tushare()
    
    def _init_baostock(self):
        """初始化Baostock"""
        try:
            lg = bs.login()
            if lg.error_code != '0':
                logger.warning(f"Baostock登录失败: {lg.error_msg}")
            else:
                logger.info(f"Baostock登录成功")
        except Exception as e:
            logger.warning(f"Baostock初始化失败: {e}")
    
    def _init_tushare(self):
        """初始化Tushare"""
        if self.tushare_token:
            try:
                import tushare as ts
                self.tushare = ts
                logger.info("Tushare Pro 初始化成功")
            except Exception as e:
                logger.warning(f"Tushare 初始化失败: {e}")
    
    def _init_baostock(self):
        """初始化Baostock"""
        try:
            lg = bs.login()
            if lg.error_code != '0':
                logger.warning(f"Baostock登录失败: {lg.error_msg}")
            else:
                logger.info(f"Baostock登录成功")
        except Exception as e:
            logger.warning(f"Baostock初始化失败: {e}")
    
    def get_kline(self, symbol: str, start_date: str, end_date: str,
                   adjust: str = "qfq") -> pd.DataFrame:
        """获取K线数据（自动选择可用数据源）
        
        优先级: akshare -> Baostock(仅股票)
        """
        symbol = str(symbol).zfill(6)
        
        is_etf = symbol.startswith(("51", "15", "16", "50", "56"))
        
        # 1. 优先akshare（ETF和股票都支持）
        df = self._get_kline_akshare(symbol, start_date, end_date)
        if not df.empty:
            logger.info(f"akshare获取{symbol}成功: {len(df)}条")
            return df
        
        # 2. 仅对普通股票使用Baostock备用（ETF/LOF不支持）
        if not is_etf:
            df = self._get_etf_kline_baostock(symbol, start_date, end_date)
            if not df.empty:
                logger.info(f"Baostock获取{symbol}成功: {len(df)}条")
                return df
        
        # 3. Tushare Pro作为最后备用
        if self.tushare_token:
            df = self._get_kline_tushare(symbol, start_date, end_date)
            if not df.empty:
                logger.info(f"Tushare获取{symbol}成功: {len(df)}条")
                return df
        
        if is_etf:
            logger.warning(f"akshare无法获取{symbol}")
        else:
            logger.warning(f"所有数据源均无法获取{symbol}")
        return pd.DataFrame()
    
    def _get_etf_kline_baostock(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用Baostock获取ETF/LOF K线"""
        try:
            # 判断市场：50/51/56开头用sh，15/16开头用sz
            if symbol.startswith(("15", "16")):
                market = f"sz{symbol}"
            else:
                market = f"sh{symbol}"
            
            rs = bs.query_history_k_data_plus(
                market,
                "date,code,open,high,low,close,volume,amount",
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="2"
            )
            
            if rs.error_code != '0':
                return pd.DataFrame()
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            df.columns = [c.lower() for c in df.columns]
            
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
        except Exception as e:
            logger.debug(f"Baostock获取{symbol}失败: {e}")
            return pd.DataFrame()
    
    def _get_kline_tushare(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用Tushare Pro获取K线"""
        if not self.tushare_token:
            return pd.DataFrame()
        
        try:
            import tushare as ts
            pro = ts.pro_api(self.tushare_token)
            
            # 判断市场
            if symbol.startswith(("5", "6")):
                ts_code = f"{symbol}.SH"
            else:
                ts_code = f"{symbol}.SZ"
            
            # 尝试获取ETF/LOF数据
            df = pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if df is None or df.empty:
                # 尝试股票接口
                df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            df = df.rename(columns={
                "ts_code": "symbol",
                "trade_date": "date"
            })
            df["date"] = pd.to_datetime(df["date"])
            df.columns = [c.lower() for c in df.columns]
            
            # 重命名列
            col_map = {
                "open": "open",
                "high": "high",
                "low": "low", 
                "close": "close",
                "vol": "volume",
                "amount": "amount"
            }
            for old, new in col_map.items():
                if old in df.columns and new not in df.columns:
                    df[new] = df[old]
            
            return df
        except Exception as e:
            logger.debug(f"Tushare获取{symbol}失败: {e}")
            return pd.DataFrame()
    
    def _get_kline_akshare(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用akshare获取K线（最后备用）"""
        try:
            import akshare as ak
            
            if symbol.startswith(("51", "15", "16", "50", "56")):
                df = ak.fund_etf_hist_em(symbol=symbol, start_date=start_date, end_date=end_date)
            else:
                df = ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date)
            
            if df is None or df.empty:
                return pd.DataFrame()
            
            df.columns = [c.lower() for c in df.columns]
            
            col_mapping = {
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
            }
            df = df.rename(columns=col_mapping)
            
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            
            return df
        except Exception as e:
            logger.debug(f"akshare获取{symbol}失败: {e}")
            return pd.DataFrame()
    
    def get_etf_list(self) -> pd.DataFrame:
        """获取ETF列表"""
        # 优先akshare
        try:
            import akshare as ak
            df = ak.fund_etf_spot_em()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"akshare获取ETF列表失败: {e}")
        
        # 备用Tushare
        if self.tushare_token:
            try:
                import tushare as ts
                pro = ts.pro_api(self.tushare_token)
                df = pro.fund_basic(market="E")
                if df is not None and not df.empty:
                    df = df.rename(columns={"ts_code": "代码", "name": "名称"})
                    df["成交额"] = 1000000000
                    return df
            except Exception as e:
                logger.warning(f"Tushare获取ETF列表失败: {e}")
        
        return self._get_default_etf_list()
    
    def get_lof_list(self) -> pd.DataFrame:
        """获取LOF列表"""
        # 优先akshare
        try:
            import akshare as ak
            df = ak.fund_lof_spot_em()
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"akshare获取LOF列表失败: {e}")
        
        # 备用Tushare
        if self.tushare_token:
            try:
                import tushare as ts
                pro = ts.pro_api(self.tushare_token)
                df = pro.fund_basic(market="O")
                if df is not None and not df.empty:
                    df = df.rename(columns={"ts_code": "代码", "name": "名称"})
                    df["成交额"] = 1000000000
                    return df
            except Exception as e:
                logger.warning(f"Tushare获取LOF列表失败: {e}")
        
        return self._get_default_lof_list()
    
    def _get_default_etf_list(self) -> pd.DataFrame:
        df = pd.DataFrame([item for item in DEFAULT_ETF_LIST if item["type"] == "ETF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1000000000
        return df
    
    def _get_default_lof_list(self) -> pd.DataFrame:
        df = pd.DataFrame([item for item in DEFAULT_ETF_LIST if item["type"] == "LOF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1000000000
        return df
    
    def get_default_pool(self) -> List[dict]:
        """获取默认ETF/LOF股票池"""
        products = []
        for item in DEFAULT_ETF_LIST:
            products.append({
                "code": item["code"],
                "name": item["name"],
                "amount": 1000000000,
                "type": item["type"],
                "t0": item["code"].startswith(("51", "15"))
            })
        products.sort(key=lambda x: (-x["amount"], not x["t0"]))
        return products
    
    def get_etf_lof_pool(self, min_amount: float = 300000000, prefer_t0: bool = True) -> List[dict]:
        """获取ETF/LOF股票池"""
        try:
            etf_df = self.get_etf_list()
            lof_df = self.get_lof_list()
            
            all_products = []
            
            if not etf_df.empty:
                etf_df.columns = [c.lower() for c in etf_df.columns]
                if "成交额" in etf_df.columns:
                    for _, row in etf_df.iterrows():
                        code = str(row.get("代码", ""))
                        if code.startswith(("51", "15", "16")):
                            all_products.append({
                                "code": code,
                                "name": str(row.get("名称", "")),
                                "amount": row.get("成交额", 0),
                                "type": "ETF",
                                "t0": True
                            })
            
            if not lof_df.empty:
                lof_df.columns = [c.lower() for c in lof_df.columns]
                if "成交额" in lof_df.columns:
                    for _, row in lof_df.iterrows():
                        code = str(row.get("代码", ""))
                        if code.startswith(("15", "16")):
                            all_products.append({
                                "code": code,
                                "name": str(row.get("名称", "")),
                                "amount": row.get("成交额", 0),
                                "type": "LOF",
                                "t0": True
                            })
            
            if not all_products:
                return self.get_default_pool()
            
            all_products.sort(key=lambda x: (-x["amount"], not x["t0"]))
            return all_products
        except Exception as e:
            logger.warning(f"获取ETF/LOF股票池失败: {e}")
            return self.get_default_pool()
    
    def __del__(self):
        """退出时登出Baostock"""
        try:
            bs.logout()
        except:
            pass
