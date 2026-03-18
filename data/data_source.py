# -*- coding: utf-8 -*-
"""
A股数据源 - 基于akshare/baostock
"""
import akshare as ak
import baostock as bs
import pandas as pd
import time
from datetime import datetime
from typing import Optional, List
from utils.logger import get_logger

logger = get_logger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 3

# Baostock 登录状态
_bs_logged_in = False


def _ensure_bs_login():
    """确保baostock已登录"""
    global _bs_logged_in
    if not _bs_logged_in:
        lg = bs.login()
        if lg.error_code == '0':
            _bs_logged_in = True
            logger.info("Baostock登录成功")
        else:
            logger.warning(f"Baostock登录失败: {lg.error_msg}")


# 常用ETF代码列表（当网络不可用时使用）
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
    {"code": "512880", "name": "证券ETF", "type": "ETF"},
    {"code": "512480", "name": "半导体ETF", "type": "ETF"},
    {"code": "515220", "name": "煤炭ETF", "type": "ETF"},
]


def retry_on_failure(func):
    """重试装饰器"""
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = str(e)
                if "ConnectionError" in str(type(e).__name__) or "RemoteDisconnected" in error_msg:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"{func.__name__} 网络错误，{RETRY_DELAY}秒后重试 ({attempt + 1}/{MAX_RETRIES})")
                        time.sleep(RETRY_DELAY)
                    else:
                        logger.error(f"{func.__name__} 失败: {e}")
                        raise
                else:
                    raise
    return wrapper


class DataSource:
    """A股数据源"""
    
    def __init__(self, cache_dir: str = "./data/cache"):
        self.cache_dir = cache_dir
        import os
        os.makedirs(cache_dir, exist_ok=True)
    
    @retry_on_failure
    def get_kline(self, symbol: str, start_date: str, end_date: str, 
                   adjust: str = "qfq") -> pd.DataFrame:
        """
        获取K线数据
        
        Args:
            symbol: 股票代码 (如: 000001, 511880)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            adjust: 复权类型 qfq/hfq/null
        """
        symbol = str(symbol).zfill(6)
        
        if symbol.startswith(("51", "15", "16", "50", "56")):
            return self._get_etf_kline(symbol, start_date, end_date)
        
        # 首先尝试 akshare
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            if df is not None and not df.empty:
                df.columns = [c.lower() for c in df.columns]
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"])
                return df
        except Exception as e:
            logger.warning(f"akshare获取{symbol}失败，尝试baostock: {e}")
        
        # akshare失败，使用baostock
        return self._get_kline_baostock(symbol, start_date, end_date)
    
    def _get_kline_baostock(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """使用baostock获取K线数据"""
        _ensure_bs_login()
        
        # 转换日期格式
        if len(start_date) == 8:
            start_date = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        if len(end_date) == 8:
            end_date = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        
        # 确定市场前缀
        if symbol.startswith("6"):
            bs_code = f"sh.{symbol}"
        elif symbol.startswith(("0", "3")):
            bs_code = f"sz.{symbol}"
        else:
            return pd.DataFrame()
        
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="2"
            )
            
            if rs.error_code != '0':
                logger.warning(f"Baostock查询失败: {rs.error_msg}")
                return pd.DataFrame()
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            df["date"] = pd.to_datetime(df["date"])
            
            # 转换数据类型
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            return df
            
        except Exception as e:
            logger.error(f"Baostock获取K线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    @retry_on_failure
    def _get_etf_kline(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取ETF/LOF K线数据"""
        try:
            df = ak.fund_etf_hist_em(symbol=symbol, start_date=start_date, end_date=end_date)
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
                "振幅": "amplitude",
                "涨跌幅": "pct_change",
                "涨跌额": "change",
                "换手率": "turnover"
            }
            df = df.rename(columns=col_mapping)
            
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            try:
                df = ak.fund_lof_hist_em(symbol=symbol, start_date=start_date, end_date=end_date)
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
            except Exception as e2:
                logger.error(f"获取ETF/LOF K线失败 {symbol}: {e2}")
                return pd.DataFrame()
    
    def get_realtime_quotes(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """获取实时行情"""
        # 首先尝试 akshare
        try:
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                if symbols:
                    df = df[df["代码"].isin(symbols)]
                return df
        except Exception as e:
            logger.warning(f"akshare获取实时行情失败，尝试baostock: {e}")
        
        # akshare失败，使用baostock
        return self._get_realtime_quotes_baostock(symbols)
    
    def _get_realtime_quotes_baostock(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """使用baostock获取实时行情"""
        import baostock as bs
        
        _ensure_bs_login()
        
        try:
            rs = bs.query_history_k_data_plus(
                "sh.600000" if not symbols else f"sh.{symbols[0]}" if symbols[0].startswith("6") else f"sz.{symbols[0]}",
                "date,code,open,high,low,close,volume,amount",
                start_date=datetime.now().strftime("%Y-%m-%d"),
                end_date=datetime.now().strftime("%Y-%m-%d"),
                frequency="d",
                adjustflag="2"
            )
            
            if rs.error_code != '0':
                logger.warning(f"Baostock实时行情查询失败: {rs.error_msg}")
                return pd.DataFrame()
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            
            # 转换为标准格式
            if not df.empty:
                df = df.rename(columns={
                    "code": "代码",
                    "close": "最新价",
                    "open": "开盘",
                    "high": "最高",
                    "low": "最低",
                    "volume": "成交量",
                    "amount": "成交额",
                })
                df["涨跌幅"] = 0.0  # baostock不提供实时涨跌幅
            
            return df
            
        except Exception as e:
            logger.error(f"Baostock获取实时行情失败: {e}")
            return pd.DataFrame()
    
    def get_stock_info(self, symbol: str) -> dict:
        """获取股票基本信息"""
        try:
            df = ak.stock_individual_info_em(symbol=symbol)
            info = dict(zip(df["item"], df["value"]))
            return info
        except Exception as e:
            logger.error(f"获取股票信息失败 {symbol}: {e}")
            return {}
    
    def get_index_daily(self, symbol: str = "000300") -> pd.DataFrame:
        """获取指数日线"""
        try:
            df = ak.stock_zh_index_daily(symbol=f"sh{symbol}")
            df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            logger.error(f"获取指数日线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def get_industry_stocks(self, industry: str) -> List[str]:
        """获取行业成分股"""
        try:
            df = ak.stock_board_industry_name_em()
            code = df[df["板块名称"] == industry]["板块代码"].values[0]
            df = ak.stock_board_industry_cons_em(symbol=code)
            return df["代码"].tolist()
        except Exception as e:
            logger.error(f"获取行业成分股失败 {industry}: {e}")
            return []
    
    def get_concept_stocks(self, concept: str) -> List[str]:
        """获取概念成分股"""
        try:
            df = ak.stock_board_concept_name_em()
            code = df[df["板块名称"] == concept]["板块代码"].values[0]
            df = ak.stock_board_concept_cons_em(symbol=code)
            return df["代码"].tolist()
        except Exception as e:
            logger.error(f"获取概念成分股失败 {concept}: {e}")
            return []
    
    def get_financial_data(self, symbol: str, type_: str = "balancesheet") -> pd.DataFrame:
        """获取财务数据"""
        try:
            func_map = {
                "balancesheet": ak.stock_balance_sheet,
                "income": ak.stock_income,
                "cashflow": ak.stock_cashflow
            }
            df = func_map[type_](symbol=symbol)
            return df
        except Exception as e:
            logger.error(f"获取财务数据失败 {symbol}: {e}")
            return pd.DataFrame()

    @retry_on_failure
    def get_etf_list(self) -> pd.DataFrame:
        """获取ETF列表"""
        try:
            df = ak.fund_etf_spot_em()
            return df
        except Exception as e:
            logger.warning(f"获取ETF列表失败，使用默认列表: {e}")
            return self._get_default_etf_list()
    
    @retry_on_failure
    def get_lof_list(self) -> pd.DataFrame:
        """获取LOF列表"""
        try:
            df = ak.fund_lof_spot_em()
            return df
        except Exception as e:
            logger.warning(f"获取LOF列表失败，使用默认列表: {e}")
            return self._get_default_lof_list()
    
    def _get_default_etf_list(self) -> pd.DataFrame:
        """获取默认ETF列表"""
        df = pd.DataFrame([item for item in DEFAULT_ETF_LIST if item["type"] == "ETF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1000000000
        return df
    
    def _get_default_lof_list(self) -> pd.DataFrame:
        """获取默认LOF列表"""
        df = pd.DataFrame([item for item in DEFAULT_ETF_LIST if item["type"] == "LOF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1000000000
        return df
    
    def get_default_pool(self) -> List[dict]:
        """获取默认ETF/LOF股票池（网络不可用时）"""
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

    def get_etf_lof_pool(self, min_amount: float = 300000000, 
                         prefer_t0: bool = True) -> List[dict]:
        """
        获取ETF/LOF股票池
        
        Args:
            min_amount: 最小成交额(元)，默认3亿
            prefer_t0: 是否优先T+0产品
        
        Returns:
            符合条件的产品列表 [{code, name, amount, t0}, ...]
        """
        etf_list = []
        lof_list = []
        
        try:
            etf_df = self.get_etf_list()
            if not etf_df.empty:
                etf_df.columns = [c.lower() for c in etf_df.columns]
                if "成交额" in etf_df.columns:
                    etf_df = etf_df[etf_df["成交额"] >= min_amount]
                    etf_list = etf_df.to_dict("records")
        except Exception as e:
            logger.warning(f"获取ETF列表部分失败: {e}")
        
        try:
            lof_df = self.get_lof_list()
            if not lof_df.empty:
                lof_df.columns = [c.lower() for c in lof_df.columns]
                if "成交额" in lof_df.columns:
                    lof_df = lof_df[lof_df["成交额"] >= min_amount]
                    lof_list = lof_df.to_dict("records")
        except Exception as e:
            logger.warning(f"获取LOF列表部分失败: {e}")
        
        # 如果都获取失败，使用默认列表
        if not etf_list and not lof_list:
            logger.warning("网络获取失败，使用默认ETF/LOF列表")
            return self.get_default_pool()
        
        all_products = []
        
        for item in etf_list:
            code = str(item.get("代码", ""))
            name = str(item.get("名称", ""))
            amount = item.get("成交额", 0)
            t0 = self._is_t0_etf(code)
            all_products.append({
                "code": code,
                "name": name,
                "amount": amount,
                "type": "ETF",
                "t0": t0
            })
        
        for item in lof_list:
            code = str(item.get("代码", ""))
            name = str(item.get("名称", ""))
            amount = item.get("成交额", 0)
            t0 = self._is_t0_lof(code)
            all_products.append({
                "code": code,
                "name": name,
                "amount": amount,
                "type": "LOF",
                "t0": t0
            })
        
        if prefer_t0:
            all_products.sort(key=lambda x: (not x["t0"], -x["amount"]))
        else:
            all_products.sort(key=lambda x: -x["amount"])
        
        return all_products

    def _is_t0_etf(self, code: str) -> bool:
        """判断ETF是否支持T+0"""
        if not code:
            return False
        if code.startswith("51") or code.startswith("15"):
            return True
        return False

    def _is_t0_lof(self, code: str) -> bool:
        """判断LOF是否支持T+0"""
        if not code:
            return False
        if code.startswith("16") or code.startswith("15"):
            return True
        return False
