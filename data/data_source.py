# -*- coding: utf-8 -*-
"""
A股数据源 - 基于akshare
"""
import akshare as ak
import pandas as pd
from typing import Optional, List
from utils.logger import get_logger

logger = get_logger(__name__)


class DataSource:
    """A股数据源"""
    
    def __init__(self, cache_dir: str = "./data/cache"):
        self.cache_dir = cache_dir
        import os
        os.makedirs(cache_dir, exist_ok=True)
    
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
        
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            if df is None or df.empty:
                return pd.DataFrame()
            df.columns = [c.lower() for c in df.columns]
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            logger.error(f"获取K线失败 {symbol}: {e}")
            return pd.DataFrame()
    
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
        try:
            df = ak.stock_zh_a_spot_em()
            if symbols:
                df = df[df["代码"].isin(symbols)]
            return df
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
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

    def get_etf_list(self) -> pd.DataFrame:
        """获取ETF列表"""
        try:
            df = ak.fund_etf_spot_em()
            return df
        except Exception as e:
            logger.error(f"获取ETF列表失败: {e}")
            return pd.DataFrame()

    def get_lof_list(self) -> pd.DataFrame:
        """获取LOF列表"""
        try:
            df = ak.fund_lof_spot_em()
            return df
        except Exception as e:
            logger.error(f"获取LOF列表失败: {e}")
            return pd.DataFrame()

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
