# -*- coding: utf-8 -*-
"""
A股数据源 - baostock历史K线 + futu实时行情 + akshare辅助数据
- 历史K线: baostock (支持股票+ETF)
- 实时行情: futu-api (Futu OpenD)
- ETF/LOF列表: akshare
"""
import akshare as ak
import baostock as bs
import pandas as pd
import os
from datetime import datetime
from typing import Optional, List
from utils.logger import get_logger

logger = get_logger(__name__)

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


def _symbol_to_baostock(symbol: str) -> str:
    """将6位代码转换为baostock格式 (sh.600000 / sz.000001)"""
    symbol = str(symbol).zfill(6)
    if symbol.startswith(("6", "5", "9")):
        return f"sh.{symbol}"
    return f"sz.{symbol}"


def _adjust_flag(adjust: str) -> str:
    """将adjust参数映射到baostock adjustflag"""
    if adjust == "qfq":
        return "2"
    if adjust == "hfq":
        return "1"
    return "3"


class DataSource:
    
    def __init__(self, cache_dir: str = None):
        if cache_dir is None:
            cache_dir = os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        self._futu_ctx = None
        self._futu_host = os.environ.get("FUTU_HOST", "127.0.0.1")
        self._futu_port = int(os.environ.get("FUTU_PORT", 11111))
        self._futu_connected = False
        self._subscribed: set = set()
    
    def _init_futu(self) -> bool:
        """初始化Futu连接 (用于实时行情)"""
        if self._futu_connected and self._futu_ctx is not None:
            return True
        try:
            from futu import OpenQuoteContext
            self._futu_ctx = OpenQuoteContext(host=self._futu_host, port=self._futu_port)
            self._futu_connected = True
            logger.info(f"Futu连接成功: {self._futu_host}:{self._futu_port} (futu-api)")
            return True
        except ImportError:
            try:
                from futuquant import OpenQuoteContext
                self._futu_ctx = OpenQuoteContext(host=self._futu_host, port=self._futu_port)
                self._futu_connected = True
                logger.info(f"Futu连接成功: {self._futu_host}:{self._futu_port} (futuquant)")
                return True
            except Exception as e:
                logger.warning(f"Futu连接失败: {e}")
                self._futu_connected = False
                return False
        except Exception as e:
            logger.warning(f"Futu连接失败: {e}")
            self._futu_connected = False
            return False
    
    def close(self):
        """关闭Futu连接"""
        if self._futu_ctx:
            try:
                self._futu_ctx.close()
            except Exception:
                pass
            self._futu_ctx = None
            self._futu_connected = False
    
    def _futu_normalize(self, symbol: str) -> str:
        """Futu代码标准化"""
        symbol = str(symbol).strip().upper().zfill(6)
        if symbol.startswith(("SH.", "SZ.")):
            return symbol
        if symbol.startswith(("6", "5", "9")):
            return f"SH.{symbol}"
        return f"SZ.{symbol}"
    
    def _ensure_futu_sub(self, codes: List[str]):
        """确保Futu订阅"""
        from futu import SubType
        need = [c for c in codes if c not in self._subscribed]
        if need:
            try:
                self._futu_ctx.subscribe(need, [SubType.QUOTE])
                self._subscribed.update(need)
            except Exception as e:
                logger.warning(f"Futu订阅失败: {e}")
    
    def get_kline(self, symbol: str, start_date: str, end_date: str,
                   adjust: str = "qfq") -> pd.DataFrame:
        """
        获取K线数据 (历史 → baostock, 股票+ETF均支持)
        
        Args:
            symbol: 代码 (如: 000001, 512050)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            adjust: 复权类型 qfq/hfq/None
        """
        symbol = str(symbol).zfill(6)
        if symbol.startswith(("51", "15", "16", "50", "56", "58")):
            return self._get_etf_kline(symbol, start_date, end_date, adjust)
        return self._get_kline_baostock(symbol, start_date, end_date, adjust)
    
    def _get_kline_baostock(self, symbol: str, start_date: str,
                             end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """使用baostock获取股票/ETF历史K线"""
        _ensure_bs_login()
        
        start_str = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if len(start_date) == 8 else start_date
        end_str = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}" if len(end_date) == 8 else end_date
        
        bs_code = _symbol_to_baostock(symbol)
        flag = _adjust_flag(adjust)
        
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,pctChg",
                start_date=start_str,
                end_date=end_str,
                frequency="d",
                adjustflag=flag
            )
            
            if rs.error_code != '0':
                logger.error(f"Baostock K线查询失败 {symbol}: {rs.error_msg}")
                return pd.DataFrame()
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            df["date"] = pd.to_datetime(df["date"])
            
            for col in ["open", "high", "low", "close", "volume", "amount", "pctChg"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            df = df.rename(columns={"pctChg": "pct_change"})
            return df
        except Exception as e:
            logger.error(f"Baostock获取K线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def _get_etf_kline(self, symbol: str, start_date: str,
                        end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """获取ETF/LOF K线 (baostock)"""
        return self._get_kline_baostock(symbol, start_date, end_date, adjust)
    
    def get_realtime_quotes(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取实时行情 (Futu优先, akshare兜底)
        
        Returns DataFrame with columns:
            code, name, last_price, change_rate, volume, turnover, ...
        """
        df = self._get_quotes_futu(symbols)
        if df is not None and not df.empty:
            return df
        
        logger.warning("Futu实时行情失败，尝试akshare")
        return self._get_quotes_akshare(symbols)
    
    def _get_quotes_futu(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """使用Futu获取实时行情"""
        if not self._init_futu():
            return pd.DataFrame()
        if not symbols:
            return pd.DataFrame()
        
        try:
            codes = [self._futu_normalize(s) for s in symbols]
            self._ensure_futu_sub(codes)
            
            ret, data = self._futu_ctx.get_stock_quote(codes)
            if ret == 0 and data is not None and not data.empty:
                df = data.copy()
                df.columns = [c.lower() for c in df.columns]
                
                if "code" in df.columns:
                    df["code"] = df["code"].str.replace("SH.", "").str.replace("SZ.", "")
                
                if "last_price" in df.columns and "prev_close_price" in df.columns:
                    df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce")
                    df["prev_close_price"] = pd.to_numeric(df["prev_close_price"], errors="coerce")
                    df["change_rate"] = ((df["last_price"] - df["prev_close_price"]) / df["prev_close_price"] * 100).round(2)
                
                return df
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Futu获取实时行情失败: {e}")
            return pd.DataFrame()
    
    def _get_quotes_akshare(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """akshare实时行情兜底"""
        try:
            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                if symbols:
                    df = df[df["代码"].isin([str(s).zfill(6) for s in symbols])]
                return df
        except Exception as e:
            logger.error(f"akshare获取实时行情失败: {e}")
        return pd.DataFrame()
    
    def get_stock_info(self, symbol: str) -> dict:
        """获取股票基本信息 (akshare)"""
        try:
            df = ak.stock_individual_info_em(symbol=str(symbol).zfill(6))
            return dict(zip(df["item"], df["value"]))
        except Exception as e:
            logger.error(f"获取股票信息失败 {symbol}: {e}")
            return {}
    
    def get_index_daily(self, symbol: str = "000300") -> pd.DataFrame:
        """获取指数日线 (akshare)"""
        try:
            df = ak.stock_zh_index_daily(symbol=f"sh{symbol}")
            df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            logger.error(f"获取指数日线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def get_industry_stocks(self, industry: str) -> List[str]:
        """获取行业成分股 (akshare)"""
        try:
            df = ak.stock_board_industry_name_em()
            code = df[df["板块名称"] == industry]["板块代码"].values[0]
            df = ak.stock_board_industry_cons_em(symbol=code)
            return df["代码"].tolist()
        except Exception as e:
            logger.error(f"获取行业成分股失败 {industry}: {e}")
            return []
    
    def get_concept_stocks(self, concept: str) -> List[str]:
        """获取概念成分股 (akshare)"""
        try:
            df = ak.stock_board_concept_name_em()
            code = df[df["板块名称"] == concept]["板块代码"].values[0]
            df = ak.stock_board_concept_cons_em(symbol=code)
            return df["代码"].tolist()
        except Exception as e:
            logger.error(f"获取概念成分股失败 {concept}: {e}")
            return []
    
    def get_financial_data(self, symbol: str, type_: str = "balancesheet") -> pd.DataFrame:
        """获取财务数据 (akshare)"""
        try:
            func_map = {
                "balancesheet": ak.stock_balance_sheet,
                "income": ak.stock_income,
                "cashflow": ak.stock_cashflow
            }
            df = func_map[type_](symbol=str(symbol).zfill(6))
            return df
        except Exception as e:
            logger.error(f"获取财务数据失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def get_etf_list(self) -> pd.DataFrame:
        """获取ETF列表 (akshare)"""
        try:
            return ak.fund_etf_spot_em()
        except Exception as e:
            logger.warning(f"获取ETF列表失败: {e}")
            return self._get_default_etf_list()
    
    def get_lof_list(self) -> pd.DataFrame:
        """获取LOF列表 (akshare)"""
        try:
            return ak.fund_lof_spot_em()
        except Exception as e:
            logger.warning(f"获取LOF列表失败: {e}")
            return self._get_default_lof_list()
    
    def _get_default_etf_list(self) -> pd.DataFrame:
        df = pd.DataFrame([i for i in DEFAULT_ETF_LIST if i["type"] == "ETF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1_000_000_000
        return df
    
    def _get_default_lof_list(self) -> pd.DataFrame:
        df = pd.DataFrame([i for i in DEFAULT_ETF_LIST if i["type"] == "LOF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1_000_000_000
        return df
    
    def get_default_pool(self) -> List[dict]:
        """默认ETF/LOF股票池"""
        products = []
        for item in DEFAULT_ETF_LIST:
            products.append({
                "code": item["code"],
                "name": item["name"],
                "amount": 1_000_000_000,
                "type": item["type"],
                "t0": item["code"].startswith(("51", "15"))
            })
        products.sort(key=lambda x: (-x["amount"], not x["t0"]))
        return products
    
    def get_etf_lof_pool(self, min_amount: float = 300_000_000,
                         prefer_t0: bool = True) -> List[dict]:
        """获取ETF/LOF股票池"""
        etf_list, lof_list = [], []
        
        try:
            df = self.get_etf_list()
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                if "成交额" in df.columns:
                    df = df[df["成交额"] >= min_amount]
                    etf_list = df.to_dict("records")
        except Exception as e:
            logger.warning(f"获取ETF列表失败: {e}")
        
        try:
            df = self.get_lof_list()
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                if "成交额" in df.columns:
                    df = df[df["成交额"] >= min_amount]
                    lof_list = df.to_dict("records")
        except Exception as e:
            logger.warning(f"获取LOF列表失败: {e}")
        
        if not etf_list and not lof_list:
            logger.warning("ETF/LOF列表获取失败，使用默认池")
            return self.get_default_pool()
        
        all_products = []
        for item in etf_list:
            code = str(item.get("代码", "")).zfill(6)
            all_products.append({
                "code": code,
                "name": str(item.get("名称", "")),
                "amount": item.get("成交额", 0),
                "type": "ETF",
                "t0": code.startswith(("51", "15"))
            })
        for item in lof_list:
            code = str(item.get("代码", "")).zfill(6)
            all_products.append({
                "code": code,
                "name": str(item.get("名称", "")),
                "amount": item.get("成交额", 0),
                "type": "LOF",
                "t0": code.startswith(("16", "15"))
            })
        
        if prefer_t0:
            all_products.sort(key=lambda x: (not x["t0"], -x["amount"]))
        else:
            all_products.sort(key=lambda x: -x["amount"])
        
        return all_products
