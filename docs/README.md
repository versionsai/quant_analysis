# API 文档

## Futu API (futu-api v10.0)

- [API 参考](futu_api/reference.md) - Futu OpenD 实时行情 + 历史K线

## Baostock API

- [API 参考](../baostock_api.md) - Baostock 历史K线 (股票+ETF)

## 架构说明

```
实时行情  →  futu-api (Futu OpenD)     → get_stock_quote()
历史K线  →  baostock (支持股票+ETF)   → query_history_k_data_plus()
ETF/LOF  →  akshare (辅助)             → fund_etf_spot_em(), fund_lof_spot_em()
```

### ETF 代码映射表

| 代码 | 市场 | Futu | Baostock |
|------|------|------|----------|
| 600xxx | 上海股票 | SH.600xxx | sh.600xxx |
| 000xxx | 深圳主板 | SZ.000xxx | sz.000xxx |
| 601xxx | 上海股票 | SH.601xxx | sh.601xxx |
| 51xxxx | 上海ETF | SH.51xxxx | sh.51xxxx |
| 50xxxx | 上海ETF | SH.50xxxx | sh.50xxxx |
| 56xxxx | 上海ETF | SH.56xxxx | sh.56xxxx |
| 159xxx | 深圳ETF | SZ.159xxx | sz.159xxx |
| 16xxxx | 深圳ETF/LOF | SZ.16xxxx | sz.16xxxx |

### 关键发现

1. **`futuquant` (旧SDK) 不支持ETF历史K线** - 返回"未知的协议ID"
2. **`futu-api` (新SDK, v10.0.6008) 支持ETF历史K线**
3. **Futu `get_stock_quote()` 无 `change_rate` 字段** - 需自行计算: `(last_price - prev_close_price) / prev_close_price * 100`
4. **Baostock 支持 ETF/LOF 代码** - 经测试: `sh.512050`, `sz.159915`, `sh.511880` 均可用
