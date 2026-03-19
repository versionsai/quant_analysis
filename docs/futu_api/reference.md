# Futu API 参考 (futu-api v10.0)

## 模块导入

```python
from futu import OpenQuoteContext, SubType, KLType, AuType, RET_OK
```

**注意**: 新版 `futu-api` 使用 `from futu import ...`，旧版 `futuquant` 使用 `from futuquant import ...`。

## 连接管理

```python
from futu import OpenQuoteContext

quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
# 完成后关闭连接
quote_ctx.close()
```

## 订阅 (subscribe)

```python
from futu import SubType

ret, err = quote_ctx.subscribe(
    code_list=['SH.600000', 'SZ.000001'],
    subtype_list=[SubType.QUOTE],  # 实时报价
    subscribe_push=False  # False=仅拉取, True=推送回调
)
```

**注意**: `subscribe_push=False` 适用于仅使用 `get_stock_quote()` 拉取数据的场景，可节省性能。

## 获取实时报价 (get_stock_quote)

```python
from futu import RET_OK

ret, data = quote_ctx.get_stock_quote(['SH.600000'])
if ret == RET_OK:
    print(data)
```

**必须先订阅才能获取数据。**

### 返回字段

| 字段 | 类型 | 说明 |
|------|------|------|
| code | str | 股票代码 |
| name | str | 股票名称 |
| data_date | str | 日期 (yyyy-MM-dd) |
| data_time | str | 更新时间 (yyyy-MM-dd HH:mm:ss) |
| last_price | float | 最新价格 |
| open_price | float | 今日开盘价 |
| high_price | float | 最高价格 |
| low_price | float | 最低价格 |
| prev_close_price | float | 昨收盘价格 |
| volume | int | 成交数量 |
| turnover | float | 成交金额 |
| turnover_rate | float | 换手率 (小数形式, 如 0.01 = 1%) |
| amplitude | int | 振幅 |
| suspension | bool | 是否停牌 |
| listing_date | str | 上市日期 (yyyy-MM-dd) |
| pre_price / pre_high_price / pre_low_price | float | 盘前价格/最高/最低 |
| pre_change_val / pre_change_rate | float | 盘前涨跌额/涨跌幅 |
| after_price / after_high_price / after_low_price | float | 盘后价格/最高/最低 |
| after_change_val / after_change_rate | float | 盘后涨跌额/涨跌幅 |

**关键**: `get_stock_quote()` 返回的 DataFrame 中 **没有 `change_rate` 字段**！需要自己计算：
```python
change_rate = (last_price - prev_close_price) / prev_close_price * 100
```

## 获取历史K线 (request_history_kline)

```python
from futu import KLType, AuType

ret, data, page_key = quote_ctx.request_history_kline(
    code='SH.600000',
    start='2024-01-01',
    end='2024-12-31',
    ktype=KLType.K_DAY,  # 日K
    autype=AuType.QFQ,   # 前复权
    max_count=1000       # 每页最大条数
)
```

### KLType 枚举

```python
KLType.K_DAY   # 日K
KLType.K_WEEK  # 周K
KLType.K_MON   # 月K
KLType.K_1Min  # 1分钟
KLType.K_5Min  # 5分钟
KLType.K_15Min # 15分钟
KLType.K_30Min # 30分钟
KLType.K_60Min # 60分钟
```

### AuType 枚举

```python
AuType.QFQ  # 前复权
AuType.HFQ  # 后复权
AuType.NONE # 不复权
```

### 返回字段

| 字段 | 类型 | 说明 |
|------|------|------|
| code | str | 股票代码 |
| name | str | 股票名称 |
| time_key | str | K线时间 (yyyy-MM-dd HH:mm:ss) |
| open | float | 开盘价 |
| close | float | 收盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| pe_ratio | float | 市盈率 |
| turnover_rate | float | 换手率 |
| volume | int | 成交量 |
| turnover | float | 成交额 |
| change_rate | float | 涨跌幅 (%) |
| last_close | float | 昨收价 |

### 分页请求

```python
ret, data, page_key = quote_ctx.request_history_kline(...)
while page_key is not None:
    ret, data, page_key = quote_ctx.request_history_kline(
        code='SH.600000',
        page_req_key=page_key
    )
```

**注意**: 
- `futuquant` (旧SDK) **不支持ETF历史K线**！会返回"未知的协议ID"错误。
- `futu-api` (新SDK) 支持ETF历史K线。
- 历史K线接口每30秒最多请求60次首页。

## 获取实时K线 (get_cur_kline)

```python
from futu import KLType, AuType, SubType

quote_ctx.subscribe(['SH.600000'], [SubType.K_DAY], subscribe_push=False)
ret, data = quote_ctx.get_cur_kline('SH.600000', num=10, ktype=KLType.K_DAY)
```

**必须先订阅，且只能获取最近1000根。**

返回字段与 `request_history_kline` 类似 (无 `change_rate`)。

## A股代码格式

Futu API 中 A 股代码格式为 `SH.6xxxxx` / `SZ.0xxxxx` / `SZ.3xxxxx`。

### ETF 代码映射

| ETF代码前缀 | 市场 | Futu格式 | Baostock格式 |
|-----------|------|---------|-------------|
| 51xxxx / 50xxxx / 56xxxx | 上海 | SH.51xxxx | sh.51xxxx |
| 159xxxx / 16xxxx | 深圳 | SZ.159xxxx | sz.159xxxx |

### 代码标准化函数参考

```python
def symbol_to_futu(symbol: str) -> str:
    """6位代码 → Futu格式 (SH./SZ.)"""
    symbol = str(symbol).strip().zfill(6)
    if symbol.startswith(('SH.', 'SZ.')):
        return symbol
    if symbol.startswith(('6', '5', '9')):
        return f"SH.{symbol}"
    return f"SZ.{symbol}"

def symbol_to_baostock(symbol: str) -> str:
    """6位代码 → Baostock格式 (sh./sz.)"""
    symbol = str(symbol).strip().zfill(6)
    if symbol.startswith(('6', '5', '9')):
        return f"sh.{symbol}"
    return f"sz.{symbol}"
```
