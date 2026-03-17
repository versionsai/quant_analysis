# AGENTS.md - Guidelines for AI Agents

This document provides guidelines for working on this A-share quantitative trading project.

## Project Overview

A quantitative trading system for A-shares (Chinese stock market) focusing on ETF/LOF products with Price Action + MACD strategies.

## Project Structure

```
sai/
├── backtest/       # Backtesting engine
├── config/         # Configuration files
├── data/           # Data source and stock pool
├── strategy/       # Trading strategies (base class + implementations)
├── trading/        # Broker and order handling
├── utils/          # Utilities (logger, validators)
├── main.py         # Entry point
└── requirements.txt
```

## Build & Run Commands

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Run the Application
```bash
python main.py --mode pool        # Get ETF/LOF stock pool
python main.py --mode backtest    # Run backtest (default)
python main.py --mode compare     # Compare strategies
python main.py --strategy pa_macd --symbols 000001 600000
```

### Running Tests
No formal test suite exists. Test manually by running:
```bash
python -m pytest tests/                    # If pytest is added
python -m pytest tests/test_file.py::test_name  # Single test
python main.py --mode backtest             # Manual test via main
```

### Linting
No linting tool is configured. Install and run manually:
```bash
pip install ruff
ruff check .
ruff check src/file.py --fix
```

## Code Style Guidelines

### File Headers
All Python files must start with:
```python
# -*- coding: utf-8 -*-
"""
Module description (in Chinese)
"""
```

### Imports
- Standard library imports first
- Third-party imports second
- Local imports last
- Use type hints from `typing` module
- Sort imports alphabetically within each group

```python
# -*- coding: utf-8 -*-
"""
Description
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

from config.config import BACKTEST_CONFIG
from utils.logger import get_logger
```

### Naming Conventions
- **Classes**: PascalCase (e.g., `BacktestEngine`, `DataSource`)
- **Functions/variables**: snake_case (e.g., `get_kline`, `initial_capital`)
- **Private methods**: prefix with `_` (e.g., `_on_date`, `_calc_result`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `LOG_DIR`, `BACKTEST_CONFIG`)
- **Dataclasses**: PascalCase (e.g., `Signal`, `Trade`)

### Type Hints
Use type hints for all function parameters and return values:
```python
def get_kline(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
```

### Dataclasses
Use `@dataclass` for simple data structures:
```python
@dataclass
class Signal:
    symbol: str
    date: datetime
    signal: float  # 1: buy, -1: sell, 0: hold
    weight: float = 0.0
```

### Error Handling
- Use try/except blocks with specific exception types when possible
- Always log errors with `logger.error()`
- Return empty/default values rather than raising exceptions for expected failures

```python
try:
    df = ak.stock_zh_a_hist(symbol=symbol, ...)
    if df is None or df.empty:
        return pd.DataFrame()
    return df
except Exception as e:
    logger.error(f"获取K线失败 {symbol}: {e}")
    return pd.DataFrame()
```

### Logging
Use the custom logger from `utils.logger`:
```python
from utils.logger import get_logger

logger = get_logger(__name__)

logger.info("message")
logger.warning("message")
logger.error("message")
```

### DataFrame Conventions
- Use lowercase column names: `df.columns = [c.lower() for c in df.columns]`
- Convert date columns: `df["date"] = pd.to_datetime(df["date"])`
- Set date as index when appropriate: `df = df.set_index("date")`

### Configuration
- Store constants in `config/config.py`
- Use uppercase dictionary names: `BACKTEST_CONFIG`, `DATA_CONFIG`
- Use descriptive keys and include units in comments

### Docstrings
Use Chinese docstrings for user-facing documentation:
```python
def get_kline(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取K线数据
    
    Args:
        symbol: 股票代码 (如: 000001, 511880)
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
    
    Returns:
        K线DataFrame
    """
```

### Minimum Code Requirements
- Always use f-strings for string formatting
- Always check for `None` and empty DataFrames before operations
- Use `Optional[T]` when a value can be `None`
- Use `List[T]` and `Dict[K, V]` from typing

### Common Patterns

**DataFrame empty check**:
```python
if df is None or df.empty:
    return pd.DataFrame()
```

**Dictionary with default factory**:
```python
from dataclasses import field

@dataclass
class Portfolio:
    positions: Dict[str, Position] = field(default_factory=dict)
```

**Iterating with enumerate**:
```python
for i, p in enumerate(top_products, 1):
    print(f"  {i}. {p.get('code')} {p.get('name')}")
```

## Adding New Strategies

1. Inherit from `BaseStrategy` in `strategy/base.py`
2. Implement `on_bar` method
3. Return `Signal` with signal value (-1, 0, 1) and optional weight
4. Use `@dataclass` for strategy parameters

```python
class MyStrategy(BaseStrategy):
    def __init__(self, param1: int = 10):
        super().__init__(name="my_strategy")
        self.param1 = param1
    
    def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        if df is None or df.empty:
            return None
        # Strategy logic here
        return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=0.5)
```

## Database/Cache

- Data is cached in `./data/cache` directory
- Logs are written to `./logs` directory
- Create directories with: `os.makedirs(path, exist_ok=True)`

## Dependencies

- pandas>=2.0.0
- numpy>=1.24.0
- akshare>=1.12.0
- requests>=2.28.0
- scipy>=1.10.0
- scikit-learn>=1.3.0
- plotly>=5.18.0
