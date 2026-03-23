# AGENTS.md - AI Agent Working Guide

This document describes how AI agents should work in this repository.

## Project Summary

This repository is no longer a backtest-only demo. It is now an A-share quantitative trading and monitoring project with:

- ETF/LOF and stock pool management
- Backtesting and strategy comparison
- Intraday realtime monitoring
- Recommendation, review, and dashboard services
- News-driven TACO / TACO-OIL strategies
- AI analysis and push notification workflows
- Local run support and NAS Docker deployment

The current codebase is centered on:

- [main.py](D:/SAI_PROJECT/quant_agent/sai/main.py): unified CLI entry
- [docker_start.py](D:/SAI_PROJECT/quant_agent/sai/docker_start.py): scheduled service startup flow
- [dashboard.py](D:/SAI_PROJECT/quant_agent/sai/dashboard.py): dashboard backend
- [dashboard/index.html](D:/SAI_PROJECT/quant_agent/sai/dashboard/index.html): dashboard frontend
- [strategy/examples/taco_strategy.py](D:/SAI_PROJECT/quant_agent/sai/strategy/examples/taco_strategy.py): TACO and TACO-OIL strategies
- [trading/realtime_monitor.py](D:/SAI_PROJECT/quant_agent/sai/trading/realtime_monitor.py): realtime scan and monitoring
- [data/data_source.py](D:/SAI_PROJECT/quant_agent/sai/data/data_source.py): market data access

## Repository Skills

- When working on Windows and the task touches Chinese text, logs, console output, HTML templates, or suspected mojibake, read [skills/windows-utf8-guard/SKILL.md](D:/SAI_PROJECT/quant_agent/sai/skills/windows-utf8-guard/SKILL.md) first.
- Treat garbled Chinese as a source-text issue until proven otherwise. Do not blindly copy mojibake back into source files.
- After any encoding-related fix, validate with real commands such as `python main.py --help` and a short Chinese logging check.

## Current Repository Layout

```text
sai/
|-- agents/           # AI agents and tools
|-- backtest/         # Backtesting engine and analysis
|-- config/           # Runtime config and event calendars
|-- dashboard/        # Dashboard frontend assets
|-- data/             # Data source, stock pool, cache access
|-- docs/             # Project documentation
|-- runtime/          # Runtime DB, generated data, reports
|-- skills/           # Repository-specific skills
|-- strategy/         # Strategy base classes and implementations
|-- trading/          # Realtime monitor, review, execution helpers
|-- tradingagents/    # Trading-related agent flows
|-- utils/            # Logger and shared utilities
|-- dashboard.py      # Dashboard backend service
|-- docker_start.py   # Production-ish scheduled startup script
|-- main.py           # Main CLI entry
|-- taco_compare.py   # TACO batch compare utility
|-- README.md         # Human-oriented project overview
`-- AGENTS.md         # This file
```

## Main Run Modes

The CLI is defined in [main.py](D:/SAI_PROJECT/quant_agent/sai/main.py). As of 2026-03-23, the supported `--mode` values are:

- `pool`: fetch or print ETF/LOF pool data
- `backtest`: run a strategy backtest
- `compare`: compare strategy performance
- `realtime`: run intraday realtime scan or scheduler
- `pool-update`: refresh runtime stock pool
- `weak-strong`: run weak-to-strong scan
- `emotion-scan`: run emotion / market scan
- `review`: build runtime review report
- `taco-compare`: batch compare TACO variants
- `taco-monitor`: run TACO priority scan

Current `--strategy` choices are:

- `pa_macd`
- `macd`
- `pa`
- `breakout`
- `weak_strong`
- `taco`
- `taco_oil`

Important strategy routing rule:

- `taco` and `taco_oil` should prioritize ETF/LOF candidates.
- Other strategies should continue to use stock-first candidate pools unless the task explicitly requires something else.

## Recommended Local Commands

Install dependencies:

```bash
pip install -r requirements.txt
```

Common local commands:

```bash
python main.py --help
python main.py --mode pool-update
python main.py --mode backtest --strategy pa_macd
python main.py --mode backtest --strategy taco
python main.py --mode compare
python main.py --mode realtime --once
python main.py --mode review
python main.py --mode taco-compare
python main.py --mode taco-monitor --strategy taco
python dashboard.py
python docker_start.py
```

Dashboard default address:

```text
http://127.0.0.1:18675
```

## Current Product Features

### Core Quant Flow

- Build and refresh pools for stocks and ETF/LOF products
- Pull K-line and market data from the configured data providers
- Generate strategy signals
- Run backtests and comparisons
- Output recommendations, reports, and dashboard snapshots

### Realtime and Review

- Realtime scan and monitoring are handled through the trading layer
- Runtime review reports are generated from live recommendation data
- `docker_start.py` is the primary production startup flow for scheduled jobs and push pipelines

### Dashboard

The dashboard currently includes:

- overview metrics
- market mode display
- latest updates
- current positions
- signal pool
- TACO diagnostics
- TACO hot topics

The backend is in [dashboard.py](D:/SAI_PROJECT/quant_agent/sai/dashboard.py), and the frontend is in [dashboard/index.html](D:/SAI_PROJECT/quant_agent/sai/dashboard/index.html).

### TACO Strategy Family

The TACO strategy family is implemented in [strategy/examples/taco_strategy.py](D:/SAI_PROJECT/quant_agent/sai/strategy/examples/taco_strategy.py).

Current design expectations:

- Follow live news and hot topics instead of relying only on static dates
- Use a 30-day event window
- Expose event score, threshold, window length, reasons, and matched keywords
- Prefer ETF/LOF products for TACO candidate selection
- Keep TACO focused on macro event repair trades and topical rotations

Current dashboard helper functions include:

- `build_taco_snapshot(...)`
- `build_taco_hot_topics(...)`

## Windows Encoding Rules

This repository is used heavily on Windows. Encoding mistakes are expensive here.

- Python files must be saved as UTF-8.
- Chinese copy in logs, CLI help, HTML, JSON, and Markdown must remain readable on Windows.
- When you see mojibake in PowerShell, verify the source file before editing it.
- Use the repo logger utilities instead of ad-hoc console handling when possible.
- Do not assume terminal output equals file content.

When changing Windows-facing output, validate at least:

```bash
python main.py --help
python -c "from utils.logger import get_logger; print('中文输出测试'); logger=get_logger('encoding_test'); logger.info('中文日志测试')"
```

## Coding Guidelines

### Python File Header

All Python source files should start with:

```python
# -*- coding: utf-8 -*-
"""
模块说明
"""
```

### Imports

- Standard library imports first
- Third-party imports second
- Local imports last
- Keep imports grouped and readable
- Use type hints from `typing`

### Naming

- Classes: `PascalCase`
- Functions and variables: `snake_case`
- Private helpers: `_leading_underscore`
- Constants: `UPPER_SNAKE_CASE`
- Dataclasses: `PascalCase`

### Type Hints

Use type hints for parameters and return values.

```python
def get_kline(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    ...

def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
    ...
```

### Dataclasses

Use `@dataclass` for simple data containers.

```python
@dataclass
class Signal:
    symbol: str
    date: datetime
    signal: float
    weight: float = 0.0
```

### Error Handling

- Prefer explicit `try/except` blocks around expected external failures
- Log with `logger.error(...)`
- Return safe empty defaults for expected data failures

```python
try:
    df = ak.stock_zh_a_hist(symbol=symbol, ...)
    if df is None or df.empty:
        return pd.DataFrame()
    return df
except Exception as exc:
    logger.error(f"获取K线失败 {symbol}: {exc}")
    return pd.DataFrame()
```

### Logging

Use the custom logger from [utils/logger.py](D:/SAI_PROJECT/quant_agent/sai/utils/logger.py):

```python
from utils.logger import get_logger

logger = get_logger(__name__)
logger.info("message")
logger.warning("message")
logger.error("message")
```

### DataFrame Conventions

- Normalize columns to lowercase when practical
- Convert date columns with `pd.to_datetime`
- Set date as index when the downstream flow expects time-series operations
- Always guard against `None` and empty DataFrames

```python
if df is None or df.empty:
    return pd.DataFrame()
```

### String and Comment Rules

- Prefer f-strings
- Keep comments short and useful
- User-facing docstrings should be in Chinese
- Avoid stale comments that no longer match the code

## Strategy Development Rules

When adding or modifying a strategy:

1. Inherit from `BaseStrategy`
2. Keep the signal contract consistent
3. Use dataclasses for parameter bundles where appropriate
4. Make the candidate universe explicit
5. Keep stock-first vs ETF/LOF-first behavior intentional
6. Add or update the CLI wiring in [main.py](D:/SAI_PROJECT/quant_agent/sai/main.py) if the strategy should be runnable
7. If the strategy is dashboard-visible, update both [dashboard.py](D:/SAI_PROJECT/quant_agent/sai/dashboard.py) and [dashboard/index.html](D:/SAI_PROJECT/quant_agent/sai/dashboard/index.html)

Example skeleton:

```python
class MyStrategy(BaseStrategy):
    def __init__(self, param1: int = 10):
        super().__init__(name="my_strategy")
        self.param1 = param1

    def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        if df is None or df.empty:
            return None
        return Signal(symbol=symbol, date=datetime.now(), signal=1, weight=0.5)
```

## Data, Runtime, and Cache Notes

- Runtime DB and generated artifacts are usually under `./runtime`
- Logs are written under `./logs`
- Cache and pool helpers live under `./data`
- Create directories with `os.makedirs(path, exist_ok=True)`

Be careful with:

- local cache shape changes
- runtime database schema assumptions
- remote data provider latency
- fallback behavior when dynamic pools are empty

## Validation Checklist

There is no stable full automated test suite yet. Manual verification matters.

For normal Python changes:

```bash
python -m py_compile main.py dashboard.py taco_compare.py
```

For strategy changes:

```bash
python -m py_compile strategy/examples/taco_strategy.py strategy/__init__.py strategy/examples/__init__.py
python main.py --help
python main.py --mode taco-compare
```

For dashboard changes:

```bash
python -m py_compile dashboard.py
python dashboard.py
```

For Windows encoding changes:

```bash
python main.py --help
python -c "from utils.logger import get_logger; print('中文输出测试'); logger=get_logger('encoding_test'); logger.info('中文日志测试')"
```

For realtime candidate-pool changes:

```bash
python main.py --mode taco-monitor --strategy taco
python main.py --mode taco-monitor --strategy taco_oil
```

## Deployment Notes

- The repo includes Docker and Gitea workflow files for NAS deployment
- `docker_start.py` is the most relevant Python entry for scheduled production behavior
- Push-related changes should be checked against `README.md`, deployment scripts, and `.gitea/workflows`

## Documentation Expectations

When updating the project:

- Keep [README.md](D:/SAI_PROJECT/quant_agent/sai/README.md) user-oriented
- Keep [AGENTS.md](D:/SAI_PROJECT/quant_agent/sai/AGENTS.md) agent-oriented
- Update this file when modes, strategy names, dashboard sections, runtime flow, or Windows encoding rules change
- Prefer replacing outdated sections instead of layering new contradictory notes on top

## Current Priorities To Preserve

- Keep TACO strategy paths ETF/LOF-first
- Keep other strategies stock-first unless explicitly changed
- Keep dashboard diagnostics readable and useful
- Keep Windows Chinese output clean
- Keep realtime flows responsive and avoid unnecessary remote-pool latency
