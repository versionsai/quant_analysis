---
name: windows-utf8-guard
description: Use when editing or running this repository on Windows, especially if the task touches Chinese text, console output, logs, HTML templates, or any file that may show garbled characters. Helps prevent and fix UTF-8/GBK mojibake in this project.
---

# Windows UTF-8 Guard

Use this skill whenever work happens on Windows and any of these are true:

- Chinese output appears garbled in terminal or logs
- A file already contains mojibake such as `鍒`, `鏃`, `鑾`, `绛`, `淇`, `馃`
- You need to edit user-facing Chinese copy
- You are touching `main.py`, `dashboard/index.html`, `dashboard.py`, `data/*`, or logging paths

## Rules

1. Keep source files in UTF-8.
2. Do not paste back garbled text you see in a broken console. Read files with UTF-8 and repair the actual source text.
3. On Windows, prefer configuring stdout/stderr and console code page to UTF-8 before trusting terminal output.
4. When a file mixes normal Chinese and mojibake, treat the mojibake as corrupted source text, not as a display-only problem.
5. After fixing copy, validate by actually running the relevant command and checking visible output.

## Project-specific checks

- `utils/logger.py`
  Ensure Windows console output is configured for UTF-8.
- `main.py`
  This file is the most visible CLI surface. Keep help text, mode descriptions, scan output, and backtest summaries readable.
- `dashboard/index.html`
  This is the most visible browser surface. Fix visible headings, buttons, labels, and empty states before lower-priority copy.
- `data/stock_pool.py` and `data/data_source.py`
  Fix log lines and fallback ETF/LOF names that appear in CLI flows.

## Fast workflow

1. Search for obvious mojibake markers like `鍒`, `鏃`, `鑾`, `绛`, `淇`, `馃`.
2. Fix user-facing strings first.
3. Run one or two representative commands:
   `python main.py --help`
   `python main.py --mode taco-monitor --strategy taco`
4. If logs are involved, also run a small Python one-liner that prints Chinese and writes one log line.

## Editing guidance

- Prefer replacing whole user-facing strings with clean Chinese or simple English instead of trying to decode bad text in place.
- For high-traffic CLI text, concise Chinese is preferred.
- For fallback-safe labels, simple ASCII English is acceptable if Chinese repair would be risky.
- Do not rewrite unrelated business logic while cleaning copy.

## Validation

Use these checks after edits:

```bash
python -m py_compile main.py dashboard.py utils/logger.py
python main.py --help
python main.py --mode taco-monitor --strategy taco_oil
python -c "from utils.logger import get_logger; print('中文输出测试'); get_logger('utf8_guard').info('中文日志测试')"
```

## Warning signs

- `ThreadPoolExecutor` timeout still blocks because the executor waits on shutdown
- A terminal looks fixed but browser text is still garbled
- A file was saved in ANSI/GBK by an editor after you repaired it
- You only fixed console encoding but left corrupted source strings untouched
