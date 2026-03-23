# -*- coding: utf-8 -*-
"""
日志工具
"""
import logging
import os
import sys
from datetime import datetime

LOG_DIR = "./logs"
os.makedirs(LOG_DIR, exist_ok=True)


def _configure_windows_console_utf8() -> None:
    """
    在 Windows 控制台下尽量统一到 UTF-8，减少中文乱码。
    """
    if os.name != "nt":
        return

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleOutputCP(65001)
        kernel32.SetConsoleCP(65001)
    except Exception:
        pass

    for stream_name in ["stdout", "stderr"]:
        stream = getattr(sys, stream_name, None)
        try:
            if stream is not None and hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            continue


_configure_windows_console_utf8()


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    fh = logging.FileHandler(
        f"{LOG_DIR}/{name}_{datetime.now().strftime('%Y%m%d')}.log",
        encoding="utf-8"
    )
    fh.setFormatter(formatter)
    
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger
