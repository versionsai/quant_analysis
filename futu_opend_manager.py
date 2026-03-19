# -*- coding: utf-8 -*-
"""
富途 OpenD 配置和本地部署脚本
"""
import os
import sys
import subprocess
import time

# 富途 OpenD 配置
FUTU_OPEND_CONFIG = """
# 富途 OpenD 配置文件
# Windows: C:\\Users\\<用户名>\\AppData\\Local\\FutuOpenD\\config
# Mac: ~/FutuOpenD/config
# Linux: ~/.FutuOpenD/config

# 连接配置
[Connection]
Host=0.0.0.0
Port=11111

# API 协议配置
[Protocol]
ApiProtocol=websocket

# 日志配置
[Log]
LogLevel=INFO
LogPath=logs
"""

FUTU_START_SCRIPT_LINUX = """#!/bin/bash
# 富途 OpenD 启动脚本 (Linux/Mac)
# 下载地址: https://www.futunn.com/market-center/developers

FUTU_OPEND_PATH="/opt/FutuOpenD"

if [ ! -d "$FUTU_OPEND_PATH" ]; then
    echo "富途 OpenD 未安装"
    echo "请从以下地址下载并解压:"
    echo "https://www.futunn.com/market-center/developers"
    exit 1
fi

cd "$FUTU_OPEND_PATH"
./futuopend
"""

FUTU_START_SCRIPT_MAC = """#!/bin/bash
# 富途 OpenD 启动脚本 (Mac)
# 下载地址: https://www.futunn.com/market-center/developers

FUTU_OPEND_PATH="$HOME/FutuOpenD"

if [ ! -d "$FUTU_OPEND_PATH" ]; then
    echo "富途 OpenD 未安装"
    echo "请从以下地址下载并解压:"
    echo "https://www.futunn.com/market-center/developers"
    exit 1
fi

cd "$FUTU_OPEND_PATH"
./futuopend
"""

LOCAL_FUTU_CONFIG = """
# -*- coding: utf-8 -*-
# 富途本地配置
# 复制此文件为 futu_local.py 并填入实际配置

# 本地 OpenD 配置
FUTU_HOST = "127.0.0.1"      # 本地开发
# FUTU_HOST = "192.168.5.6"  # NAS 虚拟机

FUTU_PORT = 11111

# 数据源配置
FUTU_DATA_SOURCE = "local"  # local: 本地 OpenD, remote: NAS OpenD
"""


def check_futu_opend():
    """检查富途 OpenD 是否运行"""
    import socket

    host = os.environ.get("FUTU_HOST", "127.0.0.1")
    port = int(os.environ.get("FUTU_PORT", 11111))

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()

        if result == 0:
            return True
        else:
            return False
    except:
        return False


def start_futu_opend():
    """启动富途 OpenD"""
    import platform

    system = platform.system()

    if system == "Darwin":  # Mac
        script = FUTU_START_SCRIPT_MAC
        path = os.path.expanduser("~/FutuOpenD")
    else:  # Linux
        script = FUTU_START_SCRIPT_LINUX
        path = "/opt/FutuOpenD"

    print(f"富途 OpenD 路径: {path}")

    if os.path.exists(path):
        print("富途 OpenD 已安装")
        print("请手动启动:")
        print(f"  cd {path} && ./futuopend")
    else:
        print("富途 OpenD 未安装")
        print()
        print("安装步骤:")
        print("1. 访问 https://www.futunn.com/market-center/developers")
        print("2. 下载 FutuOpenD (根据您的操作系统)")
        print("3. 解压到目标目录")
        print("4. 运行启动脚本")


def test_local_connection():
    """测试连接"""
    from futuquant import OpenQuoteContext, RET_OK

    host = os.environ.get("FUTU_HOST", "127.0.0.1")
    port = int(os.environ.get("FUTU_PORT", 11111))

    print(f"尝试连接富途 OpenD: {host}:{port}")

    try:
        quote_ctx = OpenQuoteContext(host=host, port=port)

        print("连接成功! 获取报价...")
        ret, data = quote_ctx.get_stock_quote("SH.600036")

        if ret == RET_OK:
            print(data[['code', 'name', 'last_price', 'change_rate']])
        else:
            print(f"获取报价失败: {data}")

        quote_ctx.close()
        print("测试完成!")
        return True

    except Exception as e:
        print(f"连接失败: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="富途 OpenD 管理工具")
    parser.add_argument("action", choices=["check", "start", "test"], help="操作")

    args = parser.parse_args()

    if args.action == "check":
        if check_futu_opend():
            print("✅ 富途 OpenD 正在运行")
        else:
            print("❌ 富途 OpenD 未运行")
            print("请先启动 OpenD")

    elif args.action == "start":
        start_futu_opend()

    elif args.action == "test":
        if not check_futu_opend():
            print("❌ 富途 OpenD 未运行，请先启动")
        else:
            test_local_connection()
