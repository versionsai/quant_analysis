#!/bin/bash
# 富途 HTTP 服务部署脚本
# 在 NAS Ubuntu 虚拟机中运行此脚本

# 安装依赖
pip3 install futuquant flask

# 启动服务
python3 futu_http_server.py
