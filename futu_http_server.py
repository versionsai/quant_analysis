# -*- coding: utf-8 -*-
"""
富途 OpenAPI HTTP 服务
在 NAS 虚拟机上运行，转发富途数据到外部
"""
import os
import json
import time
from flask import Flask, jsonify, request
from futuquant import *

app = Flask(__name__)

HOST = "0.0.0.0"
PORT = int(os.environ.get("FUTU_HTTP_PORT", 8080))
FUTU_HOST = os.environ.get("FUTU_HOST", "127.0.0.1")
FUTU_PORT = int(os.environ.get("FUTU_PORT", 11111))

quote_ctx = None


def init_futu():
    """初始化富途连接"""
    global quote_ctx
    try:
        quote_ctx = OpenQuoteContext(host=FUTU_HOST, port=FUTU_PORT)
        print(f"富途连接成功: {FUTU_HOST}:{FUTU_PORT}")
        return True
    except Exception as e:
        print(f"富途连接失败: {e}")
        return False


@app.route("/health", methods=["GET"])
def health():
    """健康检查"""
    return jsonify({"status": "ok", "futu_connected": quote_ctx is not None})


@app.route("/quote/<path:symbol>", methods=["GET"])
def get_quote(symbol):
    """获取股票报价"""
    if not quote_ctx:
        return jsonify({"error": "Futu not connected"}), 500

    try:
        if not symbol.startswith("SH.") and not symbol.startswith("SZ."):
            symbol = f"SH.{symbol}" if symbol.startswith("6") else f"SZ.{symbol}"

        ret, data = quote_ctx.get_stock_quote(symbol)
        if ret == RET_OK:
            return jsonify({"code": 0, "data": data.to_dict(orient="records")[0]})
        else:
            return jsonify({"code": 1, "error": str(data)}), 400
    except Exception as e:
        return jsonify({"code": 1, "error": str(e)}), 500


@app.route("/kline/<path:symbol>", methods=["GET"])
def get_kline(symbol):
    """获取K线数据"""
    if not quote_ctx:
        return jsonify({"error": "Futu not connected"}), 500

    try:
        if not symbol.startswith("SH.") and not symbol.startswith("SZ."):
            symbol = f"SH.{symbol}" if symbol.startswith("6") else f"SZ.{symbol}"

        start = request.args.get("start", "2026-01-01")
        end = request.args.get("end", time.strftime("%Y-%m-%d"))
        ktype_str = request.args.get("ktype", "K_DAY")
        ktype = KTYPE_MAP.get(ktype_str, KLType.K_DAY)

        ret, data, page_key = quote_ctx.request_history_kline(
            symbol, start=start, end=end, ktype=ktype, max_count=1000
        )
        if ret == RET_OK:
            return jsonify({"code": 0, "data": data.to_dict(orient="records")})
        else:
            return jsonify({"code": 1, "error": str(data)}), 400
    except Exception as e:
        return jsonify({"code": 1, "error": str(e)}), 500


@app.route("/batch_quote", methods=["POST"])
def batch_quote():
    """批量获取报价"""
    if not quote_ctx:
        return jsonify({"error": "Futu not connected"}), 500

    try:
        symbols = request.json.get("symbols", [])
        codes = []
        for s in symbols:
            if not s.startswith("SH.") and not s.startswith("SZ."):
                s = f"SH.{s}" if s.startswith("6") else f"SZ.{s}"
            codes.append(s)

        ret, data = quote_ctx.get_stock_quote(codes)
        if ret == RET_OK:
            return jsonify({"code": 0, "data": data.to_dict(orient="records")})
        else:
            return jsonify({"code": 1, "error": str(data)}), 400
    except Exception as e:
        return jsonify({"code": 1, "error": str(e)}), 500


@app.route("/rt_kline/<path:symbol>", methods=["GET"])
def get_rt_kline(symbol):
    """获取实时K线"""
    if not quote_ctx:
        return jsonify({"error": "Futu not connected"}), 500

    try:
        if not symbol.startswith("SH.") and not symbol.startswith("SZ."):
            symbol = f"SH.{symbol}" if symbol.startswith("6") else f"SZ.{symbol}"

        ret, data = quote_ctx.get_rt_kline(symbol, num=100, ktype=KLType.K_DAY)
        if ret == RET_OK:
            return jsonify({"code": 0, "data": data.to_dict(orient="records")})
        else:
            return jsonify({"code": 1, "error": str(data)}), 400
    except Exception as e:
        return jsonify({"code": 1, "error": str(e)}), 500


@app.route("/trade_days", methods=["GET"])
def get_trade_days():
    """获取交易日历"""
    if not quote_ctx:
        return jsonify({"error": "Futu not connected"}), 500

    try:
        start_date = request.args.get("start", "20260101")
        end_date = request.args.get("end", time.strftime("%Y%m%d"))

        ret, data = quote_ctx.get_trading_days("SH", start_date, end_date)
        if ret == RET_OK:
            return jsonify({"code": 0, "data": data["trade_date"].tolist()})
        else:
            return jsonify({"code": 1, "error": str(data)}), 400
    except Exception as e:
        return jsonify({"code": 1, "error": str(e)}), 500


KTYPE_MAP = {
    "K_DAY": KLType.K_DAY,
    "K_WEEK": KLType.K_WEEK,
    "K_MON": KLType.K_MON,
    "K_15M": KLType.K_15M,
    "K_30M": KLType.K_30M,
    "K_60M": KLType.K_60M,
    "K_1M": KLType.K_1M,
}


def main():
    import sys
    print("=" * 50)
    print("富途 OpenAPI HTTP 服务")
    print("=" * 50)
    print(f"监听地址: http://{HOST}:{PORT}")
    print(f"富途连接: {FUTU_HOST}:{FUTU_PORT}")
    print()
    print("环境变量说明:")
    print("  FUTU_HOST      - Futu OpenD 地址 (默认 127.0.0.1)")
    print("  FUTU_PORT     - Futu OpenD 端口 (默认 11111)")
    print("  FUTU_HTTP_PORT - HTTP 服务端口 (默认 8080)")
    print()

    if init_futu():
        print("服务启动成功!")
    else:
        print("警告: 富途连接失败，API将不可用")
        print("请确保 Futu OpenD 已启动并登录")

    app.run(host=HOST, port=PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
