#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenClaw 量化交易 Agent 闭环逻辑
实现功能：
1. 监听信号目录 /opt/openclaw/signals/
2. 自动读取信号文件，调用订单生成插件
3. 记录订单日志到 /opt/openclaw/logs/order_log.csv
4. 异常处理和持续运行
"""

import os
import time
import json
import csv
from typing import Dict, Any, Optional
from generate_trade_order import generate_trade_order

# 配置
SIGNAL_DIR = "/opt/openclaw/signals"
LOG_FILE = "/opt/openclaw/logs/order_log.csv"
CHECK_INTERVAL = 5  # 检查间隔，秒

# 已处理文件记录
processed_files = set()


def init_environment():
    """初始化目录和日志文件"""
    os.makedirs(SIGNAL_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    
    # 如果日志文件不存在，写入表头
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                "order_id", "strategy_id", "signal_type", "action",
                "ticker/stock_pool", "order_status", "create_time", "msg"
            ])


def read_signal_file(filepath: str) -> Optional[Dict[str, Any]]:
    """读取并解析信号文件"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"信号文件读取失败 {filepath}: {str(e)}")
        return None


def write_order_log(order: Dict[str, Any]):
    """写入订单日志"""
    try:
        # 提取标的/选股池信息
        if order["signal_type"] == "cross_section":
            stock_pool = ",".join(order["order_detail"].keys()) if order["order_detail"] else ""
            ticker_pool = stock_pool
        else:
            ticker_pool = order["order_detail"].get("ticker", "") if order["order_detail"] else ""

        with open(LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                order["order_id"],
                order["strategy_id"],
                order["signal_type"],
                order["action"],
                ticker_pool,
                order["status"],
                order["create_time"],
                order["msg"]
            ])
        print(f"订单日志已记录: {order['order_id']}")
    except Exception as e:
        print(f"日志写入失败: {str(e)}")


def process_signal_file(filepath: str):
    """处理单个信号文件"""
    print(f"\n处理信号文件: {filepath}")
    
    # 读取信号
    signal_data = read_signal_file(filepath)
    if not signal_data:
        return
    
    # 生成订单
    order = generate_trade_order(signal_data)
    
    # 记录日志
    write_order_log(order)
    
    # 打印结果
    if order["status"] == "generated":
        print(f"✅ 订单生成成功: {order['order_id']}")
    else:
        print(f"❌ 订单生成失败: {order['msg']}")


def main():
    """主循环"""
    print("🚀 OpenClaw 量化交易 Agent 启动")
    print(f"📂 信号监听目录: {SIGNAL_DIR}")
    print(f"📝 日志文件: {LOG_FILE}")
    print(f"⏱️  检查间隔: {CHECK_INTERVAL}秒")
    
    init_environment()
    
    # 先创建测试信号文件
    test_cross_signal = {
        "signal_type": "cross_section",
        "strategy_id": "STR001",
        "action": "adjust",
        "signal_time": "2026-02-26 14:30:00",
        "stock_pool": ["600000.SH", "000001.SZ", "601318.SH"],
        "target_weight": {"600000.SH": 0.3, "000001.SZ": 0.5, "601318.SH": 0.2},
        "total_capital": 1000000
    }
    
    test_time_signal = {
        "signal_type": "time_series",
        "strategy_id": "STR001",
        "action": "add",
        "signal_time": "2026-02-26 14:30:00",
        "ticker": "600000.SH",
        "current_position": 800,
        "target_position": 1000
    }
    
    # 写入测试信号
    with open(os.path.join(SIGNAL_DIR, "cross_section_signal.json"), 'w', encoding='utf-8') as f:
        json.dump(test_cross_signal, f, ensure_ascii=False, indent=2)
    
    with open(os.path.join(SIGNAL_DIR, "time_series_signal.json"), 'w', encoding='utf-8') as f:
        json.dump(test_time_signal, f, ensure_ascii=False, indent=2)
    
    print("\n📝 已创建测试信号文件，开始处理...")
    
    try:
        while True:
            # 扫描信号目录
            for filename in os.listdir(SIGNAL_DIR):
                filepath = os.path.join(SIGNAL_DIR, filename)
                if not os.path.isfile(filepath):
                    continue
                
                # 只处理json文件
                if not filename.endswith(".json"):
                    continue
                
                # 跳过已处理文件
                if filepath in processed_files:
                    continue
                
                # 处理信号
                process_signal_file(filepath)
                processed_files.add(filepath)
            
            # 等待下次检查
            time.sleep(CHECK_INTERVAL)
            # 测试模式：处理完测试文件后退出
            if len(processed_files) >= 2:
                print("\n✅ 测试完成，退出Agent")
                break
            
    except KeyboardInterrupt:
        print("\n👋 Agent 已停止")


if __name__ == "__main__":
    main()
