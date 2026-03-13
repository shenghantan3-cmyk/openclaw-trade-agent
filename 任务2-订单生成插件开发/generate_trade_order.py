#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenClaw 量化交易订单生成插件
实现功能：
1. 支持截面选股(cross_section)和时序仓位(time_series)两类信号
2. 区分换仓(adjust)/加仓(add)/减仓(reduce)三种操作类型
3. 字段校验和标准化订单生成
"""

import json
import time
import random
import re
from typing import Dict, Any, Optional


def generate_trade_order(signal_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    生成标准化交易订单（仅生成，不执行下单）
    :param signal_data: 交易信号字典
    :return: 标准化订单信息字典
    """
    # 1. 初始化返回结果
    result = {
        "order_id": "",
        "strategy_id": "",
        "signal_type": "",
        "action": "",
        "order_detail": {},
        "create_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": "generated",
        "msg": ""
    }

    try:
        # 2. 通用字段校验
        required_fields = ["signal_type", "strategy_id", "action", "signal_time"]
        for field in required_fields:
            if field not in signal_data:
                raise ValueError(f"通用字段缺失：{field}")

        signal_type = signal_data["signal_type"]
        action = signal_data["action"]
        result["strategy_id"] = signal_data["strategy_id"]
        result["signal_type"] = signal_type
        result["action"] = action

        # 校验信号类型
        if signal_type not in ["cross_section", "time_series"]:
            raise ValueError(f"不支持的信号类型：{signal_type}")

        # 校验操作类型
        if action not in ["adjust", "add", "reduce"]:
            raise ValueError(f"不支持的操作类型：{action}")

        # 3. 生成唯一订单ID
        timestamp = time.strftime("%Y%m%d%H%M%S")
        random_suffix = random.randint(100000, 999999)
        result["order_id"] = f"ORD{timestamp}{random_suffix}"

        # 4. 处理截面选股信号
        if signal_type == "cross_section":
            # 截面信号必填字段校验
            cross_required = ["stock_pool", "target_weight", "total_capital"]
            for field in cross_required:
                if field not in signal_data:
                    raise ValueError(f"截面信号字段缺失：{field}")

            stock_pool = signal_data["stock_pool"]
            target_weight = signal_data["target_weight"]
            total_capital = signal_data["total_capital"]

            # 校验选股池和目标权重匹配
            for stock in stock_pool:
                if stock not in target_weight:
                    raise ValueError(f"选股池标的{stock}无对应目标权重")

            # 校验权重和为1（允许0.01误差）
            weight_sum = sum(target_weight.values())
            if abs(weight_sum - 1.0) > 0.01:
                raise ValueError(f"目标权重和错误：{weight_sum:.2f}，应为1.0")

            # 计算各标的持仓数量（按收盘价估算，简化处理）
            order_detail = {}
            for stock, weight in target_weight.items():
                # 提取股票代码，去除市场后缀
                code = re.sub(r'\.(SH|SZ|BJ)$', '', stock)
                # 简化计算：假设股价10元，实际场景需要获取实时行情
                stock_value = total_capital * weight
                quantity = int(stock_value / 10)  # 简化处理，实际需除当前价格
                order_detail[stock] = {
                    "weight": weight,
                    "value": round(stock_value, 2),
                    "quantity": quantity
                }

            result["order_detail"] = order_detail

        # 5. 处理时序仓位信号
        elif signal_type == "time_series":
            # 时序信号必填字段校验
            time_required = ["ticker", "current_position", "target_position"]
            for field in time_required:
                if field not in signal_data:
                    raise ValueError(f"时序信号字段缺失：{field}")

            ticker = signal_data["ticker"]
            current_pos = signal_data["current_position"]
            target_pos = signal_data["target_position"]

            # 校验操作类型和仓位变化匹配
            if action == "add" and target_pos <= current_pos:
                raise ValueError(f"加仓操作目标仓位({target_pos})需大于当前仓位({current_pos})")
            if action == "reduce" and target_pos >= current_pos:
                raise ValueError(f"减仓操作目标仓位({target_pos})需小于当前仓位({current_pos})")
            if action == "adjust" and target_pos == current_pos:
                raise ValueError(f"换仓操作目标仓位需与当前仓位不同")

            # 计算仓位变动
            position_change = target_pos - current_pos
            order_detail = {
                "ticker": ticker,
                "current_position": current_pos,
                "target_position": target_pos,
                "position_change": position_change,
                "operation": "买入" if position_change > 0 else "卖出",
                "change_quantity": abs(position_change)
            }

            result["order_detail"] = order_detail

        result["status"] = "generated"
        result["msg"] = "订单生成成功"

    except Exception as e:
        result["status"] = "failed"
        result["msg"] = str(e)

    return result


# 测试用例
if __name__ == "__main__":
    # 测试1：截面选股信号
    print("=== 测试1：截面选股信号 ===")
    cross_signal = {
        "signal_type": "cross_section",
        "strategy_id": "STR001",
        "action": "adjust",
        "signal_time": "2026-02-26 14:30:00",
        "stock_pool": ["600000.SH", "000001.SZ", "601318.SH"],
        "target_weight": {"600000.SH": 0.3, "000001.SZ": 0.5, "601318.SH": 0.2},
        "total_capital": 1000000
    }
    cross_order = generate_trade_order(cross_signal)
    print(json.dumps(cross_order, ensure_ascii=False, indent=2))

    print("\n=== 测试2：时序加仓信号 ===")
    time_add_signal = {
        "signal_type": "time_series",
        "strategy_id": "STR001",
        "action": "add",
        "signal_time": "2026-02-26 14:30:00",
        "ticker": "600000.SH",
        "current_position": 800,
        "target_position": 1000
    }
    time_add_order = generate_trade_order(time_add_signal)
    print(json.dumps(time_add_order, ensure_ascii=False, indent=2))

    print("\n=== 测试3：时序减仓信号 ===")
    time_reduce_signal = {
        "signal_type": "time_series",
        "strategy_id": "STR001",
        "action": "reduce",
        "signal_time": "2026-02-26 14:30:00",
        "ticker": "600000.SH",
        "current_position": 1000,
        "target_position": 800
    }
    time_reduce_order = generate_trade_order(time_reduce_signal)
    print(json.dumps(time_reduce_order, ensure_ascii=False, indent=2))

    print("\n=== 测试4：异常信号（字段缺失） ===")
    bad_signal = {
        "signal_type": "cross_section",
        "strategy_id": "STR001",
        "action": "adjust",
        "signal_time": "2026-02-26 14:30:00",
        "stock_pool": ["600000.SH", "000001.SZ", "601318.SH"],
        # 缺失target_weight
        "total_capital": 1000000
    }
    bad_order = generate_trade_order(bad_signal)
    print(json.dumps(bad_order, ensure_ascii=False, indent=2))
