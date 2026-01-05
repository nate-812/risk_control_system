# -*- coding: utf-8 -*-
import json
import random
import time
import argparse
import os
import sys
from datetime import datetime, timedelta
from faker import Faker

# 初始化 Faker
fake = Faker('zh_CN')

# 模拟的事件类型
EVENTS = ['login', 'view_product', 'add_cart', 'submit_order', 'pay', 'get_coupon']

# 黑产名单定义
BAD_USER_GEO = 'u_hacker_geo_001'  # 瞬移怪
BAD_USER_SPEED = 'u_hacker_speed_002'  # 快手怪


def get_random_ip():
    return fake.ipv4()


def ensure_dir(file_path):
    """确保目录存在"""
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)


def get_random_time_in_date(target_date_str):
    """
    在指定日期内生成一个随机时间
    :param target_date_str: 格式 '2026-01-03'
    :return: 格式 '2026-01-03 14:23:01'
    """
    # 当天的 00:00:00
    start_ts = datetime.strptime(target_date_str, "%Y-%m-%d").timestamp()
    # 当天的 23:59:59
    end_ts = start_ts + 86399

    random_ts = random.randint(int(start_ts), int(end_ts))
    return datetime.fromtimestamp(random_ts).strftime('%Y-%m-%d %H:%M:%S')


def generate_batch(file_path, target_date, num_rows=100):
    """
    生成一批数据（追加模式）
    """
    buffer = []

    # --- 1. 生成普通用户正常数据 ---
    for _ in range(num_rows):
        user_id = f"u_{random.randint(1000, 9999)}"
        # 使用指定日期内的随机时间
        event_time = get_random_time_in_date(target_date)

        data = {
            "user_id": user_id,
            "event_time": event_time,
            "event_type": random.choice(EVENTS),
            "ip_address": get_random_ip(),
            "device_id": fake.uuid4(),
            "item_id": f"goods_{random.randint(1, 100)}"
        }
        buffer.append(json.dumps(data, ensure_ascii=False))

    # --- 2. 注入【地理位置异常】黑产数据 (瞬移怪) ---
    # 场景：同一天内，IP 极其离谱地变化
    # 事件A: 上午 10:00 在 北京
    time_a = f"{target_date} 10:00:00"
    buffer.append(json.dumps({
        "user_id": BAD_USER_GEO,
        "event_time": time_a,
        "event_type": "login",
        "ip_address": "110.242.68.3",  # 北京联通
        "device_id": "device_hacker_1",
        "item_id": ""
    }, ensure_ascii=False))

    # 事件B: 上午 10:05 在 深圳 (5分钟跨越两千公里)
    time_b = f"{target_date} 10:05:00"
    buffer.append(json.dumps({
        "user_id": BAD_USER_GEO,
        "event_time": time_b,
        "event_type": "pay",
        "ip_address": "113.108.81.6",  # 广东电信
        "device_id": "device_hacker_1",
        "item_id": "goods_888"
    }, ensure_ascii=False))

    # ... 前面代码不变 ...

    # --- 3. 注入【高频刷单】黑产数据 (快手怪) ---
    hack_time = f"{target_date} 12:00:00"
    for i in range(10):
        buffer.append(json.dumps({
            "user_id": BAD_USER_SPEED,
            "event_time": hack_time,
            "event_type": "get_coupon",
            "ip_address": "192.168.1.100",
            "device_id": "device_hacker_2",
            # 👇 核心修改：让 item_id 每次都不一样！
            # 这样它们就是 10 条“购买不同商品”的记录，而不是重复记录
            "item_id": f"coupon_hack_{i}",
        }, ensure_ascii=False))

    # ... 后面写入代码不变 ...

    # --- 写入文件 (Append模式) ---
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            for line in buffer:
                f.write(line + "\n")
            # 强制刷盘，确保Flume能立刻读到
            f.flush()
        print(f"[{datetime.now()}] ✅ 已追加 {len(buffer)} 条数据 (日期: {target_date})")
    except Exception as e:
        print(f"❌ 写入失败: {str(e)}")


if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="电商风控日志生成器")
    parser.add_argument("--date", type=str, default=datetime.now().strftime('%Y-%m-%d'),
                        help="指定生成数据的日期 (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="/root/risk_project/data/risk_access.log", help="输出文件路径")
    parser.add_argument("--count", type=int, default=100, help="每批次生成的正常数据量")
    parser.add_argument("--interval", type=float, default=2.0, help="每批次生成间隔(秒)")
    parser.add_argument("--once", action="store_true", help="是否只运行一次(不循环)")

    args = parser.parse_args()

    # 确保目录存在
    ensure_dir(args.output)

    print(f"🚀 启动造数脚本...")
    print(f"   - 目标日期: {args.date}")
    print(f"   - 输出文件: {args.output}")
    print(f"   - 模式: {'单次运行' if args.once else '持续循环'}")

    try:
        if args.once:
            generate_batch(args.output, args.date, args.count)
        else:
            while True:
                generate_batch(args.output, args.date, args.count)
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n🛑 停止造数")