import pendulum
from datetime import datetime

def from_timestamp(x):
    """将时间戳转换为 UTC+8 (Asia/Shanghai) 的 datetime"""
    try:
        utc_time = datetime.fromtimestamp(int(x) / 1000)  # 先转换为 UTC
        local_time = pendulum.instance(utc_time).in_timezone("Asia/Shanghai")  # 转换为 UTC+8
    except ValueError:
        local_time = None
    return local_time


def process_keys(item, key_mapping):
    return {
        key_mapping[k]: None if v in ['', '-'] else v
        for k, v in item.items()
        if k in key_mapping
    }