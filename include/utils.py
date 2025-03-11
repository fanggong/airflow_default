from datetime import datetime as dt

def from_timestamp(x):
    try:
        result = dt.fromtimestamp(int(x) / 1000)
    except ValueError:
        result = None
    return result

def process_keys(item, key_mapping):
    return {
        key_mapping[k]: None if v in ['', '-'] else v
        for k, v in item.items()
        if k in key_mapping
    }