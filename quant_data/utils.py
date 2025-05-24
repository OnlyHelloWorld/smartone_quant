# utils.py - 通用工具函数，属于 quant_data 包的基础工具层
# 主要为配置加载、日志、时间序列等通用功能，供各业务模块复用

import yaml
import logging
import pandas as pd

def load_config(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def init_logger(name: str = 'quant_data', level: int = logging.INFO):
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        level=level
    )
    return logging.getLogger(name)

def resample_timeseries(df: pd.DataFrame, rule: str, on: str = 'datetime') -> pd.DataFrame:
    return df.set_index(on).resample(rule).agg('mean').reset_index()
