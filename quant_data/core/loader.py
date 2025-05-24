import os
import pandas as pd
from typing import Optional

class DataLoader:
    """
    实现从不同数据源加载原始数据，统一为 DataFrame。
    """
    def __init__(self, config: dict):
        self.config = config

    def load_from_csv(self, rel_path: str, **kwargs) -> pd.DataFrame:
        path = os.path.join(self.config['paths']['raw'], rel_path)
        return pd.read_csv(path, **kwargs)

    # 可扩展：API、数据库等多种数据源
