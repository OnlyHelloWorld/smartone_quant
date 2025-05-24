import os
import pandas as pd
from typing import Optional

class DataStore:
    """
    封装本地 CSV 文件的增删改查操作，自动管理目录结构。
    """
    def __init__(self, base_path: str):
        self.base_path = base_path

    def _full_path(self, rel_path: str) -> str:
        return os.path.join(self.base_path, rel_path)

    def read(self, rel_path: str, **kwargs) -> pd.DataFrame:
        path = self._full_path(rel_path)
        return pd.read_csv(path, **kwargs)

    def write(self, rel_path: str, df: pd.DataFrame, **kwargs):
        path = self._full_path(rel_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False, **kwargs)

    def update(self, rel_path: str, df: pd.DataFrame, **kwargs):
        # 简单覆盖写入，可扩展为增量更新
        self.write(rel_path, df, **kwargs)

    def delete(self, rel_path: str):
        path = self._full_path(rel_path)
        if os.path.exists(path):
            os.remove(path)
