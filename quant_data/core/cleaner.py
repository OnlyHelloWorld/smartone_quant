import pandas as pd

class DataCleaner:
    """
    数据清洗与校验：填充缺失、剔除异常、复权等。
    """
    def fill_missing(self, df: pd.DataFrame, method: str = 'ffill') -> pd.DataFrame:
        return df.fillna(method=method)

    def remove_outliers(self, df: pd.DataFrame, col: str, threshold: float = 3.0) -> pd.DataFrame:
        mean = df[col].mean()
        std = df[col].std()
        return df[(df[col] > mean - threshold * std) & (df[col] < mean + threshold * std)]

    # 可扩展：复权、异常检测等
