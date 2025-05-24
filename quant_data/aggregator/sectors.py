import pandas as pd

def aggregate_sector(df: pd.DataFrame, sector_col: str = 'sector') -> pd.DataFrame:
    """
    板块级聚合：如计算 avg_close, total_volume。
    """
    agg = df.groupby(sector_col).agg({
        'close': 'mean',
        'volume': 'sum'
    }).rename(columns={'close': 'avg_close', 'volume': 'total_volume'})
    return agg.reset_index()
