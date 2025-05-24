import pandas as pd

def aggregate_industry(df: pd.DataFrame, industry_col: str = 'industry') -> pd.DataFrame:
    """
    行业级聚合：同一行业下标的的汇总。
    """
    agg = df.groupby(industry_col).agg({
        'close': 'mean',
        'volume': 'sum'
    }).rename(columns={'close': 'avg_close', 'volume': 'total_volume'})
    return agg.reset_index()
