import pandas as pd

def aggregate_index(df: pd.DataFrame, weight_col: str = 'weight') -> pd.DataFrame:
    """
    指数级聚合：支持加权平均、权重调整等。
    """
    df['weighted_close'] = df['close'] * df[weight_col]
    total_weight = df[weight_col].sum()
    avg_close = df['weighted_close'].sum() / total_weight if total_weight != 0 else None
    return pd.DataFrame({'avg_close': [avg_close]})
