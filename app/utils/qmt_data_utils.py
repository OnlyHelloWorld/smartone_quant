from datetime import datetime

import numpy as np

from models.qmt_stock_daily import QmtStockDailyOri
from utils.quant_logger import init_logger
import pandas as pd

logger = init_logger()

# 公共解析数据的方法，将stock_data解析为股票对象
def parse_stock_data(stock_data: dict, model_cls=QmtStockDailyOri) -> list:
    stock_list = []
    stock_data_time = stock_data['time']
    stock_codes = stock_data_time.index.tolist()
    stock_time_list = stock_data_time.columns.tolist()

    for stock_code in stock_codes:
        logger.info(f'正在解析股票数据: {stock_code}')
        for stock_time in stock_time_list:
            stock_open = stock_data['open'].loc[stock_code, stock_time].round(2)
            stock_high = stock_data['high'].loc[stock_code, stock_time].round(2)
            stock_low = stock_data['low'].loc[stock_code, stock_time].round(2)
            stock_close = stock_data['close'].loc[stock_code, stock_time].round(2)
            stock_volume = int(stock_data['volume'].loc[stock_code, stock_time])  # 转为 Python int
            stock_amount = float(stock_data['amount'].loc[stock_code, stock_time])

            #转换时间字段为 datetime
            if isinstance(stock_time, str) and stock_time.isdigit() and len(stock_time) == 8:
                stock_time = datetime.strptime(stock_time, "%Y%m%d")
            elif isinstance(stock_time, pd.Timestamp):
                stock_time = stock_time.to_pydatetime()
            elif isinstance(stock_time, np.datetime64):
                stock_time = pd.to_datetime(stock_time).to_pydatetime()

            stock_obj = model_cls(
                stock_code=stock_code,
                time=stock_time,
                open=stock_open,
                high=stock_high,
                low=stock_low,
                close=stock_close,
                volume=stock_volume,
                amount=stock_amount
            )
            stock_list.append(stock_obj)

    return stock_list

def clean_kline_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗K线数据：
    1. 验证 high >= max(open, close, low)，low <= min(open, close, high)，不符合的记录仅打印日志警告
    2. 其他清洗逻辑可扩展
    """
    for idx, row in df.iterrows():
        max_val = max(row['open'], row['close'], row['low'])
        min_val = min(row['open'], row['close'], row['high'])
        if row['high'] < max_val or row['low'] > min_val:
            logger.warning(f"数据异常: stock_code={row.get('stock_code', '')}, time={row.get('time', '')}, open={row['open']}, high={row['high']}, low={row['low']}, close={row['close']}")
    return df
