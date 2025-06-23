from models.qmt_stock_daily import QmtStockDailyOri
from utils.quant_logger import init_logger
import pandas as pd

logger = init_logger()

# 公共解析数据的方法，将stock_data解析为股票对象
def parse_stock_data(stock_data: dict, model_cls = QmtStockDailyOri) -> list:
    """
    解析股票行情数据，支持日/周/月K模型
    get_market_data函数返回数据格式为：
    {field1 : value1, field2 : value2, ...}
            field1, field2, ... : 数据字段
            value1, value2, ... : pd.DataFrame 字段对应的数据，各字段维度相同，index为stock_list，columns为time_list

    :param stock_data: 股票行情数据
    :param model_cls: 股票数据模型类
    :return: 股票对象列表
    """
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
            stock_volume = stock_data['volume'].loc[stock_code, stock_time]
            stock_amount = stock_data['amount'].loc[stock_code, stock_time]
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
            logging.warning(f"数据异常: stock_code={row.get('stock_code', '')}, time={row.get('time', '')}, open={row['open']}, high={row['high']}, low={row['low']}, close={row['close']}")
    return df
