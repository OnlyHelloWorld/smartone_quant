from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import List, Type, Any, TypeVar

import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlmodel import Session, SQLModel
from xtquant import xtdata

from models.qmt_stock_daily import QmtStockDailyOri
from utils.db_utils import insert_on_duplicate_update_for_kline, download_kline_callback
from utils.quant_logger import init_logger

logger = init_logger()

# 定义类型变量，约束为SQLModel的子类
T = TypeVar('T', bound=SQLModel)

def sync_stock_klines_to_db_single(
        stock_code: str,
        start_sync_time: datetime,
        end_sync_time: datetime,
        engine,
        model_cls: Type[T],
        period: str,
        period_name: str
) -> int:
    """
    独立线程使用的同步函数：为每只股票单独创建 Session，避免跨线程共享

    Args:
        stock_code: 股票代码
        start_sync_time: 开始同步时间
        end_sync_time: 结束同步时间
        engine: 数据库引擎
        model_cls: 数据模型类
        period: 数据周期 ('1d', '1w', '1mon')
        period_name: 周期名称用于日志 ('日K', '周K', '月K')

    Returns:
        int: 影响的行数
    """
    with Session(engine) as db:
        try:
            start_sync_time = datetime.combine(start_sync_time, datetime.min.time())
            end_sync_time = datetime.combine(end_sync_time, datetime.min.time()) + timedelta(days=1) - timedelta(
                seconds=1)

            logger.info(f"开始同步股票{stock_code}的{period_name}数据，时间范围：{start_sync_time} - {end_sync_time}")

            # 查询最新数据
            latest_data = db.exec(
                select(model_cls.time)
                .where(model_cls.stock_code == stock_code)
                .order_by(model_cls.time.desc())
            ).first()

            if latest_data:
                latest_time = latest_data.time
                if latest_time >= end_sync_time:
                    logger.info(f"股票{stock_code}已有最新数据 {latest_time}，跳过同步")
                    return 0
                if latest_time >= start_sync_time:
                    start_sync_time = latest_time
                    logger.info(f"股票{stock_code}已有数据，起始时间调整为 {latest_time}")
            else:
                logger.info(f"股票{stock_code}在数据库中无数据，使用初始起始时间 {start_sync_time}")

            start_time_str = start_sync_time.strftime('%Y%m%d')
            end_time_str = end_sync_time.strftime('%Y%m%d')

            # 获取市场数据
            market_data = xtdata.get_market_data(
                field_list=["time", "open", "high", "low", "close", "volume", "amount"],
                stock_list=[stock_code],
                period=period,
                start_time=start_time_str,
                end_time=end_time_str,
                count=-1,
                dividend_type='front',
                fill_data=False
            )

            if not market_data:
                logger.warning(f"未获取到股票{stock_code}的{period_name}数据")
                return 0

            # 解析股票数据
            stock_objs = parse_stock_data(market_data, model_cls=model_cls)

            # 插入或更新数据
            affected_rows = insert_on_duplicate_update_for_kline(
                db=db,
                model_cls=model_cls,
                objs=stock_objs,
                auto_commit=True
            )
            logger.info(f"股票{stock_code}同步完成，成功写入或更新 {affected_rows} 条记录")
            return affected_rows

        except Exception as e:
            logger.error(f"股票{stock_code}同步失败: {e}")
            return 0


def batch_download_stocks_data(
        stock_codes: List[str],
        start_time_str: str,
        end_time_str: str,
        period: str,
        period_name: str
):
    """
    批量下载股票数据

    Args:
        stock_codes: 股票代码列表
        start_time_str: 开始时间字符串
        end_time_str: 结束时间字符串
        period: 数据周期 ('1d', '1w', '1mon')
        period_name: 周期名称用于日志 ('日K', '周K', '月K')
    """
    try:
        logger.info(f'开始批量下载{len(stock_codes)}只股票的{period_name}历史数据：{start_time_str} - {end_time_str}')
        xtdata.download_history_data2(
            stock_list=stock_codes,
            period=period,
            start_time=start_time_str,
            end_time=end_time_str,
            callback=download_kline_callback,
            incrementally=True
        )
        logger.info(f"批量下载完成")
    except Exception as e:
        logger.error(f"批量下载失败: {e}")
        raise


def sync_stocks_klines_with_threadpool(
        stock_codes: List[str],
        begin_time: datetime,
        end_time: datetime,
        engine,
        model_cls: Type[T],
        period: str,
        period_name: str,
        max_workers: int = 4
) -> int:
    """
    使用线程池并发同步股票K线数据

    Args:
        stock_codes: 股票代码列表
        begin_time: 开始时间
        end_time: 结束时间
        engine: 数据库引擎
        model_cls: 数据模型类
        period: 数据周期 ('1d', '1w', '1mon')
        period_name: 周期名称用于日志 ('日K', '周K', '月K')
        max_workers: 最大工作线程数

    Returns:
        int: 总成功同步的记录数
    """
    logger.info(f"开始多线程同步{period_name}数据，线程数: {max_workers}")
    total_success = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for stock_code in stock_codes:
            futures.append(executor.submit(
                sync_stock_klines_to_db_single,
                stock_code,
                begin_time,
                end_time,
                engine,
                model_cls,
                period,
                period_name
            ))

        for future in futures:
            try:
                total_success += future.result()
            except Exception as e:
                logger.error(f"线程任务异常: {e}")

    return total_success


def get_time_range_for_sync():
    """
    获取用于同步的时间范围

    Returns:
        tuple: (begin_time, today)
    """
    begin_time = datetime.now() - timedelta(days=3*365)
    today = datetime.now().date() if datetime.now().hour >= 16 else (datetime.now() - timedelta(days=1)).date()
    return begin_time, today


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

            # 转换时间字段为 datetime
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
    logger.info(f'解析完成，共解析出 {len(stock_list)} 条股票数据')
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
            logger.warning(
                f"数据异常: stock_code={row.get('stock_code', '')}, time={row.get('time', '')}, open={row['open']}, high={row['high']}, low={row['low']}, close={row['close']}")
    return df
