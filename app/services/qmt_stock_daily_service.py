from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List
from sqlalchemy import select
from sqlmodel import Session, create_engine
from xtquant import xtdata

from app.core.config import settings
from app.cruds.qmt_stock_daily_crud import delete_daily_klines_by_stock_code_and_date_range
from cruds.qmt_sector_stock_crud import get_qmt_sector_stocks_by_sector_name
from models.qmt_stock_daily import QmtStockDailyOri
from utils.db_utils import insert_on_duplicate_update_for_kline, download_kline_callback
from utils.qmt_data_utils import parse_stock_data
from utils.quant_logger import init_logger

logger = init_logger()


def sync_stock_daily_klines_to_db_single(
        stock_code: str,
        start_sync_time: datetime,
        end_sync_time: datetime,
        engine
) -> int:
    """独立线程使用的同步函数：为每只股票单独创建 Session，避免跨线程共享"""
    with Session(engine) as db:
        try:
            start_sync_time = datetime.combine(start_sync_time, datetime.min.time())
            end_sync_time = datetime.combine(end_sync_time, datetime.min.time()) + timedelta(days=1) - timedelta(seconds=1)

            logger.info(f"开始同步股票{stock_code}的日K数据，时间范围：{start_sync_time} - {end_sync_time}")

            latest_data = db.exec(
                select(QmtStockDailyOri.time)
                .where(QmtStockDailyOri.stock_code == stock_code)
                .order_by(QmtStockDailyOri.time.desc())
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

            daily_data = xtdata.get_market_data(
                field_list=["time", "open", "high", "low", "close", "volume", "amount"],
                stock_list=[stock_code],
                period='1d',
                start_time=start_time_str,
                end_time=end_time_str,
                count=-1,
                dividend_type='front',
                fill_data=False
            )

            if not daily_data:
                logger.warning(f"未获取到股票{stock_code}的日K数据")
                return 0

            stock_objs = parse_stock_data(daily_data, model_cls=QmtStockDailyOri)

            affected_rows = insert_on_duplicate_update_for_kline(
                db=db,
                model_cls=QmtStockDailyOri,
                objs=stock_objs,
                auto_commit=True
            )
            logger.info(f"股票{stock_code}同步完成，成功写入或更新 {affected_rows} 条记录")
            return affected_rows

        except Exception as e:
            logger.error(f"股票{stock_code}同步失败: {e}")
            return 0


def batch_download_all_stocks(stock_codes: List[str], start_time_str: str, end_time_str: str):
    try:
        logger.info(f'开始批量下载{len(stock_codes)}只股票的历史数据：{start_time_str} - {end_time_str}')
        xtdata.download_history_data2(
            stock_list=stock_codes,
            period='1d',
            start_time=start_time_str,
            end_time=end_time_str,
            callback=download_kline_callback,
            incrementally=True
        )
        logger.info(f"批量下载完成")
    except Exception as e:
        logger.error(f"批量下载失败: {e}")
        raise


if __name__ == "__main__":
    start_time = datetime.now()
    logger.info(f"开始同步日K数据，当前时间：{start_time}")

    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=False)

    begin_time = (datetime.now() - timedelta(days=8))
    today = datetime.now()

    # 获取沪深300成分股
    with Session(engine) as session:
        sector_stocks = get_qmt_sector_stocks_by_sector_name(
            session=session,
            sector_name="沪深300"
        )

    # 提取去重后的股票代码列表
    all_stock_codes = sorted({s.stock_code for s in sector_stocks})
    total_stocks = len(all_stock_codes)
    logger.info(f"获取到沪深300成分股共{total_stocks}只股票")

    # 执行批量数据下载
    start_time_str = begin_time.strftime('%Y%m%d')
    end_time_str = today.strftime('%Y%m%d')
    download_start_time = datetime.now()
    batch_download_all_stocks(all_stock_codes, start_time_str, end_time_str)
    download_end_time = datetime.now()

    # 使用线程池并发同步每支股票
    max_workers = 4
    logger.info(f"开始多线程同步，线程数: {max_workers}")

    sync_start_time = datetime.now()
    total_success = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for stock_code in all_stock_codes:
            futures.append(executor.submit(
                sync_stock_daily_klines_to_db_single,
                stock_code,
                begin_time,
                today,
                engine
            ))

        for future in futures:
            try:
                total_success += future.result()
            except Exception as e:
                logger.error(f"线程任务异常: {e}")

    sync_end_time = datetime.now()

    logger.info("数据同步完成")
    logger.info(f"总股票数：{total_stocks}")
    logger.info(f"成功同步：{total_success}")
    logger.info(f"下载耗时：{download_end_time - download_start_time}")
    logger.info(f"同步耗时：{sync_end_time - sync_start_time}")
    logger.info(f"总耗时：{datetime.now() - start_time}")
