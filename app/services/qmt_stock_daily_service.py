from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import List

from sqlalchemy import select
from sqlmodel import Session
from sqlmodel import create_engine
from xtquant import xtdata

from app.core.config import settings
from app.cruds.qmt_stock_daily_crud import (
    delete_daily_klines_by_stock_code_and_date_range
)
from cruds.qmt_sector_stock_crud import get_qmt_sector_stocks_by_sector_name
from models.qmt_stock_daily import QmtStockDailyOri
from utils.db_utils import insert_on_duplicate_update_for_kline, download_kline_callback
from utils.qmt_data_utils import parse_stock_data
from utils.quant_logger import init_logger

logger = init_logger()


def sync_stock_daily_klines_to_db(
        db: Session,
        stock_code: str,
        start_sync_time: datetime,
        end_sync_time: datetime
) -> int:
    """
    从QMT获取指定股票的日K线数据并同步到数据库

    Args:
        db: 数据库会话
        stock_code: 股票代码
        start_sync_time: 开始时间，格式：'20200101'
        end_sync_time: 结束时间，格式：'20201231'

    Returns:
        int: 同步的记录数量
    """
    try:
        # 获取start_time end_time当天0点的时间戳
        start_sync_time = datetime.combine(start_sync_time, datetime.min.time())
        end_sync_time = datetime.combine(end_sync_time, datetime.min.time()) + timedelta(days=1) - timedelta(seconds=1)

        logger.info(f"开始同步股票{stock_code}的日K数据，时间范围：{start_sync_time} - {end_sync_time}")

        # 计算实际同步的时间范围，如果数据库中该股票已有数据，
        # 则从获取最新的数据日期，如果最新数据日期大于同步开始日期，则从最新数据日期开始同步
        # 如果最新数据日期小于同步开始日期，则从同步开始日期开始同步
        latest_data = db.exec(
            select(QmtStockDailyOri.time)
            .where(QmtStockDailyOri.stock_code == stock_code)
            .order_by(QmtStockDailyOri.time.desc())
        ).first()
        if latest_data:
            latest_time = latest_data.time
            # 如果最新数据日期大于等于同步结束时间，则不需要同步
            if latest_time >= end_sync_time:
                logger.info(f"股票{stock_code}在数据库中已有最新数据，最新数据日期为{latest_time}，无需同步")
                return 0

            # 如果最新数据日期大于等于同步开始时间，则从最新数据日期开始同步
            if latest_time >= start_sync_time:
                start_sync_time = latest_time
                logger.info(f"股票{stock_code}在数据库中已有数据，使用最新数据日期{latest_time}作为起始时间")
        else:
            logger.info(f"股票{stock_code}在数据库中没有日K数据，使用同步开始时间{start_sync_time}作为起始时间")

        start_time_str = start_sync_time.strftime('%Y%m%d')
        end_time_str = end_sync_time.strftime('%Y%m%d')

        # 获取日K数据
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
        else:
            logger.info(f"获取到股票{stock_code}的日K数据，共{len(daily_data['time'])}条记录")

        # 格式化数据
        stock_objs = parse_stock_data(daily_data, model_cls=QmtStockDailyOri)

        # 批量插入数据库，遇到主键(stock_code+time)冲突时自动跳过
        try:
            affected_rows = insert_on_duplicate_update_for_kline(
                db=db,
                model_cls=QmtStockDailyOri,
                objs=stock_objs,
                auto_commit=False
            )
            logger.info(f"成功同步股票{stock_code}的日K数据，共尝试插入{affected_rows}条记录（唯一索引冲突时自动更新）")
        except Exception as e:
            logger.error(f"插入日K数据失败: {e}")
            raise
        return len(stock_objs)

    except Exception as e:
        logger.error(f"同步日K数据失败 - {stock_code}: {str(e)}")
        raise


def sync_stock_batch(
        stock_batch: List,
        engine,
        start_sync_time: datetime,
        end_sync_time: datetime,
        batch_num: int,
        total_batches: int
) -> int:
    """
    批量处理一组股票的日K数据同步

    Args:
        stock_batch: 股票批次列表
        engine: 数据库引擎
        start_sync_time: 开始时间
        end_sync_time: 结束时间
        batch_num: 当前批次号
        total_batches: 总批次数

    Returns:
        int: 成功同步的股票数量
    """
    success_count = 0
    batch_start_time = datetime.now()

    logger.info(f"开始处理第{batch_num}/{total_batches}批股票，共{len(stock_batch)}只股票")

    with Session(engine) as session:
        for i, sector_stock in enumerate(stock_batch, 1):
            try:
                logger.info(f"批次{batch_num}/{total_batches}，股票{i}/{len(stock_batch)}：{sector_stock.stock_code}")
                result = sync_stock_daily_klines_to_db(
                    db=session,
                    stock_code=sector_stock.stock_code,
                    start_sync_time=start_sync_time,
                    end_sync_time=end_sync_time
                )
                logger.info(f"股票{sector_stock.stock_code}同步完成，共同步{result}条数据")
                success_count += 1

            except Exception as e:
                logger.error(f"股票{sector_stock.stock_code}同步失败: {e}")

        # 批次处理完成后进行一次提交
        try:
            session.commit()
            logger.info(f"第{batch_num}批股票数据提交完成")
        except Exception as e:
            logger.error(f"第{batch_num}批股票数据提交失败: {e}")
            session.rollback()
            raise

    batch_end_time = datetime.now()
    batch_duration = batch_end_time - batch_start_time
    logger.info(
        f"第{batch_num}/{total_batches}批处理完成，成功同步{success_count}/{len(stock_batch)}只股票，耗时：{batch_duration}")

    return success_count


def batch_download_all_stocks(stock_codes: List[str], start_time_str: str, end_time_str: str):
    """
    批量下载所有股票的历史数据

    Args:
        stock_codes: 股票代码列表
        start_time_str: 开始时间字符串
        end_time_str: 结束时间字符串
    """
    try:
        # 打印函数入参
        logger.info(f'批量下载股票代码列表: {stock_codes},开始时间: {start_time_str}, 结束时间: {end_time_str}')
        logger.info(f"开始批量下载{len(stock_codes)}只股票的历史数据，时间范围：{start_time_str} - {end_time_str}")

        # 批量下载所有股票数据
        xtdata.download_history_data2(
            stock_list=stock_codes,
            period='1d',
            start_time=start_time_str,
            end_time=end_time_str,
            callback=download_kline_callback,
            incrementally=True  # 使用增量下载
        )

        logger.info(f"批量下载{len(stock_codes)}只股票的历史数据完成")

    except Exception as e:
        logger.error(f"批量下载股票数据失败: {e}")
        raise


# 用于测试的 main 函数
if __name__ == "__main__":
    # 记录开始时间
    start_time = datetime.now()
    logger.info(f"开始同步日K数据，当前时间：{start_time}")

    # 创建数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=False)

    # 取一周前的日期
    begin_time = (datetime.now() - timedelta(days=8))
    logger.info(f'begin_time为：{begin_time}')

    # 获取今天日期
    today = datetime.now()
    logger.info(f'今天的日期为：{today}')

    # 获取沪深A股所有成分股
    with Session(engine) as session:
        sector_stocks = get_qmt_sector_stocks_by_sector_name(
            session=session,
            sector_name="沪深300"
        )

    # 提取所有股票代码
    all_stock_codes = [sector_stock.stock_code for sector_stock in sector_stocks]
    total_stocks = len(all_stock_codes)
    logger.info(f"获取到沪深A股成分股共{total_stocks}只股票")

    # 1. 前置批量下载所有股票的历史数据
    start_time_str = begin_time.strftime('%Y%m%d')
    end_time_str = today.strftime('%Y%m%d')

    download_start_time = datetime.now()
    batch_download_all_stocks(all_stock_codes, start_time_str, end_time_str)
    download_end_time = datetime.now()
    logger.info(f"批量下载耗时：{download_end_time - download_start_time}")

    # 2. 分批处理，每批20只股票
    batch_size = 20
    max_workers = 4  # 并发数设置为4

    # 将股票列表分批
    stock_batches = []
    for i in range(0, total_stocks, batch_size):
        batch = sector_stocks[i:i + batch_size]
        stock_batches.append(batch)

    total_batches = len(stock_batches)
    logger.info(f"总共分为{total_batches}批，每批{batch_size}只股票，使用{max_workers}个并发线程")

    # 3. 使用线程池并发处理各批次
    sync_start_time = datetime.now()
    total_success = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有批次任务
        futures = []
        for batch_num, batch in enumerate(stock_batches, 1):
            future = executor.submit(
                sync_stock_batch,
                batch,
                engine,
                begin_time,
                today,
                batch_num,
                total_batches
            )
            futures.append(future)

        # 等待所有任务完成并收集结果
        for future in futures:
            try:
                success_count = future.result()
                total_success += success_count
            except Exception as e:
                logger.error(f"批次处理异常: {e}")

    sync_end_time = datetime.now()

    # 记录结束时间和统计信息
    end_time = datetime.now()
    total_duration = end_time - start_time
    sync_duration = sync_end_time - sync_start_time

    logger.info(f"数据同步完成！")
    logger.info(f"总股票数：{total_stocks}")
    logger.info(f"成功同步：{total_success}")
    logger.info(f"失败数量：{total_stocks - total_success}")
    logger.info(f"数据下载耗时：{download_end_time - download_start_time}")
    logger.info(f"数据同步耗时：{sync_duration}")
    logger.info(f"总耗时：{total_duration}")
    logger.info(f"结束时间：{end_time}")