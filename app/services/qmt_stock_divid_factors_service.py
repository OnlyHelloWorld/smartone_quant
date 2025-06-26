import datetime
from datetime import date, timedelta
from typing import List, Tuple, Dict, Any

import pandas as pd
from sqlmodel import Session
from xtquant import xtdata

from app.cruds.qmt_stock_divid_factors_crud import (
    batch_upsert_qmt_stock_divid_factors,
    delete_qmt_stock_divid_factors_by_stock_and_date_range,
    get_qmt_stock_divid_factors_by_date_range
)
from app.models.qmt_stock_divid_factors import QmtStockDividFactors
from utils.quant_logger import init_logger
from app.cruds.qmt_sector_stock_crud import get_qmt_sector_stocks_by_sector_name

logger = init_logger()


def convert_timestamp_to_date(timestamp: int) -> date:
    """将时间戳(毫秒)转换为日期"""
    return datetime.datetime.fromtimestamp(timestamp / 1000).date()


def sync_stock_divid_factors_by_date_range(
        db: Session, start_date: str, end_date: str, stock_codes: List[str] = None
) -> Tuple[int, List[str]]:
    """
    同步指定日期范围的股票除权记录到数据库

    Args:
        db: 数据库会话
        start_date: 开始日期，格式：'YYYY-MM-DD'
        end_date: 结束日期，格式：'YYYY-MM-DD'
        stock_codes: 股票代码列表，如果为None则获取所有股票

    Returns:
        Tuple[int, List[str]]: (成功同步的记录数量, 失败的股票列表)
    """
    try:
        start_time = datetime.datetime.now()
        logger.info(f'开始同步除权数据，日期范围: {start_date} 到 {end_date}，开始时间: {start_time}')

        failed_stocks: List[str] = []
        total_records = 0

        # 如果未指定股票列表，获取所有股票
        if stock_codes is None:
            logger.info("获取所有股票列表...")
            # 这里可以从其他表获取股票列表，或者使用QMT接口获取
            # stock_codes = xtdata.get_stock_list_in_sector('沪深A股')
            stock_codes = []  # 需要根据实际情况实现

        if not stock_codes:
            logger.warning("股票代码列表为空")
            return 0, []

        logger.info(f"准备同步{len(stock_codes)}只股票的除权数据")

        # 分批处理股票，避免一次性处理过多数据
        batch_size = 100
        for i in range(0, len(stock_codes), batch_size):
            batch_stocks = stock_codes[i:i + batch_size]
            logger.info(f"处理第{i // batch_size + 1}批股票，共{len(batch_stocks)}只")

            batch_records = 0
            for stock_code in batch_stocks:
                try:
                    # 从QMT获取除权数据
                    divid_data = xtdata.get_divid_factors(stock_code, start_date, end_date)

                    if divid_data is None or divid_data.empty:
                        continue

                    # 转换数据格式
                    divid_factors_list = []
                    for index, row in divid_data.iterrows():
                        # 将时间戳转换为日期
                        record_date = convert_timestamp_to_date(int(row['time']))

                        divid_factor = QmtStockDividFactors(
                            stock_code=stock_code,
                            time=int(row['time']),
                            divid_date=record_date,
                            interest=float(row.get('interest', 0)),
                            stock_bonus=float(row.get('stockBonus', 0)),
                            stock_gift=float(row.get('stockGift', 0)),
                            allot_num=float(row.get('allotNum', 0)),
                            allot_price=float(row.get('allotPrice', 0)),
                            gugai=float(row.get('gugai', 0)),
                            dr=float(row.get('dr', 0))
                        )
                        divid_factors_list.append(divid_factor)

                    if divid_factors_list:
                        # 批量插入数据库
                        records_count = batch_upsert_qmt_stock_divid_factors(
                            session=db, divid_factors_list=divid_factors_list
                        )
                        batch_records += len(divid_factors_list)
                        logger.debug(f"股票{stock_code}同步了{len(divid_factors_list)}条除权记录")

                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"同步股票{stock_code}除权数据失败: {error_msg}")
                    failed_stocks.append(f"{stock_code}({error_msg})")
                    continue

            total_records += batch_records
            logger.info(f"第{i // batch_size + 1}批股票同步完成，本批同步{batch_records}条记录")

        end_time = datetime.datetime.now()
        logger.info(f"除权数据同步完成，结束时间: {end_time}, 总耗时: {end_time - start_time}")
        logger.info(f"总共同步{total_records}条除权记录，失败股票数: {len(failed_stocks)}")

        return total_records, failed_stocks

    except Exception as e:
        logger.error(f"同步除权数据失败: {str(e)}")
        raise


def sync_stock_divid_factors_by_stocks_and_date_range(
        db: Session, stock_codes: List[str], start_date: str, end_date: str
) -> Tuple[int, List[str]]:
    """
    同步指定股票列表在指定日期范围的除权记录

    Args:
        db: 数据库会话
        stock_codes: 股票代码列表
        start_date: 开始日期，格式：'YYYY-MM-DD'
        end_date: 结束日期，格式：'YYYY-MM-DD'

    Returns:
        Tuple[int, List[str]]: (成功同步的记录数量, 失败的股票列表)
    """
    return sync_stock_divid_factors_by_date_range(db, start_date, end_date, stock_codes)


def sync_yesterday_divid_factors(db: Session, stock_codes: List[str] = None) -> Tuple[int, List[str]]:
    """
    同步昨天发生的除权记录

    Args:
        db: 数据库会话
        stock_codes: 股票代码列表，如果为None则获取所有股票

    Returns:
        Tuple[int, List[str]]: (成功同步的记录数量, 失败的股票列表)
    """
    yesterday = (datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    logger.info(f"开始同步昨日({yesterday})的除权记录")

    return sync_stock_divid_factors_by_date_range(db, yesterday, yesterday, stock_codes)


def sync_single_stock_divid_factors(
        db: Session, stock_code: str, start_date: str, end_date: str
) -> Tuple[int, str]:
    """
    同步单个股票的除权记录

    Args:
        db: 数据库会话
        stock_code: 股票代码
        start_date: 开始日期，格式：'YYYY-MM-DD'
        end_date: 结束日期，格式：'YYYY-MM-DD'

    Returns:
        Tuple[int, str]: (同步的记录数量, 错误信息，成功时为空字符串)
    """
    try:
        logger.info(f"开始同步股票{stock_code}的除权数据，日期范围: {start_date} 到 {end_date}")

        # 从QMT获取除权数据
        divid_data = xtdata.get_divid_factors(stock_code, start_date, end_date)

        if divid_data is None or divid_data.empty:
            logger.info(f"股票{stock_code}在{start_date}到{end_date}期间无除权记录")
            return 0, ""

        logger.info(f'获取到股票{stock_code}的除权数据，共{len(divid_data)}条记录')

        # 转换数据格式
        divid_factors_list = []
        for index, row in divid_data.iterrows():
            # 将时间戳转换为日期
            record_date = convert_timestamp_to_date(int(row['time']))

            divid_factor = QmtStockDividFactors(
                stock_code=stock_code,
                time=int(row['time']),
                divid_date=record_date,
                interest=float(row.get('interest', 0)),
                stock_bonus=float(row.get('stockBonus', 0)),
                stock_gift=float(row.get('stockGift', 0)),
                allot_num=float(row.get('allotNum', 0)),
                allot_price=float(row.get('allotPrice', 0)),
                gugai=float(row.get('gugai', 0)),
                dr=float(row.get('dr', 0))
            )
            divid_factors_list.append(divid_factor)

        # 批量插入数据库
        records_count = batch_upsert_qmt_stock_divid_factors(
            session=db, divid_factors_list=divid_factors_list
        )

        logger.info(f"股票{stock_code}同步了{len(divid_factors_list)}条除权记录")
        return len(divid_factors_list), ""

    except Exception as e:
        error_msg = str(e)
        logger.error(f"同步股票{stock_code}除权数据失败: {error_msg}")
        return 0, error_msg


def get_divid_statistics_by_date_range(
        db: Session, start_date: date, end_date: date
) -> Dict[str, Any]:
    """
    获取指定日期范围内的除权统计信息

    Args:
        db: 数据库会话
        start_date: 开始日期
        end_date: 结束日期

    Returns:
        Dict[str, Any]: 统计信息字典
    """
    try:
        # 获取指定日期范围的所有除权记录
        records = get_qmt_stock_divid_factors_by_date_range(
            session=db, start_date=start_date, end_date=end_date
        )

        if not records:
            return {
                "total_records": 0,
                "total_stocks": 0,
                "date_range": f"{start_date} to {end_date}",
                "records_by_date": {},
                "stocks_by_date": {}
            }

        # 按日期统计
        records_by_date = {}
        stocks_by_date = {}
        all_stocks = set()

        for record in records:
            date_str = record.divid_date.strftime('%Y-%m-%d')

            # 统计每日记录数
            if date_str not in records_by_date:
                records_by_date[date_str] = 0
                stocks_by_date[date_str] = set()

            records_by_date[date_str] += 1
            stocks_by_date[date_str].add(record.stock_code)
            all_stocks.add(record.stock_code)

        # 转换set为count
        stocks_count_by_date = {date_str: len(stocks) for date_str, stocks in stocks_by_date.items()}

        return {
            "total_records": len(records),
            "total_stocks": len(all_stocks),
            "date_range": f"{start_date} to {end_date}",
            "records_by_date": records_by_date,
            "stocks_by_date": stocks_count_by_date
        }

    except Exception as e:
        logger.error(f"获取除权统计信息失败: {str(e)}")
        raise


# 增加 main 函数便于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings
    import datetime

    # 创建MySQL数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    with Session(engine) as session:
        start_time = datetime.datetime.now()
        logger.info(f"开始同步除权数据，开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        start_date = "20200101"
        end_date = "20251231"
        # 从数据库获取沪深A股成分股列表
        sector_stocks = get_qmt_sector_stocks_by_sector_name(session=session, sector_name="沪深A股")
        if not sector_stocks:
            logger.error("未找到沪深A股板块的成分股数据，请先同步板块成分股数据")
            exit(1)

        stock_codes = [stock.stock_code for stock in sector_stocks]
        logger.info(f"获取到沪深A股成分股列表，共{len(stock_codes)}只股票")

        # 同步所有股票的除权数据
        count, failed_stocks = sync_stock_divid_factors_by_stocks_and_date_range(
            session, stock_codes, start_date, end_date
        )

        # 记录结束时间和耗时
        end_time = datetime.datetime.now()
        duration = end_time - start_time

        # 输出统计信息
        logger.info(f"同步结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"总耗时: {duration}")
        logger.info(f"同步日期范围: {start_date} 至 {end_date}")
        logger.info(f"处理股票数: {len(stock_codes)}")
        logger.info(f"成功同步记录数: {count}")

        if failed_stocks:
            logger.error(f"同步失败的股票数: {len(failed_stocks)}")
            logger.error(f"同步失败的股票: {failed_stocks}")

        # 计算成功率
        success_rate = ((len(stock_codes) - len(failed_stocks)) / len(stock_codes)) * 100
        logger.info(f"同步成功率: {success_rate:.2f}%")
