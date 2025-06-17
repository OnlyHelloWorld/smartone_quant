from datetime import datetime, timedelta
from sqlalchemy import select
from sqlmodel import Session
from xtquant import xtdata
from app.cruds.qmt_stock_monthly_crud import (
    create_monthly_klines,
    delete_monthly_klines_by_stock_code_and_date_range
)
from sqlmodel import create_engine
from app.core.config import settings
from cruds.qmt_sector_stock_crud import get_qmt_sector_stocks_by_sector_name
from models.qmt_stock_monthly import QmtStockMonthlyOri
from utils.quant_logger import init_logger

logger = init_logger()

def sync_stock_monthly_klines_to_db(
    db: Session,
    stock_code: str,
    start_sync_time: datetime,
    end_sync_time: datetime
) -> int:
    """
    从QMT获取指定股票的月K线数据并同步到数据库
    """
    try:
        start_sync_time = datetime.combine(start_sync_time, datetime.min.time())
        end_sync_time = datetime.combine(end_sync_time, datetime.min.time()) + timedelta(days=1) - timedelta(seconds=1)
        logger.info(f"开始同步股票{stock_code}的月K数据，时间范围：{start_sync_time} - {end_sync_time}")
        # 计算实际同步的时间范围
        latest_data = db.exec(
            select(QmtStockMonthlyOri.time)
            .where(QmtStockMonthlyOri.stock_code == stock_code)
            .order_by(QmtStockMonthlyOri.time.desc())
        ).first()
        if latest_data:
            latest_time = latest_data.time
            if latest_time >= end_sync_time:
                logger.info(f"股票{stock_code}在数据库中已有最新数据，最新数据日期为{latest_time}，无需同步")
                return 0
            if latest_time >= start_sync_time:
                start_sync_time = latest_time
                logger.info(f"股票{stock_code}在数据库中已有数据，使用最新数据日期{latest_time}作为起始时间")
        else:
            logger.info(f"股票{stock_code}在数据库中没有月K数据，使用同步开始时间{start_sync_time}作为起始时间")
        start_time_str = start_sync_time.strftime('%Y%m%d')
        end_time_str = end_sync_time.strftime('%Y%m%d')
        xtdata.download_history_data(
            stock_code=stock_code,
            period='1m',
            start_time=start_time_str,
            end_time=end_time_str,
            incrementally=False
        )
        logger.info(f"下载股票{stock_code}的月K数据完成，开始获取数据")
        monthly_data = xtdata.get_market_data(
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=[stock_code],
            period='1m',
            start_time=start_time_str,
            end_time=end_time_str,
            count=-1,
            dividend_type='front',
            fill_data=False
        )
        if not monthly_data:
            logger.warning(f"未获取到股票{stock_code}的月K数据")
            return 0
        else:
            logger.info(f"获取到股票{stock_code}的月K数据，共{len(monthly_data['time'])}条记录")
        deleted = delete_monthly_klines_by_stock_code_and_date_range(
            session=db,
            stock_code=stock_code,
            start_time=start_sync_time,
            end_time=end_sync_time
        )
        logger.info(f"删除旧的月K数据{deleted}条")

        # 格式化数据
        stock_objs = parse_stock_data(daily_data, model_cls=QmtStockMonthlyOri)
        if not stock_objs:
            logger.warning(f"解析股票{stock_code}的月K数据失败")
            return 0
        # 批量插入数据库
        db.add_all(stock_objs)
        db.commit()
        logger.info(f"成功同步股票{stock_code}的月K数据，共同步{len(stock_objs)}条记录")
        return len(stock_objs)
        
    except Exception as e:
        logger.error(f"同步月K数据失败 - {stock_code}: {str(e)}")
        raise

# 用于测试的 main 函数
if __name__ == "__main__":

    # 记录开始时间
    start_time = datetime.now()
    logger.info(f"开始同步月K数据，当前时间：{start_time}")
    # 创建数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)
    # 获取三年前的日期,yyyyMMdd格式
    three_years_ago = (datetime.now() - timedelta(days=3 * 365))
    logger.info(f'三年前的日期为：{three_years_ago}')
    # 获取今天日期
    today = datetime.now()
    logger.info(f'今天的日期为：{today}')

    # 测试同步月K数据
    with Session(engine) as session:
        sector_stocks = get_qmt_sector_stocks_by_sector_name(
            session=session,
            sector_name="沪深A股"
        )
        for sector_stock in sector_stocks:
            logger.info(f"开始同步股票{sector_stock.stock_code}的月K数据")
            result = sync_stock_monthly_klines_to_db(
                db=session,
                stock_code=sector_stock.stock_code,
                start_sync_time=three_years_ago,
                end_sync_time=today
            )
            logger.info(f"同步完成，共同步{result}条月K数据")

    # 记录结束时间
    end_time = datetime.now()
    logger.info(f"结束同步月K数据，当前时间：{end_time}")
    logger.info(f"总耗时：{end_time - start_time}")
