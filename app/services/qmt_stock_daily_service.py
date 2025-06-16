from datetime import datetime, timedelta
from sqlmodel import Session

from xtquant import xtdata
from app.cruds.qmt_stock_daily_crud import (
    create_daily_klines,
    delete_daily_klines_by_stock_code_and_date_range
)
from models.qmt_stock_daily import QmtStockDailyOri
from utils.quant_logger import init_logger
from utils.qmt_data_utils import parse_stock_data

logger = init_logger()

def sync_stock_daily_klines_to_db(
    db: Session,
    stock_code: str,
    start_time: datetime,
    end_time: datetime
) -> int:
    """
    从QMT获取指定股票的日K线数据并同步到数据库

    Args:
        db: 数据库会话
        stock_code: 股票代码
        start_time: 开始时间，格式：'2020-01-01'
        end_time: 结束时间，格式：'2020-12-31'

    Returns:
        int: 同步的记录数量
    """
    try:
        start_time_str = start_time.strftime('%Y%m%d')
        end_time_str = end_time.strftime('%Y%m%d')
        logger.info(f"开始同步股票{stock_code}的日K数据，时间范围：{start_time_str} - {end_time_str}")
        logger.info(f'开始下载股票{stock_code}的日K数据，时间范围：{start_time_str} - {end_time_str}')
        xtdata.download_history_data(
            stock_code=stock_code,
            period='1d',
            start_time=start_time_str,
            end_time=end_time_str,
            incrementally=False
        )
        logger.info(f"下载股票{stock_code}的日K数据完成，开始获取数据")
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

        # 删除该日期段内的旧数据
        deleted = delete_daily_klines_by_stock_code_and_date_range(
            session=db,
            stock_code=stock_code,
            start_time=start_time,
            end_time=end_time
        )

        logger.info(f'删除股票{stock_code}在{start_time_str} - {end_time_str}日期段内的旧数据，共删除{deleted}条记录')

        # 格式化数据
        stock_objs = parse_stock_data(daily_data, model_cls=QmtStockDailyOri)

        # 批量插入数据库
        db.add_all(stock_objs)
        db.commit()
        logger.info(f"成功同步股票{stock_code}的日K数据，共同步{len(stock_objs)}条记录")
        return len(stock_objs)

    except Exception as e:
        logger.error(f"同步日K数据失败 - {stock_code}: {str(e)}")
        raise

# 用于测试的 main 函数
if __name__ == "__main__":
    from sqlmodel import create_engine
    from app.core.config import settings

    # 创建数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)
    # 获取三年前的日期,yyyyMMdd格式
    three_years_ago = (datetime.now() - timedelta(days=3 * 365))
    logger.info(f'三年前的日期为：{three_years_ago}')
    # 获取今天日期
    today = datetime.now()
    logger.info(f'今天的日期为：{today}')

    # 测试同步日K数据
    with Session(engine) as session:
        result = sync_stock_daily_klines_to_db(
            db=session,
            stock_code="000001.SZ",
            start_time=three_years_ago,
            end_time=today
        )
        logger.info(f"同步完成，共同步{result}条日K数据")

