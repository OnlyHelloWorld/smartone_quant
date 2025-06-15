from datetime import datetime
import logging
from sqlmodel import Session

from xtquant import xtdata
from app.cruds.qmt_stock_weekly_crud import (
    create_weekly_klines,
    delete_weekly_klines_by_stock_code_and_date_range
)
from utils.quant_logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

def sync_stock_weekly_klines_to_db(
    db: Session,
    stock_code: str,
    start_time: datetime,
    end_time: datetime
) -> int:
    """
    从QMT获取指定股票的周K线数据并同步到数据库

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
        logger.info(f"开始同步股票{stock_code}的周K数据，时间范围：{start_time_str} - {end_time_str}")
        # 获取周K数据
        weekly_data = xtdata.get_market_data(
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=[stock_code],
            period='1W',
            start_time=start_time_str,
            end_time=end_time_str,
            count=-1,
            dividend_type=None,
            fill_data=True
        )

        if not weekly_data:
            logger.warning(f"未获取到股票{stock_code}的周K数据")
            return 0
        else:
            logger.info(f"获取到股票{stock_code}的周K数据，共{len(weekly_data['time'])}条记录")

        # 删除该日期段内的旧数据
        delete_weekly_klines_by_stock_code_and_date_range(
            session=db,
            stock_code=stock_code,
            start_time=start_time,
            end_time=end_time
        )

        # 格式化数据
        weekly_records = []
        for i in range(len(weekly_data['time'])):
            record = {
                'stock_code': stock_code,
                'time': datetime.fromtimestamp(weekly_data['time'][i]),
                'open': weekly_data['open'][i],
                'high': weekly_data['high'][i],
                'low': weekly_data['low'][i],
                'close': weekly_data['close'][i],
                'volume': weekly_data['volume'][i],
                'amount': weekly_data['amount'][i]
            }
            weekly_records.append(record)

        # 批量插入数据库
        if weekly_records:
            create_weekly_klines(session=db, kline_list=weekly_records)
            logger.info(f"同步周K数据完成 - {stock_code} - {len(weekly_records)}条记录")
            return len(weekly_records)

        return 0

    except Exception as e:
        logger.error(f"同步周K数据失败 - {stock_code}: {str(e)}")
        raise

# 用于测试的 main 函数
if __name__ == "__main__":
    from sqlmodel import create_engine
    from app.core.config import settings

    # 创建数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 测试同步周K数据
    with Session(engine) as session:
        result = sync_stock_weekly_klines_to_db(
            db=session,
            stock_code="000001.SZ",
            start_time=datetime(2020, 1, 1),
            end_time= datetime(2023, 10, 1)
        )
        logger.info(f"同步完成，共同步{result}条周K数据")
