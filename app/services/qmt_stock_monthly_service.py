from datetime import datetime
import logging
from sqlmodel import Session

from xtquant import xtdata
from app.cruds.qmt_stock_monthly_crud import (
    create_monthly_klines,
    delete_monthly_klines_by_stock_code
)

logger = logging.getLogger(__name__)

def sync_stock_monthly_klines_to_db(
    db: Session,
    stock_code: str,
    start_time: str,
    end_time: str
) -> int:
    """
    从QMT获取指定股票的月K线数据并同步到数据库

    Args:
        db: 数据库会话
        stock_code: 股票代码
        start_time: 开始时间，格式：'2020-01-01'
        end_time: 结束时间，格式：'2020-12-31'

    Returns:
        int: 同步的记录数量
    """
    try:
        # 获取月K数据
        monthly_data = xtdata.get_market_data(
            field_list=["time", "open", "high", "low", "close", "volume", "amount"],
            stock_list=[stock_code],
            start_time=start_time,
            end_time=end_time,
            period='1m'
        )

        if not monthly_data:
            logger.warning(f"未获取到股票{stock_code}的月K数据")
            return 0

        # 删除旧数据
        delete_monthly_klines_by_stock_code(session=db, stock_code=stock_code)

        # 格式化数据
        monthly_records = []
        for i in range(len(monthly_data['time'])):
            record = {
                'stock_code': stock_code,
                'time': datetime.fromtimestamp(monthly_data['time'][i]),
                'open': monthly_data['open'][i],
                'high': monthly_data['high'][i],
                'low': monthly_data['low'][i],
                'close': monthly_data['close'][i],
                'volume': monthly_data['volume'][i],
                'amount': monthly_data['amount'][i]
            }
            monthly_records.append(record)

        # 批量插入数据库
        if monthly_records:
            create_monthly_klines(session=db, kline_list=monthly_records)
            logger.info(f"同步月K数据完成 - {stock_code} - {len(monthly_records)}条记录")
            return len(monthly_records)

        return 0

    except Exception as e:
        logger.error(f"同步月K数据失败 - {stock_code}: {str(e)}")
        raise

# 用于测试的 main 函数
if __name__ == "__main__":
    from sqlmodel import create_engine
    from app.core.config import settings

    # 创建数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 测试同步月K数据
    with Session(engine) as session:
        result = sync_stock_monthly_klines_to_db(
            db=session,
            stock_code="000001.SZ",
            start_time="2024-01-01",
            end_time="2024-06-09"
        )
        print(f"同步完成，共同步{result}条月K数据")
