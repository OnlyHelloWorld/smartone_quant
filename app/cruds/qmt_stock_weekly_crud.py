from datetime import datetime
from typing import List

from sqlmodel import Session, select

from app.models.qmt_stock_weekly import QmtStockWeeklyOri


def create_weekly_klines(*, session: Session, kline_list: List[dict]) -> List[QmtStockWeeklyOri]:
    """批量创建周K线数据"""
    klines_to_insert = [QmtStockWeeklyOri(**kline_data) for kline_data in kline_list]
    session.bulk_save_objects(klines_to_insert)
    session.commit()
    return klines_to_insert

def delete_weekly_klines_by_stock_code(*, session: Session, stock_code: str):
    """删除指定股票的所有周K线数据"""
    session.exec(select(QmtStockWeeklyOri).where(QmtStockWeeklyOri.stock_code == stock_code)).delete()
    session.commit()

# 删除该日期段内的旧数据并返回删除条数
def delete_weekly_klines_by_stock_code_and_date_range(
    *,
    session: Session,
    stock_code: str,
    start_time: datetime,
    end_time: datetime
) -> int:
    """删除指定股票在时间范围内的周K线数据"""
    statement = select(QmtStockWeeklyOri).where(
        QmtStockWeeklyOri.stock_code == stock_code,
        QmtStockWeeklyOri.time >= start_time,
        QmtStockWeeklyOri.time <= end_time
    )
    deleted_count = session.exec(statement).delete()
    session.commit()
    return deleted_count


def get_weekly_klines_by_stock_code_and_date_range(
    *,
    session: Session,
    stock_code: str,
    start_time: datetime,
    end_time: datetime
) -> List[QmtStockWeeklyOri]:
    """获取指定股票在时间范围内的周K线数据"""
    statement = select(QmtStockWeeklyOri).where(
        QmtStockWeeklyOri.stock_code == stock_code,
        QmtStockWeeklyOri.time >= start_time,
        QmtStockWeeklyOri.time <= end_time
    ).order_by(QmtStockWeeklyOri.time)
    return session.exec(statement).all()

# 用于测试的 main 函数
if __name__ == "__main__":
    from sqlmodel import create_engine
    from app.core.config import settings

    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    with Session(engine) as session:
        # 测试数据
        test_data = {
            "stock_code": "000001.SZ",
            "time": datetime.now(),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 1000000,
            "amount": 10500000.0
        }

        # 测试创建
        klines = create_weekly_klines(session=session, kline_list=[test_data])
        print(f"创建的周K数据: {klines[0]}")
