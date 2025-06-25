from typing import Any, List, Optional
from datetime import date, datetime

from sqlalchemy import text, and_, or_
from sqlmodel import Session, select

from app.models.akshare_trade_calendar import AkshareTradeCalendar

"""
对AkshareTradeCalendar模型的增删改查操作
"""


def create_trade_calendar(*, session: Session, trade_calendar_create: AkshareTradeCalendar) -> AkshareTradeCalendar:
    """创建单个交易日历记录"""
    db_obj = AkshareTradeCalendar(**trade_calendar_create.model_dump(exclude={'id'}))
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def batch_create_trade_calendars(*, session: Session, trade_calendars: List[AkshareTradeCalendar]) -> int:
    """批量创建交易日历记录"""
    db_objs = [AkshareTradeCalendar(**calendar.model_dump(exclude={'id'})) for calendar in trade_calendars]
    session.add_all(db_objs)
    session.commit()
    return len(db_objs)


def update_trade_calendar(*, session: Session, db_trade_calendar: AkshareTradeCalendar,
                          trade_calendar_in: AkshareTradeCalendar) -> Any:
    """更新交易日历记录"""
    trade_calendar_data = trade_calendar_in.model_dump(exclude_unset=True)
    db_trade_calendar.sqlmodel_update(trade_calendar_data)
    session.add(db_trade_calendar)
    session.commit()
    session.refresh(db_trade_calendar)
    return db_trade_calendar


def get_trade_calendar_by_date(*, session: Session, trade_date: date) -> AkshareTradeCalendar | None:
    """根据日期获取交易日历记录"""
    statement = select(AkshareTradeCalendar).where(AkshareTradeCalendar.trade_date == trade_date)
    return session.exec(statement).first()


def is_trade_date(*, session: Session, check_date: date) -> bool:
    """检查某个日期是否为交易日"""
    statement = select(AkshareTradeCalendar).where(AkshareTradeCalendar.trade_date == check_date)
    return session.exec(statement).first() is not None


def get_trade_dates_in_range(*, session: Session, start_date: date, end_date: date) -> List[AkshareTradeCalendar]:
    """获取指定时间范围内的所有交易日"""
    statement = select(AkshareTradeCalendar).where(
        and_(
            AkshareTradeCalendar.trade_date >= start_date,
            AkshareTradeCalendar.trade_date <= end_date
        )
    ).order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_trade_dates_by_year(*, session: Session, year: int) -> List[AkshareTradeCalendar]:
    """获取某年的所有交易日"""
    statement = select(AkshareTradeCalendar).where(
        AkshareTradeCalendar.year == year
    ).order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_trade_dates_by_month(*, session: Session, year: int, month: int) -> List[AkshareTradeCalendar]:
    """获取某月的所有交易日"""
    statement = select(AkshareTradeCalendar).where(
        and_(
            AkshareTradeCalendar.year == year,
            AkshareTradeCalendar.month == month
        )
    ).order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_trade_dates_by_quarter(*, session: Session, year: int, quarter: int) -> List[AkshareTradeCalendar]:
    """获取某季度的所有交易日"""
    statement = select(AkshareTradeCalendar).where(
        and_(
            AkshareTradeCalendar.year == year,
            AkshareTradeCalendar.quarter == quarter
        )
    ).order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_month_end_trade_dates(*, session: Session, year: Optional[int] = None) -> List[AkshareTradeCalendar]:
    """获取月末交易日"""
    statement = select(AkshareTradeCalendar).where(AkshareTradeCalendar.is_month_end == True)
    if year:
        statement = statement.where(AkshareTradeCalendar.year == year)
    statement = statement.order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_quarter_end_trade_dates(*, session: Session, year: Optional[int] = None) -> List[AkshareTradeCalendar]:
    """获取季末交易日"""
    statement = select(AkshareTradeCalendar).where(AkshareTradeCalendar.is_quarter_end == True)
    if year:
        statement = statement.where(AkshareTradeCalendar.year == year)
    statement = statement.order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_year_end_trade_dates(*, session: Session, start_year: Optional[int] = None,
                             end_year: Optional[int] = None) -> List[AkshareTradeCalendar]:
    """获取年末交易日"""
    statement = select(AkshareTradeCalendar).where(AkshareTradeCalendar.is_year_end == True)
    if start_year and end_year:
        statement = statement.where(
            and_(
                AkshareTradeCalendar.year >= start_year,
                AkshareTradeCalendar.year <= end_year
            )
        )
    elif start_year:
        statement = statement.where(AkshareTradeCalendar.year >= start_year)
    elif end_year:
        statement = statement.where(AkshareTradeCalendar.year <= end_year)
    statement = statement.order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_trade_dates_by_weekday(*, session: Session, weekday: int, year: Optional[int] = None) -> List[
    AkshareTradeCalendar]:
    """获取指定星期几的交易日"""
    statement = select(AkshareTradeCalendar).where(AkshareTradeCalendar.weekday == weekday)
    if year:
        statement = statement.where(AkshareTradeCalendar.year == year)
    statement = statement.order_by(AkshareTradeCalendar.trade_date)
    return list(session.exec(statement).all())


def get_latest_trade_date(*, session: Session) -> AkshareTradeCalendar | None:
    """获取最新的交易日"""
    statement = select(AkshareTradeCalendar).order_by(AkshareTradeCalendar.trade_date.desc())
    return session.exec(statement).first()


def get_earliest_trade_date(*, session: Session) -> AkshareTradeCalendar | None:
    """获取最早的交易日"""
    statement = select(AkshareTradeCalendar).order_by(AkshareTradeCalendar.trade_date.asc())
    return session.exec(statement).first()


def get_next_trade_date(*, session: Session, current_date: date) -> AkshareTradeCalendar | None:
    """获取指定日期后的下一个交易日"""
    statement = select(AkshareTradeCalendar).where(
        AkshareTradeCalendar.trade_date > current_date
    ).order_by(AkshareTradeCalendar.trade_date.asc())
    return session.exec(statement).first()


def get_previous_trade_date(*, session: Session, current_date: date) -> AkshareTradeCalendar | None:
    """获取指定日期前的上一个交易日"""
    statement = select(AkshareTradeCalendar).where(
        AkshareTradeCalendar.trade_date < current_date
    ).order_by(AkshareTradeCalendar.trade_date.desc())
    return session.exec(statement).first()


def count_trade_dates_in_range(*, session: Session, start_date: date, end_date: date) -> int:
    """统计指定时间范围内的交易日数量"""
    statement = select(AkshareTradeCalendar).where(
        and_(
            AkshareTradeCalendar.trade_date >= start_date,
            AkshareTradeCalendar.trade_date <= end_date
        )
    )
    return len(list(session.exec(statement).all()))


def delete_all_trade_calendars(session: Session) -> int:
    """删除所有交易日历数据并返回删除的数量"""
    statement = text(f"DELETE FROM {AkshareTradeCalendar.__tablename__}")
    result = session.exec(statement)
    session.commit()
    return result.rowcount


def delete_trade_calendars_by_year(*, session: Session, year: int) -> int:
    """删除指定年份的交易日历数据"""
    statement = text(f"DELETE FROM {AkshareTradeCalendar.__tablename__} WHERE year = :year")
    result = session.exec(statement, {"year": year})
    session.commit()
    return result.rowcount


# main函数用于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings
    from datetime import date

    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    with Session(engine) as session:
        # 测试查询功能
        test_date = date(2024, 1, 2)

        # 检查是否为交易日
        is_trade = is_trade_date(session=session, check_date=test_date)
        print(f"{test_date} 是否为交易日: {is_trade}")

        # 获取2024年1月的交易日
        jan_trades = get_trade_dates_by_month(session=session, year=2024, month=1)
        print(f"2024年1月交易日数量: {len(jan_trades)}")

        # 获取2024年第一季度的交易日
        q1_trades = get_trade_dates_by_quarter(session=session, year=2024, quarter=1)
        print(f"2024年第一季度交易日数量: {len(q1_trades)}")