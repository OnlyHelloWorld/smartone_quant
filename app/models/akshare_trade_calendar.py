from typing import Optional
from datetime import date

from sqlmodel import Field, SQLModel

class AkshareTradeCalendar(SQLModel, table=True):
    """
    AKShare交易日历模型，存储从AKShare获取的交易日历数据。
    该模型对应数据库表 `akshare_trade_calendar`，用于存储交易日期及相关信息。
    """
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, primary_key=True, description="自增主键")
    trade_date: date = Field(unique=True, nullable=False, description="交易日期")
    year: int = Field(nullable=False, description="年份")
    month: int = Field(nullable=False, description="月份")
    day: int = Field(nullable=False, description="日期")
    weekday: int = Field(nullable=False, description="星期几(1=周一,7=周日)")
    quarter: int = Field(nullable=False, description="季度")
    week_of_year: int = Field(nullable=False, description="年内第几周")
    is_month_end: bool = Field(default=False, description="是否月末交易日")
    is_quarter_end: bool = Field(default=False, description="是否季末交易日")
    is_year_end: bool = Field(default=False, description="是否年末交易日")

    __tablename__ = "akshare_trade_calendar"