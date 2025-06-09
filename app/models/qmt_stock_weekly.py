from typing import Optional
from datetime import datetime
from sqlmodel import Field, SQLModel

class QmtStockWeeklyOri(SQLModel, table=True):
    """周K线原始数据模型"""
    id: Optional[int] = Field(default=None, primary_key=True)
    stock_code: str = Field(max_length=20, nullable=False)
    time: datetime = Field(nullable=False)
    open: float = Field(nullable=False)
    high: float = Field(nullable=False)
    low: float = Field(nullable=False)
    close: float = Field(nullable=False)
    volume: int = Field(nullable=False)
    amount: float = Field(nullable=False)

    __tablename__ = "qmt_stock_weekly_ori"
    __table_args__ = {"comment": "股票周K线数据表"}
