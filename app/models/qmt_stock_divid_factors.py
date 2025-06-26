from datetime import date, datetime
from typing import Optional

from sqlmodel import Field, SQLModel

class QmtStockDividFactors(SQLModel, table=True):
    """
    股票除权数据模型，存储从 QMT 获取的股票除权信息。
    该模型对应数据库表 `qmt_stock_divid_factors`，用于存储股票的除权除息数据。
    """
    __table_args__ = {"extend_existing": True}

    id: Optional[int] = Field(default=None, primary_key=True)
    stock_code: str = Field(max_length=20, nullable=False, description="股票代码")
    time: int = Field(nullable=False, description="时间戳(毫秒)")
    divid_date: date = Field(nullable=False, description="除权日期")
    interest: float = Field(default=0.0, description="现金红利(每股派息金额)")
    stock_bonus: float = Field(default=0.0, description="送股比例(每股送股数)")
    stock_gift: float = Field(default=0.0, description="转增股比例(每股转增数)")
    allot_num: float = Field(default=0.0, description="配股数量(每股配股数)")
    allot_price: float = Field(default=0.0, description="配股价格")
    gugai: float = Field(default=0.0, description="股改相关数据")
    dr: float = Field(default=0.0, description="除权因子")

    __tablename__ = "qmt_stock_divid_factors"