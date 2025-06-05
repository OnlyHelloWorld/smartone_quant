import uuid

from pydantic import EmailStr
from sqlmodel import Field, Relationship, SQLModel

"""
CREATE TABLE IF NOT EXISTS smartone_sector_stock_list_from_qmt (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
    sector_id INT NOT NULL COMMENT '板块ID',
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    FOREIGN KEY (sector_id) REFERENCES smartone_sector_list_from_qmt(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='板块成分股列表存储表';
"""

class QmtSectorStock(SQLModel, table=True):
    """
    板块成分股列表模型，存储从 QMT 获取的板块成分股数据。
    该模型对应数据库表 `smartone_sector_stock_list_from_qmt`，用于存储板块ID、股票代码及其创建和更新时间。
    """
    id: int = Field(default=None, primary_key=True, autoincrement=True)
    sector_id: int = Field(foreign_key="smartone_sector_list_from_qmt.id", nullable=False, comment="板块ID")
    stock_code: str = Field(max_length=20, nullable=False, comment="股票代码")
    create_time: str = Field(default=None, comment="创建时间")
    update_time: str = Field(default=None, comment="更新时间")

    __tablename__ = "smartone_sector_stock_list_from_qmt"
    __table_args__ = {"comment": "板块成分股列表存储表"}