import uuid

from pydantic import EmailStr
from sqlmodel import Field, Relationship, SQLModel

"""
CREATE TABLE IF NOT EXISTS smartone_sector_list_from_qmt (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
    sector_name VARCHAR(255) NOT NULL COMMENT '板块名称',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='板块列表存储表';
"""

class QmtSector(SQLModel, table=True):
    """
    板块列表模型，存储从 QMT 获取的板块数据。
    该模型对应数据库表 `smartone_sector_list_from_qmt`，用于存储板块名称及其创建和更新时间。
    """
    id: int = Field(default=None, primary_key=True)
    sector_name: str = Field(max_length=255, nullable=False)

    __tablename__ = "smartone_sector_list_from_qmt"
    __table_args__ = {"comment": "板块列表存储表"}