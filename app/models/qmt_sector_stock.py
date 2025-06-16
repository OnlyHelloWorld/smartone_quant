from typing import Optional

from sqlmodel import Field, SQLModel

"""
CREATE TABLE IF NOT EXISTS qmt_sector_stock (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
    sector_id INT NOT NULL COMMENT '板块ID',
    stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    FOREIGN KEY (sector_id) REFERENCES qmt_sector(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='板块成分股列表存储表';
"""

class QmtSectorStock(SQLModel, table=True):
    """
    板块成分股列表模型，存储从 QMT 获取的板块成分股数据。
    该模型对应数据库表 `qmt_sector_stock`，用于存储板块ID、股票代码及其创建和更新时间。
    """
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    sector_id: int = Field(nullable=False)
    stock_code: str = Field(max_length=20, nullable=False)

    __tablename__ = "qmt_sector_stock"
