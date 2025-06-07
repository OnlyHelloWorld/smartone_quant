from sqlmodel import Session, select, delete
from app.models.qmt_sector_stock import QmtSectorStock

"""
对QmtSectorStock模型的增删改查操作
"""

def create_qmt_sector_stock(*, session: Session, qmt_sector_stock_create: QmtSectorStock) -> QmtSectorStock:
    db_obj = QmtSectorStock(**qmt_sector_stock_create.model_dump(exclude={'id'}))
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj

def update_qmt_sector_stock(*, session: Session, db_qmt_sector_stock: QmtSectorStock, qmt_sector_stock_in: QmtSectorStock):
    qmt_sector_stock_data = qmt_sector_stock_in.model_dump(exclude_unset=True)
    db_qmt_sector_stock.sqlmodel_update(qmt_sector_stock_data)
    session.add(db_qmt_sector_stock)
    session.commit()
    session.refresh(db_qmt_sector_stock)
    return db_qmt_sector_stock

def delete_qmt_sector_stocks_by_sector_id(session: Session, sector_id: int) -> int:
    """
    删除指定板块ID的所有成分股

    Args:
        session: 数据库会话
        sector_id: 板块ID

    Returns:
        int: 删除的记录数量

    Raises:
        Exception: 当数据库操作失败时抛出
    """
    statement = delete(QmtSectorStock).where(QmtSectorStock.sector_id == sector_id)
    result = session.exec(statement)
    session.commit()
    return result.rowcount

# 根据板块ID和股票代码获取成分股
def get_qmt_sector_stock_by_sector_and_code(*, session: Session, sector_id: int, stock_code: str) -> QmtSectorStock | None:
    statement = select(QmtSectorStock).where(
        (QmtSectorStock.sector_id == sector_id) & (QmtSectorStock.stock_code == stock_code)
    )
    return session.exec(statement).first()
