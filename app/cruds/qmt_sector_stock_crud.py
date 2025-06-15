import logging
from sqlmodel import Session, select, delete
from app.models.qmt_sector_stock import QmtSectorStock
# 导入 qmt_sector_crud
from app.cruds.qmt_sector_crud import get_qmt_sector_by_name

"""
对QmtSectorStock模型的增删改查操作
"""


def create_qmt_sector_stock(*, session: Session, qmt_sector_stock_create: QmtSectorStock) -> QmtSectorStock:
    db_obj = QmtSectorStock(**qmt_sector_stock_create.model_dump(exclude={'id'}))
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def update_qmt_sector_stock(*, session: Session, db_qmt_sector_stock: QmtSectorStock,
                            qmt_sector_stock_in: QmtSectorStock):
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
def get_qmt_sector_stock_by_sector_and_code(*, session: Session, sector_id: int,
                                            stock_code: str) -> QmtSectorStock | None:
    statement = select(QmtSectorStock).where(
        (QmtSectorStock.sector_id == sector_id) & (QmtSectorStock.stock_code == stock_code)
    )
    return session.exec(statement).first()


# 通过板块名称获取成分股列表
def get_qmt_sector_stocks_by_sector_name(*, session: Session, sector_name: str) -> list[QmtSectorStock]:
    """
    根据板块名称获取成分股列表

    Args:
        session: 数据库会话
        sector_name: 板块名称

    Returns:
        list[QmtSectorStock]: 成分股列表
    """
    # 先通过板块名称获取板块ID
    sector = get_qmt_sector_by_name(session=session, name=sector_name)
    if not sector:
        logging.warning(f"未找到板块名称为[{sector_name}]的板块")
        return []
    # 根据板块ID查询成分股
    statement = select(QmtSectorStock).where(QmtSectorStock.sector_id == sector.id)
    sector_stocks = session.exec(statement).all()
    if not sector_stocks:
        logging.warning(f"板块[{sector_name}]没有成分股")
        return []
    logging.info(f"板块[{sector_name}]获取到{len(sector_stocks)}个成分股")
    return sector_stocks

# 删除所有QmtSectorStock表数据并返回删除的数量
def delete_all_qmt_sector_stocks(session: Session) -> int:
    """
    删除所有QmtSectorStock表数据并返回删除的数量
    :param session: 数据库会话
    :return: 删除的数量
    """
    statement = delete(QmtSectorStock)
    result = session.exec(statement)
    session.commit()
    return result.rowcount

# main函数用于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings

    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    with Session(engine) as session:
        # 获取沪深A股成分股列表
        sector_name = "沪深A股"
        sector_stocks = get_qmt_sector_stocks_by_sector_name(session=session, sector_name=sector_name)
        if not sector_stocks:
            print(f"板块[{sector_name}]没有成分股数据")
        else:
            print(f"板块[{sector_name}]的成分股数量: {len(sector_stocks)}")
            for stock in sector_stocks:
                print(f"股票代码: {stock.stock_code}, 板块ID: {stock.sector_id}")
