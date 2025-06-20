from typing import Any

from sqlalchemy import text
from sqlmodel import Session, select

from app.models.qmt_sector import QmtSector

"""
对QmtSector模型的增删改查操作
"""


def create_qmt_sector(*, session: Session, qmt_sector_create: QmtSector) -> QmtSector:
    db_obj = QmtSector(**qmt_sector_create.model_dump(exclude={'id'}))
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def update_qmt_sector(*, session: Session, db_qmt_sector: QmtSector, qmt_sector_in: QmtSector) -> Any:
    qmt_sector_data = qmt_sector_in.model_dump(exclude_unset=True)
    db_qmt_sector.sqlmodel_update(qmt_sector_data)
    session.add(db_qmt_sector)
    session.commit()
    session.refresh(db_qmt_sector)
    return db_qmt_sector


# 根据板块名获取板块
def get_qmt_sector_by_name(*, session: Session, name: str) -> QmtSector | None:
    statement = select(QmtSector).where(QmtSector.sector_name == name)
    session_qmt_sector = session.exec(statement).first()
    return session_qmt_sector

# 删除所有QmtSector板块数据并返回删除的数量
def delete_all_qmt_sectors(session: Session) -> int:
    """
    删除所有QmtSector板块数据并返回删除的数量
    :param session: 数据库会话
    :return: 删除的数量
    """
    statement = text(f"DELETE FROM {QmtSector.__tablename__}")
    result = session.exec(statement)
    session.commit()
    return result.rowcount


# main函数用于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings

    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    with Session(engine) as session:
        # 获取沪深A股板块，通过get_qmt_sector_by_name

        sector_name = "沪深A股"
        sector = get_qmt_sector_by_name(session=session, name=sector_name)
        if not sector:
            print("数据库中没有板块数据，请先同步板块列表")
        else:
            print(f"板块ID: {sector.id}, 板块名称: {sector.sector_name}")
