import uuid
from typing import Any

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

def delete_all_qmt_sectors(session: Session):
    session.exec("DELETE FROM qmt_sector")
    session.commit()
