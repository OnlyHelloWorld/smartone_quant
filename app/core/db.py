from sqlmodel import Session, create_engine, select

from app import crud
from app.core.config import settings
from app.models.qmt_sector import QmtSector
from app.models.qmt_sector_stock import QmtSectorStock
from app.models.Smart_User import User, UserCreate

engine = create_engine(str(settings.SQLALCHEMY_DATABASE_URI))


# 确保在初始化数据库前已导入所有 SQLModel 模型（app.models）
# 否则，SQLModel 可能无法正确初始化关系
# 详细说明见：https://github.com/fastapi/full-stack-fastapi-template/issues/28

def init_db(session: Session) -> None:
    # 表应通过 Alembic 迁移创建
    # 如果不想使用迁移，可以取消注释下方代码手动创建表
    # from sqlmodel import SQLModel

    # 由于模型已从 app.models 导入并注册，此方法可用
    # SQLModel.metadata.create_all(engine)

    user = session.exec(
        select(User).where(User.email == settings.FIRST_SUPERUSER)
    ).first()
    if not user:
        user_in = UserCreate(
            email=settings.FIRST_SUPERUSER,
            password=settings.FIRST_SUPERUSER_PASSWORD,
            is_superuser=True,
        )
        user = crud.create_user(session=session, user_create=user_in)
