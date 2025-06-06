from typing import List
import logging
from sqlmodel import Session

from xtquant import xtdata
from app.models.qmt_sector import QmtSector
from app.cruds.qmt_sector_crud import create_qmt_sector, get_qmt_sector_by_name, delete_all_qmt_sectors

logger = logging.getLogger(__name__)

def sync_sector_list_to_db(db: Session) -> List[QmtSector]:
    """
    从QMT获取板块列表并同步到数据库

    Args:
        db: 数据库会话

    Returns:
        List[QmtSector]: 成功插入的板块列表

    Raises:
        Exception: 当获取板块列表失败或数据库操作失败时抛出
    """
    try:
        # 同步前先删除旧数据
        delete_all_qmt_sectors(db)
        # 从QMT获取板块列表
        sector_list: List[str] = xtdata.get_sector_list()
        if not sector_list:
            logger.warning("从QMT获取板块列表为空")
            return []

        logger.info(f"从QMT获取到{len(sector_list)}个板块")

        # 存储成功插入的板块
        inserted_sectors: List[QmtSector] = []

        # 遍历板块列表，逐个插入数据库
        for sector_name in sector_list:
            try:
                # 检查板块是否已存在
                existing_sector = get_qmt_sector_by_name(session=db, name=sector_name)
                if existing_sector:
                    logger.debug(f"板块[{sector_name}]已存在，跳过")
                    continue

                # 创建新板块记录
                new_sector = QmtSector(sector_name=sector_name)
                db_sector = create_qmt_sector(
                    session=db,
                    qmt_sector_create=new_sector
                )
                inserted_sectors.append(db_sector)
                logger.info(f"成功插入板块[{sector_name}]")

            except Exception as e:
                logger.error(f"插入板块[{sector_name}]失败: {str(e)}")
                # 继续处理下一个板块，不中断整个流程
                continue

        logger.info(f"本次同步完成，成功插入{len(inserted_sectors)}个板块")
        return inserted_sectors

    except Exception as e:
        logger.error(f"同步板块列表失败: {str(e)}")
        raise

# 增加 main
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config  import settings

    # 创建Mysql数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 同步板块列表到数据库
    with Session(engine) as session:
        sync_sector_list_to_db(session)
