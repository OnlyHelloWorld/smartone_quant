from typing import List
import logging
from sqlmodel import Session

from xtquant import xtdata
from app.models.qmt_sector import QmtSector
from app.cruds.qmt_sector_crud import delete_all_qmt_sectors

logger = logging.getLogger(__name__)


def sync_sector_list_to_db(db: Session) -> List[QmtSector]:
    """
    从QMT获取板块列表并批量同步到数据库

    Args:
        db: 数据库会话

    Returns:
        List[QmtSector]: 成功插入的板块列表

    Raises:
        Exception: 当获取板块列表失败或数据库操作失败时抛出
    """
    try:
        # 同步前先删除旧数据
        logger.info("开始同步QMT板块列表到数据库，先删除旧数据")
        deleted: int = delete_all_qmt_sectors(db)
        logger.info(f'已删除旧数据，删除数量: {deleted}')

        # 从QMT获取板块列表
        logger.info("从QMT获取板块列表...")
        sector_list: List[str] = xtdata.get_sector_list()
        if not sector_list:
            logger.warning("从QMT获取板块列表为空")
            return []
        logger.info(f"从QMT获取到{len(sector_list)}个板块")

        try:
            # 批量创建QmtSector对象
            logger.info("开始批量插入板块到数据库...")
            sectors_to_insert: List[QmtSector] = [QmtSector(sector_name=name) for name in sector_list]

            # 批量插入数据库并获取成功插入的条数
            db.add_all(sectors_to_insert)
            db.commit()
            logger.info(f"成功批量插入{len(sectors_to_insert)}个板块到数据库")
            return sectors_to_insert

        except Exception as e:
            db.rollback()
            logger.error(f"批量插入板块失败: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"同步板块列表失败: {str(e)}")
        raise


# 增加 main
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings

    # 创建Mysql数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 同步板块列表到数据库
    with Session(engine) as session:
        sync_sector_list_to_db(session)
