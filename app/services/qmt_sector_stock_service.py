from typing import List, Dict

from sqlmodel import Session, select
from xtquant import xtdata

from app.cruds.qmt_sector_stock_crud import delete_qmt_sector_stocks_by_sector_id
from app.models.qmt_sector import QmtSector
from app.models.qmt_sector_stock import QmtSectorStock
from utils.quant_logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

def sync_sector_stocks_to_db(db: Session) -> Dict[str, List[QmtSectorStock]]:
    """
    同步所有板块的成分股到数据库。
    处理流程：
    1. 获取数据库中所有板块
    2. 对每个板块：
        - 从QMT获取该板块的成分股列表
        - 删除该板块原有成分股记录
        - 批量插入新的成分股记录

    Args:
        db: 数据库会话

    Returns:
        Dict[str, List[QmtSectorStock]]: 以板块名为key，成分股列表为value的字典

    Raises:
        Exception: 当获取成分股列表失败或数据库操作失败时抛出
    """
    result: Dict[str, List[QmtSectorStock]] = {}

    try:
        # 获取所有板块
        sectors: List[QmtSector] = db.exec(select(QmtSector)).all()
        if not sectors:
            logger.warning("数据库中没有板块数据，请先同步板块列表")
            return result

        logger.info(f"开始同步{len(sectors)}个板块的成分股")

        for sector in sectors:
            try:
                # 获取板块成分股列表
                stock_codes: List[str] = xtdata.get_stock_list_in_sector(sector.sector_name)
                if not stock_codes:
                    logger.warning(f"板块[{sector.sector_name}]没有成分股")
                    continue

                logger.info(f"板块[{sector.sector_name}]获取到{len(stock_codes)}个成分股")

                # 删除原有成分股
                delete_qmt_sector_stocks_by_sector_id(db, sector.id)

                # 批量创建成分股对象
                stocks_to_insert: List[QmtSectorStock] = [
                    QmtSectorStock(
                        sector_id=sector.id,
                        stock_code=code
                    ) for code in stock_codes
                ]

                # 批量插入数据库
                db.bulk_save_objects(stocks_to_insert)
                db.commit()

                # 刷新对象以获取数据库生成的ID
                for stock in stocks_to_insert:
                    db.refresh(stock)

                result[sector.sector_name] = stocks_to_insert
                logger.info(f"板块[{sector.sector_name}]成功同步{len(stocks_to_insert)}个成分股")

            except Exception as e:
                db.rollback()
                logger.error(f"同步板块[{sector.sector_name}]成分股失败: {str(e)}")
                # 继续处理下一个板块，不中断整体同步流程
                continue

        logger.info(f"所有板块成分股同步完成，成功同步{len(result)}个板块")
        return result

    except Exception as e:
        logger.error(f"同步板块成分股过程发生错误: {str(e)}")
        raise

# 增加 main 便于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings

    # 创建MySQL数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 同步板块成分股到数据库
    with Session(engine) as session:
        sync_sector_stocks_to_db(session)
