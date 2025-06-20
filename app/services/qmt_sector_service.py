import datetime
from typing import List, Tuple

from sqlmodel import Session, select
from xtquant import xtdata

from app.cruds.qmt_sector_crud import delete_all_qmt_sectors
from app.cruds.qmt_sector_stock_crud import delete_all_qmt_sector_stocks
from app.models.qmt_sector import QmtSector
from app.models.qmt_sector_stock import QmtSectorStock
from utils.quant_logger import init_logger

logger = init_logger()

# 允许的板块前缀列表
ALLOWED_PREFIXES = {
    "GN", "TGN", "THY", "1000SW", "500SW",
    "300", "300SW", "HKSW",
    "SW1", "SW2", "SW3", "CSRC",
    "沪深A股","沪深300"
}

def should_include_sector(sector_name: str) -> bool:
    """
    检查板块名称是否以允许的前缀开头

    Args:
        sector_name: 板块名称

    Returns:
        bool: 如果以允许的前缀开头返回True，否则返回False
    """
    return any(sector_name.startswith(prefix) for prefix in ALLOWED_PREFIXES)

def sync_sector_and_stocks_to_db(db: Session) -> List[str]:
    """
    从QMT获取板块列表及其成分股并同步到数据库
    仅同步指定前缀的板块：GN TGN THY 1000SW 500SW 300 300SW HKSW SW1 SW2 SW3 CSRC

    Args:
        db: 数据库会话

    Returns:
        List[str]: 同步失败的板块列表

    Raises:
        Exception: 当获取板块列表失败时抛出
    """
    try:
        # 记录开始时间
        start_time = datetime.datetime.now()
        logger.info(f'开始同步QMT板块及成分股数据，开始时间: {start_time}')

        # 从QMT获取板块列表
        logger.info("从QMT获取板块列表...")
        sector_list: List[str] = xtdata.get_sector_list()
        if not sector_list:
            logger.warning("从QMT获取板块列表为空")
            return []
        logger.info(f"从QMT获取到{len(sector_list)}个板块")

        # 删除所有旧数据
        logger.info("删除所有板块和成分股旧数据...")
        deleted_sectors = delete_all_qmt_sectors(db)
        logger.info(f'已删除旧板块数据，删除数量: {deleted_sectors}')
        deleted_stocks = delete_all_qmt_sector_stocks(db)
        logger.info(f'已删除旧成分股数据，删除数量: {deleted_stocks}')

        # 用于存储结果
        failed_sectors: List[str] = []
        skipped_sectors: List[str] = []
        processed_count = 0
        success_count = 0
        current_sector_id = 0  # 板块ID从0开始

        # 遍历处理每个板块
        for sector_name in sector_list:
            try:
                # 检查是否为允许的板块
                if not should_include_sector(sector_name):
                    skipped_sectors.append(sector_name)
                    continue

                processed_count += 1

                logger.info(f"开始处理板块[{sector_name}]...")

                # 获取该板块的成分股
                stock_codes: List[str] = xtdata.get_stock_list_in_sector(sector_name)

                if not stock_codes:
                    logger.warning(f"板块[{sector_name}]没有成分股")
                    failed_sectors.append(f"{sector_name}(无成分股)")
                    continue

                # 创建板块记录，使用递增的ID
                sector = QmtSector(id=current_sector_id, sector_name=sector_name)
                db.add(sector)
                db.flush()

                logger.info(f"板块[{sector_name}]获取到{len(stock_codes)}个成分股")

                # 批量创建该板块的成分股记录
                sector_stocks = [
                    QmtSectorStock(
                        sector_id=current_sector_id,
                        stock_code=code
                    ) for code in stock_codes
                ]

                db.add_all(sector_stocks)
                db.commit()

                logger.info(f"板块[{sector_name}](ID:{current_sector_id})及其{len(stock_codes)}个成分股数据已插入")
                current_sector_id += 1  # 板块ID递增
                success_count += 1

            except Exception as e:
                db.rollback()
                error_msg = str(e)
                logger.error(f"处理板块[{sector_name}]时发生错误: {error_msg}")
                failed_sectors.append(f"{sector_name}({error_msg})")
                continue

        end_time = datetime.datetime.now()
        logger.info(f"同步完成，结束时间: {end_time}, 总耗时: {end_time - start_time}")

        # 输出统计信息
        logger.info("\n=== 同步结果统计 ===")
        logger.info(f"跳过的板块数量: {len(skipped_sectors)}")
        logger.info(f"处理的板块数量: {processed_count}")
        logger.info(f"成功同步数量: {success_count}")
        logger.info(f"失败的板块数量: {len(failed_sectors)}")

        # 如果有失败的板块，输出详细信息
        if failed_sectors:
            logger.info("\n=== 同步失败的板块 ===")
            for sector in failed_sectors:
                logger.info(f"- {sector}")

        return failed_sectors

    except Exception as e:
        logger.error(f"同步板块及成分股数据失败: {str(e)}")
        raise


# 同步指定板块及其成分股到数据库
def sync_sector_and_stocks_to_db_by_name(db: Session, sector_name: str) -> Tuple[List[str], List[str]]:
    """
    同步指定板块及其成分股到数据库

    Args:
        db: 数据库会话
        sector_name: 指定的板块名称

    Returns:
        Tuple[List[str], List[str]]: 成功同步的成分股列表和失败的成分股列表
    """
    try:
        # 获取该板块的成分股
        stock_codes: List[str] = xtdata.get_stock_list_in_sector(sector_name)
        if not stock_codes:
            logger.warning(f"板块[{sector_name}]没有成分股")
            return [], []

        logger.info(f"板块[{sector_name}]获取到{len(stock_codes)}个成分股")

        # 如果数据库中已经存在该板块，先删除该板块以及对应成分股记录
        existing_sector = db.exec(
            select(QmtSector).where(QmtSector.sector_name == sector_name)
        ).first()
        if existing_sector:
            logger.info(f"板块[{sector_name}]已存在，删除旧数据")
            # 删除该板块
            db.exec(
                delete(QmtSector).where(QmtSector.id == existing_sector.id)
            )
            # 删除该板块的成分股记录
            db.exec(
                delete(QmtSectorStock).where(QmtSectorStock.sector_id == existing_sector.id)
            )
            db.commit()
            # 删除板块记录
            db.delete(existing_sector)
            db.commit()

        # 创建新的板块记录
        sector = QmtSector(sector_name=sector_name)
        db.add(sector)
        db.flush()  # 确保板块ID被生成
        current_sector_id = sector.id  # 获取新创建的板块ID
        logger.info(f"创建板块[{sector_name}]，ID为{current_sector_id}")

        # 批量创建该板块的成分股记录
        sector_stocks = [
            QmtSectorStock(
                sector_id=current_sector_id,
                stock_code=code
            ) for code in stock_codes
        ]

        db.add_all(sector_stocks)
        db.commit()

        logger.info(f"板块[{sector_name}]及其{len(stock_codes)}个成分股数据已插入")
        return stock_codes, []

    except Exception as e:
        db.rollback()
        error_msg = str(e)
        logger.error(f"处理板块[{sector_name}]时发生错误: {error_msg}")
        return [], [error_msg]

# 增加 main 函数便于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings

    # 创建MySQL数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 同步板块列表和成分股到数据库
    with Session(engine) as session:
        # sync_sector_and_stocks_to_db(session)
        # 同步指定板块及其成分股到数据库
        sector_name = "沪深A股"
        stock_codes, errors = sync_sector_and_stocks_to_db_by_name(session, sector_name)
        if stock_codes:
            logger.info(f"成功同步板块[{sector_name}]的成分股: {stock_codes}")