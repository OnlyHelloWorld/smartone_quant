from typing import List, Dict, Tuple
import datetime
from sqlmodel import Session

from xtquant import xtdata
from app.models.qmt_sector import QmtSector
from app.models.qmt_sector_stock import QmtSectorStock
from app.cruds.qmt_sector_crud import delete_all_qmt_sectors
from app.cruds.qmt_sector_stock_crud import delete_all_qmt_sector_stocks
from utils.quant_logger import LoggerFactory

logger = LoggerFactory.get_logger(__name__)

# 允许的板块前缀列表
ALLOWED_PREFIXES = {
    "GN", "TGN", "THY", "1000SW", "500SW",
    "300", "300SW", "HKSW",
    "SW1", "SW2", "SW3", "CSRC"
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

def sync_sector_and_stocks_to_db(db: Session) -> Tuple[List[QmtSector], Dict[str, List[str]]]:
    """
    从QMT获取板块列表及其成分股并同步到数据库
    仅同步指定前缀的板块：GN TGN THY 1000SW 500SW 300 300SW HKSW SW1 SW2 SW3 CSRC

    Args:
        db: 数据库会话

    Returns:
        Tuple[List[QmtSector], Dict[str, List[str]]]:
            - 成功插入的板块列表
            - 以板块名为key，成分股代码列表为value的字典

    Raises:
        Exception: 当获取板块列表失败或数据库操作失败时抛出
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
            return [], {}
        logger.info(f"从QMT获取到{len(sector_list)}个板块")

        # 删除所有旧数据
        logger.info("删除所有板块和成分股旧数据...")
        deleted_sectors = delete_all_qmt_sectors(db)
        logger.info(f'已删除旧板块数据，删除数量: {deleted_sectors}')
        deleted_stocks = delete_all_qmt_sector_stocks(db)
        logger.info(f'已删除旧成分股数据，删除数量: {deleted_stocks}')

        # 用于存储结果
        inserted_sectors: List[QmtSector] = []
        sector_stocks_map: Dict[str, List[str]] = {}
        skipped_sectors = []

        # 按前缀分类计数器
        prefix_counts = {prefix: 0 for prefix in ALLOWED_PREFIXES}

        # 遍历处理每个板块
        for sector_name in sector_list:
            try:
                # 检查是否为允许的板块
                if not should_include_sector(sector_name):
                    skipped_sectors.append(sector_name)
                    continue

                # 更新前缀计数
                for prefix in ALLOWED_PREFIXES:
                    if sector_name.startswith(prefix):
                        prefix_counts[prefix] += 1
                        break

                logger.info(f"开始处理板块[{sector_name}]...")

                # 获取该板块的成分股
                stock_codes: List[str] = xtdata.get_stock_list_in_sector(sector_name)

                # 创建板块记录
                sector = QmtSector(sector_name=sector_name)
                db.add(sector)
                db.flush()  # 获取板块ID

                if not stock_codes:
                    logger.warning(f"板块[{sector_name}]没有成分股")
                    sector_stocks_map[sector_name] = []
                    continue

                logger.info(f"板块[{sector_name}]获取到{len(stock_codes)}个成分股")

                # 批量创建该板块的成分股记录
                sector_stocks = [
                    QmtSectorStock(
                        sector_id=sector.id,
                        stock_code=code
                    ) for code in stock_codes
                ]

                db.add_all(sector_stocks)
                sector_stocks_map[sector_name] = stock_codes
                inserted_sectors.append(sector)
                db.commit()

                logger.info(f"板块[{sector_name}]及其{len(stock_codes)}个成分股数据已插入")

            except Exception as e:
                logger.error(f"处理板块[{sector_name}]时发生错误: {str(e)}")
                continue

        end_time = datetime.datetime.now()
        logger.info(f"同步完成，结束时间: {end_time}, 总耗时: {end_time - start_time}")

        # 输出每个前缀的板块数量统计
        logger.info("各类板块数量统计:")
        for prefix, count in prefix_counts.items():
            if count > 0:
                logger.info(f"{prefix}: {count}个板块")

        logger.info(f"成功同步{len(inserted_sectors)}个板块，包含成分股的板块数: {len([k for k, v in sector_stocks_map.items() if v])}")

        # 报告包含成分股最多的十个板块
        if sector_stocks_map:
            sorted_sectors = sorted(sector_stocks_map.items(), key=lambda x: len(x[1]), reverse=True)
            top_sectors = sorted_sectors[:10]
            logger.info("包含成分股最多的十个板块:")
            for sector_name, stocks in top_sectors:
                logger.info(f"板块[{sector_name}] - 成分股数量: {len(stocks)}")

        return inserted_sectors, sector_stocks_map

    except Exception as e:
        db.rollback()
        logger.error(f"同步板块及成分股数据失败: {str(e)}")
        raise


# 增加 main 函数便于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings

    # 创建MySQL数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 同步板块列表和成分股到数据库
    with Session(engine) as session:
        sync_sector_and_stocks_to_db(session)
