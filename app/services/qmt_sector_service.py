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

# 需要过滤的板块名称关键词
FILTERED_KEYWORDS = {
    "上期所", "A股", "期权", "转债", "中金所", "大商所",
    "债券", "基金", "指数", "B股", "科创板", "合约",
    "郑商所", "香港联交所", "ETF", "能源中心", "沪深",
    "股票", "转债", "等权"
}

def should_skip_sector(sector_name: str) -> bool:
    """
    检查板块名称是否包含需要过滤的关键词

    Args:
        sector_name: 板块名称

    Returns:
        bool: 如果包含需要过滤的关键词返回True，否则返回False
    """
    return any(keyword in sector_name for keyword in FILTERED_KEYWORDS)

def sync_sector_and_stocks_to_db(db: Session) -> Tuple[List[QmtSector], Dict[str, List[str]]]:
    """
    从QMT获取板块列表及其成分股并同步到数据库

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

        # 删除所有旧板块数据
        logger.info("删除所有板块和成分股旧数据...")
        deleted_sectors = delete_all_qmt_sectors(db)
        logger.info(f'已删除旧板块数据，删除数量: {deleted_sectors}')
        # 删除所有旧成分股数据
        deleted_stocks = delete_all_qmt_sector_stocks(db)
        logger.info(f'已删除旧成分股数据，删除数量: {deleted_stocks}')

        # 用于存储结果
        inserted_sectors: List[QmtSector] = []
        sector_stocks_map: Dict[str, List[str]] = {}
        skipped_sectors = []

        # 遍历处理每个板块
        for sector_name in sector_list:
            try:
                # 检查是否需要跳过此板块
                if should_skip_sector(sector_name):
                    skipped_sectors.append(sector_name)
                    logger.info(f"跳过板块[{sector_name}]，因为包含过滤关键词")
                    continue

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
                # 提交所有更改
                db.commit()

                logger.info(f"板块[{sector_name}]及其{len(stock_codes)}个成分股数据已插入")

            except Exception as e:
                logger.error(f"处理板块[{sector_name}]时发生错误: {str(e)}")
                continue

        end_time = datetime.datetime.now()
        logger.info(f"同步完成，结束时间: {end_time}, 总耗时: {end_time - start_time}")
        logger.info(f"跳过的板块数量: {len(skipped_sectors)}")
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
