from datetime import datetime
from sqlmodel import Session, create_engine

from app.core.config import settings
from cruds.qmt_sector_stock_crud import get_qmt_sector_stocks_by_sector_name
from models.qmt_stock_daily import QmtStockDailyOri
from models.qmt_stock_weekly import QmtStockWeeklyOri
from models.qmt_stock_monthly import QmtStockMonthlyOri
from utils.qmt_data_utils import (
    batch_download_stocks_data,
    sync_stocks_klines_with_threadpool,
    get_time_range_for_sync
)
from utils.quant_logger import init_logger

logger = init_logger()

# 周期配置映射
PERIOD_CONFIG = {
    'daily': {
        'period': '1d',
        'period_name': '日K',
        'model_cls': QmtStockDailyOri
    },
    'weekly': {
        'period': '1w',
        'period_name': '周K',
        'model_cls': QmtStockWeeklyOri
    },
    'monthly': {
        'period': '1mon',
        'period_name': '月K',
        'model_cls': QmtStockMonthlyOri
    }
}


def sync_stock_klines(
        period_type: str,
        stock_codes: list[str] = None,
        begin_time_str: str = None,
        end_time_str: str = None,
        max_workers: int = 4
):
    """K线数据同步主函数

    Args:
        period_type (str): 周期类型，可选值: 'daily', 'weekly', 'monthly'
        stock_codes (list[str], optional): 股票代码列表. 如果为None, 则同步所有沪深A股
        begin_time_str (str, optional): 开始时间，格式为YYYYMMDD. 如果为None, 则自动计算
        end_time_str (str, optional): 结束时间，格式为YYYYMMDD. 如果为None, 则使用当天
        max_workers (int, optional): 最大线程数，默认为4
    """
    # 验证周期类型
    if period_type not in PERIOD_CONFIG:
        raise ValueError(f"不支持的周期类型: {period_type}，支持的类型: {list(PERIOD_CONFIG.keys())}")

    config = PERIOD_CONFIG[period_type]

    start_time = datetime.now()
    logger.info(f"开始同步{config['period_name']}数据，当前时间：{start_time}")

    # 创建数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=False)

    # 获取时间范围
    begin_time, today = get_time_range_for_sync()
    if begin_time_str:
        begin_time = datetime.strptime(begin_time_str, '%Y%m%d')
    if end_time_str:
        today = datetime.strptime(end_time_str, '%Y%m%d')

    # 如果没有指定股票代码，则获取所有沪深A股
    if not stock_codes:
        sector_name = "沪深A股"
        with Session(engine) as session:
            sector_stocks = get_qmt_sector_stocks_by_sector_name(
                session=session,
                sector_name=sector_name
            )
        stock_codes = sorted({s.stock_code for s in sector_stocks})

    total_stocks = len(stock_codes)
    logger.info(f"需要同步的股票数量：{total_stocks}")

    # 执行批量数据下载
    start_time_str = begin_time.strftime('%Y%m%d')
    end_time_str = today.strftime('%Y%m%d')
    download_start_time = datetime.now()

    batch_download_stocks_data(
        stock_codes=stock_codes,
        start_time_str=start_time_str,
        end_time_str=end_time_str,
        period=config['period'],
        period_name=config['period_name']
    )

    download_end_time = datetime.now()

    # 使用线程池并发同步每支股票
    sync_start_time = datetime.now()

    total_success = sync_stocks_klines_with_threadpool(
        stock_codes=stock_codes,
        begin_time=begin_time,
        end_time=today,
        engine=engine,
        model_cls=config['model_cls'],
        period=config['period'],
        period_name=config['period_name'],
        max_workers=max_workers
    )

    sync_end_time = datetime.now()

    # 输出统计信息
    logger.info("数据同步完成")
    logger.info(f"总股票数：{total_stocks}")
    logger.info(f"成功同步：{total_success}")
    logger.info(f"下载耗时：{download_end_time - download_start_time}")
    logger.info(f"同步耗时：{sync_end_time - sync_start_time}")
    logger.info(f"总耗时：{datetime.now() - start_time}")


def sync_daily_klines(stock_codes: list[str] = None, begin_time_str: str = None, end_time_str: str = None):
    """同步日K数据的便捷函数"""
    sync_stock_klines('daily', stock_codes, begin_time_str, end_time_str)


def sync_weekly_klines(stock_codes: list[str] = None, begin_time_str: str = None, end_time_str: str = None):
    """同步周K数据的便捷函数"""
    sync_stock_klines('weekly', stock_codes, begin_time_str, end_time_str)


def sync_monthly_klines(stock_codes: list[str] = None, begin_time_str: str = None, end_time_str: str = None):
    """同步月K数据的便捷函数"""
    sync_stock_klines('monthly', stock_codes, begin_time_str, end_time_str)


if __name__ == "__main__":
    import sys

    # 从命令行参数获取周期类型，默认为日K
    period_type = sys.argv[1] if len(sys.argv) > 1 else 'daily'

    # 示例用法
    sync_stock_klines(period_type)

    # 或者可以分别调用
    sync_daily_klines()
    sync_weekly_klines()
    sync_monthly_klines()