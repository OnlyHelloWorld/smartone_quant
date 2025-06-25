import datetime
from typing import List
from datetime import date, datetime as dt
import calendar

import akshare as ak
from sqlmodel import Session

from app.cruds.akshare_trade_calendar_crud import delete_all_trade_calendars, batch_create_trade_calendars
from app.models.akshare_trade_calendar import AkshareTradeCalendar
from utils.quant_logger import init_logger

logger = init_logger()


def calculate_trade_calendar_fields(trade_date: date) -> dict:
    """
    计算交易日历相关字段

    Args:
        trade_date: 交易日期

    Returns:
        dict: 包含所有计算字段的字典
    """
    # 基本日期信息
    year = trade_date.year
    month = trade_date.month
    day = trade_date.day
    weekday = trade_date.weekday() + 1  # 1=周一, 7=周日

    # 计算季度
    quarter = (month - 1) // 3 + 1

    # 计算年内第几周
    week_of_year = trade_date.isocalendar()[1]

    return {
        'year': year,
        'month': month,
        'day': day,
        'weekday': weekday,
        'quarter': quarter,
        'week_of_year': week_of_year
    }


def determine_special_dates(trade_dates: List[date]) -> dict:
    """
    确定月末、季末、年末交易日

    Args:
        trade_dates: 交易日期列表

    Returns:
        dict: 特殊日期标记字典
    """
    special_dates = {
        'month_end': set(),
        'quarter_end': set(),
        'year_end': set()
    }

    # 按年月分组
    year_month_groups = {}
    for trade_date in trade_dates:
        key = (trade_date.year, trade_date.month)
        if key not in year_month_groups:
            year_month_groups[key] = []
        year_month_groups[key].append(trade_date)

    # 确定月末交易日
    for (year, month), dates in year_month_groups.items():
        month_end_date = max(dates)
        special_dates['month_end'].add(month_end_date)

        # 确定季末交易日 (3, 6, 9, 12月)
        if month in [3, 6, 9, 12]:
            special_dates['quarter_end'].add(month_end_date)

        # 确定年末交易日 (12月)
        if month == 12:
            special_dates['year_end'].add(month_end_date)

    return special_dates


def sync_trade_calendar_to_db(db: Session) -> List[str]:
    """
    从AKShare获取交易日历数据并同步到数据库

    Args:
        db: 数据库会话

    Returns:
        List[str]: 同步过程中的错误信息列表

    Raises:
        Exception: 当获取交易日历数据失败时抛出
    """
    try:
        # 记录开始时间
        start_time = datetime.datetime.now()
        logger.info(f'开始同步AKShare交易日历数据，开始时间: {start_time}')

        # 从AKShare获取交易日历数据
        logger.info("从AKShare获取交易日历数据...")
        trade_calendar_df = ak.tool_trade_date_hist_sina()

        if trade_calendar_df.empty:
            logger.warning("从AKShare获取交易日历数据为空")
            return ["获取交易日历数据为空"]

        logger.info(f"从AKShare获取到{len(trade_calendar_df)}条交易日历数据")

        # 删除所有旧数据
        logger.info("删除所有交易日历旧数据...")
        deleted_count = delete_all_trade_calendars(db)
        logger.info(f'已删除旧交易日历数据，删除数量: {deleted_count}')

        # 转换日期格式并排序
        trade_dates = []
        for _, row in trade_calendar_df.iterrows():
            trade_date = dt.strptime(str(row['trade_date']), '%Y-%m-%d').date()
            trade_dates.append(trade_date)

        trade_dates.sort()
        logger.info(f"交易日期范围: {trade_dates[0]} 到 {trade_dates[-1]}")

        # 确定特殊日期
        logger.info("计算月末、季末、年末交易日...")
        special_dates = determine_special_dates(trade_dates)

        # 构建交易日历记录
        trade_calendar_records = []
        for i, trade_date in enumerate(trade_dates):
            # 计算基本字段
            fields = calculate_trade_calendar_fields(trade_date)

            # 创建交易日历记录
            trade_calendar = AkshareTradeCalendar(
                id=i + 1,  # 手动指定ID，从1开始
                trade_date=trade_date,
                year=fields['year'],
                month=fields['month'],
                day=fields['day'],
                weekday=fields['weekday'],
                quarter=fields['quarter'],
                week_of_year=fields['week_of_year'],
                is_month_end=trade_date in special_dates['month_end'],
                is_quarter_end=trade_date in special_dates['quarter_end'],
                is_year_end=trade_date in special_dates['year_end']
            )
            trade_calendar_records.append(trade_calendar)

        # 批量插入数据
        logger.info("批量插入交易日历数据...")
        inserted_count = batch_create_trade_calendars(
            session=db,
            trade_calendars=trade_calendar_records
        )

        end_time = datetime.datetime.now()
        logger.info(f"同步完成，结束时间: {end_time}, 总耗时: {end_time - start_time}")

        # 输出统计信息
        logger.info("\n=== 同步结果统计 ===")
        logger.info(f"成功插入交易日历记录: {inserted_count}")
        logger.info(f"月末交易日数量: {len(special_dates['month_end'])}")
        logger.info(f"季末交易日数量: {len(special_dates['quarter_end'])}")
        logger.info(f"年末交易日数量: {len(special_dates['year_end'])}")

        return []

    except Exception as e:
        error_msg = str(e)
        logger.error(f"同步交易日历数据失败: {error_msg}")
        return [error_msg]


def sync_trade_calendar_by_year(db: Session, year: int) -> List[str]:
    """
    同步指定年份的交易日历数据到数据库

    Args:
        db: 数据库会话
        year: 指定年份

    Returns:
        List[str]: 同步过程中的错误信息列表
    """
    try:
        logger.info(f"开始同步{year}年交易日历数据...")

        # 获取完整交易日历数据
        trade_calendar_df = ak.tool_trade_date_hist_sina()

        if trade_calendar_df.empty:
            logger.warning("从AKShare获取交易日历数据为空")
            return ["获取交易日历数据为空"]

        # 筛选指定年份的数据
        year_trade_dates = []
        for _, row in trade_calendar_df.iterrows():
            trade_date = dt.strptime(str(row['trade_date']), '%Y-%m-%d').date()
            if trade_date.year == year:
                year_trade_dates.append(trade_date)

        if not year_trade_dates:
            logger.warning(f"{year}年没有交易日历数据")
            return [f"{year}年没有交易日历数据"]

        year_trade_dates.sort()
        logger.info(f"{year}年交易日数量: {len(year_trade_dates)}")

        # 删除该年份的旧数据
        from app.cruds.akshare_trade_calendar_crud import delete_trade_calendars_by_year
        deleted_count = delete_trade_calendars_by_year(session=db, year=year)
        logger.info(f'已删除{year}年旧交易日历数据，删除数量: {deleted_count}')

        # 确定特殊日期
        special_dates = determine_special_dates(year_trade_dates)

        # 构建交易日历记录
        trade_calendar_records = []
        for trade_date in year_trade_dates:
            fields = calculate_trade_calendar_fields(trade_date)

            trade_calendar = AkshareTradeCalendar(
                trade_date=trade_date,
                year=fields['year'],
                month=fields['month'],
                day=fields['day'],
                weekday=fields['weekday'],
                quarter=fields['quarter'],
                week_of_year=fields['week_of_year'],
                is_month_end=trade_date in special_dates['month_end'],
                is_quarter_end=trade_date in special_dates['quarter_end'],
                is_year_end=trade_date in special_dates['year_end']
            )
            trade_calendar_records.append(trade_calendar)

        # 批量插入数据
        inserted_count = batch_create_trade_calendars(
            session=db,
            trade_calendars=trade_calendar_records
        )

        logger.info(f"{year}年交易日历数据同步完成，插入记录数: {inserted_count}")
        return []

    except Exception as e:
        error_msg = str(e)
        logger.error(f"同步{year}年交易日历数据失败: {error_msg}")
        return [error_msg]


# 增加 main 函数便于单独调试
if __name__ == "__main__":
    from sqlmodel import create_engine, Session
    from app.core.config import settings

    # 创建MySQL数据库引擎
    engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)

    # 同步交易日历到数据库
    with Session(engine) as session:
        # 同步所有交易日历数据
        errors = sync_trade_calendar_to_db(session)
        if errors:
            logger.error(f"同步过程中发生错误: {errors}")
        else:
            logger.info("交易日历数据同步成功")

        # 或者同步指定年份的数据
        # errors = sync_trade_calendar_by_year(session, 2024)
        # if errors:
        #     logger.error(f"同步2024年数据时发生错误: {errors}")
        # else:
        #     logger.info("2024年交易日历数据同步成功")