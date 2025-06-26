import os
import pandas as pd
from datetime import datetime, timedelta
from sqlmodel import Session, select
from app.models.qmt_stock_daily import QmtStockDailyOri
from app.models.qmt_stock_weekly import QmtStockWeeklyOri
from app.models.qmt_stock_monthly import QmtStockMonthlyOri
from app.models.akshare_trade_calendar import AkshareTradeCalendar
from cruds.qmt_sector_stock_crud import get_qmt_sector_stocks_by_sector_name
from utils.qmt_data_utils import clean_kline_data
from utils.quant_logger import init_logger
from app.core.config import settings
from sqlmodel import create_engine, Session

logger = init_logger()

def export_kline_to_csv(session: Session, stock_code: str, start_time: datetime, end_time: datetime, kline_type: str):
    """
    从数据库读取指定股票指定日期范围的数据，清洗后追加到csv文件，保证时间升序，无表头。
    对于缺失的交易日数据，插入带有特殊标记的记录。
    kline_type: 'daily'/'weekly'/'monthly'
    """
    model_map = {
        'daily': QmtStockDailyOri,
        'weekly': QmtStockWeeklyOri,
        'monthly': QmtStockMonthlyOri
    }
    dir_map = {
        'daily': '../stock_data/daily',
        'weekly': '../stock_data/weekly',
        'monthly': '../stock_data/monthly'
    }
    model = model_map[kline_type]
    out_dir = dir_map[kline_type]
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{stock_code}.csv")

    # 1. 查询数据库
    statement = select(model).where(
        model.stock_code == stock_code,
        model.time >= start_time,
        model.time <= end_time
    ).order_by(model.time)
    records = session.exec(statement).all()

    # 2. 转为DataFrame
    columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
    df_new = pd.DataFrame([r.model_dump() for r in records])[columns] if records else pd.DataFrame(columns=columns)

    # 获取交易日历数据
    calendar_stmt = select(AkshareTradeCalendar).where(
        AkshareTradeCalendar.trade_date >= start_time.date(),
        AkshareTradeCalendar.trade_date <= end_time.date()
    ).order_by(AkshareTradeCalendar.trade_date)
    trade_dates = [cal.trade_date for cal in session.exec(calendar_stmt)]

    # 根据K线类型处理日期
    if kline_type == 'daily':
        # 创建完整的交易日DataFrame
        df_calendar = pd.DataFrame({
            'time': [datetime.combine(date, datetime.min.time()) for date in trade_dates]
        })
    elif kline_type == 'weekly':
        # 将交易日按周分组，取每周最后一个交易日
        df_dates = pd.DataFrame({'trade_date': trade_dates})
        df_dates['year_week'] = df_dates['trade_date'].apply(lambda x: f"{x.year}-{x.isocalendar()[1]}")
        weekly_dates = df_dates.groupby('year_week')['trade_date'].last().tolist()
        df_calendar = pd.DataFrame({
            'time': [datetime.combine(date, datetime.min.time()) for date in weekly_dates]
        })
    elif kline_type == 'monthly':
        # 将交易日按月分组，取每月最后一个交易日
        df_dates = pd.DataFrame({'trade_date': trade_dates})
        df_dates['year_month'] = df_dates['trade_date'].apply(lambda x: f"{x.year}-{x.month}")
        monthly_dates = df_dates.groupby('year_month')['trade_date'].last().tolist()
        df_calendar = pd.DataFrame({
            'time': [datetime.combine(date, datetime.min.time()) for date in monthly_dates]
        })

    # 标记缺失数据
    df_new = df_new.copy()
    df_new['time'] = pd.to_datetime(df_new['time'])
    df_merged = pd.merge(df_calendar, df_new, on='time', how='left')
    df_merged['is_missing'] = df_merged['open'].isna()

    # 对缺失数据填充特殊值
    missing_fill = {
        'open': -1,
        'high': -1,
        'low': -1,
        'close': -1,
        'volume': 0,
        'amount': 0
    }
    df_merged = df_merged.fillna(missing_fill)
    df_new = df_merged[columns]

    # 3. 清洗
    df_new = clean_kline_data(df_new)

    # 4. 处理本地csv
    if os.path.exists(out_path):
        columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
        df_old = pd.read_csv(out_path, header=None, names=columns)
        df_old['time'] = pd.to_datetime(df_old['time'], errors='coerce')
        logger.info(f'读取已有的 {out_path}，包含 {len(df_old)} 条记录')
        # 分为三段：前段（小于start_time），新数据，后段（大于end_time）
        df_before = df_old[df_old['time'] < start_time]
        logger.info(f'前段数据 {len(df_before)} 条')
        df_after = df_old[df_old['time'] > end_time]
        logger.info(f'后端数据 {len(df_after)} 条')
        # 拼接，保证时间升序
        df_all = pd.concat([df_before, df_new, df_after], ignore_index=True)
        logger.info(f'合并后数据 {len(df_all)} 条')
        df_all = df_all.sort_values('time').reset_index(drop=True)
    else:
        df_all = df_new.sort_values('time').reset_index(drop=True)

    # 5. 只保留数据库字段顺序，无表头
    df_all = df_all[df_new.columns]
    df_all.to_csv(out_path, index=False, header=False)
    return len(df_new)

if __name__ == "__main__":
        # 记录开始时间
        start_time = datetime.now()
        logger.info(f"开始同步K线数据，当前时间：{start_time}")
        # 创建数据库引擎
        engine = create_engine(settings.SQLALCHEMY_MYSQL_DATABASE_URI, echo=True)
        # 取三年前的日期
        three_years_ago = (datetime.now() - timedelta(days=3 * 365))
        logger.info(f'三年前的日期为：{three_years_ago}')
        # 获取今天日期
        today = datetime.now()
        logger.info(f'今天的日期为：{today}')

        with Session(engine) as session:
            sector_stocks = get_qmt_sector_stocks_by_sector_name(
                session=session,
                sector_name="沪深A股"
            )
            total_count = len(sector_stocks)

            # 同步日K数据
            logger.info("开始同步日K数据...")
            current_count = 0
            for sector_stock in sector_stocks:
                current_count += 1
                logger.info(f"开始同步{sector_stock.stock_code}的日K数据到CSV, 当前进度：{current_count}/{total_count}")
                result = export_kline_to_csv(
                    session=session,
                    stock_code=sector_stock.stock_code,
                    start_time=three_years_ago,
                    end_time=today,
                    kline_type='daily'
                )
                logger.info(f"同步完成，共同步{result}条日K数据")

            # 同步周K数据
            logger.info("开始同步周K数据...")
            current_count = 0
            for sector_stock in sector_stocks:
                current_count += 1
                logger.info(f"开始同步{sector_stock.stock_code}的周K数据到CSV, 当前进度：{current_count}/{total_count}")
                result = export_kline_to_csv(
                    session=session,
                    stock_code=sector_stock.stock_code,
                    start_time=three_years_ago,
                    end_time=today,
                    kline_type='weekly'
                )
                logger.info(f"同步完成，共同步{result}条周K数据")

            # 同步月K数据
            logger.info("开始同步月K数据...")
            current_count = 0
            for sector_stock in sector_stocks:
                current_count += 1
                logger.info(f"开始同步{sector_stock.stock_code}的月K数据到CSV, 当前进度：{current_count}/{total_count}")
                result = export_kline_to_csv(
                    session=session,
                    stock_code=sector_stock.stock_code,
                    start_time=three_years_ago,
                    end_time=today,
                    kline_type='monthly'
                )
                logger.info(f"同步完成，共同步{result}条月K数据")

        # 记录结束时间
        end_time = datetime.now()
        logger.info(f"结束同步所有K线数据，当前时间：{end_time}")
        logger.info(f"总耗时：{end_time - start_time}")
