import pandas as pd
import os
from datetime import datetime

class TradingCalendar:
    """
    交易日历管理，加载 trading_days.csv，判断是否为交易日。
    """
    def __init__(self, calendar_path: str):
        # 将从 CSV 文件读取的日期转换为 datetime 对象列表
        self.trading_days = [datetime.strptime(d, '%Y-%m-%d') for d in pd.read_csv(calendar_path)['date'].tolist()]

    def is_trading_day(self, date: str) -> bool:
        try:
            # 将传入的日期字符串转换为 datetime 对象
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            return date_obj in self.trading_days
        except ValueError:
            # 处理日期格式错误的情况
            return False

    def get_trading_days(self, start: str, end: str) -> list:
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        return [d.strftime('%Y-%m-%d') for d in self.trading_days if start_date <= d <= end_date]