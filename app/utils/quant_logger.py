"""
此模块提供了一个初始化日志记录器的函数，用于将日志信息同时输出到文件和控制台。
"""
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

def init_logger(name="smartone_quant", log_dir=None, show_sql=False):
    """
    初始化一个日志记录器，将日志信息同时输出到文件和控制台。

    参数:
    name (str): 日志记录器的名称，默认为 "quant_data"。
    log_dir (str): 日志文件存储的目录，默认为 app/logs。
    show_sql (bool): 是否显示SQLAlchemy SQL语句日志，默认为False。

    返回:
    logging.Logger: 配置好的日志记录器实例。
    """
    # 固定日志目录为 app/logs
    if log_dir is None:
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    # 创建日志目录，如果目录已存在则不会报错
    os.makedirs(log_dir, exist_ok=True)
    # 生成日志文件的完整路径，文件名包含当前日期
    log_path = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")

    # 获取指定名称的日志记录器
    logger = logging.getLogger(name)
    # 设置日志记录器的日志级别为 INFO，即只记录 INFO 及以上级别的日志
    logger.setLevel(logging.INFO)

    # 检查日志记录器是否已经有处理器，避免重复添加处理器
    if not logger.handlers:
        # 创建一个RotatingFileHandler，单个日志文件最大10M，最多保留5个备份
        fh = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=10, encoding="utf-8")
        fh.setLevel(logging.INFO)

        # 创建一个流处理器，将日志信息输出到控制台
        ch = logging.StreamHandler()
        # 设置流处理器的日志级别为 INFO
        ch.setLevel(logging.INFO)

        # 定义日志信息的格式，包含时间、日志级别和日志消息
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        # 为文件处理器设置日志格式
        fh.setFormatter(formatter)
        # 为流处理器设置日志格式
        ch.setFormatter(formatter)

        # 将文件处理器添加到日志记录器中
        logger.addHandler(fh)
        # 将流处理器添加到日志记录器中
        logger.addHandler(ch)

    # 控制SQLAlchemy SQL语句日志显示
    sa_logger = logging.getLogger('sqlalchemy.engine')
    if show_sql:
        sa_logger.setLevel(logging.INFO)
    else:
        sa_logger.setLevel(logging.DEBUG)

    # 防止日志重复打印
    logger.propagate = False

    return logger