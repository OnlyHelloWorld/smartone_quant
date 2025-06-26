import logging
import os
from datetime import datetime
from concurrent_log_handler import ConcurrentRotatingFileHandler  # 替换标准库
from logging.handlers import RotatingFileHandler


def init_logger(
    name="smartone_quant",
    log_dir=None,
    show_sql=False,
    sql_log_to_file=False,
    sql_log_to_console=False
):
    """
    初始化日志记录器，使用线程安全的日志文件轮转。
    """
    # 设置日志目录
    if log_dir is None:
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    today_str = datetime.now().strftime('%Y%m%d')
    log_path = os.path.join(log_dir, f"{name}_{today_str}.log")
    sql_log_path = os.path.join(log_dir, f"sql_{today_str}.log")

    # 初始化主日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 防止重复添加 handler
    if logger.handlers:
        return logger

    # 使用线程安全的 handler
    fh = ConcurrentRotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=10, encoding="utf-8")
    ch = logging.StreamHandler()

    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False  # 避免重复输出

    # 初始化 SQLAlchemy 日志记录器（可选）
    sa_logger = logging.getLogger('sqlalchemy.engine')

    if show_sql:
        sa_logger.setLevel(logging.INFO)

        if sql_log_to_file and not any(isinstance(h, RotatingFileHandler) and h.baseFilename == sql_log_path for h in sa_logger.handlers):
            sql_fh = ConcurrentRotatingFileHandler(sql_log_path, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
            sql_formatter = logging.Formatter('[%(asctime)s] [SQL] %(message)s')
            sql_fh.setFormatter(sql_formatter)
            sa_logger.addHandler(sql_fh)

        if sql_log_to_console and not any(isinstance(h, logging.StreamHandler) for h in sa_logger.handlers):
            sql_ch = logging.StreamHandler()
            sql_formatter = logging.Formatter('[%(asctime)s] [SQL] %(message)s')
            sql_ch.setFormatter(sql_formatter)
            sa_logger.addHandler(sql_ch)

        sa_logger.propagate = False
    else:
        sa_logger.setLevel(logging.WARNING)
        sa_logger.propagate = False

    return logger
