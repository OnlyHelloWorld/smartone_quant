import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


def init_logger(
    name="smartone_quant",
    log_dir=None,
    show_sql=False,
    sql_log_to_file=False,
    sql_log_to_console=False
):
    """
    初始化日志记录器，将主日志和SQL日志分别输出到不同文件和/或控制台。

    参数:
        name (str): 主日志记录器名称，默认 "smartone_quant"。
        log_dir (str): 日志文件目录，默认项目 app/logs。
        show_sql (bool): 是否开启 SQLAlchemy SQL 语句日志（无论是否输出）。
        sql_log_to_file (bool): 是否将 SQL 日志写入独立文件。
        sql_log_to_console (bool): 是否将 SQL 日志打印到控制台。

    返回:
        logging.Logger: 主日志记录器。
    """
    # 统一日志路径
    if log_dir is None:
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    today_str = datetime.now().strftime('%Y%m%d')
    log_path = os.path.join(log_dir, f"{name}_{today_str}.log")
    sql_log_path = os.path.join(log_dir, f"sql_{today_str}.log")

    # -----------------------------
    # 初始化主日志记录器
    # -----------------------------
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fh = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=10, encoding="utf-8")
        ch = logging.StreamHandler()

        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

    logger.propagate = False

    # -----------------------------
    # 初始化 SQLAlchemy 日志记录器
    # -----------------------------
    sa_logger = logging.getLogger('sqlalchemy.engine')

    if show_sql:
        sa_logger.setLevel(logging.INFO)

        # 添加 SQL 文件日志
        if sql_log_to_file:
            if not any(isinstance(h, RotatingFileHandler) and h.baseFilename == sql_log_path for h in sa_logger.handlers):
                sql_fh = RotatingFileHandler(sql_log_path, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
                sql_formatter = logging.Formatter('[%(asctime)s] [SQL] %(message)s')
                sql_fh.setFormatter(sql_formatter)
                sa_logger.addHandler(sql_fh)

        # 添加 SQL 控制台输出
        if sql_log_to_console:
            if not any(isinstance(h, logging.StreamHandler) for h in sa_logger.handlers):
                sql_ch = logging.StreamHandler()
                sql_formatter = logging.Formatter('[%(asctime)s] [SQL] %(message)s')
                sql_ch.setFormatter(sql_formatter)
                sa_logger.addHandler(sql_ch)

        sa_logger.propagate = False
    else:
        sa_logger.setLevel(logging.WARNING)
        sa_logger.propagate = False

    return logger