"""
此模块提供了一个初始化日志记录器的函数，用于将日志信息同时输出到文件和控制台。
"""
import logging
import os
from datetime import datetime

def init_logger(name="smartone_quant", log_dir="logs", debug=False):
    """
    初始化一个日志记录器，将日志信息同时输出到文件和控制台。

    参数:
    name (str): 日志记录器的名称，默认为 "quant_data"。
    log_dir (str): 日志文件存储的目录，默认为 "logs"。
    debug (bool): 是否启用DEBUG日志级别，默认为False（INFO级别）。

    返回:
    logging.Logger: 配置好的日志记录器实例。
    """
    # 创建日志目录，如果目录已存在则不会报错
    os.makedirs(log_dir, exist_ok=True)
    # 生成日志文件的完整路径，文件名包含当前日期
    log_path = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")

    # 获取指定名称的日志记录器
    logger = logging.getLogger(name)
    # 根据debug参数设置日志级别
    log_level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(log_level)

    # 检查日志记录器是否已经有处理器，避免重复添加处理器
    if not logger.handlers:
        # 创建一个文件处理器，将日志信息写入指定的日志文件
        fh = logging.FileHandler(log_path)
        # 设置文件处理器的日志级别
        fh.setLevel(log_level)

        # 创建一个流处理器，将日志信息输出到控制台
        ch = logging.StreamHandler()
        # 设置流处理器的日志级别
        ch.setLevel(log_level)

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

    return logger

