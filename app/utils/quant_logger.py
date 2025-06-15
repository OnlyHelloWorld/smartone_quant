import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

class LoggerFactory:
    # 默认日志目录为项目根目录下的 logs 文件夹
    DEFAULT_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "logs")
    # 所有日志统一输出到这个文件
    DEFAULT_LOG_FILE = os.path.join(DEFAULT_LOG_DIR, "smartone_quant.log")
    # 确保日志目录存在
    if not os.path.exists(DEFAULT_LOG_DIR):
        os.makedirs(DEFAULT_LOG_DIR)

    # 创建一个统一的文件处理器
    _file_handler = None
    _formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    @classmethod
    def _get_file_handler(cls, level: int) -> RotatingFileHandler:
        """获取统一的文件处理器"""
        if cls._file_handler is None:
            cls._file_handler = RotatingFileHandler(
                cls.DEFAULT_LOG_FILE,
                maxBytes=3*1024*1024,  # 3MB
                backupCount=5,
                encoding='utf-8'
            )
            cls._file_handler.setFormatter(cls._formatter)
            cls._file_handler.setLevel(level)
        return cls._file_handler

    @classmethod
    def get_logger(
        cls,
        name: Optional[str] = None,
        level: int = logging.INFO,
        to_console: bool = True
    ) -> logging.Logger:
        """
        获取日志记录器

        Args:
            name: 日志记录器名称
            level: 日志级别
            to_console: 是否同时输出到控制台

        Returns:
            logging.Logger: 配置好的日志记录器
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # 如果已经有handlers，说明logger已经配置过，直接返回
        if logger.handlers:
            return logger

        # 添加文件处理器（所有logger共用同一个file_handler）
        logger.addHandler(cls._get_file_handler(level))

        # 如果需要同时输出到控制台
        if to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(cls._formatter)
            console_handler.setLevel(level)
            logger.addHandler(console_handler)

        return logger
