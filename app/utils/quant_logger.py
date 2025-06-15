import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

class LoggerFactory:
    # 默认日志目录为项目根目录下的 logs 文件夹
    DEFAULT_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "logs")

    @staticmethod
    def get_logger(
        name: Optional[str] = None,
        level: int = logging.INFO,
        to_console: bool = True
    ) -> logging.Logger:
        """
        获取日志记录器

        Args:
            name: 日志记录器名称，也用作日志文件名
            level: 日志级别
            to_console: 是否同时输出到控制台

        Returns:
            logging.Logger: 配置好的日志记录器
        """
        # 确保日志目录存在
        if not os.path.exists(LoggerFactory.DEFAULT_LOG_DIR):
            os.makedirs(LoggerFactory.DEFAULT_LOG_DIR)

        logger = logging.getLogger(name)
        logger.setLevel(level)

        # 如果已经有handlers，说明logger已经配置过，直接返回
        if logger.handlers:
            return logger

        # 创建格式化器
        formatter = logging.Formatter(
            fmt='%(asctime)s %(levelname)s [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 添加文件处理器
        log_file = os.path.join(LoggerFactory.DEFAULT_LOG_DIR, f"{name or 'app'}.log")
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

        # 如果需要同时输出到控制台
        if to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.setLevel(level)
            logger.addHandler(console_handler)

        return logger
