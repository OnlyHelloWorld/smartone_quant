import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

class LoggerFactory:
    @staticmethod
    def get_logger(
        name: Optional[str] =   None,
        log_dir: str = "logs",
        level: int = logging.INFO,
        to_console: bool = True
    ) -> logging.Logger:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        logger = logging.getLogger(name)
        logger.setLevel(level)
        formatter = logging.Formatter(
            fmt='%(asctime)s %(levelname)s [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        log_file = os.path.join(log_dir, f"{name or 'app'}.log")
        # 防止重复添加handler
        if not logger.handlers:
            file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            if to_console:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)
        return logger

