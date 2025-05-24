
import os
import logging
import glob
from ..loggerUtil import init_logger

def test_logger_file_and_console_output(tmp_path, capsys):
    # 使用临时目录存放日志
    log_dir = tmp_path / "logs"
    logger = init_logger(name="testlog", log_dir=str(log_dir))

    # 记录一条日志
    logger.info("hello test log")
    logger.error("error test log")

    # 检查日志文件是否生成
    log_files = list(log_dir.glob("testlog_*.log"))
    assert len(log_files) == 1, "日志文件未生成"

    # 检查日志文件内容
    with open(log_files[0], "r", encoding="utf-8") as f:
        content = f.read()
        assert "hello test log" in content
        assert "error test log" in content

    # 检查控制台输出
    out, err = capsys.readouterr()
    assert "hello test log" in out or "hello test log" in err
    assert "error test log" in out or "error test log" in err

def test_logger_no_duplicate_handlers(tmp_path):
    log_dir = tmp_path / "logs"
    logger = init_logger(name="dup", log_dir=str(log_dir))
    handler_count = len(logger.handlers)
    # 再次初始化，不应重复添加handler
    logger = init_logger(name="dup", log_dir=str(log_dir))
    assert len(logger.handlers) == handler_count
