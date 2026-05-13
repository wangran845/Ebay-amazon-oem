import logging, logging.handlers, sys
from pathlib import Path

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.hasHandlers():          # 避免重复
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    # 关键点：先建目录
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    # 文件 Handler
    fh = logging.handlers.TimedRotatingFileHandler(
        log_dir / "run.log", when="midnight", backupCount=7,
        encoding="utf-8")
    fh.setFormatter(fmt)

    # 控制台 Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


import logging
import os
from logging.handlers import RotatingFileHandler
import datetime


class SafeRotatingFileHandler(RotatingFileHandler):
    """安全的日志轮转处理器，处理 Windows 文件占用问题"""

    def doRollover(self):
        """重写轮转方法，添加异常处理"""
        try:
            super().doRollover()
        except PermissionError:
            # 如果重命名失败，使用时间戳创建新文件
            if os.path.exists(self.baseFilename):
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                new_name = f"{self.baseFilename}.{timestamp}"
                try:
                    os.rename(self.baseFilename, new_name)
                except PermissionError:
                    # 实在无法轮转，就继续写入原文件
                    pass


def setup_logger(name="Amazonoem", log_dir="logs"):
    """配置日志"""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "run.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 使用安全轮转处理器
    file_handler = SafeRotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8',
        delay=True  # 延迟打开文件，避免句柄冲突
    )
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger