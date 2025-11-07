# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
日志处理模块。
"""

import logging
import sys


class XLogger(logging.Logger):
    """
    自定义 Logger。
    """
    pass


logging.setLoggerClass(XLogger)


class StdoutFilter(logging.Filter):
    def filter(self, rec):
        return rec.levelno in (logging.DEBUG, 
                               logging.INFO, 
                               logging.WARN)


ROOT_LOGGER = logging.getLogger('xflow')
ROOT_LOGGER.setLevel('DEBUG')
FORMATTER = logging.Formatter(
    '[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s'
)

def enable_console_logging() -> None:
    """
    启用控制台日志。
    """
    if 'console_logging_enabled' in globals():
        return
    stdout = logging.StreamHandler(sys.stdout)
    stdout.addFilter(StdoutFilter())
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setLevel('ERROR')
    stdout.setFormatter(FORMATTER)
    stderr.setFormatter(FORMATTER)
    ROOT_LOGGER.addHandler(stdout)
    ROOT_LOGGER.addHandler(stderr)
    global console_logging_enabled
    console_logging_enabled = True


enable_console_logging()


def getlogger(name) -> XLogger:
    """
    获取子 Logger。
    """
    return ROOT_LOGGER.getChild(name)
