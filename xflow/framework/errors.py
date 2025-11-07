# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
异常类。
"""


class SSHConnectError(Exception):
    """
    SSH 连接错误。
    """
    pass


class SSHCommandError(Exception):
    """
    SSH 命令错误。
    """
    pass
