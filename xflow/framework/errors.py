# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
异常类。
"""


class SSHConnectError(Exception):
    """
    SSH 连接错误。
    """
    pass


class CommandError(Exception):
    """
    命令执行错误。
    """
    pass


class NoSuchNodeError(Exception):
    """
    无此节点。
    """
    pass


class NoSuchDockerError(Exception):
    """
    无此 Docker。
    """
    pass