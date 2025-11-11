# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
节点。
"""

from typing import TYPE_CHECKING, Generator
import inspect

from xflow.framework.ssh import SSHConnection
from xflow.framework.sftp import SFTPConnection, SFTPFile
from xflow.framework.utils import copy_signature

if TYPE_CHECKING:
    from xflow.framework.pipeline import Pipeline


class Node(object):
    """
    节点类。
    """
    def __init__(
        self,
        name: str,
        ip: str,
        sshport: int,
        user: str,
        password: str,
        bwd: str,
        envs: dict[str, str] = {}
    ):
        """
        :param name: 名称。
        :param ip: IP 地址。
        :param sshport: SSH 端口。
        :param user: 用户名。
        :param password: 用户密码。
        :param bwd: 基础工作目录。
        :param envs: 环境变量。
        """
        self.__name = name
        self.__bwd = bwd
        self.__cwd: str = ''
        self.__ssh = SSHConnection(ip, user, password, port=sshport, envs=envs)
        self.__sftp = SFTPConnection(ip, user, password, port=sshport)

    @property
    def name(self) -> str:
        """
        节点名。
        """
        return self.__name
    
    @property
    def bwd(self) -> str:
        """
        基础工作目录。
        """
        return self.__bwd
    
    @property
    def cwd(self) -> str:
        """
        当前工作目录（与 pipeline 相关）。
        """
        # 如果已指定绝对路径，则直接返回，否则自动拼接。
        if self.__cwd.startswith('/'):
            return self.__cwd
        p = self.pipeline
        return f'{self.bwd.rstrip("/")}/{p.name}/{p.taskid}/{self.__cwd}'
    
    @property
    def pipeline(self) -> 'Pipeline':
        """
        当前 pipeline。
        """
        for frame_info in inspect.stack():
            frame = frame_info.frame
            for obj in frame.f_locals.values():
                if hasattr(obj, '__class__') and obj.__class__.__name__ == 'Pipeline':
                    return obj
    
    @copy_signature(SSHConnection.exec)
    def exec(self, *args, **kwargs):
        with self.__ssh.cd(self.cwd):
            return self.__ssh.exec(*args, **kwargs)

    def cd(self, path) -> Generator[None, str, None]:
        """
        切换工作目录。

        >>> with cd('/my/workdir'):     # doctest: +SKIP
        ...     d = exec('pwd')         # doctest: +SKIP
        ...                             # doctest: +SKIP
        >>> d                           # doctest: +SKIP
        '/my/workdir'                   # doctest: +SKIP
        """
        try:
            self.__cwd = path
            yield
        finally:
            self.__cwd = ''

    @copy_signature(SFTPConnection.getfile)
    def getfile(self, *args, **kwargs):
        return self.__sftp.getfile(*args, **kwargs)
    
    @copy_signature(SFTPConnection.putfile)
    def putfile(self, *args, **kwargs):
        return self.__sftp.putfile(*args, **kwargs)
    
    @copy_signature(SFTPConnection.getdir)
    def getdir(self, *args, **kwargs):
        return self.__sftp.getdir(*args, **kwargs)
    
    @copy_signature(SFTPConnection.putdir)
    def putdir(self, *args, **kwargs):
        return self.__sftp.putdir(*args, **kwargs)
    
    def openfile(
        self, 
        filepath: str, 
        mode: str = 'r'
    ) -> Generator[SFTPFile, str, None]:
        """
        打开远端文件

        :param filepath: 文件路径。
        :param mode: 打开模式。

        >>> with openfile('/my/file') as f:     # doctest: +SKIP
        ...     content = f.read()              # doctest: +SKIP
        """
        return self.__sftp.openfile(filepath=filepath, mode=mode)
