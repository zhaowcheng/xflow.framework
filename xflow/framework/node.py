# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
节点。
"""

from typing import Sequence, Generator

from xflow.framework.ssh import SSHConnection
from xflow.framework.sftp import SFTPConnection, SFTPFile
from xflow.framework.utils import copy_signature

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
        workdir: str,
        labels: Sequence[str] = [],
        envs: dict[str, str] = {}
    ):
        """
        :param name: 名称。
        :param ip: IP 地址。
        :param sshport: SSH 端口。
        :param user: 用户名。
        :param password: 用户密码。
        :param workdir: 工作目录。
        :param labels: 标签。
        :param envs: 环境变量。
        """
        self.__name = name
        self.__labels = list(labels)
        self.__workdir = workdir
        self.__ssh = SSHConnection(ip, user, password, port=sshport, envs=envs, 
                                   initcmd=f'grep -q "cd {self.__workdir}" ~/.profile || ' +
                                           f'echo "cd {self.__workdir}" >> ~/.profile')
        self.__sftp = SFTPConnection(ip, user, password, port=sshport)

    @property
    def name(self) -> str:
        """
        节点名。
        """
        return self.__name
    
    @property
    def labels(self) -> tuple[str]:
        """
        节点标签。
        """
        return tuple(self.__labels)

    @copy_signature(SSHConnection.exec)
    def exec(self, *args, **kwargs):
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
        return self.__ssh.cd(path)

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
