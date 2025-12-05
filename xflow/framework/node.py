# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
节点。
"""
import inspect
from pathlib import PurePosixPath, Path

from typing import Generator, Dict, Optional, Union
from contextlib import contextmanager

from xflow.framework.ssh import SSHConnection
from xflow.framework.container import ContainerConnection
from xflow.framework.utils import CommandResult
from xflow.framework.pipeline import Pipeline


class Node(object):
    """
    节点基类。
    """
    def __init__(
        self,
        name: str,
        user: str,
        bwd: str,
        conn: Union[SSHConnection, ContainerConnection],
        envs: Optional[Dict[str, str]] = None
    ):
        """
        :param name: 名称。
        :param user: 用户名。
        :param bwd: 基础工作目录。
        :param envs: 环境变量。
        """
        self.__name = name
        self.__user = user
        self.__envs = envs
        self.__bwd: PurePosixPath = PurePosixPath(bwd)
        self.__cwd: PurePosixPath = PurePosixPath('')
        self.__conn = conn

    @property
    def name(self) -> str:
        """
        节点名。
        """
        return self.__name
    
    @property
    def bwd(self) -> PurePosixPath:
        """
        基础工作目录。
        """
        return self.__bwd
    
    @property
    def cwd(self) -> PurePosixPath:
        """
        当前工作目录（与 pipeline 相关）。
        """
        # 如果已指定绝对路径，则直接返回，否则自动拼接。
        if self.__cwd.is_absolute():
            return self.__cwd
        p = self.pipeline
        return self.bwd.joinpath(p.name, f'{p.taskid}', self.__cwd)
    
    @property
    def pipeline(self) -> 'Pipeline':
        """
        当前 pipeline。
        """
        for frame_info in inspect.stack():
            frame = frame_info.frame
            for obj in frame.f_locals.values():
                if hasattr(obj, '__class__') and issubclass(obj.__class__, Pipeline):
                    return obj
                
    @property
    def is_native(self) -> bool:
        """
        是否原生节点。
        """
        return self.__class__.__name__ == 'NativeNode'
    
    @property
    def is_container(self) -> bool:
        """
        是否容器节点。
        """
        return self.__class__.__name__ == 'ContainerNode'

    @property
    def existed(self) -> bool:
        """
        是否一个已存在的容器（即不是从镜像临时创建的）。
        """
        return self.__conn.existed

    def remove(self, force: bool = False) -> None:
        """
        删除本容器。

        :param force: 是否强制删除正在运行的容器。
        """
        self.__conn.remove(force=force)
                
    def mkcwd(self) -> None:
        """
        创建当前工作目录。
        """
        self.__conn.exec(f'mkdir -p {self.cwd}')

    def rmcwd(self) -> None:
        """
        删除当前工作目录。
        """
        self.__conn.exec(f'rm -rf {self.cwd}')
    
    @contextmanager
    def cd(self, path: str) -> Generator[None, str, None]:
        """
        切换工作目录。

        >>> with cd('/my/workdir'):     # doctest: +SKIP
        ...     d = exec('pwd')         # doctest: +SKIP
        ...                             # doctest: +SKIP
        >>> d                           # doctest: +SKIP
        '/my/workdir'                   # doctest: +SKIP
        """
        try:
            self.__cwd = PurePosixPath(path)
            yield
        finally:
            self.__cwd = PurePosixPath('')
            
    def exec(
        self,
        cmd: str,
        envs: Optional[Dict[str, str]] = None
    ) -> CommandResult:
        """
        执行命令。

        :param cmd: 被执行的命令。
        :param envs: 环境变量。
        :return: 命令输出。

        :raises: 
            `CommandError` -- 命令返回码不为 0。

        >>> exec('ls /home')  # successful                              # doctest: +SKIP
        >>> exec('ls /errpath')  # CommandError                         # doctest: +SKIP
        """
        with self.__conn.cd(self.cwd):
            return self.__conn.exec(cmd, envs=envs)
        
    def getfile(
        self, 
        rfile: Union[str, PurePosixPath], 
        ldir: Union[str, Path]
    ) -> None:
        """
        从远端下载文件 `rfile` 到本地目录 `ldir`。
        
        :param rfile: 远端文件。
        :param ldir: 本地目录。

        >>> getfile('/tmp/myfile', '/home')  # /home/myfile
        >>> getfile('/tmp/myfile', 'D:\\')  # D:\\myfile
        """
        self.__conn.getfile(rfile, ldir)
    
    def putfile(
        self, 
        lfile: Union[str, Path], 
        rdir: Union[str, PurePosixPath]
    ) -> None:
        """
        上传本地文件 `lfile` 到远端目录 `rdir`。

        :param lfile: 本地文件。
        :param rdir: 远端目录。

        >>> putfile('/home/myfile', '/tmp')  # /tmp/myfile
        >>> putfile('D:\\myfile', '/tmp')  # /tmp/myfile
        """
        return self.__conn.putfile(lfile, rdir)
        

class NativeNode(Node):
    """
    原生节点（非容器）。
    """
    def __init__(
        self,
        name: str,
        ip: str,
        sshport: int,
        user: str,
        password: str,
        bwd: str,
        envs: Optional[Dict[str, str]] = None
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
        conn = SSHConnection(ip, user, password, port=sshport, envs=envs)
        super().__init__(name, user, bwd, conn, envs=envs)


class ContainerNode(Node):
    """
    容器节点。
    """
    def __init__(
        self,
        name: str,
        ip: str,
        port: int,
        bwd: str,
        user: Optional[str] = None,
        container: Optional[str] = None,
        image: Optional[str] = None,
        runargs: Optional[Dict] = None,
        cacert: Optional[str] = None,
        clientcert: Optional[str] = None,
        clientkey: Optional[str] = None,
        envs: Optional[Dict[str, str]] = None
    ):
        """
        :param name: 名称。
        :param ip: IP 地址。
        :param port: Docker 服务端口。
        :param user: 用户名。
        :param bwd: 基础工作目录。
        :param container: 容器名，和 `image` 为互斥参数，必须且只能指定其中一个。
        :param image: 镜像名，和 `container` 为互斥参数，必须且只能指定其中一个，
                      如果该参数指定，则每次连接用该镜像创建一个新容器。
        :param runargs: 使用镜像启动容器时的参数。
        :param cacert: CA 证书。
        :param clientcert: 客户端证书，当指定 `clientkey` 参数时必须同时指定该参数。
        :param clientkey: 客户端私钥，当指定 `clientcert` 参数时必须同时指定该参数。
        :param envs: 环境变量。
        """
        conn = ContainerConnection(ip, 
                                   port=port, 
                                   user=user, 
                                   name=container, 
                                   image=image, 
                                   runargs=runargs,
                                   cacert=cacert, 
                                   clientcert=clientcert, 
                                   clientkey=clientkey, 
                                   envs=envs)
        super().__init__(name, user, bwd, conn, envs=envs)
    