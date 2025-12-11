# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
容器模块。
"""

import tarfile
import io
import os

from typing import Generator, Optional, Union, Dict
from select import select
from socket import SocketIO
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from decorator import decorator
from docker.client import DockerClient
from docker.models.containers import Container
from docker.tls import TLSConfig

from xflow.framework.errors import CommandError
from xflow.framework.ssh import CommandResult


@decorator
def autopen(func, *args, **kwargs):
    """
    自动连接器。
    """
    conn: ContainerConnection = args[0]
    conn.open()
    return func(*args, **kwargs)


class ContainerConnection(object):
    """
    容器连接。
    """
    def __init__(
        self, 
        ip: str,
        port: int = 2375,
        user: Optional[str] = None,
        name: Optional[str] = None,
        image: Optional[str] = None,
        runargs: Optional[Dict] = None,
        cacert: Optional[str] = None,
        clientcert: Optional[str] = None,
        clientkey: Optional[str] = None,
        envs: Optional[Dict[str, str]] = None
    ):
        """
        :param ip: IP 地址。
        :param port: Docker 服务端口。
        :param user: 用户名。
        :param name: 容器名，和 `image` 为互斥参数，必须且只能指定其中一个。
        :param image: 镜像名，和 `name` 为互斥参数，必须且只能指定其中一个，
                      如果该参数指定，则每次连接用该镜像创建一个新容器。
        :param runargs: 使用镜像启动容器时的参数。
        :param cacert: CA 证书。
        :param clientcert: 客户端证书，当指定 `clientkey` 参数时必须同时指定该参数。
        :param clientkey: 客户端私钥，当指定 `clientcert` 参数时必须同时指定该参数。
        :param envs: 连接的默认环境变量，其中：
            `LANG` 默认值为 `en_US.UTF-8`。
            `LANGUAGE` 默认值为 `en_US.UTF-8`。
        """
        # 必须且只能指定 `name` 和 `image` 参数中的一个。
        if (name == None and image == None) or \
                (name != None and image != None):
            raise ValueError('Must specify only one of `name` and `image`.')
        # `clientcert` 指定的同时必须指定 `clientkey`，反之亦然。
        if (clientcert != None and clientkey == None) or \
                (clientcert == None and clientkey != None):
            raise ValueError('`clientcert` must be specified along with `clientkey`, and vice versa.')
        self._ip = ip
        self._port = port
        self._user = user
        self._envs = envs or {}
        self._name = name
        self._image = image
        self._runargs = runargs or {}
        self._cacert = cacert
        self._clientcert = clientcert
        self._clientkey = clientkey
        self._dockerclient: DockerClient = None
        self._container: Container = None
        self._cwd: str = ''
        for k, v in {'LANG': 'en_US.UTF-8',
                     'LANGUAGE': 'en_US.UTF-8'}.items():
            self._envs.setdefault(k, v)

    @property
    def _connstr(self) -> str:
        """
        连接字符串。
        """
        if self._user:
            prefix = f'docker://{self._user}@{self._ip}:{self._port}'
        else:
            prefix = f'docker://{self._ip}:{self._port}'
        if self._name:
            return f'{prefix}:{self._name}'
        else:  # 从镜像自动创建容器
            if self._container:
                # 容器已创建
                return f'{prefix}:{self._image}->{self._container.name}'
            else:
                # 容器未创建
                return f'{prefix}:{self._image}->...'

    def open(self) -> None:
        """
        开启连接。
        """
        if self._container:
            return
        print(f'[{self._connstr}] Connecting...')
        if self._cacert or self._clientcert:
            tls = TLSConfig(
                ca_cert=self._cacert,
                verify=True if self._cacert else False,
                client_cert=(self._clientcert, self._clientkey) if self._clientcert else None
            )
        else:
            tls = False
        self._dockerclient = DockerClient(base_url=f'tcp://{self._ip}:{self._port}', tls=tls)
        if self._name:
            self._container = self._dockerclient.containers.get(self._name)
        else:
            self._container = self._dockerclient.containers.run(self._image, detach=True, **self._runargs)
        print(f'[{self._connstr}] Connected')

    def close(self) -> None:
        """
        关闭连接。
        """
        self._dockerclient.close()

    def remove(self, force: bool = False) -> None:
        """
        删除本容器。

        :param force: 是否强制删除正在运行的容器。
        """
        self._container.remove(force=force)
        print(f'[{self._connstr}] Removed container {self._container.name}')

    @property
    def existed(self) -> bool:
        """
        是否一个已存在的容器（即不是从镜像临时创建的）。
        """
        return not self._image

    @autopen()
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
        envs = envs or {}
        environment = self._envs.copy()
        environment.update(envs)
        print(f'[{self._connstr}:{self._cwd or "~"}] {cmd}')
        cmdid = self._dockerclient.api.exec_create(
            self._container.id,
            cmd,
            stdout=True,
            stderr=True,
            stdin=True,
            tty=True,
            environment=environment,
            user=self._user,
            workdir=self._cwd or None
        )['Id']
        sock: SocketIO = self._dockerclient.api.exec_start(cmdid, tty=True, socket=True)
        encoding = environment['LANG'].split('.')[-1]
        output = ''
        while True:
            rlist, _, _ = select([sock], [], [], 0.1)
            if sock in rlist:
                data = sock.read(1024).decode(encoding=encoding, errors='ignore')
                if data == '':
                    break
                output += data
                print(data, end='')
        rc = self._dockerclient.api.exec_inspect(cmdid)['ExitCode']
        if rc != 0:
            raise CommandError(f'ExitCode {rc}: `{cmd}`')
        return CommandResult(output, rc=rc, cmd=cmd)

    @contextmanager
    def cd(self, path: Union[str, PurePosixPath]) -> Generator[None, str, None]:
        """
        切换工作目录。

        >>> with cd('/my/workdir'):     # doctest: +SKIP
        ...     d = exec('pwd')         # doctest: +SKIP
        ...                             # doctest: +SKIP
        >>> d                           # doctest: +SKIP
        '/my/workdir'                   # doctest: +SKIP
        """
        try:
            self._cwd = str(path)
            yield
        finally:
            self._cwd = ''

    @autopen()
    def getfile(
        self, 
        rfile: Union[str, PurePosixPath], 
        ldir: Union[str, Path]
    ) -> None:
        """
        从容器下载文件 `rfile` 到本地目录 `ldir`。
        
        :param rfile: 容器内文件。
        :param ldir: 本地目录。

        >>> getfile('/tmp/myfile', '/home')  # /home/myfile
        >>> getfile('/tmp/myfile', 'D:\\')  # D:\\myfile
        """
        rfile = PurePosixPath(rfile)
        ldir = Path(ldir)
        lfile = ldir.joinpath(rfile.name)
        ltarfile = ldir.joinpath(f'{rfile.name}.tar')
        print(f'[{self._connstr}] Get {lfile} <= {rfile}')
        stream, _ = self._container.get_archive(str(rfile))
        with open(ltarfile, 'wb') as f:
            for chunk in stream:
                f.write(chunk)
        with tarfile.open(ltarfile) as tar:
            tar.extractall(ldir)
        os.remove(str(ltarfile))

    @autopen()
    def putfile(
        self, 
        lfile: Union[str, Path], 
        rdir: Union[str, PurePosixPath]
    ) -> None:
        """
        上传本地文件 `lfile` 到容器目录 `rdir`。

        :param lfile: 本地文件。
        :param rdir: 容器内目录。

        >>> putfile('/home/myfile', '/tmp')  # /tmp/myfile
        >>> putfile('D:\\myfile', '/tmp')  # /tmp/myfile
        """
        lfile = Path(lfile)
        rdir = PurePosixPath(rdir)
        rfile = rdir.joinpath(lfile.name)
        print(f'[{self._connstr}] Put {lfile} => {rfile}')
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode='w') as tar:
            tar.add(lfile)
        stream.seek(0)
        self._container.put_archive(str(rdir), stream)
