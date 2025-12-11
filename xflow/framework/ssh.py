# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
SSH 模块。
"""

import time
import socket

from typing import Generator, Optional, Dict, Union, Callable
from select import select
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from decorator import decorator
from paramiko import SSHClient, AutoAddPolicy, SFTPClient
from paramiko.ssh_exception import (AuthenticationException, 
                                    NoValidConnectionsError, 
                                    SSHException)

from xflow.framework.errors import SSHConnectError, CommandError
from xflow.framework.utils import remove_ansi_escape_chars, remove_unprintable_chars


@decorator
def autopen(func, *args, **kwargs):
    """
    自动连接器。
    """
    conn: SSHConnection = args[0]
    conn.open()
    return func(*args, **kwargs)


class CommandResult(str):
    """
    命令输出结果。
    """
    def __new__(cls, out: str, rc: int = 0, cmd: str = '') -> str:
        """
        :param out: 输出。
        :param rc: 返回码。
        :param cmd: 执行的命令。
        """
        out = remove_ansi_escape_chars(out)
        out = remove_unprintable_chars(out)
        out = '\n'.join(out.splitlines())
        o = str.__new__(cls, out.strip())
        o.__rc = rc
        o.__cmd = cmd
        return o

    @property
    def rc(self) -> int:
        """
        返回码。
        """
        return self.__rc

    @property
    def cmd(self) -> str:
        """
        执行的命令。
        """
        return self.__cmd

    def getfield(
        self,
        key: str,
        col: int,
        sep: str = None
    ) -> Optional[str]:
        """
        从输出中获取指定字段。

        :param key: 用来筛选行的关键字。
        :param col: 筛选行中字段所在的列号（从 1 开始）。
        :param sep: 用来分割行的符号。

        >>> r = CommandResult('''\\
        ... UID        PID   CMD
        ... postgres   45    /opt/pgsql/bin/postgres
        ... postgres   51    postgres: checkpointer process
        ... postgres   52    postgres: writer process
        ... postgres   53    postgres: wal writer process''', 0, '')
        >>> r.getfield('/opt/pgsql', 2)
        '45'
        >>> r.getfield('checkpointer', 1, sep=':')
        'postgres   51    postgres'
        """
        matchline = ''
        lines = self.splitlines()
        if isinstance(key, str):
            for line in self.splitlines():
                if key in line:
                    matchline = line
        elif isinstance(key, int):
            matchline = lines[key-1]
        if matchline:
            fields = matchline.split(sep)
            return fields[col-1].strip()

    def getcol(
        self,
        col: int,
        sep: str = None
    ) -> list:
        """
        从输出中获取指定列。

        :param col: 列号（从 1 开始）。
        :param sep: 用来分割行的符号。

        >>> r = CommandResult('''\\
        ... UID        PID   CMD
        ... postgres   45    /opt/pgsql/bin/postgres
        ... postgres   51    postgres: checkpointer process
        ... postgres   52    postgres: writer process
        ... postgres   53    postgres: wal writer process''', 0, '')
        >>> r.getcol(2)
        ['PID', '45', '51', '52', '53']
        """
        fields = []
        for line in self.splitlines():
            segs = line.split(sep)
            if col <= len(segs):
                fields.append(segs[col-1])
        return fields


class SSHConnection(object):
    """
    SSH 连接。
    """
    def __init__(
        self, 
        ip: str,
        user: str,
        password: str,
        port: int = 22,
        envs: Optional[Dict[str, str]] = None
    ):
        """
        :param ip: IP 地址。
        :param user: 用户名。
        :param password: 用户密码。
        :param port: SSH 端口。
        :param envs: 连接的默认环境变量，其中：
            `LANG` 默认值为 `en_US.UTF-8`。
            `LANGUAGE` 默认值为 `en_US.UTF-8`。
        """
        self._ip = ip
        self._user = user
        self._password = password
        self._port = port
        self._envs = envs or {}
        self._connstr = f'ssh://{user}@{ip}:{port}'
        self._cwd: str = ''
        self._sshclient = SSHClient()
        self._sshclient.set_missing_host_key_policy(AutoAddPolicy())
        self._sftpclient: SFTPClient = None
        for k, v in {'LANG': 'en_US.UTF-8',
                     'LANGUAGE': 'en_US.UTF-8'}.items():
            self._envs.setdefault(k, v)

    def _progress_bar_generator(
        self,
        method: str,
        local: Union[str, Path],
        remote: Union[str, PurePosixPath],
        interval: int = 1
    ) -> Callable:
        """
        文件传输进度展示函数生成器。

        :param method: 传输方法，`get` 或 `put`。
        :param local: 本地文件路径。
        :param remote: 远端文件路径。
        :param interval: 展示间隔（秒）。
        :return: 进度展示函数。
        """
        local = Path(local)
        remote = PurePosixPath(remote)
        premsg = {
            'get': f'Get {local} <= {remote}',
            'put': f'Put {local} => {remote}'
        }[method]
        anchor = {'start': int(time.time()), 'last': None}
        def progress_bar(transferred: int, total: int):
            def getsize(b: int) -> str:
                kb = b // 1024
                mb = round(kb / 1024, 1)
                gb = round(mb / 1024, 2)
                if gb >= 1:
                    return f'{gb}GB'
                elif mb >= 1:
                    return f'{mb}MB'
                elif kb >= 1:
                    return f'{kb}KB'
                else:
                    return f'{b}B'
            totalsize = getsize(total)
            transsize = getsize(transferred)
            percent = int(transferred / total * 100)
            now = int(time.time())
            if (now - anchor['start']) % interval == 0 \
                    and now != anchor['last'] or percent == 100:
                print(f'[{self._connstr}] {premsg} {transsize}/{totalsize} {percent}%')
                anchor['last'] = now
        return progress_bar

    def open(self) -> None:
        """
        开启连接。
        """
        transport = self._sshclient.get_transport()
        if transport and transport.active:
            return
        timeout = 10
        try:
            print(f'[{self._connstr}] Connecting...')
            self._sshclient.connect(self._ip, 
                                    port=self._port, 
                                    username=self._user, 
                                    password=self._password, 
                                    timeout=10)
            self._sftpclient = self._sshclient.open_sftp()
            print(f'[{self._connstr}] Connected')
        except AuthenticationException:
            raise SSHConnectError(
                f'Authentication failed when SSH connect to {self._ip} with user `{self._user}`, '
                f'please check whether the username and password are correct.'
            ) from None
        except socket.timeout:
            raise SSHConnectError(
                f'Timed out when SSH connect to {self._ip}({timeout}s), '
                'please check whether the network is normal.'
            ) from None
        except NoValidConnectionsError:
            raise SSHConnectError(
                f'Could not connect to port {self._port} on {self._ip}, '
                'please check whether the port is opened.'
            ) from None
        except SSHException as e:
            msg = str(e)
            if 'Error reading SSH protocol banner' in msg:
                raise SSHConnectError(
                    f'Read SSH protocol banner failed when connect to port {self._port} '
                    f'on {self._ip}, please check whether the port is correct.'
                ) from None
            raise e from None

    def close(self) -> None:
        """
        关闭连接。
        """
        self._sshclient.close()
        self._sftpclient.close()

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
        stdin, stdout, stderr = self._sshclient.exec_command(
            f'cd {self._cwd} && {cmd}' if self._cwd else cmd, 
            get_pty=True, 
            environment=environment)
        output = ''
        encoding = environment['LANG'].split('.')[-1]
        while True:
            rlist, _, _ = select([stdout.channel], [], [], 0.1)
            if stdout.channel in rlist:
                data = stdout.channel.recv(1024).decode(encoding=encoding, errors='ignore')
                if data == '':
                    break
                output += data
                print(data, end='')
        rc = stdout.channel.recv_exit_status()
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
        从远端下载文件 `rfile` 到本地目录 `ldir`。
        
        :param rfile: 远端文件。
        :param ldir: 本地目录。

        >>> getfile('/tmp/myfile', '/home')  # /home/myfile
        >>> getfile('/tmp/myfile', 'D:\\')  # D:\\myfile
        """
        rfile = PurePosixPath(rfile)
        ldir = Path(ldir)
        lfile = ldir.joinpath(rfile.name)
        self._sftpclient.get(str(rfile), str(lfile), 
            callback=self._progress_bar_generator('get', lfile, rfile))

    @autopen()
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
        lfile = Path(lfile)
        rdir = PurePosixPath(rdir)
        rfile = rdir.joinpath(lfile.name)
        self._sftpclient.put(str(lfile), str(rfile),
            callback=self._progress_bar_generator('put', lfile, rfile))

    @autopen()
    def exists(self, path: Union[str, PurePosixPath]) -> bool:
        """
        检查远端路径是否存在。
        """
        try:
            self._sftpclient.stat(str(path))
            return True
        except FileNotFoundError:

            return False

