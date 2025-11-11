# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
SSH 模块。
"""

import socket

from typing import Generator, Optional
from select import select
from contextlib import contextmanager

from decorator import decorator
from paramiko import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import (AuthenticationException, 
                                    NoValidConnectionsError, 
                                    SSHException)

from xflow.framework.errors import SSHConnectError, SSHCommandError
from xflow.framework.utils import (remove_ansi_escape_chars, 
                                   remove_unprintable_chars)


@decorator
def open(func, *args, **kwargs):
    """
    自动连接器。
    """
    conn: SSHConnection = args[0]
    conn.open()
    return func(conn, *args, **kwargs)


class SSHCommandResult(str):
    """
    SSH 命令输出结果。
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

        >>> r = SSHCommandResult('''\\
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

        >>> r = SSHCommandResult('''\\
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
        envs: Optional[dict] = None
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
        for k, v in {'LANG': 'en_US.UTF-8',
                     'LANGUAGE': 'en_US.UTF-8'}.items():
            self._envs[k] = self._envs.get(k, v)

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

    @open()
    def exec(
        self,
        cmd: str,
        prompts: dict = {},
        envs: dict = {}
    ) -> SSHCommandResult:
        """
        执行命令。

        :param cmd: 被执行的命令。
        :param prompts: 用于交互式命令的提示符和输入。
        :param envs: 环境变量。
        :return: 命令输出。

        :raises: 
            `.SSHCommandError` -- 命令返回码不为 0。

        >>> exec('cd /home')  # successful                                                                      # doctest: +SKIP
        >>> exec('cd /errpath')  # SSHCommandError                                                              # doctest: +SKIP
        >>> exec('passwd xflow', prompts={'New password:': 'xflow@123', 'Retype new password:': 'xflow@123'})   # doctest: +SKIP
        """
        if self._cwd:
            cmd = f'cd {self._cwd} && {cmd}'
        environment = self._envs.copy()
        environment.update(envs)
        print(f'[{self._connstr}] {cmd}')
        stdin, stdout, _ = self._sshclient.exec_command(
            cmd, get_pty=True, environment=environment)
        output = ''
        encoding = envs['LANG'].split('.')[-1]
        while True:
            rlist, _, _ = select([stdout.channel], [], [], 0.1)
            if stdout.channel in rlist:
                data = stdout.channel.recv(1024).decode(encoding=encoding, errors='ignore')
                if data == '':
                    break
                output += data
                print(data, end='')
            if prompts and output:
                lastline = output.splitlines()[-1]
                written = None
                for k, v in prompts.items():
                    if k in lastline:
                        stdin.write(v + '\n')
                        stdin.flush()
                        written = k
                        break
                if written:
                    prompts.pop(written)
        rc = stdout.channel.recv_exit_status()
        if rc != 0:
            raise SSHCommandError(f'Exit code of `{cmd}` = {rc}.')
        return SSHCommandResult(output, rc=rc, cmd=cmd)

    def sudo(self, cmd, *args, **kwargs) -> SSHCommandResult:
        """
        sudo 执行命令。
        """
        kwargs['prompts'] = {'[sudo] password': self._password}
        return self.exec(f'sudo {cmd}', *args, **kwargs)

    @contextmanager
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
            self._cwd = path
            yield
        finally:
            self._cwd = ''
