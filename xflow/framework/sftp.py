# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
SFTP 模块
"""

import os
import stat
import time

from typing import Generator, Callable
from contextlib import contextmanager
from threading import Lock

from decorator import decorator
from paramiko import Transport, SFTPClient, SFTPFile

from xflow.framework.logger import getlogger


logger = getlogger(__name__)


@decorator
def open(func, *args, **kwargs):
    """
    自动连接器。
    """
    conn: SFTPConnection = args[0]
    conn.openfile()
    return func(conn, *args, **kwargs)


class SFTPConnection(object):
    """
    SFTP 连接.
    """
    def __init__(
        self,
        ip: str,
        user: str,
        password: str,
        port: int = 22
    ):
        """
        :param ip: IP 地址。
        :param user: 用户名。
        :param password: 用户密码。
        :param port: SSH 端口。
        """
        self._ip = ip
        self._user = user
        self._password = password
        self._port = port
        self._sftpclient = None
        self._openlock = Lock()

    def _progress_bar_generator(
        self,
        method: str,
        local: str,
        remote: str,
        interval: int = 3
    ) -> Callable:
        """
        文件传输进度展示函数生成器。

        :param method: 传输方法，`get` 或 `put`。
        :param local: 本地文件路径。
        :param remote: 远端文件路径。
        :param interval: 展示间隔（秒）。
        :return: 进度展示函数。
        """
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
                logger.info(f'{premsg} {transsize}/{totalsize} {percent}')
                anchor['last'] = now
        return progress_bar

    def open(self) -> None:
        """
        开启连接。
        """
        self._openlock.acquire()
        try:
            if self._sftpclient and self._sftpclient.sock.active:
                return
            logger.info('Connecting...')
            t = Transport((self._ip, self._port))
            t.connect(username=self._user, password=self._password)
            self._sftpclient = SFTPClient.from_transport(t)
        finally:
            self._openlock.release()

    def close(self) -> None:
        """
        关闭连接。
        """
        self._sftpclient.close()

    @open()
    def getfile(self, rfile: str, ldir: str, filename: str = None) -> None:
        """
        从远端下载文件 `rfile` 到本地目录 `ldir`。
        
        :param rfile: 远端文件。
        :param ldir: 本地目录。
        :param filename: 下载后的文件名，不指定则保留原名。

        >>> getfile('/tmp/myfile', '/home')  # /home/myfile
        >>> getfile('/tmp/myfile', 'D:\\')  # D:\\myfile
        >>> getfile('/tmp/myfile', '/home', 'newfile')  # /home/newfile
        """
        ldir = os.path.join(ldir, '')
        filename = filename or self.basename(rfile)
        lfile = os.path.join(ldir, filename)
        self._sftpclient.get(rfile, lfile, 
            callback=self._progress_bar_generator('get', lfile, rfile))

    @open()
    def putfile(self, lfile: str, rdir: str, filename: str = None) -> None:
        """
        上传本地文件 `lfile` 到远端目录 `rdir`。

        :param lfile: 本地文件。
        :param rdir: 远端目录。
        :param filename: 上传后的文件名，不指定则保留原名。

        >>> putfile('/home/myfile', '/tmp')  # /tmp/myfile
        >>> putfile('D:\\myfile', '/tmp')  # /tmp/myfile
        >>> putfile('/home/myfile', '/tmp', 'newfile')  # /tmp/newfile
        """
        rdir = self.join(rdir, '')
        filename = filename or os.path.basename(lfile)
        rfile = self.join(rdir, filename)
        self._sftpclient.put(lfile, rfile,
            callback=self._progress_bar_generator('put', lfile, rfile))
    
    @open()
    def getdir(self, rdir: str, ldir: str) -> None:
        """
        下载远端目录 `rdir` 到本地目录 `ldir`。
        
        :param rdir: 远端目录。
        :param ldir: 本地目录。

        >>> getdir('/tmp/mydir', '/home')  # /home/mydir
        >>> getdir('/tmp/mydir', 'D:\\')  # D:\\mydir
        """
        rdir = self.normpath(rdir)
        ldir = os.path.join(ldir, '')
        for top, dirs, files in self.walk(rdir):
            basename = self.basename(top)
            ldir = os.path.join(ldir, basename)
            if not os.path.exists(ldir):
                os.makedirs(ldir)
            for f in files:
                r = self.join(top, f)
                l = os.path.join(ldir, f)
                self._sftpclient.get(r, l,
                    callback=self._progress_bar_generator('get', l, r))
            for d in dirs:
                l = os.path.join(ldir, d)
                if not os.path.exists(l):
                    os.makedirs(l)

    @open()
    def putdir(self, ldir: str, rdir: str) -> None:
        """
        上传本地目录 `ldir` 到远端目录 `rdir`。

        :param ldir: 本地目录。
        :param rdir: 远端目录。

        >>> putdir('/tmp/mydir', '/home')  # /home/mydir
        >>> putdir('D:\\mydir', '/home')  # /home/mydir
        """
        ldir = os.path.normpath(ldir)
        rdir = self.join(rdir, '')
        for top, dirs, files in os.walk(os.path.normpath(ldir)):
            basename = os.path.basename(top)
            rdir = self.join(rdir, basename)
            if not self.exists(rdir):
                self.makedirs(rdir)
            for f in files:
                l = os.path.join(top, f)
                r = self.join(rdir, f)
                self._sftpclient.put(l, r,
                    callback=self._progress_bar_generator('put', l, r))
            for d in dirs:
                r = self.join(rdir, d)
                if not self.exists(r):
                    self.makedirs(r)

    def join(self, *paths: str) -> str:
        """
        连接多个远端路径。
        """
        paths = [p.rstrip('/') for p in paths]
        return '/'.join(paths)

    def normpath(self, path: str) -> str:
        """
        规范化远端路径。
        """
        segs = [s.strip('/') for s in path.split('/')]
        path = self.join(*segs)
        return path.rstrip('/')

    def basename(self, path: str) -> str:
        """
        获取远端路径的 basename。
        """
        return path.rsplit('/', 1)[-1]

    @open()
    def exists(self, path: str) -> str:
        """
        检查远端路径是否存在。
        """
        try:
            self._sftpclient.stat(path)
            return True
        except FileNotFoundError:
            return False

    @open()
    def walk(self, path: str):
        """
        遍历远端目录。
        """
        dirs, files =  [], []
        for a in self._sftpclient.listdir_attr(path):
            if stat.S_ISDIR(a.st_mode):
                dirs.append(a.filename)
            else:
                files.append(a.filename)
        yield path, dirs, files

        for d in dirs:
            for w in self.walk(self.join(path, d)):
                yield w

    @open()
    def makedirs(self, path: str) -> str:
        """
        创建远端目录。
        """
        logger.info('Makedirs %s' % path)
        curpath = '/'
        for p in path.split('/'):
            curpath = self.join(curpath, p)
            if not self.exists(curpath):
                self._sftpclient.mkdir(curpath)

    @contextmanager
    @open()
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
        logger.info('Open %s with mode=%s' % (filepath, mode))
        f = self._sftpclient.open(filepath, mode)
        try:
            yield f
        finally:
            f.close()
