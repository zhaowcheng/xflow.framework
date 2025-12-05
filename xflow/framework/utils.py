# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
实用函数。
"""

import re
import string
import functools

from typing import Callable, Any, TypeVar, Optional


T = TypeVar('T')


def remove_ansi_escape_chars(s: str) -> str:
    """
    移除 `s` 中的 ANSI 转义字符。

    >>> remove_ansi_escape_chars('\x1b[31mhello\x1b[0m')
    'hello'
    """
    escapes = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return escapes.sub('', s)


def remove_unprintable_chars(s: str) -> str:
    """
    移除 `s` 中的不可打印字符。

    >>> remove_unprintable_chars('hello\xe9')
    'hello'
    """
    return ''.join(c for c in s if c in string.printable)

def copy_signature(f: T) -> Callable[[Any], T]:
    """
    拷贝函数签名。

    示例：
    class Child(Parent):
        @copy_signature(Parent.__init__)
        def __init(self, *args, **kwargs):
            ...
    """
    def decorator(func):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator


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
