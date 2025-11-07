# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
实用函数。
"""

import re
import string
import functools
import inspect

from typing import Callable, Any, TypeVar


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
