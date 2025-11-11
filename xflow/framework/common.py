# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
公共变量/常量。
"""

from pathlib import Path


XFLOW_DIR = Path(f'{__file__}/..').absolute()

STATICS_DIR = XFLOW_DIR.joinpath('statics')

INIT_DIR = STATICS_DIR.joinpath('initdir')
