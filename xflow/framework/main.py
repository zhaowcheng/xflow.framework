# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
入口脚本。
"""

import os
import sys
import shutil
import argparse

from typing import Type
from importlib import import_module
from pathlib import Path

from xflow.framework.version import __version__
from xflow.framework.common import INIT_DIR
from xflow.framework.pipeline import Pipeline, Result
from xflow.framework.env import Env


def create_parser() -> argparse.ArgumentParser:
    """
    命令行参数解析器。
    """
    parser = argparse.ArgumentParser(prog='')
    parser.add_argument('command', choices=['init', 'run'])
    parser.add_argument('-d', '--directory', required=('init' in sys.argv), 
                        help='directory to init (required by `init` command)')
    parser.add_argument('-p', '--pplfile', required=('run' in sys.argv), 
                        help='pipeline file (required by `run` command)')
    parser.add_argument('-e', '--envfile', required=('run' in sys.argv), 
                        help='environment file (required by `run` command)')
    parser.add_argument('-n', '--nodename', required=('run' in sys.argv), 
                        help='name of the node to run pipeline (required by `run` command)')
    parser.add_argument('extra_args', nargs=argparse.REMAINDER, 
                        help='additional arguments passed to pipeline')
    parser.add_argument('-v', '--version', action='version', version=f'xflow {__version__}')
    return parser


def parse_extra_args(extra_args: list) -> tuple[list, dict]:
    """
    解析额外的命令行参数为 args 和 kwargs。
    
    :param extra_args: 额外的参数列表。
    :return: (args, kwargs) 元组。
    """
    args = []
    kwargs = {}
    
    i = 0
    while i < len(extra_args):
        arg = extra_args[i]
        if arg.startswith('--'):
            if i + 1 < len(extra_args) and not extra_args[i + 1].startswith('--'):
                key = arg[2:].replace('-', '_')
                kwargs[key] = extra_args[i + 1]
                i += 2
            else:
                key = arg[2:].replace('-', '_')
                kwargs[key] = True
                i += 1
        elif arg.startswith('-'):
            if i + 1 < len(extra_args) and not extra_args[i + 1].startswith('-'):
                key = arg[1:].replace('-', '_')
                kwargs[key] = extra_args[i + 1]
                i += 2
            else:
                key = arg[1:].replace('-', '_')
                kwargs[key] = True
                i += 1
        else:
            args.append(arg)
            i += 1
    
    return args, kwargs


def init(directory: str) -> None:
    """
    初始化一个项目。

    :param directory: 项目目录。
    """
    if os.path.exists(directory):
        print(f'Error: {directory} already exists')
        exit(1)
    shutil.copytree(INIT_DIR, directory)
    print(f'Initialized {directory}')
    

def run(
        pplfile: str,
        envfile: str,
        nodename: str,
        *args,
        **kwargs
    ) -> None:
    """
    执行 pipeline。

    :param pplfile: pipeline 文件路径。
    :param envfile: 环境信息文件路径。
    :param nodename: 节点名称。
    """
    env = Env(envfile)
    sys.path.insert(0, os.getcwd())
    pplname = Path(envfile).name
    pplmod = import_module(pplfile)
    pplcls: Type[Pipeline] = getattr(pplmod, pplname)
    pplinst: Pipeline = pplcls(env.workdir, env.get_node(nodename), *args, **kwargs)
    result: Result = pplinst.run()
    if result == 'FAILED':
        exit(1)

def main() -> None:
    """
    Entry function.
    """
    parser = create_parser()
    args = parser.parse_args()
    if args.command == 'init':
        init(args.directory)
    elif args.command == 'run':
        extra_args, extra_kwargs = parse_extra_args(args.extra_args)
        run(args.pplfile, args.envfile, args.nodename, *extra_args, **extra_kwargs)


if __name__ == '__main__':
    main()
