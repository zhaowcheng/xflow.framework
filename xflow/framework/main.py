# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
入口脚本。
"""

import os
import sys
import shutil
import argparse

from typing import Type, Dict, Optional
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
    parser = argparse.ArgumentParser(prog='xflow')
    parser.add_argument('command', choices=['init', 'run'])
    parser.add_argument('-d', '--directory', required=('init' in sys.argv), 
                        help='directory to init (required by `init` command)')
    parser.add_argument('-p', '--pplfile', required=('run' in sys.argv), 
                        help='pipeline file (required by `run` command)')
    parser.add_argument('-e', '--envfile', required=('run' in sys.argv), 
                        help='environment file (required by `run` command)')
    parser.add_argument('-n', '--nodename', required=('run' in sys.argv), 
                        help='name of the node to run pipeline (required by `run` command)')
    parser.add_argument('-a', '--pplargs',
                        help='pipeline arguments (optional for `run` command)')
    parser.add_argument('-v', '--version', action='version', version=f'xflow {__version__}')
    return parser


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

def parse_pplargs(pplargs: Optional[str]) -> Dict[str, str]:
    """
    解析流水线参数。
    """
    pplargs = pplargs or ''
    kwargs = {}
    for arg in pplargs.split():
        name, value = arg.split('=')
        kwargs[name] = value
    return kwargs
    

def run(
        pplfile: str,
        envfile: str,
        nodename: str,
        **pplargs
    ) -> None:
    """
    执行 pipeline。

    :param pplfile: pipeline 文件路径。
    :param envfile: 环境信息文件路径。
    :param nodename: 节点名称。
    :param pplargs: 流水线参数。
    """
    env = Env(envfile)
    sys.path.insert(0, os.getcwd())
    pplname = Path(pplfile).name.replace('.py', '')
    modname = pplfile.replace('.py', '').replace(os.sep, '.')
    pplmod = import_module(modname)
    pplcls: Type[Pipeline] = getattr(pplmod, pplname)
    pplinst: Pipeline = pplcls(env.workdir, env.get_node(nodename), **pplargs)
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
        run(args.pplfile, args.envfile, args.nodename, **parse_pplargs(args.pplargs))


if __name__ == '__main__':
    main()
