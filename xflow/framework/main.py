# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
入口脚本。
"""

import os
import sys
import shutil

import click

from typing import Type
from importlib import import_module
from pathlib import Path

from typed_settings import click_options

from xflow.framework.version import __version__
from xflow.framework.common import INIT_DIR
from xflow.framework.pipeline import Pipeline, TResult
from xflow.framework.env import Env


class RunGroup(click.Group):
    """
    run 命令组。
    """
    def import_pipeline(self, projdir: str, pplname: str) -> Type[Pipeline]:
        """
        导入 Pipeline。

        :param projdir:  项目目录。
        :param pplname: pipeline 名称。
        """
        if '.' not in sys.path:
            sys.path.insert(0, '.')
        os.chdir(projdir)
        pplmod = import_module(f'pipelines.{pplname}')
        pplcls: Type[Pipeline] = getattr(pplmod, pplname)
        return pplcls
    
    def list_commands(self, ctx: click.Context):
        """
        把 pipelines 目录下的 `.py` 文件名作为子命令（不递归）。
        """
        cmds = []
        ppldir = os.path.join(ctx.obj['projdir'], 'pipelines')
        for f in os.listdir(ppldir):
            if f.endswith('.py') and f not in ('__init__.py', 'template.py'):
                cmds.append(f.replace('.py', ''))
        return cmds
                
    def get_command(self, ctx: click.Context, cmd_name: str):
        """
        返回执行 pipeline 的函数。
        """
        pplcls = self.import_pipeline(ctx.obj['projdir'], cmd_name)
        @click.command(cmd_name)
        @click_options(pplcls.Options, 'xflow')
        @click.pass_context
        def command(ctx: click.Context, options: Pipeline.Options):
            env = Env(Path(ctx.obj['projdir']).joinpath('env.yml'))
            pplinst = pplcls(ctx.obj['projdir'], 
                             env,
                             ctx.obj['nodename'], 
                             options)
            result: TResult = pplinst.run()
            if result == 'FAILED':
                exit(1)
        return command


@click.group()
@click.version_option(__version__)
@click.option('--projdir', '-p', required=True, envvar='XFLOW_PROJDIR', show_envvar=True)
@click.pass_context
def main(ctx: click.Context, projdir: str):
    """
    xflow
    """
    ctx.ensure_object(dict)
    ctx.obj['projdir'] = projdir


@main.command()
@click.pass_context
def init(ctx: click.Context):
    """
    Initialize the project directory.
    """
    projdir = ctx.obj['projdir']
    if os.path.exists(projdir) and os.listdir(projdir):
        print(f'Error: {projdir} exists and is not empty.')
        exit(1)
    shutil.copytree(INIT_DIR, projdir, dirs_exist_ok=True)
    print(f'Initialized {projdir}')
    

@main.group(cls=RunGroup)
@click.option('--nodename', '-n', required=True)
@click.pass_context
def run(ctx: click.Context, nodename: str):
    """
    Run a pipeline.
    """
    ctx.obj['nodename'] = nodename


if __name__ == '__main__':
    main()
