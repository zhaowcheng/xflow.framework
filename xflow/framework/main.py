# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
入口脚本。
"""

import os
import sys
import shutil

import click
import typed_settings

from typing import Type
from importlib import import_module

from xflow.framework.version import __version__
from xflow.framework.common import INIT_DIR
from xflow.framework.pipeline import Pipeline, TResult
from xflow.framework.env import Env


class RunGroup(click.Group):
    """
    run 命令组。
    """
    def import_pipeline(self, pplname: str) -> Type[Pipeline]:
        """
        导入 Pipeline。

        :param pplname: pipeline 名称。
        """
        if '.' not in sys.path:
            sys.path.insert(0, '.')
        ppldir = self.find_pipelines_dir()
        pplfile = os.path.join(ppldir, f'{pplname}.py')
        modname = pplfile.replace('.py', '').replace(os.sep, '.').strip('.')
        pplmod = import_module(modname)
        pplcls: Type[Pipeline] = getattr(pplmod, pplname)
        return pplcls

    def find_pipelines_dir(self):
        """
        查找 pipelines 目录。
        """
        envar = 'XFLOW_PIPELINES_DIR'
        # 如果存在环境变量，则使用环境变量指定的目录。
        if os.environ.get(envar):
            return os.environ[envar]
        # 环境变量未指定，则递归查找当前目录下名为 `pipelines` 的子目录。
        for top, dirs, files in os.walk('.'):
            for d in dirs:
                if d == 'pipelines':
                    return os.path.join(top, d)
        # 未找到，报错退出。
        print(f'Error: The `pipelines` directory was not found, '
            f'please specify via {envar}.')
        exit(1)

    def list_commands(self, ctx):
        """
        把 pipelines 目录下的 `.py` 文件名作为子命令（不递归）。
        """
        cmds = []
        for f in os.listdir(self.find_pipelines_dir()):
            if f.endswith('.py') and f not in ('__init__.py', 'template.py'):
                cmds.append(f.replace('.py', ''))
        return cmds
                
    def get_command(self, ctx, cmd_name):
        """
        返回执行 pipeline 的函数。
        """
        pplcls = self.import_pipeline(cmd_name)
        @click.command(cmd_name)
        @typed_settings.click_options(pplcls.Options.normalize(), 'xflow')
        @click.pass_context
        def command(ctx: click.Context, options: Pipeline.Options):
            env = Env(ctx.obj['envfile'])
            pplinst = pplcls(env.workdir, 
                             env.get_node(ctx.obj['nodename']), 
                             options)
            result: TResult = pplinst.run()
            if result == 'FAILED':
                exit(1)
        return command


@click.group()
@click.version_option(__version__)
def main():
    """
    xflow
    """
    pass


@main.command()
@click.argument('directory')
def init(directory: str):
    """
    Initialize a project directory.
    """
    if os.path.exists(directory):
        print(f'Error: {directory} already exists')
        exit(1)
    shutil.copytree(INIT_DIR, directory)
    print(f'Initialized {directory}')
    

@main.group(cls=RunGroup)
@click.option('--envfile', '-e', required=True)
@click.option('--nodename', '-n', required=True)
@click.pass_context
def run(ctx: click.Context, envfile: str, nodename: str):
    """
    Run a pipeline.
    """
    ctx.ensure_object(dict)
    ctx.obj['envfile'] = envfile
    ctx.obj['nodename'] = nodename


if __name__ == '__main__':
    main()
