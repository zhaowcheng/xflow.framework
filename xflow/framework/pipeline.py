# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
流水线模块。
"""

import re
import traceback

import click

from typing import List, Literal, Any, Optional, Iterable, TYPE_CHECKING
from pathlib import Path

from filelock import FileLock
from pydantic import BaseModel
from pydantic.fields import FieldInfo

if TYPE_CHECKING:
    from xflow.framework.env import Env

TResult = Literal['FAILED', 'SUCCESSFUL']


class Pipeline(object):
    """
    流水线。
    """ 
    class Option(FieldInfo):
        """
        流水线参数。
        """
        def __init__(
            self, 
            desc: Optional[str] = None,
            default: Any = None,
            choices: Optional[Iterable] = None
        ):
            """
            :param desc: 描述。
            :param default: 默认值。
            :param choices: 枚举值。
            """
            kwargs = {}
            if desc is not None:
                kwargs['description'] = desc
            if default is not None:
                kwargs['default'] = default
            if choices is not None:
                kwargs.update(
                    {
                        'json_schema_extra': {
                            'typed-settings': {
                                'click': {
                                    'type': click.Choice(choices)
                                }
                            }
                        }
                    }
                )
            super().__init__(**kwargs)
           

    class Options(BaseModel):
        """
        流水线参数表。
        """
        pass

    def __init__(
        self, 
        projdir: str | Path,
        env: 'Env',
        nodename: str, 
        options: Options
    ):
        """
        :param projdir: 项目目录。
        :param env: 环境信息。
        :param nodename: 执行节点名称。
        :param options: 流水线参数。
        """
        self.projdir = Path(projdir)
        self.bwd = self.projdir.joinpath('workdir')  # base workdir
        self.env = env
        self.node = self.env.get_node(nodename)
        self.options = options
        self.result: TResult = None
        self.taskid: int = None

    @property
    def name(self) -> str:
        """
        名称。
        """
        return self.__class__.__name__
    
    @property
    def cwd(self) -> Path:
        """
        当前工作目录。
        """
        return self.bwd.joinpath(self.name, f'{self.taskid}')
    
    def setup(self) -> None:
        """
        前置步骤，如果失败将不会执行任何 stage，直接执行 teardown。
        """
        # 获取 taskid
        print(f'Getting taskid: ', end='')
        parent = self.bwd.joinpath(self.name)
        parent.mkdir(parents=True, exist_ok=True)
        idfile = parent.joinpath('taskid.txt')
        idfile.touch()
        with FileLock(f'{idfile}.lock'):
            with open(idfile, 'r+') as f:
                newid = int(f.read() or 0) + 1
            with open(idfile, 'w') as f:
                f.write(f'{newid}')
        self.taskid = newid
        print(self.taskid)

        # 打印参数
        print(f'options: {self.options}')

        # 创建远端工作目录
        self.node.mkcwd()

        # 创建本地工作目录
        self.cwd.mkdir(parents=True, exist_ok=True) 
    
    def stage1(self) -> None:
        """
        阶段 1，一个 pipeline 至少包含一个 stage1，增加则按照
        stage2, stage3, ... 这样依次增加，执行时也将按此顺序。
        """
        raise NotImplementedError

    @property
    def stages(self) -> List[str]:
        """
        所有阶段名称。
        """
        return sorted([n for n in dir(self.__class__) if re.match(r'stage\d+', n)])

    def run(self) -> TResult:
        """
        执行 pipeline。
        """
        title = lambda t: print(t.center(80, '='))
        try:
            title('setup')
            self.setup()
            for stage in self.stages:
                title(stage)
                getattr(self, stage)()
            self.result = 'SUCCESSFUL'
        except:
            self.result = 'FAILED'
            traceback.print_exc()
        finally:
            title('teardown')
            self.teardown()
        return self.result
            
    def teardown(self) -> None:
        """
        后置步骤，不管成功与否，都会执行该步骤。
        """
        if self.result == 'SUCCESSFUL':
            if self.node.is_container and not self.node.existed:
                self.node.remove(force=True)
            else:
                self.node.rmcwd()

