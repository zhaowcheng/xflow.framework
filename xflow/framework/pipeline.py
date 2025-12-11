# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
流水线模块。
"""

import re
import traceback

import click

from typing import List, Literal, Any, Optional, Iterable, Annotated, TYPE_CHECKING
from pathlib import Path

from filelock import FileLock
from pydantic import BaseModel, Field, create_model
from pydantic.fields import FieldInfo

if TYPE_CHECKING:
    from xflow.framework.node import Node

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
            :param default: 默认值（注意：bool 类型的默认值设置无效，固定为 False）。
            :param choices: 枚举值列表。
            """
            clickopts = {
                'help': desc,
                'default': default
            }
            if default is not None:
                clickopts['required'] = False
            if choices:
                clickopts['type'] = click.Choice(choices)
            super().__init__(
                json_schema_extra={
                    'typed-settings': {
                        'click': clickopts
                    }
                }
            )
           

    class Options(BaseModel):
        """
        流水线参数表。
        """
        @classmethod
        def normalize(cls) -> type['Pipeline.Options']:
            """
            规范化参数后返回一个新的 Options。
            """
            newfields = {}
            for fname, finfo in cls.model_fields.items():
                fdict = finfo.asdict()
                clickopts = fdict['attributes']['json_schema_extra']['typed-settings']['click']
                # bool 类型都作为 flag 参数。
                if fdict['annotation'] is bool:
                    clickopts['param_decls'] = (f'--{fname.replace("_", "-")}',)
                    clickopts['default'] = False
                newfields[fname] = Annotated[
                    (
                        fdict['annotation'], 
                        *fdict['metadata'], 
                        Field(**fdict['attributes'])
                    )
                ]
            return create_model(cls.__name__, __base__=cls, **newfields)

    def __init__(
        self, 
        bwd: str, 
        node: 'Node', 
        options: Options
    ):
        """
        :param bwd: 基础工作目录。
        :param node: 执行节点名称。
        :param options: 流水线参数。
        """
        self.bwd = Path(bwd)
        self.node = node
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

