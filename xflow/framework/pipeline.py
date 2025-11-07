# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
流水线模块。
"""

from typing import Optional, Sequence, TypedDict, Literal
from threading import current_thread

from xflow.framework.logger import getlogger
from xflow.framework.env import Env
from xflow.framework.node import Node


class NodeSelector(TypedDict):
    """
    节点过滤器。
    """
    names: Sequence[str]
    labels: Sequence[str]

Result = Literal['FAILED', 'SUCCESSFUL']


class Pipeline(object):
    """
    流水线。
    """
    # 执行节点。
    NODES: NodeSelector = None
        
    def __init__(self, env: Env, *args, **kwargs):
        """
        :param env: 环境信息。
        """
        self.env = env
        self.args = args
        self.kwargs = kwargs
        self.logger = getlogger(self.__class__.__name__)
        self.result: Result = None

    def run(self) -> Result:
        """
        执行 pipeline。
        """
        
        
    class Stage(object):
        """
        阶段。
        """
        # 执行节点，覆盖 pipeline 的 NODES。
        NODES: NodeSelector = None

        def __init__(self, pipeline: 'Pipeline'):
            """
            :param pipeline: 该阶段所在的流水线。
            """
            self.__pipeline = pipeline
        
        @property
        def node(self) -> Optional[Node]:
            """
            当前节点。
            """
            nodename = current_thread().name.split('.')[-1]
            nodes = self.__pipeline.env.get_nodes(names=(nodename,))
            if nodes:
                return nodes[0]
            else:
                return None
            
        def steps(self) -> None:
            """
            步骤。
            """
            raise NotImplementedError
        
        def debug(self, msg) -> None:
            """
            输出 debug 日志。
            """
            self.__pipeline.logger.debug(msg)

        def info(self, msg) -> None:
            """
            输出 info 日志。
            """
            self.__pipeline.logger.info(msg)

        def warning(self, msg) -> None:
            """
            输出 warning 日志。
            """
            self.__pipeline.logger.warning(msg)

        def error(self, msg) -> None:
            """
            输出 error 日志。
            """
            self.__pipeline.logger.error(msg)
