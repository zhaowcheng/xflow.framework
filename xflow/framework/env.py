# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
环境信息管理。
"""

from ruamel import yaml

from xflow.framework.node import Node
from xflow.framework.errors import NoSuchNodeError


class Env(object):
    """
    环境信息管理类。
    """
    def __init__(self, envfile: str):
        """
        :param envfile: 环境信息文件。
        """
        with open(envfile, encoding='utf8') as f:
            self.__data = yaml.YAML(typ='safe').load(f)
        self.__nodes: list[Node] = []
        for node in self.__data['nodes']:
            self.__nodes.append(Node(**node))

    @property
    def workdir(self) -> str:
        """
        本地工作目录。
        """
        return self.__data['workdir']

    def get_node(self, name: str) -> Node:
        """
        获取节点。

        :param name: 节点名。
        """
        for node in self.__nodes:
            if node.name == name:
                return node
        raise NoSuchNodeError(f'name: {name}')
