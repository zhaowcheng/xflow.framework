# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
环境信息管理。
"""

from typing import Sequence

from ruamel import yaml

from xflow.framework.node import Node


class Env(object):
    """
    环境信息管理类。
    """
    def __init__(self, envfile: str):
        """
        :param envfile: 环境信息文件。
        """
        self.__nodes: list[Node] = []
        with open(envfile, encoding='utf8') as f:
            data = yaml.YAML(typ='safe').load(f)
            for node in data['nodes']:
                self.__nodes.append(Node(**node))

    def get_nodes(
        self,
        names: Sequence[str],
        labels: Sequence[str]
    ) -> tuple[Node]:
        """
        获取节点。

        :param names: 节点名称列表。
        :param labels: 节点标签列表。
        """
        nodes = []
        for node in self.__nodes:
            if node.name in names or set(node.labels).intersection(labels):
                nodes.append(node)
        return tuple(nodes)
