# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
环境信息管理。
"""

from typing import List

from ruamel import yaml

from xflow.framework.node import Node, NativeNode, ContainerNode
from xflow.framework.errors import NoSuchNodeError, NoSuchDockerError


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
        self.__nodes: List[Node] = []
        for nodeinfo in self.__data['nodes']:
            if nodeinfo['docker']:
                for dockerinfo in self.__data['dockers']:
                    if dockerinfo['name'] == nodeinfo['docker']:
                        self.__nodes.append(
                            ContainerNode(
                                name=nodeinfo['name'],
                                ip=dockerinfo['ip'],
                                port=dockerinfo['port'],
                                bwd=nodeinfo['workdir'],
                                user=nodeinfo['user'],
                                container=nodeinfo['container'],
                                image=nodeinfo['image'],
                                runargs=nodeinfo['runargs'],
                                cacert=dockerinfo['tls']['cacert'],
                                clientcert=dockerinfo['tls']['clientcert'],
                                clientkey=dockerinfo['tls']['clientkey'],
                                envs=nodeinfo['envs']
                            )
                        )
                        break
                else:
                    raise NoSuchDockerError(f'name: {nodeinfo["docker"]}')
            else:
                self.__nodes.append(
                    NativeNode(
                        name=nodeinfo['name'],
                        ip=nodeinfo['ip'],
                        sshport=nodeinfo['sshport'],
                        user=nodeinfo['user'],
                        password=nodeinfo['password'],
                        bwd=nodeinfo['workdir'],
                        envs=nodeinfo['envs']
                    )
                )

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
