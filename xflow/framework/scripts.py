# Copyright (c) 2025-2026, zhaowcheng <zhaowcheng@163.com>

"""
项目脚本执行。
"""
from typing import Dict, Optional, TYPE_CHECKING

from xflow.framework.ssh import CommandResult

if TYPE_CHECKING:
    from xflow.framework.node import Node


class Scripts:
    """
    项目脚本基类，项目子类定义时也必须名为 Scripts。
    """
    DEPS = {}

    def __init__(self, node: 'Node'):
        """
        :param node: 脚本执行节点。
        """
        self.__node = node

    def __exec(
        self,
        script: str,
        argstr: str = '',
        envs: Optional[Dict[str, str]] = None
    ) -> CommandResult:
        """
        上传并执行项目入口指定的目标脚本。

        :param script: 相对项目脚本根目录的路径。
        :param argstr: 由项目入口处理 Shell 引号的原始参数字符串。
        :param envs: 目标命令的环境变量。
        :return: 目标命令的执行结果。
        """
        rscript = self.__node.scriptdir.joinpath(script)
        if not self.__node.exists(rscript):
            deps = self.__resolve_deps(script)
            for file in [script, *deps]:
                lfile = self.__node.pipeline.scriptdir.joinpath(file)
                rdir = self.__node.scriptdir.joinpath(file).parent
                self.__node.exec(f'mkdir -p {rdir}')
                self.__node.putfile(lfile, rdir)
            self.__node.exec(f'chmod +x {rscript}')
        with self.__node.dir(self.__node.scriptdir):
            return self.__node.exec(
                f'./{script} {argstr}', envs=envs
            )

    def __resolve_deps(self, script: str) -> set[str]:
        """
        解析目标脚本的所有依赖（直接和间接）并返回。

        :param script: 目标脚本相对路径。
        :return: 依赖脚本列表，不包含目标脚本自身。
        """
        def resolve(script: str, deps: list) -> None:
            """
            递归解析函数。
            """
            for dep in self.DEPS.get(script, []):
                resolve(dep, deps)
                deps.append(dep)

        deps = []
        resolve(script, deps)
        return set(deps)
