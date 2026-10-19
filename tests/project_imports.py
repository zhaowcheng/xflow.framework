import sys
from pathlib import Path
from unittest.mock import patch


def use_project(testcase, project):
    """
    为临时项目配置普通导入，测试结束后恢复搜索路径和项目包缓存。

    :param testcase: 当前测试，用于登记清理操作。
    :param project: 临时项目目录。
    :return: 无返回值。
    """
    search_path = patch.object(
        sys, 'path', [str(Path(project).absolute()), *sys.path]
    )
    search_path.start()
    testcase.addCleanup(search_path.stop)
    previous = {name: module for name, module in sys.modules.items()
                if name == 'scripts' or name.startswith('scripts.')}

    def restore_modules():
        for name in list(sys.modules):
            if name == 'scripts' or name.startswith('scripts.'):
                sys.modules.pop(name)
        sys.modules.update(previous)

    testcase.addCleanup(restore_modules)
    for name in previous:
        sys.modules.pop(name)
