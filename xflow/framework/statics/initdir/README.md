# 项目脚本

项目 `Scripts` 定义入口和依赖，框架负责上传与执行；Node 提供基础操作。
Pipeline 普通导入 `from scripts import Scripts`，将绑定当前节点的实例放在 `self.scripts`。
初始化已生成空类；无脚本项目也要保留它，无需手动实例化或额外配置。
CLI 会将项目目录加入模块搜索路径；直接构造 Pipeline 时需由调用方设置好搜索路径。
项目包沿用 Python 的普通导入和模块缓存行为。

## 使用示例

创建 `scripts/build/run.sh`：

```sh
#!/bin/sh
set -eu
. common/log.sh
log "building $1 (${MODE:-development})"
```

创建依赖脚本 `scripts/common/log.sh`：

```sh
#!/bin/sh
log() {
    printf '%s\n' "$1"
}
```

在 `scripts/__init__.py` 中声明依赖和具名入口：

```python
import shlex

from xflow.framework.scripts import Scripts as BaseScripts


class Scripts(BaseScripts):
    DEPS = {'build/run.sh': ['common/log.sh']}

    def prepare_release(self, label, envs=None):
        """
        :param label: 发布名称，作为单个 Shell 参数。
        :param envs: 可选环境变量。
        :return: CommandResult。
        """
        return self.__exec('build/run.sh', shlex.quote(label), envs)
```

在 `pipelines/release.py` 的阶段中调用：

```python
from xflow.framework.pipeline import Pipeline


class release(Pipeline):
    def stage1(self):
        """
        :return: 脚本的 CommandResult。
        """
        return self.scripts.prepare_release(
            "release O'Reilly", envs={'MODE': 'release'}
        )
```

配置好 `env.yml` 后，在项目目录运行 `xflow -p . run -n local release`，
其中 `local` 为节点名。模板本身不预置示例脚本、入口或依赖。

## 约定

- 框架基类和项目子类在定义时都必须名为 `Scripts`；导入别名不影响类名。
  这是继承 `self.__exec` 的名字改写约定，并非禁止外部访问的机制。
  项目不要覆盖构造器或框架执行成员；文件、类及模块导入错误在 Pipeline 初始化时暴露。
- `__exec(script, argstr='', envs=None)` 的目标支持字符串或 `Path`。
  入口名称和参数自定，原始 `argstr` 由入口组装并处理 Shell 引号；框架不自动映射参数。
- `DEPS` 是路径字符串到直接依赖路径字符串列表的类级字典；空字典或无对应条目表示无依赖。
  路径以项目 `scripts/` 为基准，声明和调用使用一致的路径字符串。
  框架递归去重，不修改声明、不从脚本文本推断依赖；依赖脚本无需单独入口。
- 初始化只绑定节点；实际调用才读取当次 `Node.scriptdir`、解析依赖并交付。
  框架不校验脚本路径、依赖声明或循环，也不预检本地文件；解析和上传错误直接传播。
- 远端目标不存在时，上传目标及完整依赖集合，保留子目录，设置目标执行权限后只执行目标。
  目标已存在时直接复用，不补传、不重设权限、不自动刷新，也不要求本地文件存在。
- 脚本在当次 `Node.scriptdir`（即 `Node.cwd/scripts`）执行，输入、输出及脚本引用的相对路径以此为准。
  普通 `Node.exec` 仍使用 `Node.cwd`；`node.dir(...)` 影响远端目录，正常或异常退出均恢复外层上下文。
  相对目录按节点基础目录、流水线名及构建编号定位，不叠加外层相对目录；本地脚本来源不变。
- 环境变量和 `node.nixenv(...)` 沿用 Node 的执行约定；返回 `CommandResult`，非零返回码抛出 `CommandError`。
  直接调用入口或阶段会传播异常；`Pipeline.run()` 将失败记录为 `FAILED` 并执行 teardown。
