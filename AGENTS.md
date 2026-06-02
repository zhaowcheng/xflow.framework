# AGENTS.md

本文件为在本仓库中工作的代码代理提供指导。请优先遵循本文件；若子目录中存在更具体的 `AGENTS.md`，以更靠近目标文件的说明为准。

## 项目概述

`xflow.framework` 是一个用 Python 编写流水线的 CI/CD 框架。用户通过继承 `Pipeline` 定义流水线，框架在远端节点（SSH 或 Docker 容器）上执行 Shell 命令。典型生命周期为：

```text
setup -> stage1 -> stage2 -> ... -> teardown
```

## 常用命令

```bash
# 开发模式安装
pip install -e .

# 构建发布包
python setup.py sdist bdist_wheel

# 查看 CLI
xflow --help

# 初始化一个 xflow 用户项目目录
xflow -p <projdir> init

# 运行流水线
xflow -p <projdir> run -n <nodename> <pipeline> [选项]
```

`-p/--projdir` 也可以通过环境变量 `XFLOW_PROJDIR` 提供。

## 代码结构

- `xflow/framework/pipeline.py`：流水线基类，负责生命周期编排、参数模型、阶段发现、本地和远端工作目录初始化、清理逻辑。
- `xflow/framework/node.py`：节点抽象，提供命令执行、脚本执行、文件传输、目录上下文、Nix 环境上下文等能力。
- `xflow/framework/main.py`：Click CLI 入口，动态发现 `<projdir>/pipelines/` 下的流水线文件并生成命令。
- `xflow/framework/env.py`：加载 `env.yml`，根据配置构造 SSH 原生节点或 Docker 容器节点。
- `xflow/framework/ssh.py`：SSH 连接层。
- `xflow/framework/container.py`：Docker 容器连接层。
- `xflow/framework/statics/initdir/`：`xflow init` 复制到用户项目目录的模板文件。

## 核心约定

- 流水线类名必须与文件名一致，例如 `example.py` 中定义 `class example(Pipeline)`。
- 阶段方法命名为 `stage1`、`stage2`、`stage3` 等，框架通过正则 `stage\d+` 自动发现并按字典序执行。
- `Pipeline.Options` 继承 Pydantic `BaseModel`，字段使用 `Pipeline.Option()` 定义，以便 `typed-settings` 自动生成 Click 选项。
- `Pipeline.setup()` 使用 `workdir/<PipelineName>/buildid.txt` 和文件锁生成自增 `buildid`。
- 本地工作目录为 `<projdir>/workdir/<PipelineName>/<buildid>/`。
- 远端工作目录由节点基目录、流水线名和 `buildid` 组合得到。
- 使用 `image:` 创建的 Docker 节点是临时容器，成功结束后自动删除；使用 `container:` 指向的是持久容器。
- `teardown()` 总会执行；只有流水线成功时才清理临时容器或远端工作目录。

## 开发注意事项

- 保持当前轻量实现，不要引入新的框架或大型依赖，除非功能确实需要。
- 优先沿用现有风格：简洁类、显式方法、类型标注、中文 docstring。
- 兼容性要谨慎。`setup.py` 声明支持 Python `>=3.6`，但当前代码和 `requirements.txt` 已使用 Python 3.10+ 特性；修改时不要无意扩大这种不一致。
- 修改 CLI、配置加载或节点执行逻辑时，要同时考虑 SSH 节点和 Docker 节点。
- 修改 `xflow init` 模板时，同步检查 `xflow/framework/statics/initdir/` 下的示例是否仍可运行。
- 不要把 `build/`、`venv/`、`*.egg-info/` 等生成物当作源码修改目标，除非用户明确要求处理打包产物。

## 验证建议

当前仓库没有专门的测试目录。完成修改后至少按变更范围执行以下验证：

```bash
python -m compileall xflow
xflow --help
```

如果修改了打包或入口点，额外运行：

```bash
pip install -e .
python setup.py sdist bdist_wheel
```

如果修改了流水线模板或运行逻辑，建议用临时目录执行一次：

```bash
xflow -p /tmp/xflow-demo init
xflow -p /tmp/xflow-demo run -n <nodename> example
```

运行真实流水线可能依赖本机 SSH、Docker、Nix 或用户提供的 `env.yml`，不要假设这些环境一定可用。

## 代理工作准则

- 先阅读相关模块再改动，不要只凭文件名推断行为。
- 保持改动范围小，避免顺手重构无关模块。
- 不要回滚用户已有改动；遇到脏工作区时只处理与任务相关的文件。
- 修改执行命令、文件传输、容器删除等可能影响远端环境的代码时，要明确检查失败路径和清理路径。
- 新增用户可见行为时，优先更新 README、模板或本文件中对应说明。
