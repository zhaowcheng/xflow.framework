# Scripts 脚本接口重新设计

本文件记录本轮设计访谈中已逐项确认的决定。待用户确认完整方案后定稿；本文不作为开始实现的指令。

按用户要求，本轮设计沿用 `.scratch/script-dependencies/` 目录，替换旧设计记录并删除旧规格 `spec.md`。目录中已有的实施任务和验证记录属于此前实现，不作为本轮设计的验收依据。

## 第一轮已确认的决定

1. 框架提供通用的 `Scripts` 基类，负责依赖解析、上传和执行。项目在 `scripts/__init__.py` 中定义继承该基类的 `Scripts`，维护脚本依赖声明和脚本入口。`Node` 提供上传文件、执行命令等基础能力。
2. 项目 `Scripts` 的类属性 `DEPS` 为字典，新项目初始值为 `{}`。键为目标脚本相对脚本根目录的路径，值为直接依赖脚本的相对路径列表，例如 `{'myscript.sh': ['lib/mylib.sh']}`。没有条目的脚本按无声明依赖处理。
3. 脚本入口是普通方法，名称由用户自定义，不要求与脚本文件名一致。方法声明自己的参数，在方法体中调用统一执行方法并明确传入脚本相对路径。仅作为依赖使用的脚本可以不提供入口方法。
4. `Scripts` 构造器包含必选的 `node: Node` 参数。Pipeline 自动加载当前项目 `scripts/__init__.py` 中的 `Scripts`，在自身实例化时建立 `self.scripts = Scripts(self.node)`；项目 Pipeline 不需要另行指定 Scripts 类。
5. 本次只保证在 Pipeline 中使用。Scripts 初始化只绑定状态，实际调用脚本入口时才进行依赖解析、上传和执行。
6. 一次切换到新方案，移除旧的 `deps.json` 机制和 `Node.exec_script` 入口，不提供过渡期。用户说明目前没有项目使用 `deps.json` 方式。
7. 沿用现有运行规则：递归收集直接和间接依赖并去重，上传前检查声明、路径、本次所需文件及循环依赖；保留相对子目录；框架只执行目标脚本，不依次执行依赖脚本。
8. 目标脚本在当次 `Node.scriptdir` 中执行。远端目标已存在时直接执行，不检查本地依赖、不补传、不自动刷新。目标不存在时上传完整依赖集合，不逐个判断依赖在远端是否存在。
9. 沿用现有执行结果、环境变量和错误传播约定：统一执行方法返回 `CommandResult`，保留可选的 `envs` 和已有 nixenv 包装，非零返回码继续抛出 `CommandError`；脚本的解释器约定不变。

## 第二轮已确认的决定

1. 统一执行方法命名为 `__exec`，供项目 Scripts 的脚本入口在方法体内调用。Pipeline 按使用约定调用具体的脚本入口，不另设接受任意脚本路径的公开入口。
2. 统一执行方法接收原始参数字符串 `argstr`，不引入参数列表或自动参数映射。脚本入口自行组装参数字符串并负责所需的 Shell 引号；保留原有 Shell 解释语义。
3. 所有项目都必须提供 `scripts/__init__.py` 及项目 `Scripts` 类，包括没有脚本的项目。新项目模板生成空的项目 Scripts 类；缺文件、缺类或加载失败时，在 Pipeline 初始化阶段明确报错。
4. 不增加用于编辑器补全的项目 Pipeline 类型注解或其他配置，保留自动加载和实例化方式。

## 完整接口示例

以下展示拟实现的接口，不表示当前代码已具备该接口。框架在 `xflow.framework.scripts` 模块提供原名为 `Scripts` 的基类，其构造参数为 `node: Node`；统一执行方法沿用 `__exec(self, script: str | Path, argstr: str = '', envs: Optional[Dict[str, str]] = None) -> CommandResult` 的参数与结果约定。

项目的 `scripts/__init__.py`：

```python
import shlex

from xflow.framework.scripts import Scripts as BaseScripts


class Scripts(BaseScripts):
    DEPS = {
        'myscript.sh': ['lib/mylib.sh'],
    }

    def build(self, output_dir: str):
        return self.__exec('myscript.sh', shlex.quote(output_dir))
```

Pipeline 阶段中调用 `self.scripts.build('/tmp/build output')`。框架在 Pipeline 初始化时自动加载当前项目的 `Scripts` 并执行 `self.scripts = Scripts(self.node)`；项目无需自行初始化，也无需提供类型注解。新项目模板中的 `DEPS` 初始为 `{}`，不生成示例脚本的依赖条目或入口。

## 由已确认选择带来的实现约束

- 框架基类和项目子类在类定义中都固定名为 `Scripts`。Python 会把类定义内的 `__exec` 改写为 `_Scripts__exec`，两处类名相同才能通过 `self.__exec(...)` 调用继承实现；导入别名 `BaseScripts` 不改变类定义名称。任意名称的子类不在本次约定内。
- 双下划线表示内部调用约定，不承诺阻止所有外部访问。项目脚本入口不得覆盖构造器、`__exec` 等框架所需成员。
- Scripts 构造时只绑定节点，不固定尚未就绪的远端工作目录，不提前解析或验证 DEPS。每次执行读取当次节点目录，并沿用远端目标已存在时跳过本地依赖检查的规则。
- `DEPS` 是只读的类级声明；框架解析、归一化和遍历时不原地修改它。
- 自动加载按当前 Pipeline 的项目目录定位，不能因不同项目使用相同的 `scripts` 包名而串用其他项目的类。
- 文件或类缺失、构造契约不符、模块内部导入或语法错误都明确暴露，不能伪装成一个可继续运行的空脚本集合。

## 当前代码事实

- `Node.exec_script` 目前承接完整流程，参数为脚本路径、原始参数字符串和环境变量，返回 `CommandResult`。
- Pipeline 构造时已有项目路径和节点；构建编号和远端工作目录在后续 setup 中就绪，因此不能在 Scripts 构造时上传或执行脚本。
- `Node` 通过 Pipeline 调用上下文取得项目和工作目录，符合本轮只在 Pipeline 内使用的范围。
- CLI 当前把项目目录加入模块搜索路径。新设计必须按当前 Pipeline 的项目定位 Scripts，加载方式属于实现细节。
- 现有执行目录决定见 `docs/adr/0001-script-execution-directory.md`，本轮已确认沿用其路径和执行目录规则。

## 收尾确认

当前没有待选择的设计分支，等待用户确认上述完整方案是否准确表达需求。确认前不修改功能代码、不生成实施任务；本轮不要求实施功能。
