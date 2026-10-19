# xflow.framework

可以用 Python 写 Pipeline 的 CICD 框架。

项目在 `scripts/__init__.py` 中定义 `Scripts`，用 `DEPS` 声明依赖，
Pipeline 通过 `self.scripts.入口方法(...)` 调用。框架负责上传和执行。
详见[项目脚本指南](xflow/framework/statics/initdir/README.md)。
