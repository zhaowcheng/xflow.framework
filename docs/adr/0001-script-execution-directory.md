---
status: proposed
---

# 脚本在脚本根目录中执行

为让目标脚本及其依赖脚本统一使用相对脚本根目录的路径，`Node.exec_script` 的脚本参数改为相对项目 `scripts` 目录，执行工作目录由 `Node.cwd` 改为 `Node.scriptdir`。这一选择优先保证公共脚本的引用约定一致，代价是现有调用需移除 `scripts/` 前缀，脚本的相对输入、参数及输出路径也随之改变；普通 `Node.exec` 保持原有工作目录，`Node.scriptdir` 仍随 `Node.cwd` 变化。用户已明确要求上述路径及目录变化，完整功能方案尚待最终确认。
