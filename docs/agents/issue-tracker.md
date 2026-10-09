# 工作跟踪：本地 Markdown

本仓库的任务和规格保存在 `.scratch/` 中。

## 文件约定

- 每项功能一个目录：`.scratch/<feature-slug>/`。
- 规格文件：`.scratch/<feature-slug>/spec.md`。
- 实施任务：`.scratch/<feature-slug>/issues/<NN>-<slug>.md`。
  每个任务单独一个文件，从 `01` 开始编号。
- 在任务文件顶部附近用 `Status:` 记录分诊状态；
  状态字符串见 `triage-labels.md`。
- 评论和讨论追加到文件末尾的 `## Comments` 下。

## 发布与读取

技能要求“发布到工作跟踪系统”时，按上述约定创建文件，
并按需创建目录。

技能要求“读取相关任务”时，读取用户指定路径的文件；
若用户只提供编号，在对应功能目录的 `issues/` 下查找。

## Wayfinder 工作约定

使用 wayfinder 时：

- 工作地图：`.scratch/<effort>/map.md`，
  包含 Notes、Decisions-so-far、Fog。
- 子任务：`.scratch/<effort>/issues/NN-<slug>.md`，
  从 `01` 开始编号，正文记录待解决的问题。
- `Type:` 使用 `research`、`prototype`、`grilling` 或 `task`。
- 此流程的 `Status:` 使用 `open`、`claimed`、`resolved`，
  与普通任务的分诊状态区分。
- `Blocked by: NN, NN` 记录依赖；
  所有依赖均为 `resolved` 后，任务才解除阻塞。
- 选择编号最小、状态为 `open` 且未被阻塞的任务。
- 开始工作前，将状态改为 `claimed` 并保存。
- 完成后，在 `## Answer` 下追加结论，将状态改为 `resolved`，
  并向工作地图的 Decisions-so-far 追加摘要和任务链接。
