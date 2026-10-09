# 分诊标签

技能使用的标准角色与本仓库的状态字符串对应如下：

| 标准角色 | 本地状态字符串 | 含义 |
| --- | --- | --- |
| `needs-triage` | `needs-triage` | 等待维护者评估 |
| `needs-info` | `needs-info` | 等待补充信息 |
| `ready-for-agent` | `ready-for-agent` | 规格明确，可由代理实施 |
| `ready-for-human` | `ready-for-human` | 需要人工实施 |
| `wontfix` | `wontfix` | 不予处理 |

技能要求应用某种分诊标签时，将对应字符串写入任务文件的
`Status:` 行。后续更改标签名称时，更新表格第二列。
