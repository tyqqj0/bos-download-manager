# AgiBotWorld-Alpha 对账报告(整理后终验)

日期:2026-08-03(整理执行于 2026-08-02 深夜,北京时间)
执行:`scripts/consolidate_alpha.py --execute`(S1)
计划:`docs/superpowers/plans/2026-08-02-control-plane-hardening-and-alpha.md` T6/A6

## 结论

`manipulation/AgiBotWorld-Alpha/` 是 AgiBotWorld-Alpha 的**完整权威副本**:

```
repo:  agibot_world/AgiBotWorld-Alpha (ModelScope)
repo files: 258  |  total 9000.8 GiB
dst objects (manipulation/AgiBotWorld-Alpha/): 258
MISSING/MISMATCH (key+size): 0
```

以上为独立复核结果(终审 Opus agent 对着新鲜的 ModelScope 列表逐文件重跑,
与脚本自带的 post-copy 对账一致)。**该数据集无需任何下载。**

## 本次整理做了什么(仅 2 个服务端 copy,零删除)

| 对象 | 动作 | 大小 |
|------|------|------|
| `observations/352/648544-655345.tar` | `other/` → `manipulation/`(纯新增,multipart copy) | 48,561,940,480 B |
| `.gitattributes` | 先备份旧版,再从 `other/` 覆盖 | 20,361 B(旧版 2,461 B) |

备份位置(数据集前缀之外,resume filter 永远看不到):
`download-manager/backups/AgiBotWorld-Alpha/gitattributes.bak-20260802`(2,461 B,已 HEAD 验证)。

## 遗留事项

- `other/AgiBotWorld-Alpha/` 仍保留约 5.6 TB 冗余部分副本(含上述两个源对象)。
  **是否删除完全由用户决定,默认保留。**
- 指向 `other/` 旧前缀的任务 `t-20260729-cde1dc` 已 revoke,不会被重派。
