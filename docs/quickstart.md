# DLM 快速上手指南

## 连接集群

4 台下载服务器（BCC），通过 SSH 连接：

| 名称 | IP | 用户 | 密码 | 用途 |
|------|-----|------|------|------|
| S1 (主控) | 154.85.43.52 | root | (见飞书文档) | DLM 主控 + 下载 |
| S2 | 156.240.120.209 | root | (见飞书文档) | 下载 |
| S3 | 154.85.44.11 | root | (见飞书文档) | 下载 |
| S4 | 154.85.53.152 | root | (见飞书文档) | 下载 |

```bash
ssh root@154.85.43.52    # 连接 S1 主控机
```

## 使用 DLM

S1 已部署好，连上即可使用：

```bash
# 查看所有服务器状态（当前任务、队列深度）
dlm status

# 列出所有下载任务
dlm ls

# 只看正在下载的
dlm ls --status downloading

# 只看某台服务器
dlm ls --server S3
```

## 添加新下载任务

```bash
# HuggingFace 数据集（自动选服务器）
dlm add https://huggingface.co/datasets/org/dataset-name -c manipulation

# 指定分类和服务器
dlm add org/dataset-name -c ego-centric -s S2

# ModelScope 数据集
dlm add https://modelscope.cn/datasets/org/name -c whole-body

# 只加入队列不立即派发
dlm add org/name -c multimodal --no-dispatch
```

分类可选：`manipulation`, `whole-body`, `ego-centric`, `navigation`, `driving-vqa`, `multimodal`, `other`

## 同步状态

当服务器上有任务完成或失败时，同步到中心状态：

```bash
# 预览变更（不写入）
dlm sync

# 确认并写入
dlm sync --update
```

## 重试失败任务

```bash
dlm retry OmniDrive
dlm retry OmniDrive --server S2   # 换个服务器重试
```

## 服务器管理

```bash
dlm server list          # 查看所有服务器
dlm server test --all    # 测试连通性
dlm server setup S2      # 重启某台的 worker
```

## 健康检查

```bash
dlm doctor               # 检查 worker + 检测卡住任务，自动重启死掉的 worker
dlm doctor --apply       # 额外：自动重试所有卡住（>24h 无进展）的任务
dlm doctor --dry         # 只看报告不做任何修复
```

**断点续传**：retry 不会从头下载。HuggingFace CLI 内置断点续传，重试时会跳过已完成的文件，只续传中断的部分。

## 文件位置（S1 上）

```
/root/code/bos-download-manager/    ← DLM 代码
/root/code/bos-download-manager/.env ← 凭证 (AK/SK/HF_TOKEN)
~/.dlm/servers.yaml                 ← 服务器配置
/mnt/auwomo-data/                   ← 数据存储（bosfs2 → BOS）
/root/code/auwomo-tools/            ← Worker 脚本
```

## 数据在哪

所有下载的数据通过 bosfs2 实时同步到 BOS 对象存储：

```
BOS bucket: auwomo-data
路径结构: /{category}/{dataset-name}/
示例:     /manipulation/droid_lerobot/
          /driving-vqa/DriveLM/
```

在任何挂载了 bosfs2 的机器上都能访问 `/mnt/auwomo-data/` 看到同样的数据。

## 常见操作场景

### "我想下载一个新数据集"

```bash
ssh root@154.85.43.52
dlm add https://huggingface.co/datasets/xxx/yyy -c manipulation
# 自动选最空闲的服务器，添加到队列，开始下载
```

### "现在整体进度怎么样"

```bash
dlm status    # 实时状态
dlm ls        # 完整列表
```

### "某个下载失败了"

```bash
dlm sync --update    # 先同步最新状态
dlm retry <name>     # 重试（自动断点续传，不会重头下载）
```

### "下载卡住了很久没动静"

```bash
dlm doctor           # 检查哪些任务卡住
dlm doctor --apply   # 自动重试所有卡住的任务
```

### "想看实时刷新"

```bash
dlm status --watch   # 每 15 秒自动刷新
```
