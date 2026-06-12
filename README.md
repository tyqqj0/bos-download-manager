# DLM — Dataset Download Manager

多服务器数据集下载管理 CLI。通过 SSH 编排多台 worker，状态集中存储在 BOS。

## 架构

```
BOS state.json  ←  唯一真相源
       ↑
   dlm CLI (主控机)
       ↓ SSH
  S1  S2  S3  S4 ...  (worker, 运行 queue-worker.sh)
       ↓
  /mnt/auwomo-data/ (bosfs2 → BOS)
```

## 安装

```bash
git clone https://github.com/tyqqj0/bos-download-manager.git
cd bos-download-manager
cp .env.example .env   # 填入 BAIDU_AK, BAIDU_SK, HF_TOKEN
pip install .
```

## 初始化

```bash
# 创建空配置，之后逐个添加服务器
dlm init

# 或一次性注册多台
dlm init --host 1.2.3.4 --host 5.6.7.8

# 添加单台服务器（自动验证 SSH）
dlm server add S1 --host 1.2.3.4 --local      # 本机
dlm server add S2 --host 5.6.7.8              # 远程
dlm server add S3 --host 9.10.11.12 --setup-key  # 远程 + 自动配 SSH key
```

## 日常使用

```bash
# 查看所有服务器实时状态
dlm status

# 添加下载任务（自动选服务器 + 派发）
dlm add https://huggingface.co/datasets/org/name -c manipulation

# 列出任务
dlm ls
dlm ls --status downloading --server S2

# 同步真实状态（对比服务器日志 vs state.json）
dlm sync              # 预览
dlm sync --update     # 写入修正

# 重试失败任务
dlm retry OmniDrive --server S3

# 校验数据完整性
dlm verify
```

## 服务器管理

```bash
dlm server list          # 列出所有服务器 + worker 状态
dlm server test --all    # 并行测试连通性
dlm server setup S2      # 远程部署脚本 + 启动 worker
dlm server setup-key S2  # 配置 SSH 免密
dlm server remove S2     # 移除
```

## 配置文件

| 文件 | 位置 | 用途 |
|------|------|------|
| `.env` | 项目根目录 | AK/SK/HF_TOKEN |
| `~/.dlm/servers.yaml` | 主控机本地 | 服务器注册（不入 git） |
| BOS `state.json` | 云端 | 任务状态（唯一真相源） |

## 设计原则

- **BOS state.json 是唯一真相源** — 任何机器运行 `dlm ls` 看到的都一样
- **Worker 脚本不改动** — queue-worker.sh 已稳定运行，DLM 只通过 SSH 观察和控制
- **幂等操作** — `dlm sync` 重复执行不产生多余写入，`dlm add` 自动去重
- **并行 SSH** — status/sync/test 并行查询所有服务器，10 台也秒回
- **无框架依赖** — 纯 Python + Click + BOS SDK，适合 4-10 台规模
