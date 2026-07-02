# D-Robotics Cloud 安全测试报告

- **测试日期**: 2026-06-30
- **测试平台**: cloud.d-robotics.cc (地瓜云)
- **测试账号**: kaiwenguo (auwomo 组织)
- **测试目标**: 验证 BFF 文件 API 是否存在越权访问

## 结论

**组织内部无隔离，跨组织未发现问题。**

JuiceFS 实例按组织隔离（根目录下仅有 `*auwomo*` 相关条目），但同一组织内不同 namespace/用户之间无任何访问控制。

## 确认的漏洞

### 1. 路径穿越 (Path Traversal) — 高危

API 不对 `../` 做规范化处理，可以从自己的 volume 跳到 JuiceFS 根目录。

```
请求路径: /727a2f92-30c/../../../
返回结果: JuiceFS 根目录 (6 个条目)
```

穿越深度 5-9 层 `../` 效果相同，均到达同一根目录。

### 2. 跨 Namespace 读取 — 高危

通过路径穿越可列出并下载其他 namespace 的文件：

| Namespace | 内容 |
|-----------|------|
| `fat-auwomo-fat-auwomo` | zulifeng/kitti (435MB), 付程玲test, hanks-成功测试 等 |
| `computing-auwomo` | .Trash-0 |
| `pretrain-auwomo` | danjiao/ |

**文件下载验证**: 使用 Range 请求下载了 `fat-auwomo/zulifeng/kitti.zip` 前 16 字节，确认为有效 ZIP 文件头 (`PK\x03\x04`)。

### 3. 跨 Namespace 写入 — 高危

在 `fat-auwomo-fat-auwomo` 的 dataSpace 下成功创建文件夹：

```json
POST /api/infrabffApi/files/folder
{
  "path": "/727a2f92-30c/../../../fat-auwomo-fat-auwomo/dataSpace/",
  "name": "security_write_test_from_auwomo"
}
// 返回: {"status": 0, "message": "success"}
```

列出目录确认文件夹已创建。

### 4. Tenant-ID 不验证 — 低危

请求中的 `d-tenant-id` header 可以伪造或留空，不影响 API 正常执行。

## JuiceFS 根目录结构

```
/
├── .minio.sys/          (系统)
├── .sys/                (系统)
├── auwomo-auwomo/       (我们的主 namespace)
├── fat-auwomo-fat-auwomo/ (训练平台 namespace)
├── computing-auwomo/    (计算 namespace)
└── pretrain-auwomo/     (预训练 namespace)
```

所有条目均包含 "auwomo"，说明 JuiceFS 是按组织隔离的，**未发现跨公司数据**。

## 受影响的 API 端点

| API | Method | 漏洞 |
|-----|--------|------|
| `/api/infrabffApi/files` | POST | 路径穿越 + 跨 namespace 列出 |
| `/api/infrabffApi/files/download` | GET | 路径穿越 + 跨 namespace 下载 |
| `/api/infrabffApi/files/folder` | POST | 路径穿越 + 跨 namespace 创建目录 |
| `/api/infrabffApi/files/import` | POST | 路径穿越 + 跨 namespace 导入 |

## 未发现的

- 无法访问其他公司/组织的数据（JuiceFS 实例隔离）
- `/api/infrabffApi/users/info` 返回 404（无用户枚举）
- `/api/infrabffApi/volumes` 返回 404
- 异步任务列表 API 路径变更，原 endpoint 404

## 待清理

以下测试目录需要从地瓜云 UI 手动删除（delete API 返回 404）：

- `/security_test_root`
- `/test-security-probe`
- `/fake-tenant-test`
- `/no-tenant-test`
- `/security_test_traversal`
- `fat-auwomo-fat-auwomo/dataSpace/security_write_test_from_auwomo`

## 建议

1. **反馈给 D-Robotics**: BFF 层需要对所有文件路径做规范化（resolve `../`），并验证最终路径在用户授权的 namespace 内
2. **我方注意**: 不在 JuiceFS 存放敏感/机密数据，因为同组织任何成员可读写
3. **监控**: 定期检查我们的 namespace 下是否出现非预期的文件/目录
