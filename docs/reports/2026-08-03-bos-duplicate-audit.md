# BOS 重复存储审计(2026-08-03)

工具:`scripts/verify_duplicates.py`(只读,逐对象子集校验:相对路径+精确大小)
完整产物:S1 `/root/dup_manifest-20260803.json`、日志 `/root/dup_verify-20260803.log`

**用户决定(2026-08-03):全部保留,不执行删除。** 本文档仅为审计记录;
若未来执行清理,须按下表"可删"清单重新逐对象复验后进行。

## 可删(已证明每个对象在主副本中同名同大小存在)— 共 5.80 TiB / 14 前缀

| 冗余前缀 | 大小 | 对象数 | 主副本 |
|---|---|---|---|
| other/AgiBotWorld-Alpha/ | 5.44 TiB | 135 | manipulation/AgiBotWorld-Alpha/ |
| test-shard/PhysicalAI-Robotics-Manipulation-Kitchen-Demos/ | 0.32 TiB | 404,602 | manipulation/同名/ |
| driving-vqa/MapLM/ | 0.02 TiB | 55,106 | driving-vqa/maplm_v2/ |
| driving-vqa/DriveGPT4/ | 0.02 TiB | 3 | driving-vqa/Drivegpt4-BDD/ |
| manipulation/LIBERO-{10,goal,object,spatial}/ | ~0.01 TiB | ~5,099 | manipulation/libero_*_no_noops_1.0.0_lerobot/ |
| driving-vqa/{DriveAction,LingoQA,OmniDrive}/ | ~0 | 23 | 小写规范版本 |
| datasets/{InternData-A1,utaustin_mutex}/、ego-centric/Something-Something-v2/ | ~0 | 17 | 各规范位置 |

## 不可删(含独有内容,已自动排除)— 18 前缀,要点

- `other/Sekai/`:41.33 GiB 独有 tar(sekai-real-drone part003 等)——**Sekai 全网仅
  0.13 TiB,账面 9.4 TiB 是 6 月老系统假 done**,需源仓库审计重下,这几个 tar 是仅存部分
- `manipulation/InternData-A1/`:432.56 GiB 独有(大量 `.cache/huggingface/` 下载残留
  ——价值存疑但未验证,先留)
- `manipulation/AgiBotWorld-Beta/`:175.66 GiB 独有(16 对象;与早前"仅 9 个空占位"
  的结论不一致,可能是同名不同大小版本,值得日后细查)
- `multimodal/DL3DV-ALL-4K/`:3 对象 9.82 GiB 独有——已列入二期搬运(force top-up)
  把它们补进 JuiceFS;BOS 侧两份副本均保留
- GRAIL 的 `manipulation/`、`datasets/` 两处旧副本各有少量独有小文件(README 等)
- `.Trash-0/`(2 TiB)与 `others/`、`video/` 等顶层杂项:本轮未校验,未动

## 相关

- 搬运计划:`docs/superpowers/plans/2026-08-03-bos-to-dcloud-transfer.md`
- 对账:S1 `/root/transfer_reconcile-20260803-v2.json`
