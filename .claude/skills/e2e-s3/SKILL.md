---
name: e2e-s3
description: 验证 S3 backend — 跑 examples/s3_walkdir，特别验证 404 → FileNotFound (commit 7eb3046) 和 multipart upload。
---

# e2e-s3

**触发关键词**：验证 S3 / 测 multipart / 测 404 映射 / e2e s3。

## 输入 (.env)

```
S3_HOST=s3.amazonaws.com         # 或 minio.local 等
S3_BUCKET=my-bucket
S3_AK=ACCESS_KEY
S3_SK=SECRET_KEY
S3_USE_HTTPS=false               # true → s3+https://
S3_PREFIX=test                   # bucket 内的子路径
```

## 步骤

1. cargo build --example s3_walkdir
2. s3_walkdir 列 bucket — 应返回。`run.py` 把带凭据的 URL 经环境变量 `S3_LISTING_URL` 交给例子（不上命令行）；
   认 `S3_ENV`（服务器的 `.env`）与 `S3_COMPAT`（`dxn` / `sg` → `s3+dxn://` / `s3+sg://`），调用方设的 `S3_BUCKET` 优先。
3. **404 验证**：用 `s3_walkdir <bucket>/this-key-does-not-exist-{timestamp}` 触发 NoSuchKey，应映射为 `FileNotFound`，**不重试**。

4. **写入策略矩阵**（role-based 目的端）：`bash .claude/skills/e2e-s3/scripts/staged_matrix.sh` ——
   大小 0 / 1 KiB / 8 MiB / 8 MiB+1 / 20 MiB / 200 MiB × Checkpointed / AtomicReplace / Direct × 读回开关，原生 S3→S3，
   取消与 SIGKILL 后续传；`S3_ENV=<服务器的 .env>` 选服务器，调用方设的 `S3_BUCKET` 优先于 `.env` 的（只写
   `data-mover-*` 桶）。每行打印结果（含 `prepare` / `reused_bytes` / `native_bytes` / `native_requests`）、耗时和遗留：
   `artifacts=`（前缀下的 `.data-mover-*` 对象：`.upload` 指针；C18 起原生拷贝不再有 temp key）、
   `key_uploads=`（该用例精确 key 上未完成的上传）。续传段另打印 `left at the destination: pointers=… key_uploads=…`
   （切断后期望 1 / 1，续传完成后 0 / 0）。只写/删 `staged-<run>/`
   （`S3_MATRIX_PREFIX=` 可改，必须以 `staged-` 或 `data-mover-` 开头 —— 清理会 abort 本次写过的每个 key 上的上传，再删
   前缀下的一切）。Direct 行（ADR-0006 C14c 起必须成功）另打印 `direct: equal=… key_uploads=…`：下载比对源文件、
   数该精确 key 上未完成的上传，期望 `equal=yes key_uploads=0`。原生行（C18 起写最终 key）另打印
   `native: equal=… stage_objects=… key_uploads=…`，期望 `equal=yes stage_objects=0 key_uploads=0`。凭据经
   `curl -K <(…)` 传入，不上命令行。
   MinIO 2023 的 `ListMultipartUploads` 只认精确 key，所以脚本都按精确 key 数上传（按前缀列再过滤 Key）。
   基线与解读见 `.claude/docs/storage-s3.md`「目的端写入策略基线」。

5. **容器重启后续传**：`DEST=s3:data-mover-<ts> bash .claude/skills/_shared/resume_matrix.sh`。
   S3 的恢复状态在目的端（最终 key 上的 upload + `.data-mover-<d>.upload` 指针），运行处什么都不记（C21 删了本地恢复存储）：
   期望每次运行 `fresh HOME=empty`（否则脚本退出 1）、切断后 `1 objects + 1 open uploads`、续传 `Resumed{…}`、
   reused + streamed = SIZE、BLAKE3 相等、之后 0 / 0。C16 起 > 64 MiB 的 checkpointed upload 一开始就写指针，默认 6 s
   切断（早于第一个 64 MiB checkpoint）也会续传：cancel 与 SIGKILL 都 `Resumed{…}`（取消丢不到一段，SIGKILL 丢在途分段）。

   在版本化桶里跑：`S3_BUCKET_OVERRIDE=<临时桶>` 指向同一服务器上的另一个桶（绝不改 `.env`），每个模式另打印
   `final versions=… markers=… artifact entries=…`（ListObjectVersions：版本化桶期望 1 / 0 / 0；未开版本的桶把对象列成
   `"null"` 版本，也是 1 / 0 / 0）。

6. **版本化桶矩阵**（ADR-0006 C17）：`bash .claude/skills/e2e-s3/scripts/versioning_matrix.sh`。在 `.env` 的服务器上
   **新建两个临时桶** `data-mover-c17-<run>`（开版本）与 `data-mover-c17-lock-<run>`（带 Object Lock），退出时（trap）
   彻底删除：去掉 legal hold、按 id 删每个版本与删除标记、abort 每个上传、删桶，并打印 `cleanup: <桶> deleted`。
   只用可撤销的 legal hold，**从不设 retention**（COMPLIANCE / GOVERNANCE 可能让桶删不掉）；不碰 `.env` 的桶。
   建桶不是 200（例如同名桶已存在，409）就停下，清理只碰本次建出来的桶；`RUN` 必须匹配 `[a-z0-9-]{1,30}`；
   凭据经 `curl -K <(…)` 传入，不上命令行。
   用例：4 MiB checkpointed（单 PUT）、200 MiB checkpointed（指针）、Direct 4 MiB / 20 MiB；`--source-version` 按 id
   拷 v1 再 v2（`--client-shaped` 流式，两版本按序、各与源逐字节相同），再原生按 id 拷一遍（C18：
   `CopyObject` / `UploadPartCopy` 带 `?versionId=`，写到 `hist/ndst`，期望同样按序、各与源相同）；按 id 拷贝 3 s 取消后续传；`resume_matrix.sh`
   cancel / SIGKILL；Object Lock 桶里拷贝进行中给指针版本加 legal hold；暂停版本后三种写法。每行打印 `result` /
   `prepare` / `reused` / `destination_version`，再打印该用例前缀下 `final versions=… delete markers=… artifact entries=…
   version=latest: yes`。期望：每次拷贝 1 个版本、0 标记、0 artifact，`destination_version` 就是最新版本；Object Lock
   行 `result=ok`、带版本：AWS / MinIO 1 条告警、1 个标记 + 2 个 artifact 条目（被扣住的指针版本与盖在上面的标记）；
   Ceph RGW / DXN 连标记也拒 → 2 条告警、0 标记、1 个 artifact 条目（被扣住的指针版本留在原处）；暂停版本
   `destination_version=null`。`SKIP_RESUME=1` 跳过续传矩阵；`LOG_DIR=<目录>` 保留各次传输的 stderr。

   **别的服务器（DXN / StorageGRID）**：`S3_ENV=<该服务器的 .env>` 选服务器（默认 `.claude/skills/e2e-s3/.env`），
   `BUCKET_PREFIX=data-mover-<名字>` 给临时桶换名（默认 `data-mover-c17`，必须匹配 `data-mover-[a-z0-9-]{1,20}`），
   `S3_LOCATION=<LocationConstraint>` 建桶时带上（DXN 的 zonegroup 没有默认 placement，不带就
   `ZonegroupDefaultPlacementMisconfiguration`）。DXN（用户批准的临时桶 `data-mover-dxn-<ts>` / `data-mover-dxn-lock-<ts>`，
   不碰别的桶，也不给 `data-mover-test` 开版本）：

   ```bash
   S3_ENV=.claude/skills/e2e-s3/dxn/.env BUCKET_PREFIX=data-mover-dxn S3_LOCATION=magnascale:tos_placement \
     timeout 5400 bash .claude/skills/e2e-s3/scripts/versioning_matrix.sh
   ```

   Ceph RGW 带 Object Lock 建的桶**不开版本**（`ObjectLockEnabled=Enabled`、版本 off），脚本检测到就自己开版本再跑锁行；
   服务器不让建锁桶就跳过锁行。`resume_matrix.sh` 同样认 `S3_ENV`。DXN 的其他差异见 storage-s3.md「DXN（Ceph RGW
   Octopus）实测行为」。

   **已有桶**（ONTAP：我们的用户不能建桶）：`S3_VERSIONED_BUCKET=data-mover-test-ver` —— 桶名必须 `data-mover-*`、
   版本必须 Enabled，否则拒跑；不建桶，所有用例写在唯一前缀 `dm-<run>/` 下，退出时删光该前缀下的全部版本、删除标记
   与未完成上传（打印 `cleanup: <桶>/dm-<run>/ emptied (bucket kept)`），桶与其设置不动 —— 所以 Object Lock 行（要自己的
   锁桶）与暂停版本行（要改桶设置）跳过。

   **RustFS（1.0.0，PVE VM 105）**：`S3_ENV=.claude/skills/e2e-s3/rustfs/.env`（`versioning_matrix.sh` /
   `traversal_versions.sh` / `resume_matrix.sh` / `staged_matrix.sh` / `run.py` 都认）。endpoint
   `http://10.131.9.14:9001`：管理网上的 console 监听也服务完整 S3 API，业务网 `10.10.1.14:9000` 从 WSL 不通；
   `S3_COMPAT=` 空 = 普通 S3，不用 `S3_LOCATION`。用户 `data-mover-test` 的策略只放行 `data-mover-test` 与
   `data-mover-*`（默认 `BUCKET_PREFIX` 就能建删临时桶）；`bigbucket` 不是我们的（403）。2026-09-26 curl 实测：
   - **ListObjectVersions 原始 XML 按元素类型分组**：每页先是全部 `DeleteMarker`、再是全部 `Version`（组内 key 升序、
     同 key 新→旧），不按 key 交错，所以非最新的标记会排在同 key 的最新版本前面。两组按 key + 新旧合并后才是 S3 逻辑序；
     `key-marker` / `version-id-marker` 分页按逻辑序切（max-keys 1 / 2 / 3 不丢不重）。带 `delimiter` 会折叠 CommonPrefixes。
   - 与 MinIO 2023 不同：条件写可用（`If-None-Match: *` / 错的 `If-Match` → 412 `PreconditionFailed`）；
     ListMultipartUploads 按前缀列可用；DeleteObjects 不带 `Content-MD5` 也 200；legal hold 的 PUT 不要求 `Content-MD5`。
   - multipart ETag = `md5(各段 md5)-N`（与 AWS 同）；非末段 < 5 MiB 在 Complete 报 400 `EntityTooSmall`；
     UploadPartCopy 带 range、CopyObject `?versionId=` 可用。`GET ?partNumber=N` 回 206 但没有 `x-amz-mp-parts-count`。
   - 未开版本 / 暂停版本的桶 PUT 不回 `x-amz-version-id` 头，ListObjectVersions 把对象列成 `null` 版本。
     按 id HEAD 删除标记 → 405 + `x-amz-delete-marker: true`；最新是标记的 key HEAD → 404 + 标记头 + version id。
   - Object Lock 建桶自动开版本；legal hold 下按 id 删版本 → 403 `AccessDenied`，不带 id 的 DELETE 照样写标记（同 AWS / MinIO）。
   - IAM：`/rustfs/admin/v3/*` 收发明文 JSON（`add-canned-policy` / `add-user` / `set-user-or-group-policy`）；
     `/minio/admin/v3/*` 回的是 madmin 加密体。

7. **原生 S3→S3 切断续传**（C18）：MinIO 上服务端拷贝很快（读回关时 1 GiB ≈ 1.1 s），`--bandwidth` 不限原生拷贝，
   所以用 1 GiB 源、读回关、`--cancel-after-ms 300`（或 `timeout -s KILL 0.8`）切断：期望切断后 1 指针 + 1 upload，
   同参数再跑一次 `Resumed { … }`（取消时在途的段都拷完，SIGKILL 丢在途的段），`--compare` 相等，之后 0 / 0。
   源先用本地 → `s3:<prefix>` 上传，再 `--source s3:<prefix> --destination s3:<prefix>`（同 endpoint 才走原生）。

8. **role-based 遍历的版本模式**（ADR-0006 C22）：`bash .claude/skills/e2e-s3/scripts/traversal_versions.sh`。新建**一个**
   临时版本化桶 `data-mover-c22-<run>`，写 `k`（v1、v2、删除、v3）、`gone`（写后删）、两个 artifact、零字节 `d/` 与
   `d/e`、`sub/deep/f`、`p/a000..a998` 加三次 `p/z`（跨第一页 1000 条），再暂停版本写两次 `n`；用
   `storage_role_operations --backend s3 traverse --versions all|current`（`S3_LISTING_URL` 只在脚本环境里）列举并断言：
   All 的 `k` = 2 B、3 B、标记、4 B 只有最后一个 latest，`gone` = 版本 + latest 标记，`p/z` = 1、2、3 B，`n` =
   `version=null latest=1`；Current 只有 `k`（4 B）没有 `gone`；无 artifact；`--order blocks` 并发 1 / 4 / 32 输出
   相同。另外直接发一次带 `delimiter` 的 ListObjectVersions（打印服务器是否按 delimiter 折叠），并用 `examples/s3_listing`
   跑 legacy `walkdir` / `walkdir_2`（先重新开版本：暂停版本的桶走 ListObjectsV2）：`k` 按写入的 id 列 v1、v2、标记、v3，
   `gone` 列版本再列 latest 标记（D4），`p/z` 跨页完整，无 artifact。
   退出时按 id 删光版本与标记、删桶，打印 `cleanup: … deleted` 与剩下的桶；只有全部通过且桶已删才退出 0（编译失败、桶不是本次建的也以非 0 退出，trap 保留原退出码）。
   同样认 `S3_ENV` / `BUCKET_PREFIX`（默认 `data-mover-c22`）/ `S3_LOCATION`。另两种选桶方式（桶名都必须 `data-mover-*`，
   都写在唯一前缀 `dm-<run>/` 下、退出时清空该前缀、不删桶、不改桶设置）：
   - `S3_VERSIONED_BUCKET=<桶>`：已有的版本桶（版本必须 Enabled，否则拒跑）。没有暂停版本那一步，`n` 是普通的一个版本。
   - `S3_UNVERSIONED_BUCKET=<桶>`：从没开过版本的桶（`GetBucketVersioning` 无 Status，或 501 —— ONTAP NAS 桶）。只跑
     Current 与 legacy（ListObjectsV2）：每个 key 一次（`k` 4 B、`p/z` 3 B、没有 `gone`、没有 artifact、999 个 `p/a*`），
     blocks 并发 1 / 4 / 32 一致；清理先删对象、再自深向浅删 NAS 桶为写过的 key 留下的目录。
   历史顺序：`EXPECT_ORDER=inferred`（RustFS：每页删除标记全在版本之前，现存问题 11）时，有删除标记的 key（`k`、`gone`）的
   每一行必须带 ` order=inferred`，其余 key 一行都不许带；默认 `server` 时哪一行都不许带 —— 解析同时接受可选的
   ` order=…` 后缀，检查是精确的，不是容忍。

9. **目录对象**（P7 F5）：`bash .claude/skills/e2e-s3/scripts/directory_objects.sh`。选桶方式同
   `traversal_versions.sh`（默认建一个临时版本桶 `data-mover-f5-<run>`，退出删光；`S3_VERSIONED_BUCKET=` /
   `S3_UNVERSIONED_BUCKET=` 用已有桶的 `dm-<run>/` 前缀；建桶总带 body 或 `Content-Length: 0`，StorageGRID 缺长度回 411）。
   写 `dirobj/`（5 B）+ 子项、`lonely/`（6 B）、`emptydir/`（0 B）+ 子项、console 式空文件夹 `e/`、`plain.txt`、`big/`
   （`BIG_MB`，默认 200 MiB、至少 150，由 data-mover 从本地文件写入：一次 PUT，P7 D13）；版本桶再写 `z/`（0 B、标记、0 B）
   与 `m/`（5 B、0 B、标记、3 B、标记）。先用 curl 探测并打印存储自己对 multipart `x/` 的行为（MinIO 拒绝、RustFS 存成
   `x`），只记录，不影响断言 —— data-mover 从不对这种键发 multipart。
   检查：当前遍历（`--sizes`）每个 key 一次、目录对象是 `File <d>/ size=<n>`、零字节的也在，计数平衡、无失败，
   blocks 并发 1 / 4 / 32 一致；`--path dirobj` 报 `failure scope=Child dirobj/ Unsupported`、不交出，`--path emptydir`
   什么都不报；All 下 `z/`、`m/` 的整段历史按序；legacy 两种 walk 只对有内容的（`dirobj/`、`lonely/`、`big/`、版本桶的
   `m/`）报 `ERROR`；拷贝（同桶）`dirobj/` / `emptydir/` 流式、`dirobj/` native；D13：每次写目录对象都是一次请求
   （ETag 不带 `-<段数>`），同名真实文件 `copy/mid`、`copy/big`、`native/big` 始终不动 —— `mid/`（12 MiB，超过 8 MiB 单
   PUT 阈值）本地→S3 三种 policy 各一次、`big/` 流式切断后什么都不留（无对象、指针、上传）、再跑从零开始（`Fresh`）、
   `big/` native，各自逐字节相等；S3→Local 在 Preflight 报目的端 `Unsupported`、不写文件。> 5 GiB 的拒绝由单测覆盖，
   不在真机上传。`D6=1 S3_UNVERSIONED_BUCKET=data-mover-nas`：只记录 NAS 桶对零字节 / 有内容的 `d/`
   在子项前后写入时存成什么（不对存储断言），末行 `RESULT: D6 recorded (no assertions)`。其余模式期望
   `RESULT: pass` 与 `cleanup: … deleted` / `… emptied`（清理还核对未完成的上传，列不出就算失败）。临时桶只在
   HEAD 为 404 时才建。
   版本桶另跑**历史重放**（P7 H1，`examples/s3_history_replay.rs`）：把 `m/`、`z/`、`dirobj/` 的历史旧→新重放进临时
   版本桶（临时桶模式再建一个 `<桶>-r`；已有桶模式写在 `replay/` 前缀下），按种类与大小逐条比对源与目的；再打印对同一
   key 连写两个标记的结果（MinIO / DXN `merged`，ONTAP / StorageGRID `written`）、临时目的桶上 DXN 的 null 版本探测，并
   检查未开版本的桶（`data-mover-test`）与暂停版本的临时桶上删除标记一律 `skipped`、从不写入。

## 真机矩阵（P7 S3：C6、F4、C10、C11、C11b、收尾清理每一步都要过）

环境、写入范围与已知差异见 `.claude/docs/test-environments.md`；**每套都跑非多版本与多版本两种桶**。每条命令都包
`timeout`（用户规则）；凭据只在各自的 `.env`（gitignored）里，脚本经 `curl -K <(…)` 传。在仓库根目录跑，`S=.claude/skills/e2e-s3`：

```bash
S=.claude/skills/e2e-s3
# MinIO（默认 .env，桶 data-mover-test；多版本用临时桶 data-mover-c17-* / data-mover-c22-*；绝不写 a-bucket）
timeout 600  python3 $S/scripts/run.py
timeout 3600 bash $S/scripts/staged_matrix.sh
S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
timeout 1800 bash $S/scripts/traversal_versions.sh
timeout 5400 bash $S/scripts/versioning_matrix.sh

# ONTAP 对象桶（不能建桶：非多版本 data-mover-test，多版本 data-mover-test-ver，都用已有桶模式；绝不写 ontap-lisa-auto-bucket）
E=$S/ontap/.env
S3_ENV=$E timeout 600  python3 $S/scripts/run.py
S3_ENV=$E timeout 3600 bash $S/scripts/staged_matrix.sh
S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
S3_ENV=$E S3_VERSIONED_BUCKET=data-mover-test-ver timeout 1800 bash $S/scripts/traversal_versions.sh
S3_ENV=$E S3_VERSIONED_BUCKET=data-mover-test-ver timeout 5400 bash $S/scripts/versioning_matrix.sh

# ONTAP NAS 桶 data-mover-nas：只有非多版本（GetBucketVersioning 501、ListObjectVersions 不实现）；平铺列举无序
# 零字节 d/ 在 NAS 桶上不留对象（只是目录）→ EXPECT_DIR_MARKER=0
S3_ENV=$E S3_BUCKET=data-mover-nas timeout 600  python3 $S/scripts/run.py
S3_ENV=$E S3_BUCKET=data-mover-nas timeout 3600 bash $S/scripts/staged_matrix.sh
S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-nas EXPECT_DIR_MARKER=0 timeout 1800 bash $S/scripts/traversal_versions.sh

# RustFS（管理网 9001；非多版本 data-mover-test，多版本临时桶 data-mover-rustfs-*；绝不写 bigbucket）
E=$S/rustfs/.env
S3_ENV=$E timeout 600  python3 $S/scripts/run.py
S3_ENV=$E timeout 3600 bash $S/scripts/staged_matrix.sh
S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
S3_ENV=$E EXPECT_ORDER=inferred BUCKET_PREFIX=data-mover-rustfs timeout 1800 bash $S/scripts/traversal_versions.sh
S3_ENV=$E BUCKET_PREFIX=data-mover-rustfs timeout 5400 bash $S/scripts/versioning_matrix.sh

# DXN（非多版本 data-mover-test —— 永不给它开版本；多版本临时桶 data-mover-dxn-*，建桶带 LocationConstraint，用户上限 5 个桶）
E=$S/dxn/.env
S3_ENV=$E timeout 600  python3 $S/scripts/run.py
S3_ENV=$E timeout 3600 bash $S/scripts/staged_matrix.sh
S3_ENV=$E S3_UNVERSIONED_BUCKET=data-mover-test timeout 1800 bash $S/scripts/traversal_versions.sh
S3_ENV=$E BUCKET_PREFIX=data-mover-dxn S3_LOCATION=magnascale:tos_placement timeout 1800 bash $S/scripts/traversal_versions.sh
S3_ENV=$E BUCKET_PREFIX=data-mover-dxn S3_LOCATION=magnascale:tos_placement timeout 5400 bash $S/scripts/versioning_matrix.sh
```

- 目录对象（P7 F5 / H1）每套都跑 `directory_objects.sh`（非版本 + 版本；版本桶含历史重放）：
  ```bash
  timeout 3500 bash $S/scripts/directory_objects.sh                                        # MinIO 临时版本桶
  S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh  # MinIO
  S3_ENV=$S/ontap/.env S3_VERSIONED_BUCKET=data-mover-test-ver timeout 3500 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/ontap/.env S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/ontap/.env S3_UNVERSIONED_BUCKET=data-mover-nas D6=1 timeout 1800 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/rustfs/.env BUCKET_PREFIX=data-mover-rustfs timeout 3500 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/rustfs/.env S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/dxn/.env BUCKET_PREFIX=data-mover-dxn S3_LOCATION=magnascale:tos_placement timeout 3500 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/dxn/.env S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/storagegrid/.env S3_VERSIONED_BUCKET=data-mover-test-ver timeout 3500 bash $S/scripts/directory_objects.sh
  S3_ENV=$S/storagegrid/.env S3_UNVERSIONED_BUCKET=data-mover-test timeout 3500 bash $S/scripts/directory_objects.sh
  ```
  各存储的差异（MinIO 拒绝对 `x/` 发 multipart、RustFS 把 multipart 的 `x/` 存成 `x` 且不给 `x/` 记版本等 —— 所以
  data-mover 只用单次请求写目录对象，D13）见
  `.claude/docs/storage-s3.md`「目录对象真机」。
- 期望：`run.py` `all checks passed`；`staged_matrix.sh` 的 Direct 行 `equal=yes key_uploads=0`、原生行
  `equal=yes stage_objects=0 key_uploads=0`、续传后 `pointers=0 key_uploads=0`、末行 `left: objects=0 key_uploads=0`；
  `traversal_versions.sh` `RESULT: pass` 且 `cleanup: … deleted` / `… emptied (bucket kept)`；`versioning_matrix.sh`
  每行 `result=ok`、`final versions=1 delete markers=0 artifact entries=0 version=latest: yes`（暂停版本行 `n/a`），
  `cleanup: … deleted` / `… emptied`。
- 不改任何桶的版本设置：`S3_UNVERSIONED_BUCKET` 遇到开过版本的桶拒跑；`S3_VERSIONED_BUCKET` 不暂停版本；临时桶只动自己建的。
- 跑完核对各服务器剩下的桶：MinIO `a-bucket data-mover-test`、RustFS `bigbucket data-mover-test`、DXN `data-mover-test`
  （traversal / versioning 的清理行会打印），ONTAP 三个桶里没有我们的前缀。

## 成功判据

- s3_walkdir 列 bucket 退出码 = 0
- 不存在的 key 返回 FileNotFound，日志中无 retry/backoff

## 失败如何排查

- 401/403 → ak/sk 错或 IAM 权限不够
- 404 走了 retry → S3 错误映射回归，看 `src/s3.rs` 中 GetObject/HeadObject 错误处理
- multipart 失败留垃圾 → 必须 abort，看 s3.rs multipart upload 路径
- 续传的 write 报 `NoSuchUpload`（NotFound）→ 自己的 upload 被当成别人的 abort 了：MinIO 列出的 upload id 与签发的
  写法不同，比较必须走 `upload_pointer::same_upload`
- 续传是 `Restarted{StageWithoutPointer}` → 开始时没写指针：对象不超过 64 MiB（D3，不续传），或 prepare 不是
  `ResumeMode::Discover`（看 `pointer_before_checkpoint`）
- 版本化桶里留下 `.data-mover-*.upload` 版本或删除标记 → 指针没按版本删：看 `upload_pointer::delete` 拿到的版本
  （写入回的 `reported_version`、读回 HEAD 的版本、discovery 读到的版本）；`MemoryS3` 的
  `plain_deletes_of_final_keys()` 必须一直为空
- 证书不受信（自签 / 私有 CA）→ 把 CA 放进 `SSL_CERT_FILE`，或 `S3_TLS_VERIFY=false`（URL 加 `tls_verify=false`）
- `the scheme s3+…:// was removed` → 旧协议名，按报错改写成 `?compat=` 参数
