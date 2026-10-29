# data-mover-rs

多源数据迁移核心库。Local / NFS / S3 / HDFS / CIFS 统一经 `storage::connect_backend`
构造 `Storage`，通过 typed roles 提供操作。另含 filter DSL 和 work-stealing 并发遍历。
从 terrasync-rs 拆分独立 (commit `4289a15`)，仅库 (无 binary，无 GUI)。

修改 crate 边界、features、公开兼容层或新增 backend 时，先读
`docs/architecture/crate-workspace.md`。根包 `data-mover` 是兼容 facade；实现归属 `crates/`。

> 这是 Claude Code 的项目入口。先读这页路由，再按场景按需展开 `.claude/docs/`。
> 不堆知识，只路由。深入内容见各 doc。

## 黄金法则

- **Grep first**。不预设接口形状，看代码再改。
- **Smallest change**。bug fix 与 refactor 分两个 commit。
- **role 契约同步**：修改操作时核对角色接口、各后端能力声明、错误分类及行为测试，通用流程保持 backend 无关。
- **`.unwrap()` / `.expect()` 编译期 deny** (Cargo.toml `[lints.clippy]` 已强制)，仅 `#[cfg(test)]` 和测试 helper 例外。
- **资源句柄走 `close_resource` helper** (`crates/backend-cifs/src/backend/protocol.rs` 已有)，不要裸 `.close()`。S99 教训。
- **`Cancelled` ≠ `Error`**，是 `CancellationToken` 信号，上游可重入队 (commit `7eb3046` split retry taxonomy)。
- **Backend 错误统一映射到 `StorageError` 的变体之一** (当前 29 个)。新增变体需要 PR 说明强需求。

## 场景路由表

| 场景 | 先读这些 |
|---|---|
| 改 CIFS (role-based backend / signing / staged) | `.claude/docs/storage-cifs.md` + `crates/backend-cifs/src/backend/` |
| 改 NFS v3/v4 retry 分类 | `.claude/docs/storage-nfs.md` + `.claude/docs/error-taxonomy.md` + `crates/backend-nfs/src/backend/client.rs` |
| 改 S3 multipart / 404 / credential | `.claude/docs/storage-s3.md` + `crates/backend-s3/src/backend/client.rs` |
| 改 S3 URL 参数（`compat=` / `tls_verify=false` / `bucket_type=nas`）/ TLS 校验 / 旧协议名报错 | `.claude/docs/storage-s3.md` "URL 形式" "TLS" + `crates/backend-s3/src/backend/client/url_options.rs`（唯一解析器）+ `crates/backend-s3/src/backend/client/url_options_tests.rs` + `tests/s3_tls_verify.rs` |
| 改 Local 操作 / ACL | `crates/backend-local/src/backend/` + `crates/backend-local/src/backend/windows_acl.rs` |
| **新增/修改 storage role 操作** | `docs/architecture/storage-transfer-architecture.md` + `crates/storage-core/src/storage/roles/` + 对应 backend |
| 改 filter DSL (lexer / `should_skip` 三元组) | `.claude/docs/filter-dsl.md` + `crates/traversal/src/filter.rs` (4849 行) + `crates/traversal/src/filter_traversal.rs` (role-based 适配器) |
| 改遍历调度 / work-stealing | `.claude/docs/walk-scheduler.md` + `crates/traversal/src/traversal/storage/{unordered,blocks,cursor}.rs` |
| 改 role-based 遍历 (filter / max_depth / inline 观察) | `crates/traversal/src/traversal/{mod,storage}.rs` (唯一实现，`local_tests.rs` / `local_unordered_tests.rs` 用真文件系统测它) + 默认的 Unordered 调度器 `crates/traversal/src/traversal/storage/unordered.rs` + `unordered/{pool,worker,page_io}.rs` + 块序驱动 / 游标 / 逐页列举任务 `crates/traversal/src/traversal/storage/{blocks,cursor,stream}.rs` + 按批的会话通道 `crates/traversal/src/traversal/channel.rs` + 续扫检查点 `crates/traversal/src/traversal/checkpoint.rs` (编解码 / 指纹) 与 `crates/traversal/src/traversal/storage/resume.rs` + `resume/{positions,relist,blocks}.rs` (`local_resume_tests.rs` 真文件系统) + 事件 / 计数 `crates/traversal/src/traversal/{events,counts}.rs` + `crates/traversal/src/filter_traversal.rs` + `tests/lab/tools/storage_role_operations/{main,traverse}.rs` |
| 改 S3 遍历 / 版本化桶每 key 一项 (`ObjectHistory`) | `crates/model/src/{history,history_codec}.rs` + `crates/backend-s3/src/backend/{namespace,listing,histories,history}.rs` + `crates/traversal/src/traversal/{s3_tests,s3_history_tests,s3_segment_tests}.rs` + `.claude/docs/storage-s3.md` "role-based 遍历与版本模式" + `.claude/skills/e2e-s3/scripts/traversal_versions.sh` |
| 改 Local namespace 角色 (逐段不跟随 symlink) | `.claude/docs/storage-local.md` "Namespace 角色" + `crates/backend-local/src/backend/namespace.rs` |
| 改递归删除 | `crates/storage-core/src/storage/delete_tree.rs` (走 `Namespace` 角色，backend 无关) |
| 改递归建目录 | `crates/storage-core/src/storage/create_dir.rs` (走 `Namespace` 角色，backend 无关) |
| 改跨端完整性比对 | `crates/integrity/src/integrity/mod.rs` (走 `ReadSource` + `Metadata` 角色) |
| 改拷贝带哪些元数据 / ACL / xattr 开关 | `.claude/docs/metadata-negotiation.md` + `crates/transfer/src/transfer/engine/negotiation.rs` + `crates/metadata/src/metadata/compile.rs` (规划) + `crates/metadata/src/metadata/mod.rs` (应用) |
| 改 error 变体或 retry 映射 | `.claude/docs/error-taxonomy.md` + `src/error.rs` |
| 改时间转换 (FileTime / NFS Time) | `crates/storage-core/src/time_util.rs` (单文件直读) |
| 改 ACL / xattr | `crates/acl/src/acl/` + `crates/backend-local/src/backend/windows_acl.rs` + `.claude/docs/storage-{nfs,local}.md` |
| 改 QoS / 速率限制 | `crates/storage-core/src/qos.rs` (用 governor crate) |
| 改 checksum / 完整性 | `crates/integrity/src/integrity/` + `crates/transfer/src/transfer/engine/` (BLAKE3 校验) |
| 改 tar 打包 | `src/tar_pack.rs` |
| URL 红化 / 日志脱敏 | `crates/storage-core/src/url_redact.rs` |
| 写 commit / PR | `.claude/docs/conventions.md` |
| 升级 smb-rs 依赖 (改 `smb-domain` rev) | `.claude/docs/storage-cifs.md` "升级 smb-rs 依赖" + `.claude/rules/storage-driver-conventions.md` D9 |
| 跑某 backend 验证 / 选真机环境 | `.claude/docs/test-environments.md`（全部真机环境、写入范围、从哪跑） + `.claude/skills/e2e-{cifs,nfs,s3,local}/SKILL.md` |
| 跑「扫描取消后续扫」矩阵 (任一 backend，P7 C11) | `.claude/skills/_shared/traversal_resume.sh` (`BIN= BACKEND= OUT=`) + `tests/lab/tools/storage_role_operations/` 的 `--checkpoint-every / --checkpoint-out / --resume-from / --cancel-after-*`；HDFS 用 `tests/hdfs_architecture_contract.rs` 的 `architecture_traversal_resumes_after_cancel` |
| 跑「容器重启后续传」矩阵 (任一 backend) | `.claude/skills/_shared/resume_matrix.sh` (`DEST=<endpoint>`) + `tests/lab/tools/transfer_resume.rs`；背景见 `docs/adr/0006-destination-resident-recovery.md` |
| 跑取消语义 / filter DSL 测试 | `.claude/skills/op-{cancel,filter-dsl}/SKILL.md` |
| 跑全套 (无外部环境) | `.claude/skills/harness-run/SKILL.md` 或 `make ci` |
| 大文件拆分候选（先运行 `make audit-large`） | 调 `architect` agent，filter 优先调 `filter-expert` |
| 改 role 契约时查漏 | 核对能力矩阵及角色行为测试 |
| commit 前自检 | 调 `reviewer` agent |
| 新增 skill / 升级 rule / 加 agent | `.claude/docs/claude-onboarding.md` |

## 强制约束

- **Edition 2024**；MSRV 以根 `Cargo.toml` 的 `[workspace.package]` 为准，CI 使用同一工具链。
- **Cargo.toml `[lints.clippy]`**：`pedantic = warn` + `unwrap_used = deny` + `expect_used = deny` + `dbg_macro / todo / unimplemented = warn`。
- **`[lints.rust]`**：`unsafe_code = deny`。新增 unsafe 必须有 SAFETY 注释 + PR 说明。
- **异步**：tokio (full)。**错误**：thiserror。**日志**：tracing。
- **依赖管理**：默认全 crates.io，无 `[patch.crates-io]` (与 terrasync-rs 不同)。升级 `smb-domain` / `nfs-rs` / `aws-sdk-s3` 是真实风险。
  经项目授权的 git 固定：`smb-domain` (package `smb`) 固定 JayTsu-sh/smb-rs 已验证提交
  `2649a0b` (smb-rs main 顶端，2026-09-27：#89 文件 `open_existing()` 只读，写 / flush 要 `.write(true)`、
  删除 / 改名要 `.delete(true)`；其前 `37d9404` #87 目录 `open_existing()` 只读、删除 / 改名目录要
  `.delete(true)`，#86 连接被 RST 后读循环不再二次 poll 而 panic；其前 `91e6cfb` #83 预备阶段取消修复、
  #82 runtime / 签名批处理 / 真机 CIFS 验收，domain ACL 只保留 account 类 ACE —— 有意为之；再前 #78 drop 连接即结束恢复、#76 丢流 /
  未建模 NTSTATUS 不再致命、#74 64 位 file id、#72 列举元数据、#73 始终直接 SMB2 协商)，`hdfs-native` 固定上游 kimahriman/hdfs-native master 提交 `695fc47` (2026-09-23 从 JayTsu-sh fork `373739f` 切回；fork 的 hsync / recover_lease 已并入上游)。不得使用浮动 branch。smb-rs 历史 API 依赖已随 legacy `CifsStorage` 删除 (#150)。

## 文件大小管理

用 `make audit-large` 查看当前大小；不沿用已删除文件的行数表。后端客户端、
协议适配和角色实现集中在各自的 `crates/backend-<backend>/src/backend/` 内，按职责拆分。

新写的代码遵守 `≤800 行 / 文件`、`≤50 行 / 函数`。已超的不强求当下拆，但**不要再继续堆大**。

## 升级路径 (lessons learned 的去向)

```
correction (用户在 session 里纠正)
    ↓
.claude/memory/corrections.jsonl  (每次 append)
    ↓ /evolve 评审
.claude/memory/learned-rules.md   (50 行硬上限)
    ↓ 提炼
.claude/rules/*.md                (可验证规则)
    ↓ 能机械化检查的
Cargo.toml `[lints.clippy]` 或 CI grep 检查
```

每一步都让"下次 session 起点更高一点点"。

## 进入 session 的标准动作

1. 读 CLAUDE.md (本页) 找路由。
2. 按场景读 1-2 个 `.claude/docs/`。
3. 跨多文件改动前调 `architect` agent。
4. 改某 backend 调 `backend-specialist` agent (传 backend 名)。
5. 改 role 契约核对能力矩阵及角色行为测试。
6. 改 filter.rs 调 `filter-expert` agent。
7. commit 前调 `reviewer` agent，跑 `make ci`。
