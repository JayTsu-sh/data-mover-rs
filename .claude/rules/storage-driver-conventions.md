# Storage Driver Conventions

针对 `src/storage/backends/*` 及其协议适配器的硬规则。

## D1 · role 契约与能力一致

**verify**: 核对 `src/storage/roles/`、对应适配器的能力声明和行为测试，运行
`make arch-guard`。协议不支持的操作返回 typed Unsupported，不能伪装成功。

**why**: 各后端有不同能力，通用传输和遍历通过角色接口协作。
**how to apply**: 遵循 [架构契约](../../docs/architecture/storage-transfer-architecture.md)，
保持通用操作不依赖 backend 分派，并保留优化路径的行为断言。

## D2 · 资源句柄走 `close_resource` helper

**verify**:

```bash
grep -nE '\.close\(\)' src/storage/backends/cifs/*.rs src/nfs.rs src/s3.rs | grep -v close_resource
```

应空 (除了 helper 自身实现)。

**why**: error path 漏 close → 句柄泄漏 → 长 session 句柄耗尽 (S99 教训)。
**how to apply**: `src/storage/backends/cifs/protocol.rs` 的 `close_resource` 是模板。打开 handle 后用 `defer`-like 模式 (async block + 总在末尾 close) 或 RAII helper。

## D3 · backend retry 决策走映射表

**verify**: 改 retry 行为时 `.claude/docs/error-taxonomy.md` 必须同步。grep 自检：

```bash
# retry 行为应该集中在 retry helper，不散写
grep -nE 'retry|backoff|sleep' src/{nfs,s3}.rs src/storage/backends/local/*.rs src/storage/backends/cifs/*.rs
```

**why**: 上游 sync 引擎根据错误变体决定重试，决策必须一致。
**how to apply**: 不在 backend 内部决定"要不要 retry"——返回正确的 `StorageError` 变体让上游决定。例外：transient 网络错误的本地短重试 (例如 nfs-rs 内部) 可以但需要文档化。

## D4 · backend URL parse 错误统一 `UrlParseError`

**verify**:

```bash
grep -nE 'UrlParseError|ConfigError' src/{nfs,s3}.rs src/storage/backends/local/*.rs src/s3/url_options.rs
```

**URL 文本自己就能判定的错误**都返回 `UrlParseError`：语法、协议名（含已删除的旧协议名，报错给出新写法）、
未知 / 重复 / 空值的参数、写出默认值的参数、参数之间的组合（例如 S3 的 `tls_verify` 用在 `s3://` 上、
`bucket_type=nas` 不带 `compat=ontap`）。要看 URL 之外的东西（环境、另一份配置、服务端）才能判定的是
`ConfigError`。

**why**: 上游能精准区分"用户写错 URL" vs "配置缺字段"；同一段 URL 文本在任何环境下报同一个错。
**how to apply**: `url::Url::parse(...)?` 失败、参数表校验失败 → `UrlParseError`；URL 本身合法、与其他配置
矛盾 → `ConfigError`。S3 的参数表只在 `src/s3/url_options.rs` 一处。报错不回显 URL 与凭据（R8）。

## D5 · backend 内部不可反向依赖 `StorageEnum`

**verify**:

```bash
grep -nE 'use crate::storage_enum::StorageEnum' src/{nfs,s3}.rs src/storage/backends/local/*.rs src/storage/backends/*/*.rs
```

应空。

**why**: 模块边界清晰，避免循环依赖。enum 是组合层，不是 backend 实现细节。
**how to apply**: backend 间数据传递走 `EntryEnum` / `NASEntry` / `S3Entry` 等通用类型，不通过 `StorageEnum`。

## D6 · 时间字段必须走 `time_util`

**verify**: 见 [rust-patterns.md](rust-patterns.md) R9。
**why**: SMB FileTime (1601 epoch, 100ns) / NFS Time (s + ns) / S3 DateTime (ms) / Unix (ns) 互转坑多。
**how to apply**: backend 拿到协议时间结构直接传 time_util 的转换函数。

## D7 · Backend 必须实现的最小操作集

所有 backend 提供 role 列举；保留的 legacy backend 操作签名与 `StorageEnum` 一致：

- role `Namespace::list` (逐页)，通过 `StorageTraversalSource` 遍历
- `get_metadata`
- `mkdir` / `mkdir_or_open`
- `read_file` / `write_file`
- `copy_file` / `copy_file_with_cancel`
- `delete_file` / `delete_dir_all`
- `rename`
- `check_connectivity`
- `probe_server_time`

不支持的操作 → 返回 `StorageError::UnsupportedType("operation X for backend Y")`，**不要静默 noop**。

## D8 · 公开 backend 类型不直接暴露给用户

**verify**:

```bash
grep -nE 'pub use crate::(nfs::NFSStorage|s3::S3Storage|local::LocalStorage)' src/lib.rs
```

应空 — 只 `pub use crate::storage_enum::StorageEnum`。

**why**: 用户拿到具体 backend 类型会绕过 `StorageEnum` 抽象，导致代码与 backend 耦合。
**how to apply**: 如果用户需要某 backend 特有功能，加到 `StorageEnum` 的方法 (匹配只对应 backend 有实际操作，其他 backend 返回 `UnsupportedType`)。

## D9 · smb-rs 依赖只固定 main 上的提交，升级走完整矩阵

**verify**:

```bash
# rev 必须是 40 位提交，不是 branch / tag
grep -nE 'smb-domain.*rev = "[0-9a-f]{40}"' Cargo.toml
# 该提交必须在 smb-rs main 上 (在 smb-rs clone 里)
git merge-base --is-ancestor <rev> origin/main && echo on-main
# Cargo.lock 里 smb-* 只能有一个 rev
grep -oE 'smb-rs\.git\?rev=[0-9a-f]+' Cargo.lock | sort -u | wc -l   # 应为 1
```

升级 rev 的 commit 必须是单独的 `chore(deps)` commit，body 列出 `.claude/docs/storage-cifs.md`
"升级 smb-rs 依赖" 第 3、4 步的结果 (clippy 门禁、单测、namespace / policy / anon 契约各跑几次)。

**why**: 2026-09-16 的 dead-code 清理让 64 MiB checkpoint 用例出现过间歇失败，只有真实
FAS2750 + 多次重跑才能区分回归与瞬时状态；`[patch.crates-io]` 与浮动分支在 #150 前造成过
两版 smb 并存。
**how to apply**: 见 [storage-cifs.md](../docs/storage-cifs.md) "升级 smb-rs 依赖" 六步；协议层
(session / connection / runtime / crypto / domain / facade) 有改动的升级不得跳过第 4 步。
