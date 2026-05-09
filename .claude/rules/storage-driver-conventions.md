# Storage Driver Conventions

针对 4 个 backend (cifs/nfs/s3/local) 的硬规则。

## D1 · 改 `StorageEnum` 操作 = 5 处同步

**verify**: 调 `dispatch-checker` agent；或 grep：

```bash
# 列出 storage_enum.rs 中的所有 match 分支 vs 4 个 backend 实现
grep -nE 'StorageEnum::(Local|NFS|S3|CIFS)' src/storage_enum.rs | wc -l
# 应该 4 的倍数 (每个操作有 4 分支)
```

**why**: enum-based dispatch 没有编译器强制 trait 完整性 (除非全 match exhaustive)。漏改一个 backend → 行为分裂。
**how to apply**: 改 `StorageEnum` 公开操作时按 [storage-enum-dispatch.md](../docs/storage-enum-dispatch.md) 五步走。

## D2 · 资源句柄走 `close_resource` helper

**verify**:

```bash
grep -nE '\.close\(\)' src/cifs.rs src/nfs.rs src/s3.rs | grep -v close_resource
```

应空 (除了 helper 自身实现)。

**why**: error path 漏 close → 句柄泄漏 → 长 session 句柄耗尽 (S99 教训)。
**how to apply**: cifs.rs 的 `close_resource(handle)` 是模板。打开 handle 后用 `defer`-like 模式 (async block + 总在末尾 close) 或 RAII helper。

## D3 · backend retry 决策走映射表

**verify**: 改 retry 行为时 `.claude/docs/error-taxonomy.md` 必须同步。grep 自检：

```bash
# retry 行为应该集中在 retry helper，不散写
grep -nE 'retry|backoff|sleep' src/{cifs,nfs,s3,local}.rs
```

**why**: 上游 sync 引擎根据错误变体决定重试，决策必须一致。
**how to apply**: 不在 backend 内部决定"要不要 retry"——返回正确的 `StorageError` 变体让上游决定。例外：transient 网络错误的本地短重试 (例如 nfs-rs 内部) 可以但需要文档化。

## D4 · backend URL parse 错误统一 `UrlParseError`

**verify**:

```bash
grep -nE 'UrlParseError|ConfigError' src/{cifs,nfs,s3,local}.rs
```

URL 解析阶段返回 `UrlParseError`；URL 解析后的语义校验返回 `ConfigError`。

**why**: 上游能精准区分"用户写错 URL" vs "配置缺字段"。
**how to apply**: `url::Url::parse(...)?` 失败 → `UrlParseError`。后续提取字段失败 → `ConfigError`。

## D5 · backend 内部不可反向依赖 `StorageEnum`

**verify**:

```bash
grep -nE 'use crate::storage_enum::StorageEnum' src/{cifs,nfs,s3,local}.rs
```

应空。

**why**: 模块边界清晰，避免循环依赖。enum 是组合层，不是 backend 实现细节。
**how to apply**: backend 间数据传递走 `EntryEnum` / `NASEntry` / `S3Entry` 等通用类型，不通过 `StorageEnum`。

## D6 · 时间字段必须走 `time_util`

**verify**: 见 [rust-patterns.md](rust-patterns.md) R9。
**why**: SMB FileTime (1601 epoch, 100ns) / NFS Time (s + ns) / S3 DateTime (ms) / Unix (ns) 互转坑多。
**how to apply**: backend 拿到协议时间结构直接传 time_util 的转换函数。

## D7 · Backend 必须实现的最小操作集

任何 backend 必须实现 (signature 与 `StorageEnum` 一致)：

- `walkdir` (流式)
- `walkdir_2` (流式，含 `NdxEvent`)
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
grep -nE 'pub use crate::(cifs::CifsStorage|nfs::NFSStorage|s3::S3Storage|local::LocalStorage)' src/lib.rs
```

应空 — 只 `pub use crate::storage_enum::StorageEnum`。

**why**: 用户拿到具体 backend 类型会绕过 `StorageEnum` 抽象，导致代码与 backend 耦合。
**how to apply**: 如果用户需要某 backend 特有功能，加到 `StorageEnum` 的方法 (匹配只对应 backend 有实际操作，其他 backend 返回 `UnsupportedType`)。
