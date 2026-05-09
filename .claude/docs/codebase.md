# Codebase 概览

## 项目身份

- **类型**：Rust 库 (`[lib]`)，**无 binary，无 GUI**。
- **用途**：多源数据迁移核心。从 terrasync-rs 拆分独立 (commit `4289a15`)。
- **Edition**：2024。**MSRV**：跟 stable Rust。
- **依赖管理**：全 crates.io，零 git patch (与 terrasync-rs 不同)。

## src/ 模块清单 (LOC 截至 2026-05-09)

| 文件 | LOC | 角色 |
|---|---|---|
| `lib.rs` | 542 | 公开 API + 模块导出 + `EntryEnum` / `Result` / `canonicalize_path` / `detect_storage_type` |
| **`storage_enum.rs`** | **1334** | **enum-based dispatch 层**，`StorageEnum::{Local, NFS, S3, CIFS}` 用 match 分派 30+ 操作 |
| `cifs.rs` | 2246 | CIFS/SMB backend (smb 0.11) |
| `nfs.rs` | 3100 | NFS v3/v4 backend (nfs-rs 0.2.0) |
| `s3.rs` | 3350 | S3 backend (aws-sdk-s3 1.129.0) |
| `local.rs` | 1134 | 本地文件系统 backend (std::fs + rayon) |
| **`filter.rs`** | **4849** | **filter DSL 引擎**：lexer + `should_skip` 三元组决策树 |
| `acl.rs` | 934 | Unix/Windows ACL 抽象 |
| `qos.rs` | 660 | QoS / 速率限制 (governor crate) |
| `dir_tree.rs` | 460 | walkdir_2 用的目录树事件类型 (`NdxEvent`) |
| `tar_pack.rs` | 244 | tar 流式打包 |
| `error.rs` | 164 | `StorageError` 24 变体 + thiserror |
| `walk_scheduler.rs` | 147 | work-stealing 调度器 (`WorkerContext<T>`) |
| `time_util.rs` | 128 | FileTime ↔ NFS Time ↔ Unix nanos 统一转换 |
| `checksum.rs` | 70 | blake3 哈希 |
| `url_redact.rs` | 66 | URL 日志脱敏 (隐藏 ak/sk/密码) |
| `async_receiver.rs` | 31 | mpsc 接收端 wrapper (`AsyncReceiver<T>`) |
| **总计** | **~19,460** | |

## 公开 API 形状 (lib.rs)

核心类型：
- `StorageEnum` — backend 联合枚举。**所有外部调用通过它**，不直接持有具体 backend。
- `EntryEnum::{NAS(NASEntry), S3(S3Entry)}` — 统一元数据。NAS 适合 Local/NFS/CIFS，S3 单独一档。
- `Result<T> = std::result::Result<T, StorageError>` — 全库统一错误。
- `DeleteDirIterator = AsyncReceiver<DeleteEvent>` — 删除流。
- `WalkDirAsyncIterator2 = AsyncReceiver<dir_tree::NdxEvent>` — 二阶遍历流。

核心函数：
- `canonicalize_path(path) -> String` — 自动检测 + 规范化。
- `detect_storage_type(path) -> StorageType` — URL scheme 4 路判别。
- `StorageEnum::create_storage(url) -> Result<Self>` — 工厂。
- `StorageEnum::create_storage_for_dest(url) -> Result<Self>` — 目标端工厂 (含 ensure_dir 语义)。
- `create_nfs_storage_ensuring_dir` — NFS 专用的目录确保入口 (commit `b11ce9d` export)。

## examples/ (9 个，手动验证入口)

| 文件 | 作用 | 外部依赖 |
|---|---|---|
| `local_walkdir.rs` | 本地递归扫描 + md5 | 无 |
| `local_walkdir_2.rs` | walkdir_2 API 演示 | 无 |
| `local_opt_dir.rs` | 创建优化目录结构 | 无 |
| `cifs_copy.rs` | SMB → SMB 拷贝 (clap CLI) | SMB 服务器 |
| `cifs_walkdir.rs` | 遍历 SMB share | SMB 服务器 |
| `nfs_walkdir.rs` | NFS mount 遍历 | NFS 服务器 |
| `nfs_export.rs` | NFS export 信息查询 | NFS 服务器 |
| `nfs_opt_dir.rs` | NFS 上创建目录 | NFS 服务器 |
| `s3_walkdir.rs` | S3 bucket 列表 | S3 endpoint |

## tests/ (2 个，integration)

| 文件 | 测什么 |
|---|---|
| `test_copy_file_cancel.rs` | `StorageEnum::copy_file_with_cancel` 三场景 (token 预取消、mid-transfer、completion) |
| `test_storage_type.rs` | `detect_storage_type` 7 路径 (Unix/Win/NFS/S3 basic/https/http/hcp/relative/empty) |

**单元测试**：散落在 `src/*.rs` 内 `#[cfg(test)] mod tests`。

## 关键依赖 (Cargo.toml)

| Crate | 版本 | 用途 |
|---|---|---|
| tokio | 1.51.1 (full) | 异步 runtime |
| smb | 0.11 | CIFS/SMB 协议栈 |
| nfs-rs | 0.2.0 | NFS v3/v4 客户端 |
| aws-sdk-s3 | 1.129.0 | S3 SDK |
| binrw | 0.15 | CIFS FileTime / 目录条目二进制解析 |
| blake3 | 1.8.4 | 完整性校验 |
| governor | 0.10.4 | QoS 速率限制 |
| moka | 0.12.15 | NFS 属性缓存 |
| glob | 0.3.3 | filter glob 匹配 |
| hyper-rustls + rustls | — | S3 HTTPS (允许自签证书) |
| thiserror | — | 错误派生 |
| tracing | — | 日志 |
| rayon | — | local.rs 并行 delete |
| tokio-util | 0.7 | CancellationToken 等 |
| async-channel | 2 | 跨 worker 通信 |

## 没有的东西 (确认事实)

- 无 `Storage` trait，**用 enum dispatch**。
- 无 binary crate (`[[bin]]` 段为空)。
- 无 workspace (单 crate)。
- 无 `[features]` 段 (无可选 feature)。
- 无 `build.rs`。
- 无外部 mock 框架 (mockall / wiremock)。**不允许 mock 网络后端**，用真实测试环境。
