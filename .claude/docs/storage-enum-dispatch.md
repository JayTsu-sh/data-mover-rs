# StorageEnum Dispatch

## 核心约束

`StorageEnum` 是 4 backend 的联合枚举，**用 match 分派**，没有 `Storage` trait。

```rust
pub enum StorageEnum {
    Local(LocalStorage),
    NFS(NFSStorage),
    S3(S3Storage),
    CIFS(CifsStorage),
}
```

每个公开操作 (~30 个) 都是这种形状：

```rust
impl StorageEnum {
    pub async fn copy_file(&self, src: &str, dst: &str) -> Result<()> {
        match self {
            StorageEnum::Local(s) => s.copy_file(src, dst).await,
            StorageEnum::NFS(s)   => s.copy_file(src, dst).await,
            StorageEnum::S3(s)    => s.copy_file(src, dst).await,
            StorageEnum::CIFS(s)  => s.copy_file(src, dst).await,
        }
    }
}
```

这意味着：**新增/修改一个操作 = 五处同步**。

## 五处同步 Checklist

每次新增或修改一个 `StorageEnum` 操作，按顺序检查：

### 1. `src/storage_enum.rs` — 加 / 改 enum impl

- 加新方法签名。
- 写 `match self { ... }` 分派 4 个分支。
- 如果操作可取消，加 `cancel_token: &CancellationToken` 参数。
- 如果操作返回流，用 `AsyncReceiver<T>` 而非 `Stream`。

### 2. 4 个 backend 文件各自实现

- `src/cifs.rs` — 用 smb crate API；资源句柄走 `close_resource` helper。
- `src/nfs.rs` — 用 nfs-rs crate；retry 决策查 [error-taxonomy.md](error-taxonomy.md)。
- `src/s3.rs` — 用 aws-sdk-s3；S3 404 → `FileNotFound`；multipart 阈值参考已有写法。
- `src/local.rs` — 用 std::fs / tokio::fs / rayon；Win ACL 走 `acl.rs`。

签名必须**完全一致** (除了不需要协议参数的 backend 可以 `#[allow(unused)]`)。

### 3. `src/lib.rs` — 公开 API 出口

如果操作需要外部直接调用 (非通过 `StorageEnum`)：
- 加 `pub use ...`。
- 或在 `lib.rs` 写 thin wrapper 函数。

如果操作只通过 `StorageEnum` 暴露，跳过这一步。

### 4. `examples/` — 至少加一个示例 (推荐)

- 命名：`{backend}_{operation}.rs` 或 `{operation}_demo.rs`。
- 用 clap 接受 URL 参数。
- 不写 assertion，是手动验证入口。

### 5. `tests/` — 加 integration test (强烈推荐)

- 至少覆盖 Local backend (无外部依赖，CI 能跑)。
- 网络 backend 测试可放但默认 `#[ignore]`，需要 `--ignored` flag 跑。

### 6. 跑 `make ci`

- `make fmt` → `make clippy` → `make test` → `make examples` → `make e2e-local`。
- 全绿才算操作落地。

## 常见漏改场景

| 漏改 | 后果 |
|---|---|
| 漏 `storage_enum.rs` 的某个 match 分支 | 编译失败 (好) |
| 漏一个 backend 文件实现 | 编译失败 (好) |
| 签名不一致 (例如某 backend 漏了 `cancel_token`) | 编译失败 (好) |
| 错误处理不一致 (例如某 backend 返回 `IoError` 别人返回 `CifsError`) | 运行时 retry 决策错乱 (差) |
| 某 backend "懒得实现" 返回 `OperationError("not supported")` | 静默降级 (最差) |
| 漏 example / test | 长期看不出回归 |

**调 `dispatch-checker` agent** 可自动 grep 这些漏改。

## 何时 *不* 用 StorageEnum

- backend 内部辅助函数 (例如 cifs.rs 的 SMB 协议帧构造) → 留在 backend 文件，不入 enum。
- 跨 backend 的纯算法 (例如 filter / checksum / time_util) → 独立模块，不入 enum。
- 一次性的迁移工具 (例如 nfs_export 查询) → 直接 backend API，不入 enum。

## 何时 *增* 一个 backend (例如 SFTP)

如果未来加第 5 个 backend：

1. 新文件 `src/sftp.rs`，实现所有 30+ 操作。
2. `storage_enum.rs` 加 `SFTP(SftpStorage)` 变体。
3. **每个 match 都要加一行**。这就是 ~30 处编辑。
4. `lib.rs` 加 `pub mod sftp` (如果对外暴露)。
5. `detect_storage_type` 加 `sftp://` scheme 判别。
6. examples/ + tests/ 各加一个。
7. `.claude/docs/storage-sftp.md` 新建。
8. CLAUDE.md 路由表加一行。
9. `.claude/skills/e2e-sftp/` 新建。

`architect` agent 应自动出这个 checklist。
