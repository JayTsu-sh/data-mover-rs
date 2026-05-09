# Rust Patterns (data-mover-rs 强制规则)

每条规则**必须可验证**。

## R1 · `.unwrap()` / `.expect()` 仅 `#[cfg(test)]`

**verify**: clippy 编译期 deny。`Cargo.toml` 已配 `unwrap_used = "deny"` + `expect_used = "deny"`。
**why**: panic 把 backend 错误变成进程崩溃，吃掉 retry 决策。
**how to apply**: 生产路径用 `?` 或显式 match。测试代码 `#[cfg(test)]` 自动豁免。

## R2 · 公开 API 返回 `crate::Result<T>`

**verify**: `grep -nE 'pub.*fn.*->' src/*.rs | grep -v 'Result' | grep -v '#\[cfg(test)\]'` 应只剩明确不需 Result 的 (例如 `pub fn name() -> &str`)。
**why**: 全库统一错误模型。混 `std::io::Result` / `anyhow::Result` 让上游 retry 决策错乱。
**how to apply**: 内部 helper 用 `Result<T>` (本模块 alias)；公开 `pub` 必须 `crate::Result<T>` 或同义。

## R3 · backend 错误映射到 `StorageError` 24 变体

**verify**: `grep -c '#\[error' src/error.rs` 应 = 24。变更必须 PR 说明。
**why**: 上游 retry taxonomy 依赖变体语义 (见 `.claude/docs/error-taxonomy.md`)。
**how to apply**: backend 内部错误用 `StorageError::S3Error / NfsError / CifsError` 透传消息；已知协议错误映射到具体变体 (FileNotFound / PermissionDenied / InsufficientSpace 等)。新加变体必须更新 error-taxonomy.md。

## R4 · use 集中文件顶部

**verify**: 排除 `mod tests` 内部，`grep -nE '^[[:space:]]+use ' src/*.rs` 不应有匹配。
**why**: 阅读时一眼看清依赖。
**how to apply**: `use foo::Bar;` 写在文件 attr 之后、第一个 item 之前。`mod tests` 内可有自己的 use。

## R5 · 函数体最深 2 段路径

**verify**: 人工 review 提示。clippy 无现成 lint。
**why**: `std::collections::HashMap::new()` 在表达式里读起来重，应 `use std::collections::HashMap` 然后 `HashMap::new()`。
**how to apply**: 表达式里最多 `Foo::bar` (2 段)。更深的 import。例外：trait 关联函数 (`<T as Iterator>::collect`) 等无法 import 的形式。

## R6 · `unsafe` 必须 SAFETY 注释

**verify**: `Cargo.toml [lints.rust] unsafe_code = "deny"` 已强制 — 任何 unsafe 默认编译失败。例外的 unsafe 必须 `#[allow(unsafe_code)]` + 上方 `// SAFETY: ...` 注释。
**why**: data-mover-rs 是数据迁移核心，安全性优先。
**how to apply**: 当前应**零 unsafe**。新增必须 PR 说明 + 引用确切的不变式。

## R7 · 不允许 `dbg!()` / `todo!()` / `unimplemented!()` 留下

**verify**: clippy `dbg_macro / todo / unimplemented = "warn"`。CI 不强制 fail，但 reviewer agent 检查。
**why**: 留下意味着代码未完成或调试痕迹未清。
**how to apply**: 提交前自检，或用 `make clippy` 看 warning。

## R8 · 错误信息不泄漏 ak/sk/密码

**verify**: `grep -E 'format!.*url' src/*.rs` 检查是否过 `url_redact`。
**why**: 错误信息可能写日志、上报错误追踪服务、贴 issue。
**how to apply**: 任何包含 URL 的错误消息必须先 `url_redact::redact_url(&url)`。

## R9 · 时间转换走 `time_util`

**verify**: `grep -nE '(FileTime|DateTime|SystemTime).*nanos' src/*.rs | grep -v time_util.rs` 应空。
**why**: CIFS FileTime / NFS Time / S3 DateTime / Unix nanos 各种坑 (1601 epoch / 100ns ticks / 毫秒 vs 纳秒)。
**how to apply**: 散写转换的逻辑必须搬到 `src/time_util.rs`。

## R10 · `Cancelled` 不当错误

**verify**: 人工 review。`grep -nE 'Err\(.*Cancelled' src/*.rs` 列出所有用 Cancelled 当错的位置，应都是上游识别为信号的代码。
**why**: 取消是用户/上游主动行为，不是失败。retry 引擎应区分。
**how to apply**: backend 检测到 token cancelled 时返回 `Err(StorageError::Cancelled)`；上游必须区分 `Cancelled` 与其他错误，**不重试，但不算失败**。
