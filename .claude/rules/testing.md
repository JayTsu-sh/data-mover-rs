# Testing Rules

## T1 · Integration test 必须 CI 跑得通

**verify**: `cargo test --no-fail-fast` 在干净 checkout 通过。
**why**: 测试在 dev 跑得通但 CI 跑不通 = 没测到。
**how to apply**: integration test 不能依赖外部 backend (网络) — 这种 test 用 `#[ignore]` 标记，跑 `cargo test -- --ignored`。当前 `tests/test_copy_file_cancel.rs` 和 `test_storage_type.rs` 都是无外部依赖。

## T2 · 单测同文件 `#[cfg(test)] mod tests`

**verify**: `grep -nE '#\[cfg\(test\)\]\s*mod\s+tests' src/*.rs` 应在每个核心模块出现。
**why**: 单测靠近实现 → 改实现时改测试摩擦最小。
**how to apply**: filter.rs / time_util.rs 这种纯逻辑文件必有内嵌 `mod tests`。backend 文件可以选择性内嵌 (网络逻辑难单测)。

## T3 · 不允许 mock 网络 backend

**verify**: `grep -nE 'mockall|wiremock|fake_smb|fake_nfs|fake_s3' Cargo.toml src/` 应空。
**why**: 协议级 mock 维护成本高，且行为偏差 (mock 通过但真服务器失败的悲剧)。terrasync 项目已踩过坑。
**how to apply**: 网络 backend 的 integration test 用真实测试服务器 (skill 的 .env 配置)。CI 跑 `e2e-local`，network skill 在本地或 self-hosted runner 跑。

## T4 · 加 backend 操作必须加 example 或 test

**verify**: 改 `StorageEnum` 公开操作的 PR 必须含 `examples/*.rs` 或 `tests/*.rs` 改动。reviewer agent 检查。
**why**: 没人验证 = 长期回归。
**how to apply**: Local 操作必须 integration test (无外部依赖)。网络 backend 操作至少 example (手动验证入口)。

## T5 · Skill 是 CI 一等公民

**verify**: `.github/workflows/ci.yml` 含 `python3 .claude/skills/e2e-local/scripts/run.py` 步骤。
**why**: skill 不只给 Claude 用，也是真实的回归测试套件。
**how to apply**: 加 skill 时，无外部依赖的加进 CI；有外部依赖的加进 `make e2e-network` 但 CI 不跑。

## T6 · 测试代码豁免 `unwrap_used`

**verify**: clippy 已自动豁免 `#[cfg(test)]` 内部。
**why**: 测试中 `unwrap` 是错误信号 (panic = 测试失败)，符合 idiom。
**how to apply**: `tests/*.rs` 和 `mod tests` 块内部可自由 `unwrap`/`expect`。

## T7 · 测试使用 `/tmp` 而不是项目目录

**verify**: `grep -nE '/tmp|tempfile|tempdir' tests/*.rs` 出现频次。
**why**: 项目目录 fixture 残留污染 git。
**how to apply**: 用 `tempfile::tempdir()` 或固定 `/tmp/data-mover-test-*` 路径，测试结束清理。当前 `test_copy_file_cancel.rs` 用 `/tmp/data-mover-cancel-{src,dst}`，改进方向是 `tempfile`。
