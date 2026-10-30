# Standards review

基准：`4149575e91fe392ee853725e110d6c1be9c6b562`；审查提交：`ce81883`、`78d6303`。只读审查生产 QoS、五类 source 调度、S3 客户端复用、lab 工具和公开证据归档。未发现阻塞合并的规范问题。以下两项低优先级维护问题建议合入前顺手修正。

- **P3 — 函数体新增深层全限定路径违反 R5。** `crates/storage-core/src/runtime/qos/wait.rs:9`（同文件 17–48 行多处）、`crates/backend-s3/src/backend/client/qos_client.rs:52`、`crates/backend-s3/src/backend/client.rs:411`、`tests/lab/tools/backend_qos_acceptance.rs:199`。触发于阅读或维护新增等待/客户端代码：函数体内使用了超过两段的路径，而 `.claude/rules/rust-patterns.md` R5 和 conventions 要求顶部 import。建议导入 `RetryConfig`、`OnceLock`、`TransferPolicy` 及时间/调度函数；两个 `Instant` 用清晰别名区分 Tokio 时钟和墙钟。该项不影响运行正确性。

- **P3 — 新增 feature 未同步架构 feature 说明。** `Cargo.toml:26` 新增公开 `qos-diagnostics`，但 `docs/architecture/crate-workspace.md` 的 Features and builds 未记录；`.claude/docs/storage-s3.md` 也未说明此次 shaped GET 共享已解析 HTTP transport。触发于维护者按仓库路由理解最小构建或 S3 请求行为。CLAUDE.md 要求改 features 先核对 crate 架构、conventions 要求代码同步项目知识。建议说明该诊断 feature 默认关闭、启用 storage-core test-support、无默认调度策略变化，并补记一次性客户端沿用原 transport、仅 shaped GET 禁用重试。

检查结论：依赖方向保持 backend → storage-core；取消仍返回既有 Cancelled 分类；诊断 trace 为显式开启且默认关闭；isolated cleanup 要求合法 run ID 且 endpoint 包含对应完整路径组件。公开 JSON 和五个 tar 归档的 S3/SMB URL 凭据模式检查未发现实际凭据；命中均为测试样例或由环境拼接凭据的源码。此静态扫描不是所有形式凭据的穷举证明。未把已有协议 fake 或真实 loopback HTTP 连接池测试误记为线上网络 backend 替代验收。

未重复报告 fmt、Clippy、文件/函数大小等可由工具检查的问题。未发现有充分依据的其他 baseline code smell。
