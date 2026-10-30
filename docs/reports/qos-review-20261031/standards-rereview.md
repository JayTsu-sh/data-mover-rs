# Standards rereview

范围：`git diff HEAD` 与新 `tests/lab/tools/qos_run_scope.rs`、`tests/qos_cleanup_scope.rs`。此前两项 P3 已修正：生产路径仅新增 imports/别名，`architect-import-equivalence.json` 对四文件给出等价结果；架构 feature 表和 S3 transport 文档已同步。空／重复／未知 profile 现在在任何 fixture 创建前拒绝。

**仍有 1 项阻塞安全问题：P1 — 未知 URL scheme 仍可绕过本地 symlink 校验。**

位置：`tests/lab/tools/qos_run_scope.rs:64`、`tests/lab/qos_evidence.py:79`。

两处把任意含 `://` 的字符串视为 remote，随后跳过本地 canonicalize；实际 `examples/support/endpoint.rs::connect_with_options` 只识别 NFS、SMB、S3、HDFS 的特定写法，其余字符串被当成本地目录。触发示例：在当前目录中准备 `file:/host/nightly-test/link -> <shared>`，传入 `file://host/nightly-test/link`。guard 接受，而 Local backend 将该字面路径 canonicalize 到 shared，接下来递归 cleanup 能删掉 run 外的数据。

已做非破坏性 Python 复现：`guard_accepted=True`、`effective_local_root_is_shared=True`、`sentinel_untouched=True`。Rust helper 相同分支，实际删除步骤未运行。

建议：guard 的 scheme 分类必须与 `endpoint_support` 一致；直接拒绝未知／不支持 scheme，或将其送入完整 Local 规范化校验。为 `file://...`／自定义 scheme＋symlink 加 Rust 和 Python 负例。现有安全修复和三个 guard 测试覆盖真实本地路径及受支持 remote，但尚未覆盖此分派差异。

除此之外，未发现新增规范、文档或解析问题；未改源码。
