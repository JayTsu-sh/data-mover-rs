## Spec

审查基准：`4149575e91fe392ee853725e110d6c1be9c6b562...HEAD`（ce81883、78d6303）。

1. **P1 — 清理根校验可被父路径/本地符号链接绕过。** `tests/lab/tools/backend_qos_cleanup.rs:32`、`:43` 仅验证原始 endpoint 路径含 run_id；`tests/lab/qos_evidence.py:70` 同样如此。对应要求：“需要在真实环境中验证”“保证没有 regression”，以及本工具明确声称“only within an explicitly isolated QoS run root”。例如 `QOS_SOURCE=/tmp/nightly-review/../shared --run-id nightly-review --tree` 通过校验；`examples/support/endpoint.rs:197` 将其交给 Local，`crates/backend-local/src/backend/mod.rs:41` canonicalize 为 `/tmp/shared`，随后递归删除其内容。只读复现 Python 也接受 `/tmp/nightly-review/../../`（实际根 `/`），未执行删除。建议拒绝父路径及编码父组件，对 Local canonicalize 后的实际根再次验证 run 组件（包括符号链接），再允许连接/删除；补兄弟目录 sentinel 的隔离测试。

2. **P2 — 未运行任何用例仍退出成功。** `tests/lab/run-backend-qos.py:118` 跳过未匹配 profile，`:327` 的 `all([])` 为真。对应要求：“分别在不同的 backend 上验证”“确保满足所说的四个场景”。`--profiles typo-profile` 或 `profiles=[]` 会输出 `FINISHED 0/0 checks passed` 并退出 0；已用临时配置和 mock fixture 复现，且无 checks.json。建议运行前拒绝未知 profile 和空选集，运行结束要求检查非空，并补负例。

**验收边界：** 最终报告如实保留持续限额 50 用例/415 判定通过、短文件失败和尾部；高 QoS 9 路中 5 路原始低于 98%、8 路精度不足。用户确认的持续传输口径没有被替换为仅 admission 计时。上述高 QoS 目标仍未完成实测验收，不能据此宣称四场景全部通过。未发现可确认的新增生产调度器/S3 请求计费缺陷；候选优化已撤回。

**代码结论：请求修改。** 修复上述两项后可复审；代码审查通过与剩余高 QoS 环境资格需分开表述。
