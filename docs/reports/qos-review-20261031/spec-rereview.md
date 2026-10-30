## Spec 复审

复审当前工作树的 profile 选择及清理根修复。QoS Python 34 项测试通过。

- 原 **P2 空用例假绿已修复**：启动前验证配置/请求集合非空、名称唯一、未知和不安全 profile；结束时还要求 checks 非空。反例覆盖到位。
- 原 **P1 父路径/编码父路径及普通 Local 符号链接** 已被新校验拦截；Windows 路径处理未发现静态编译问题（本轮未实际交叉编译 Windows）。

**仍有 P1：端点分类与实际 connector 不一致，保留 Local 符号链接绕过。** `tests/lab/tools/qos_run_scope.rs:67` 及 `tests/lab/qos_evidence.py:79` 将任意包含 `://` 的字符串视为远端，跳过 Local canonical 校验；但 `examples/support/endpoint.rs:147` 仅识别规定的 NFS/SMB/S3/HDFS 拼写，其余均作为 Local。临时目录中创建 `<tmp>/x:/host/nightly-test -> <tmp>/shared`，传入 `<tmp>/x://host/nightly-test`：Python 校验成功，而真实 `Path.resolve()` 为 shared；Rust 也直接返回原 endpoint，实际连接 Local 后 canonicalize 为 shared。未执行删除。要求“仅清理确切 run 根”的隔离语义仍未满足。

建议远端分类与 connector 的分支完全一致，其他字符串全部执行 Local canonical 校验，补含 `://` 的本地目录符号链接反例。该问题修复后再次复核。

已知高 QoS 实测资格边界保持不变，不应改写为四场景全部通过。

结论：**请求修改（剩余 1 项 P1）**。
