## Spec 最终复审

审查基准 `4149575e91fe392ee853725e110d6c1be9c6b562...baac11505b0f0825afdb903d3bcf6f42ebc4d7c1`；重点复核 8963aca 的验收/清理修复，另核对后续 imports、文档及 CI 合同变更。

**代码层结论：通过，可在必需 CI/验证全部通过后合入。当前无未关闭的 Spec 轴代码缺陷。**

- 已关闭 P1 清理隔离绕过：原始/编码父组件被拒绝；Local 使用 canonical 根及父锚点验证，拦截 run 根和其下符号链接。未知 `://` scheme 全部拒绝，`file://`、`NFS://` 及包含 `://` 的 Local 路径不再能跳过本地校验；Python/Rust 白名单一致。
- 已关闭 P2 零用例假绿：启动前拒绝空配置/空选集、未知/重复和不安全 profile；退出成功还要求 checks 非空。
- 本次独立复跑 Python QoS 34 项全部通过；新增正反例覆盖此前绕过。Rust scope 测试与真实 cleanup sentinel 验证由主审协调核对，本复审未进行外部服务写入或删除。Windows 仅作静态兼容检查，未交叉编译，不将其表述为 Windows 实测通过。
- `make harness-contract` 已包含 QoS Python 合同，feature matrix 包含独立 `qos-diagnostics` 构建。后续 clock import 改动未改变调度逻辑。

**仍未完成的性能目标单独保留：** 报告显示持续配额 50 个用例/415 项判定通过，并保留短文件端到端失败及尾部数据；高 QoS 9 路中 5 路原始比值低于 98%、8 路 A/A 精度不足。因此代码审查通过不等于用户四场景性能目标全部验收通过。合入说明须继续保留此边界，独占环境复验仍是后续工作。
