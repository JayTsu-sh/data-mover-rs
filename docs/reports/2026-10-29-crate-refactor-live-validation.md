# Crate 重构后的真实环境验证

日期：2026-10-29（Asia/Shanghai）。本报告记录主分支整合前、当时尚未提交的工作树。

**历史证据**：最终代码已整合 main 的 NFS 0.9.1、v10 快照与 ACL 自动投影；
本报告中的 vendored NFS 及旧 ACL 策略不代表最终实现。最终 review 与重新构建后的
真实环境验收见[整合验收报告](2026-10-29-crate-refactor-review-and-integration.md)。

本轮实际运行重构后的二进制，覆盖扫描、拷贝、完整性、ACL、断点续传和断点续扫。
**不把观察脚本的退出码 0 等同于恢复成功**：恢复验收另外核对复用字节、最终内容摘要、
传输身份、检查点以及清理结果。ONTAP UNIX security style 下的 SMB hash checkpoint
仍不支持，见下文限制；不能宣称所有服务器的所有能力都通过。

## 构建与证据

- Rust 1.95.0，Linux x86_64；构建目录 `/work/cargo-target`。
- 本机 `10.131.9.10`，HDFS/Kerberos 联调节点 `10.131.9.11`。
- 本轮证据目录：`/work/tmp/data-mover-crate-real-20261029-074414/`。
  `summary.json`、`asserted-matrices.json`、`remote-resume-assertions.json` 保存逐组断言；
  `remote-evidence/` 保存远端测试日志及 25 对恢复状态。
- 远端执行本地构建产物；传送后核对全部 30 个产物的 SHA-256，记录在 `artifacts.json`。
  远端 Cargo 适配器只定位已经验证的可执行文件，不重新编译，也不以旧 checkout 的程序代替。
  NFS portmap 修复后另外重建并实测 ONTAP NFSv3 恢复及最小 features 的 NFS 遍历。
- 仓库中的已有夹具仅用于读取；所有新增数据使用本轮唯一目录、前缀或临时桶。
  连接凭据不进入本报告或版本管理。

## 实测结果

| 范围 | 本轮证据与结果 |
| --- | --- |
| 跨 backend 拷贝 | Local、Linux NFSv3、Linux NFSv4.1、RustFS S3、Kerberos HDFS 的 **25 个有向组合**；小文件与 `48 MiB + 137 B` 大文件各完成一轮，逐个独立读回校验 |
| 跨 backend 续传 | 同样 **25 个组合**，每个 `96 MiB + 123 B`，中断进程后由新进程恢复；全部满足 `0 < reused_bytes < size`，复用范围 50,331,648–79,691,776 B，最终独立摘要一致 |
| 完整性 | Local/NFSv3/NFSv4.1/S3 的正向矩阵；Path、Observed 输入；空文件、缺失目标、类型、mtime、同长度内容损坏及大小变化反例通过 |
| HDFS | Kerberos HA nameservice：15 项真实契约、4 项架构契约及运行根清理通过；覆盖原生元数据、读取范围、扫描、目录隔离、传输策略和恢复 |
| NFS | ONTAP v4.0/v4.1 的遍历、目录操作及不同 UID 的 mkdir 契约通过；ONTAP v3/v4.1 与 DXN-2 v4.0 的 200 MiB 合作取消、SIGKILL 续传均复用 64 MiB，最终 BLAKE3 相同 |
| S3 | MinIO、RustFS、ONTAP、DXN 各 36 个尺寸/写入策略/read-back 组合、2 次原生拷贝、2 次中断后恢复通过；每组 40 次成功传输，12 次 Direct 和 2 次 Native 独立内容比较；临时对象和未完成上传归零 |
| StorageGRID | 真实 TLS 端点遍历通过；0 B、1 KiB、20 MiB + 1 B × 三种写入策略，共 9 次写入及 9 次独立内容比较通过；独立客户端确认 0 对象、0 上传残留 |
| S3 历史扫描 | MinIO 临时版本桶：跨 1,000 记录分页、版本/删除标记、current 视图、blocks 并发 1/4/32 与 unordered 一致性通过；临时版本桶已删除 |
| SMB | ONTAP 6 项真实契约通过，含写入策略、目录语义、停顿后继续列举、签名/认证探测、只读共享；大文件 hash checkpoint 的限制单独列出，未计为续传通过 |
| SMB 正向恢复 | 隔离的真实 Samba 4.23.8 服务，启用存储 DACL 的 `acl_xattr`；200 MiB 合作取消和 SIGKILL 后均由新进程复用 64 MiB，剩余 136 MiB 实际读取，独立 BLAKE3 一致；两轮 HOME 均无恢复状态，远端运行目录及临时服务已清理 |
| ACL | Linux nfsd POSIX ACL 与 ONTAP NFSv4 ACL 的四向复制/比较通过；命名主体、mask、优先级、组权限组合、Observed 输入、修改目标 ACL 的负例、不可无损转换的拒绝均实测 |
| 最小构建 | `--no-default-features --features backend-nfs,traversal --example storage_traverse` 构建后在 ONTAP 实际遍历完成；另复查 13 组 feature 依赖图的 SDK 隔离 |

25 对拷贝脚本的实际矩阵和附加真实文件操作全部通过。远端临时执行目录最初未带
`tests/run_cargo_test.py`，导致末尾 multipart rename 单测启动失败；补传当前 S3 crate
测试产物后，精确选择该真实单测执行，确认 **1 passed**。因此保留原脚本 exit 2
的日志，同时以 `e2e-multipart-supplement.log` 记录补跑，不篡改原始失败记录。

## 断点续扫

每种后端播种独立的 12 个目录、960 个文件；完整扫描基准为 **972 个条目**。

- 合作取消：Local、NFS、SMB、S3 × unordered/blocks，共 **8 个用例**。
- SIGKILL：NFS、SMB、S3 × unordered/blocks，共 **6 个用例**。
- 14 个用例均实际经过多轮恢复，最终条目集合与完整基准相同：`lost=0`、`invented=0`。
  允许检查点契约规定的目录重放，重复条目不冒充遗漏或新增。
- SIGKILL 的初始时间窗口过短时，S3 尚未建立检查点便被终止；调整到完成连接并产生
  检查点之后重新测试。最终 S3 两种顺序均在 2 轮内恢复完成。

## 本轮修复

1. **NFSv3 可选 ACL 服务探测 panic**：ACL discovery 请求只尝试一次，portmap 却构造
   必须至少两次的 replay policy。改为单次预算使用 `ONE_ATTEMPT`，保持重试预算，
   不扩大探测请求的重放。新增真实 RPC 交互的单次 portmap 回归测试。
2. **HDFS 测试失败退出码丢失**：在 `if` 的失败分支立即保存测试退出码，清理成功
   不能把测试失败改成成功；回归验证失败码 42 原样返回。
3. **HDFS HA 测试 URL**：运行根构造支持配置文件明确声明的无端口 nameservice；
   继续拒绝未知无端口主机和含密码 URL，保留固定且隔离的测试根结构。
4. 大文件/恢复脚本支持 `TMPDIR`。初轮本机系统盘耗尽的结果全部排除，改用 `/work/tmp`
   后完整重跑受影响矩阵。

验证：workspace **2109 passed / 0 failed / 43 ignored**；vendored NFS **596 passed**；
Python unittest **113 passed**，另执行 HDFS 脚本契约 **6 passed**；workspace Clippy `-D warnings`、格式检查及
feature 依赖隔离检查通过。未将普通测试运行中 ignored 的用例计为真机通过。

## 环境限制与诊断

### ONTAP UNIX security style 的 SMB hash 恢复

200 MiB 的首次 checkpoint 和新进程恢复均被拒绝；缩小到 100 MiB 的普通
Checkpointed 写入仍在约 0.7 秒复现。读回证据：DACL ACE 内容相同，发送的
`dacl_protected=true` 读回变为 `false`。所用 SMB SDK 已发送 PROTECTED_DACL
请求标志，因此不是遗漏设置标志或 ACE 顺序差异。

这与已有 [CIFS 权限模型研究](../design/cifs-unix-checkpoint-protection-research.md)
和 [2026-10-23 真机限制记录](../design/checkpointed-range-hash-cifs-validation.md) 一致。
本轮保留拒绝行为，没有削弱源片段的权限保护，也没有修改共享卷的安全样式。
`resume-cifs.log` 的脚本 exit 0 只表示观察脚本自身结束，**不是 SMB 恢复成功**。

正向恢复使用本机隔离容器内的真实 Samba 4.23.8，不是 fake protocol；仅挂载本轮
临时共享目录并使用随机测试密码。宿主机透明代理会截获 Docker bridge 流量，测试
客户端因此只进入该容器的网络命名空间，连接其 loopback SMB 服务，未改宿主机
代理规则。`samba-resume-result.json` 保存两次恢复的严格断言：身份相同、复用 64 MiB、
实际流式读取 136 MiB、完整内容相同、空 HOME、无远端残留。该结果不能替代
ONTAP UNIX 目录的正向认证，也不代表 Windows 原生服务已测试。

### 其他边界

- DXN-2 的 v4.0 常规操作和恢复通过；不同 UID 的 mkdir 契约被已有父目录权限挡住，
  无法在该批准子目录上完整运行。相同协议的该契约已在 ONTAP 通过；未修改 DXN 父目录权限。
- DXN-1 的旧 `data-mover-test` 身份/桶已不存在。使用用户提供的 SSH 登录方式只读取得
  现有服务身份密钥，写入限制在本轮临时 `data-mover-crates-1029-074414` 桶；测试后
  删除全部对象/上传及该桶。未创建用户、轮换密钥或调整存储配置。
- Windows 原生运行、HDFS 服务端故障切换/停机注入未执行。HA nameservice 连接成功
  不等同于完成服务端故障切换测试。

临时诊断日志已从生产源码移除。详细执行证据保留在上述本机目录。
