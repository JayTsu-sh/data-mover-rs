# Local RenameEvidence 真实挂载验证（2026-10-26）

验证工具：`tests/lab/tools/local_rename_evidence.rs`；挂载矩阵执行器：`tests/lab/run-local-rename-evidence.sh`。

## 实现与接入

- 原运行期 `SourceIdentity(dev + ino)` 保留；构造改为直接写内联区，移除临时 `Vec` 分配。
- `RenameEvidence` 内联保存 16 字节 `ino_be64 || btime_ns_i64_be`，访问器返回借用。
  证据复用已有 metadata，不增加逐条文件查询；缺失或越界创建时间返回 `None`。
- 新增 `LocalBackendConfig.rename_context`；现有结构体字面量须补 `rename_context: None`。
  需要启用时使用 `RenameContext::new(RenameScope::new(dataset_id)?, RenamePolicy::AllowHeuristic)`，
  并按扫描保存数据集标识和策略，以便重建同一上下文。默认不授权匹配。
- 本库提供证据和上下文；扫描消费者负责完整扫描的一对一、类型、硬链接歧义及内容变化判断，
  不由候选直接触发改名或删除。
- 条目快照升级到 DMES v8，v7 及更早格式拒绝解码，需要重新扫描建立基线；传输恢复绑定格式不变。

## 环境与边界

- 客户端：实验室 VM101，`10.131.9.10`，root，Linux `6.8.0-142-generic`。
- 服务端：ONTAP FAS2750 SVM lizy，LIF `10.128.61.200`。
- CIFS：`ontap_lisaauto_cifs`，Linux 内核挂载，SMB 3.1.1，分别配置 `serverino` / `noserverino`。
- NFS：`/ontap_lisaauto_nfs`，Linux 内核挂载，分别指定 v3 / v4.1，TCP、保留源端口。
- 每次只在共享或导出下新建 `data-mover-rename-<UTC时间>-<pid>` 私有目录；不修改服务端配置、不操作已有测试树、不卸载现存 `/mnt/nfs_src`。
- 凭据仅从仓库外实验室目录读取，并经 SSH/SFTP 写入远端临时 0600 文件，不包含在仓库或运行日志中。

## 预探测事实

已成功挂载四种组合。CIFS 两种配置均可读取真实创建时间；两种 NFS 配置均不提供 birth time（`stat %w` 为 `-`）。NFS 因此应输出 `RenameEvidence=None` 并按路径回退，不能把缺失时间填为 0 后输出身份。

真实硬链接探测显示：CIFS `serverino`、NFSv3、NFSv4.1 的两个路径具有相同 inode；CIFS `noserverino` 对同一硬链接对象的两个路径分配不同的客户端 inode。因此，`noserverino` 不能据此开启跨挂载自动改名匹配；现有仅依赖 `dev + ino` 的 Direct 硬链接别名保护在该模式下也没有可靠保证。本次不改变现有运行期身份契约。

预探测结束后，四个专用挂载和所有预探测夹具已清理；原有 NFS 挂载保持不变。

## 验证方法

执行器在每种配置上：挂载 → 独立进程 before → 完整卸载 → 挂载临时 tmpfs 占用释放的设备号 → 重新挂载原来源 → 独立进程 after → 清理。快照和断言状态保存在挂载外；两个扫描进程之间没有存活句柄。

工具验证 Stat、List/Traversal、Describe 的证据一致和快照往返；跨目录改名及改名后内容变化；独立传输变更内容并读回；全扫描一对一候选匹配和硬链接歧义拒绝；同 token 不同类型的消费者谓词阴性测试；不同数据集域和默认候选策略拒绝；源替换后的 read / observe_bound 拒绝；稳定 inode 环境中 Direct 硬链接别名必须在 Prepare 阶段以 Conflict 拒绝且不截断源。

`noserverino` 只启用 CandidatesOnly；不使用不可靠的 inode 尝试对真实硬链接执行可能截断源的 Direct 写入。NFS 的 missing-btime 路径必须成功正常读、拷贝、快照和保护原有运行期检查。

## 完整矩阵结果

实际执行于 2026-10-26 07:06:26 UTC 起，四种配置、8 个独立 before/after 进程全部退出 0。原始日志见 [2026-10-26-local-rename-evidence.log](2026-10-26-local-rename-evidence.log)。

| 实际内核挂载 | 重挂前 → 后 dev | birth time / 候选 | 跨重挂结果 | 运行期检查 |
| --- | --- | --- | --- | --- |
| CIFS SMB 3.1.1 `serverino` | 91 → 92 | 可用；16 字节 token 不变 | 已显式启用；唯一候选可匹配 | 替换源拒绝；Direct 硬链接在 Prepare/Conflict 拒绝 |
| CIFS SMB 3.1.1 `noserverino` | 91 → 92 | 可用；inode 部分变化，token 不同 | CandidatesOnly 拒绝匹配；安全回退 | 替换源拒绝；硬链接的 inode 不同，Direct 别名保护不作保证 |
| NFSv3 | 91 → 92 | 不支持；`None` | 按路径回退，无伪造时间 | 替换源拒绝；Direct 硬链接在 Prepare/Conflict 拒绝 |
| NFSv4.1 | 92 → 93 | 不支持；`None` | 按路径回退，无伪造时间 | 替换源拒绝；Direct 硬链接在 Prepare/Conflict 拒绝 |

每种配置均通过列举、Stat、Describe、ObservedEntry 快照往返和变更内容传输读回。CIFS `serverino` 的普通跨目录 rename 保持候选不变；加入硬链接后该候选出现两次，完整扫描消费者拒绝配对。`noserverino` 即使当次普通 rename 的候选相同，仍因未授权策略拒绝使用；完整重挂后实际观察到候选变化。

所有配置的旧运行期 identity key 在 dev 变化后均不同，直接证明不能拿现有 `dev + ino` 作为本环境跨重挂的长期改名键。`None == None` 只表示两次均缺少候选，工具仍要求 `Some` 才可匹配，NFS 从不因此通过匹配。

## 复跑与构建

```bash
timeout 900 cargo build --example local_rename_evidence
# 将可执行文件与执行器传到经批准、具有 mount 权限的实验室客户端。
# 凭据文件必须在仓库外，内容符合 mount.cifs credentials 格式。
CIFS_CREDENTIALS=/private/cifs.credentials timeout 600 \
  bash tests/lab/run-local-rename-evidence.sh /path/to/local_rename_evidence
```

实测二进制 SHA-256：`cb4ccd45dc51146bb6f4b3a709b39222b667884f44a7fa5a68e563dcb38a2046`。本次使用本地 debug 构建后传入 VM101，无需在远端编译或改服务器。

## 结论和限制

本次验证支持在本套 ONTAP 单一共享、稳定 serverino、显式可信数据集域的部署中启用启发式改名候选；不能推导为所有 CIFS/NFS 服务端的永久文件身份保证。未模拟服务器 inode 重用、创建时间篡改、NTFS tunneling、集群故障切换或跨文件系统 inode 冲突；同 token 不同类型为消费者谓词的合成阴性案例，不声称真实文件系统发生了该碰撞。

NFSv3 / v4.1 本地挂载环境本次真实不提供创建时间，因此正确行为是保留路径同步能力，不能承诺获得重命名优化。本方案没有引入逐文件 `statx` 或持久 handle 查询；本次没有执行性能基准，也不把功能验证解读为吞吐提升。

创建时间为真实 epoch 0、负数和溢出的边界由模型/时间转换单元测试负责；本次没有修改远端创建时间伪造这些边界。

## 清理

执行器四次均报告 `cleaned=true full_unmount=true separate_processes=true`；其私有远端共享目录、挂载点、tmpfs 和挂载外临时状态已删除。结束后另行核对现存 CIFS/NFS 挂载仅为本来就有的 `/mnt/nfs_src`；临时凭据与远端二进制/执行器随后一并删除。

## 仓库检查

最终 `make ci` 退出 0：格式、架构依赖守卫、harness 契约、全部 targets 的 Clippy、
workspace 测试、示例构建及 e2e-local 全部通过。库测试 1864 passed、0 failed、9 ignored；
集成测试和 doctest 均通过，外部服务测试保持原有显式 ignored 规则。
新增证据、策略、时间边界、快照及 Local 角色一致性测试均通过。
另通过 `git diff --check` 与挂载执行器 `bash -n`。

首轮检查发现并修正了测试命名、三个测试 future 的大小阈值，以及两处仍写死 v7 的快照断言；
最终重跑为上述全部通过结果。这些修正未改变已经实测通过的 Linux 挂载实现。
