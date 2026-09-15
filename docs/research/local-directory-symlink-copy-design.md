# Local 目录与软链接复制：StorageEnum 映射到新架构

日期：2026-09-10

## 结论

不要把普通文件、目录、软链接暴露成三套公共复制入口。保留一个高层 entry/tree copy interface，内部按 `ObservedEntry::kind()` 选择实现。现有 `transfer()` 继续作为普通文件的深模块；目录和软链接走 Namespace 与 Metadata role，不进入字节 stage、checkpoint 或 inflight 管线。

底层只提供创建/复用目录和设置该目录 metadata 的接口，不维护子项完成状态，也不内置深到浅的 metadata 回写流程。调用者若要保留最终目录 mtime 或只读 mode，应根据自己的遍历流程选择调用 metadata 接口的时机。

## StorageEnum 当前行为

### 类型观察

Local 枚举使用 `symlink_metadata`，先判断 symlink，再判断 directory/file，因此不会跟随软链接。这个行为应保留。新模型的 `ObservedEntry` 已同时保存 `EntryKind::Symlink`、精确 link target 和同一次观察得到的 metadata，能够替代旧 `EntryEnum`。

### 目录

`StorageEnum::create_dir_all` 对 Local 仅调用 `LocalStorage::create_dir_all`。它没有应用 uid/gid/mode/mtime。示例复制程序先按路径顺序创建目录，再复制文件和软链接，但没有在所有后代完成后反向恢复目录 metadata。

因此旧方案不能作为目录 metadata 的完整语义。若创建目录时立即设置源 mtime，后续创建子项会再次改变父目录 mtime；若立即设置只读 mode，还可能阻止后续创建子项。

### 软链接

StorageEnum 的步骤是：

1. 源端 `read_symlink` 读取 link target。
2. 目标端直接在最终路径创建 symlink。
3. Local 使用 `lchown` 设置链接自身 uid/gid。
4. 使用 symlink no-follow timestamp 操作设置 atime/mtime。

可保留 `read_link + no-follow metadata` 的基本方向，但存在以下缺口：

- 直接创建最终链接，已有目标时没有统一的 `ExistingDestinationPolicy`。
- 创建与 metadata 更新之间失败会留下可见但 metadata 不完整的链接。
- Local 旧实现拒绝绝对 target 和包含 `..` 的 target，不能无损复制合法源软链接。
- 旧实现复制 atime，与当前 Local 明确省略 atime 的策略不一致。
- Linux symlink mode 没有可设置语义，不能把 `0777` 外观值当成已复制 mode。
- 取消只由调用者零散处理，没有明确的提交点。

## 新架构映射

### 一个高层入口

增加一个高层 tree/entry copy module，其 interface 接收源、目标、复制策略和取消令牌。它消费完整的 `ObservedEntry`，内部执行：

| EntryKind | 内部实现 |
|---|---|
| File | 调用现有 `transfer()`，保留 stage、inflight、Checkpointed checkpoint 和 rename |
| Directory | Namespace 创建或复用目录；Metadata role 可独立设置目录 metadata |
| Symlink | Namespace 读取/创建链接，Metadata 以 no-follow 方式更新链接自身 |

不增加公开的 `copy_file/copy_directory/copy_symlink` 三分接口。底层 role 仍按能力分工，高层 module 隐藏类型分派，但不接管目录遍历和 metadata 调用时机。

### 目录流程

1. 创建缺失目录，或复用同类型的已有目录。
2. 提供对指定目录应用 uid/gid/mode/mtime 的 Metadata 接口；单次调用内部维持 `chown -> chmod -> mtime` 的 mutation 顺序。
3. Checkpointed 对该 metadata 操作要求的目录变化执行持久化屏障；AtomicReplace 不增加持久化屏障。

该接口不等待或检查子项，不记录遍历完成证据，也不替调用者选择多个目录之间的处理顺序。子项创建会改变父目录 mtime，提前设置只读 mode 也可能阻止后续写入，这些是上层安排调用时机时必须处理的语义。

目录没有 per-entry data stage、checkpoint 或 rename publication。取消或中途失败时，已创建的目录树可能可见，这是选择非 staged tree copy 的直接语义；重试通过幂等创建和调用方再次应用 metadata 收敛。

当前 entry/tree copy 固定使用 Overwrite，不向目录暴露文件传输的 existing-policy 分支：

- 缺失：创建。
- 已存在且是目录：复用；调用者可通过独立接口设置 metadata。
- 已存在但不是目录：报告类型冲突，不隐式递归删除。

### 软链接流程

1. 使用同一次 `ObservedEntry` 中的精确 target；不读取 target 指向的对象。
2. 编译 metadata plan 后才执行目标副作用。
3. 所有 stat、ownership 和 timestamp 操作都必须 no-follow。
4. 应用链接自身 uid/gid 和 mtime；atime 省略，mode 为 NotApplicable，ctime 由内核更新。
5. 当前固定使用 Overwrite。先直接对最终路径调用 `symlink`；成功后应用 metadata。
6. 只有返回 `EEXIST` 时，才在最终父目录创建隐藏临时 symlink，设置 metadata，取消检查后 rename 到最终名称。该临时 namespace entry 没有 recovery identity、checkpoint 或 reusable bytes，不进入 staged destination role。
7. 直接创建后的 metadata 更新失败时，清理本次创建的链接并返回失败。并发修改必须由同一路径写入权约束，避免清理或 metadata 更新作用于其他写入者替换后的链接。
8. EEXIST 覆盖可以原子替换普通文件或软链接；已有目标是目录时报告类型冲突，不递归删除。
9. Checkpointed 在直接创建完成或 rename 后同步最终父目录；AtomicReplace 跳过该同步。

复制必须保留 link target 的原始字节和相对/绝对含义。是否拒绝绝对 target 或 `..` 应是显式安全策略，不能作为 Local adapter 的固定限制，否则 Local→Local 不是无损复制。

## 必要的最小接口变化

### Namespace

在 `NamespaceRequest` 中增加 `CreateSymlink { path, target }`，或增加语义等价的单个 namespace publication request。其语义固定为 Overwrite；Local implementation 隐藏直接创建与 `EEXIST` 后临时 sibling + rename 的选择。`ReadLink` 已存在。

Local transfer connection 需要提供 Namespace role；当前它明确声明 Namespace unsupported。

### Metadata

当前 `Metadata::apply(path, mutation, cancel)` 不包含 entry kind，Local 实现通过普通 open 会跟随 symlink。应把 kind/no-follow 语义加入同一个 apply request，而不是增加 `apply_symlink_metadata` 平行方法。

ownership 模型当前把 uid/gid/mode 绑定为一个 mutation。软链接需要 uid/gid，但 mode 为 NotApplicable。应让编译后的 numeric ownership mutation 能表达可选 mode；不能悄悄跳过 mode 后仍报告 Exact。

目录和软链接不调用 `StagedDestination::apply_metadata_batch`。可以在 Metadata role 内提供同样的有序批量实现，Local 用一个阻塞任务执行同一 entry 的 `chown -> chmod(若适用) -> mtime`，保持前一轮小文件 metadata 优化成果。

## 测试矩阵

Local→Local 至少覆盖：

- 目录：创建/复用与 metadata apply 分别可用；metadata apply 正确设置 uid/gid/mode/mtime，且不等待、枚举或检查子项。
- 软链接：相对 target、绝对 target、包含 `..`、悬空链接；复制过程从不读取 target 内容。
- 软链接 metadata：uid/gid/mtime 设置在链接自身，target 文件 metadata 不变；mode 报告 NotApplicable。
- 已有目标：同 target、不同 target、已有普通文件和已有目录；文件与软链接固定覆盖，目录复用同类型目标。
- 失败与取消：临时链接提交前取消不改变最终目标；rename 后失败必须报告 final changed；临时项能清理。
- Checkpointed/AtomicReplace：两者都没有 checkpoint；Checkpointed 恰好执行所需父目录同步，AtomicReplace 为零。
- 遍历中保留 `.data-mover-` 临时 namespace 隐藏规则。

## 实施顺序

1. 先让 Metadata apply request 携带 entry kind，并支持 symlink no-follow 与 optional mode。
2. 为 Local 提供 Namespace role及 symlink publication implementation。
3. 增加统一高层 entry copy module，文件委托现有 transfer，目录暴露独立 metadata apply，软链接走 namespace。
4. 完成取消、Overwrite、残留清理和 metadata 测试矩阵。
5. 最后加入目录/软链接性能门禁；它们没有数据吞吐指标，重点测每 entry 延迟和高并发整批耗时。
