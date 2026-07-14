# issue-53 PR-A：修复 NFSStorage 整读路径短读静默截断

> 上游 issue：JayTsu-sh/terrasync-rs#53（spec 评论 VERDICT: REASONABLE）
> 分支：`fix/issue-53-nfs-read-short-read`（基于 origin/main cd6d671）

## 根因

`src/nfs.rs` `NFSStorage::read()`（~2541）对 `mount.read(fh, offset, count)` 单次
RPC、不检查返回长度、`count` 无上界；唯一调用方 `read_file()`（~1013）把整文件
size 直接透传。NFS 服务器单次最多返回协商的 rsize，超过即静默截断。

同文件已有正确参照：`read_data`（~2841）的 partial 短读串行补读循环、`write()`
（~2602）的按块切分循环；`local.rs::read`（~805）也是"循环读到 count 或 EOF"。

## 方案（维护者拍板：内联循环 + 同步纯函数）

- `read()` 拆两层：
  - `read_once(file, offset, count: u32)`：现 `read()` 原体（单次 RPC + stale
    句柄刷新重试），语义不变，仅改名 + count 类型收窄（顺带消除 >4GB 的
    `count as u32` 环绕截断）。
  - 新 `read()`：与 `read_data`/`write()` 一致的**内联**按块循环直调
    `read_once`——按 `calculate_chunk_size(count)`（即 effective block_size，
    mount 时已钳到 min(客户配置, rsize, wsize)）分段，短返回按实际字节推进
    继续补读；0 字节视为 EOF 提前返回已读字节。累积与零拷贝快路径（首段
    读满直返）留在内联循环里。
- 分段推进的纯逻辑抽成同步纯函数 `next_read_want(cur, end, block_size) ->
  Option<u32>`（无 async、无泛型），单测以镜像内联循环的同步驱动 helper
  做边界覆盖。
- **禁用形态**：泛型/异步闭包注入（`AsyncFnMut` 等）——异步闭包高阶生命
  周期的 auto-trait（Send）推导缺陷会破坏下游 `tokio::spawn`（app 层实测
  16 个 "implementation of Send is not general enough" 编译错误）。
- 语义不变项：文件比 size 短返回实际字节；错误路径原样透传。

### 联调返工记录

第一版把循环抽成 `read_full_by_blocks(..., impl AsyncFnMut)`：data-mover 自身
测试全绿，但 terrasync 侧 `[patch]` 联调时 `cargo test -p app` 编译失败（future
进 `tokio::spawn` 后 Send 推导崩，rustc 已知痛点）。经维护者拍板改为本方案，
分支历史已重写为最终形态。

## 步骤

- [x] 1. 建立本执行计划 → 验证：文件落盘、单独 commit
- [ ] 2. 实现：`read()` 拆 `read_once` + 内联分段循环 + `next_read_want`
      纯函数 → 验证：`cargo check` 通过，`read_file` 调用点无需改动
- [ ] 3. 单测（`src/nfs.rs` tests mod，同步驱动 helper）覆盖：
      size==块边界（单块/多块整倍）/ 略大于一块 / 多块+余数 / 单次短返回
      按实际字节推进补读 / EOF 提前 / 非零 offset / count==0
      → 验证：`cargo test` 新用例全绿
- [ ] 4. 回归：data-mover `cargo test` 全套 + `cargo clippy --all-targets`
      无新告警 + terrasync 联调 worktree `cargo test -p app` 编译通过且全绿
      → 验证：与 main 基线一致，无回归

## 约束

- 不跑 `cargo fmt`（存量文件未格式化，避免无关 diff）
- 不 push、不动 origin，交付本地就绪分支
