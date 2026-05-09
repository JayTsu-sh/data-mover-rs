# Conventions

## Commit 消息

格式：`<type>(<scope>): <subject>`，type 必须是以下之一：

| Type | 用途 |
|---|---|
| `feat` | 新功能 |
| `fix` | bug 修复 |
| `refactor` | 不改行为只改结构 |
| `perf` | 性能优化 |
| `test` | 加 / 改测试 |
| `docs` | 文档 (含 .claude/docs/) |
| `chore` | 维护性改动 (依赖升级、formatting) |
| `ci` | CI / Makefile / hooks |

scope 可选，常见：`cifs` / `nfs` / `s3` / `local` / `filter` / `error` / `walk` / `harness`。

### 标题语言

- **中英文混排都接受**，近期偏中文 (如 commit `af0e017` "smb2_only URL 参数，默认跳过 SMB1 多协议探测帧")。
- 标识符、文件路径、commit hash 始终英文。
- 主语清晰：写"做了什么"不是"为什么改"。Why 写在 body。

### Body

- 解释为什么 (是 bug 现象、是性能数据、是协议要求)。
- 引用相关 commit / issue / 协议 RFC 段落。
- bug fix 必须说明触发场景和复现。

### bug fix vs refactor

**两个分开 commit**。理由：

- bug fix 需要 cherry-pick 到稳定分支。
- refactor 改 diff 巨大，混在一起 review 看不清 fix。

### Examples (好)

```
fix(cifs): 支持匿名 SMB 共享（空密码 + 无签名）

匿名 share (例如 NAS 的 public 目录) 用 guest:@host 或空密码访问，
旧代码强制要求 SMB 签名导致连接失败。本 commit 在 anon=true 时
关闭签名要求，并允许空密码。

测试: smbclient -L //NAS01/public -N 通过。
```

```
feat(s3): 404 → FileNotFound, 不再重试

之前 S3 GetObject 404 走 retry 路径，浪费资源。本 commit 在
backend 错误映射层把 NoSuchKey/HeadObject 404 直接映射为
StorageError::FileNotFound，retry taxonomy 决策为 deny_list。

也修了 NFS retry taxonomy: EACCES/EPERM → deny_list (之前是
delay_backoff)。详见 .claude/docs/error-taxonomy.md。
```

## PR

- 标题同 commit 标题规则。
- Body 含：
  - 背景 (为什么改)。
  - 改了什么 (大颗粒度)。
  - 测试 (跑了哪些 skill / make 命令)。
  - 风险 (兼容性 / 性能 / 行为变化)。
  - Doc 同步 (列具体改动 .claude/docs/*.md)。

## 代码风格

### Use 语句

- **集中在文件顶部**，按 std → external crate → crate:: 三段排序。
- 函数内不写 use (`use foo::Bar;` 在 fn 体内 — 禁)。
- verify: `grep -nE '^\s+use ' src/*.rs` 排除 mod tests 后应空。

### 路径深度

- 函数体内最深 2 段 (例如 `Foo::bar`)，更深必须 import。
- 反例：`std::collections::HashMap::new()` 在函数体里 — 应 `use std::collections::HashMap` 然后 `HashMap::new()`。

### 不允许

- `.unwrap()` / `.expect()` 在生产代码 — clippy 编译期 deny。
- `dbg!()` / `todo!()` / `unimplemented!()` 留下 — clippy warn。
- `unsafe` block 无 SAFETY 注释 — `[lints.rust] unsafe_code = "deny"` 已强制。

### 函数 / 文件大小

- 函数 ≤ 50 行 (clippy `too_many_lines` 当前 allow，但人工 review 注意)。
- 文件 ≤ 800 行 (新写的)。已超的 6 个文件是 backlog。

### Log 等级

| 等级 | 用途 |
|---|---|
| `error!` | 操作失败，用户需要知道 |
| `warn!` | 异常但已处理 (例如 retry) |
| `info!` | 业务级里程碑 (操作开始/结束) |
| `debug!` | 调试细节 (协议字段、retry 次数) |
| `trace!` | 极详细 (每个 RPC) |

**所有打印 URL 的日志先过 `url_redact::redact_url`** — 隐藏 ak/sk/密码。

## 测试

- Integration test 放 `tests/`，单测放 `src/*.rs` 内 `#[cfg(test)] mod tests`。
- 不允许 mock 网络 backend — 用 examples / skill 跑真实环境。
- 80% 覆盖率是目标但不强求 (跨 backend 真实环境难全自动化)。

## 文档

- `.claude/docs/*.md` — 项目知识，按场景路由。改代码同步改 doc。
- `.claude/rules/*.md` — 可验证规则 (grep / clippy)。
- `.claude/memory/*` — 学习记录，`/evolve` 升级链。
- 公开 API 必须有 rustdoc (`pub fn` / `pub struct`)。
- 内部 helper 不强制 doc，但复杂函数加。

## 命名

- 函数 / 变量：`snake_case`。
- 类型 / trait / enum：`PascalCase`。
- 常量：`UPPER_SNAKE_CASE` (例如 `COPY_PIPELINE_CAPACITY`，`GLOB_MATCH_OPTIONS`)。
- 模块：`snake_case`。
- backend 命名：`Local` / `NFS` / `S3` / `CIFS` (大写缩写)。
