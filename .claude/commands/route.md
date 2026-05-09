---
description: 调试 CLAUDE.md 路由表 — 输入场景描述，输出"应该读哪些文件"。
---

# /route <场景>

输入：自由文本场景描述 (例如 "改 CIFS smb2_only 行为" / "测 filter DSL")。

输出：CLAUDE.md 路由表给出的应读文件列表 + 推荐 agent + 推荐 skill。

## 步骤

1. 读 `/CLAUDE.md` 路由表段。
2. 读 `.claude/docs/README.md` 索引。
3. 关键词匹配场景描述：
   - "cifs" / "smb" → storage-cifs.md + src/cifs.rs
   - "nfs" / "v3" / "v4" / "retry" → storage-nfs.md + error-taxonomy.md + src/nfs.rs
   - "s3" / "multipart" / "404" → storage-s3.md + src/s3.rs
   - "local" / "rayon" / "win acl" → storage-local.md + src/local.rs + src/acl.rs
   - "storageenum" / "新增操作" / "改公开操作" → storage-enum-dispatch.md + src/storage_enum.rs
   - "filter" / "should_skip" / "三元组" / "glob" → filter-dsl.md + src/filter.rs
   - "walk" / "调度" / "work-stealing" → walk-scheduler.md + src/walk_scheduler.rs
   - "error" / "retry" / "taxonomy" → error-taxonomy.md + src/error.rs
   - "时间" / "filetime" / "nanos" → src/time_util.rs (单文件直读)
   - "acl" / "xattr" → src/acl.rs + storage-{nfs,local}.md
   - "qos" / "速率" → src/qos.rs
   - "checksum" / "blake3" → src/checksum.rs
   - "tar" / "打包" → src/tar_pack.rs
   - "url 红化" / "脱敏" / "日志" → src/url_redact.rs
   - "commit" / "pr" → conventions.md
   - "新加 skill" / "升级 rule" / "加 agent" → claude-onboarding.md
4. 关键词匹配 agent：
   - 跨多文件 → architect
   - commit 前 → reviewer
   - 改 backend → backend-specialist + backend 名
   - 改 filter.rs → filter-expert
   - 改 StorageEnum → dispatch-checker
5. 关键词匹配 skill：
   - "验证" / "测" / "跑" + backend → e2e-<backend>
   - "取消" → op-cancel
   - "filter dsl" → op-filter-dsl
   - "clippy" / "PR" → quality-clippy
   - "覆盖率" → quality-coverage
   - "审大文件" → quality-large-file-audit
   - "分派完整性" → quality-dispatch-coverage
   - "全套" → harness-run

## 输出格式

```
SCENARIO: <用户描述>

DOCS (按读取优先级)
====
1. .claude/docs/<file>.md  — <为什么读>
2. ...

CODE
====
- src/<file>.rs (LOC) — <为什么读>
- ...

AGENT
=====
- <agent name>: <什么时候调>

SKILL
=====
- <skill name>: <验证用途>

NEXT
====
1. 读 docs (步骤 1)
2. 调 agent (步骤 2)
3. 改代码
4. 跑 skill 验证
5. commit 前调 reviewer
```

## 例子

```
$ /route 修复 CIFS write_file 在 Samba 上 ACCESS_DENIED

SCENARIO: 修复 CIFS write_file 在 Samba 上 ACCESS_DENIED

DOCS
====
1. .claude/docs/storage-cifs.md — 已记录此陷阱 (CreateDisposition::OverwriteIf, commit 4051)
2. .claude/docs/error-taxonomy.md — CIFS 错误映射段

CODE
====
- src/cifs.rs (2246) — write_file 实现
- src/error.rs (164) — 确认 PermissionDenied 变体

AGENT
=====
- backend-specialist (传 cifs): 给出诊断 + fix 方向
- reviewer: commit 前自检

SKILL
=====
- e2e-cifs (需 .env 真 SMB 服务器): 验证 fix 在 Samba + Win server 都通

NEXT
====
1. 读 storage-cifs.md "CreateDisposition" 段
2. 调 backend-specialist (cifs)
3. 改 src/cifs.rs
4. 跑 make e2e-cifs
5. 调 reviewer
6. commit
```
