# Learned Rules (草稿层)

> 50 行硬上限。超了必须升级到 `.claude/rules/*.md` 或删除老的。
> 每条规则有 `verify` (怎么机械检查) + `source` (怎么学的) + `graduation` (下一步去哪)。

## L1 · CIFS 资源句柄统一走 `close_resource`
- verify: `grep -nE '\.close\(\)' src/cifs.rs | grep -v close_resource` 应空
- source: corrected 2x in S97/S99 (handle 漏 close 导致泄漏)
- graduation: → `.claude/rules/storage-driver-conventions.md` D2 (已升级)

## L2 · CIFS write_file 必须 `CreateDisposition::OverwriteIf`
- verify: `grep -nE 'CreateDisposition::Create' src/cifs.rs` 应空 (除 mkdir 路径)
- source: corrected 1x in S99 (Samba ACCESS_DENIED) commit 4051
- graduation: → 写到 `.claude/docs/storage-cifs.md` (已写)，再观察是否需要 rule

## L3 · CIFS rename 用 share-relative 路径不是 UNC
- verify: 改 cifs rename 时人工 review
- source: corrected 1x in S99 commit 4052
- graduation: → 写到 `.claude/docs/storage-cifs.md` (已写)

## L4 · NFS retry: EACCES/EPERM → deny_list
- verify: 改 nfs retry 时同步 `.claude/docs/error-taxonomy.md`
- source: corrected 1x in S100 commit 7eb3046
- graduation: → `.claude/docs/error-taxonomy.md` (已写)，观察一段后看是否升 rule

## L5 · S3 404 → FileNotFound (deny_list)
- verify: `grep -nE 'NoSuchKey|NoSuchBucket' src/s3.rs` 应映射到 FileNotFound/DirectoryNotFound
- source: corrected 1x in S100 commit 7eb3046
- graduation: → `.claude/docs/error-taxonomy.md` (已写)

## L6 · CIFS smb2_only 默认 true
- verify: 在 src/cifs.rs URL 参数默认值检查
- source: corrected 1x in S103 commit af0e017
- graduation: 已写到 `.claude/docs/storage-cifs.md`，观察

## L7 · CIFS 匿名 share 必须 anon=true + 空密码 + 无签名
- verify: 改 cifs auth 路径时人工 review
- source: corrected 1x in S103 commit 9b332aa
- graduation: 已写到 `.claude/docs/storage-cifs.md`

(规则余量：当前 7 条 / 上限 50 行近半，下次 /evolve 时考虑哪些已稳定升级到 rules/)
