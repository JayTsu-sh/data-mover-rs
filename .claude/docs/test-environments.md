# 真实测试环境总表

所有实验室真机环境的**唯一目录**：能测什么、我们能写到哪里、凭据在哪个文件、在哪台机器上跑、已知差异、批准状态与撤销办法。
不只给 P7 用，任何功能的真机验证都从这里选环境。

- **本文件不含任何密码 / 密钥。** 凭据只在两处：仓库外的 `~/.config/data-mover-rs/lab-environments.md`（mode 600，总表），
  以及仓库内 gitignored 的 `.claude/skills/**/.env`（skill 实际读取的）。按主机名查凭据，别按行号。
- 环境是**共享**的：只写下表“我们的写入范围”列出的位置；其他桶 / 导出 / 共享都是别人的。
- 新环境、新的写入范围、新的批准：同时更新本文件与 `lab-environments.md`。

## 1. 一览

| 类型 | 环境 | 协议 / 版本 | 从哪跑 | 我们的写入范围 | 选择方式 |
|---|---|---|---|---|---|
| CIFS | ONTAP FAS2750 SVM `lizy` | SMB 3.x（签名） | WSL | share `ontap_lisaauto_cifs` 下的测试目录；匿名测试 share `dm_anon_share` | `.claude/skills/e2e-cifs/.env` |
| NFS | ONTAP FAS2750 SVM `lizy` `/ontap_lisaauto_nfs` | **v3** | **10.131.9.10（root）** | 导出下的测试子目录 | `e2e-nfs/.env` 的主机 / 导出，URL 不带 `noresvport` |
| NFS | 同上 | **v4.0、v4.1** | WSL | 同上 | `e2e-nfs/.env`（`NFS_OPTIONS` 里 `version=4.0` / `4.1`） |
| NFS | DXN-2 `10.128.136.211:/nfv3_v4` | **v3、v4.0** | WSL | 只 `data-mover-test/` 子目录 | `e2e-nfs/dxn-v3/.env`、`e2e-nfs/dxn-v40/.env` |
| NFS | openfs m1-source（`10.131.5.221` / node1 直连 `192.168.131.214`） | v3 | WSL（慢）/ node1 | 已有导出下的子目录（不建新导出） | URL 见 §3.3 |
| SMB | openfs m1-source / m1-target | SMB | — | — | guest 登录目前失败（§3.3） |
| S3 | MinIO（VM 102） | S3 | WSL | 桶 `data-mover-test`；临时版本桶 `data-mover-*`（用完删光） | `.claude/skills/e2e-s3/.env` |
| S3 | ONTAP S3 对象桶（SVM `lizy`） | S3 | WSL | 桶 `data-mover-test`（无版本）、`data-mover-test-ver`（开版本） | `e2e-s3/ontap/.env`（`S3_BUCKET` 换桶） |
| S3 | ONTAP S3 **NAS 桶** | S3（平铺列举**无序**） | WSL | 桶 `data-mover-nas` | `e2e-s3/ontap/.env` + `S3_BUCKET=data-mover-nas` |
| S3 | RustFS 1.0.0（VM 105） | S3 | WSL（管理网 9001） | 桶 `data-mover-test`；临时 `data-mover-*` 桶 | `e2e-s3/rustfs/.env` |
| S3 | DXN-1（Ceph RGW Octopus） | S3 | WSL | 桶 `data-mover-test`（**永不开版本**）；临时桶（长期批准） | `e2e-s3/dxn/.env` + `S3_LOCATION=magnascale:tos_placement` |
| S3 | StorageGRID 11.5.0（独立租户 `data-mover-test`） | S3（明文 HTTP） | WSL | 桶 `data-mover-test`（无版本）、`data-mover-test-ver`（开版本）；租户内临时 `data-mover-*` 桶（用完删光） | `e2e-s3/storagegrid/.env`（`s3+sg://`，URL 参数化后 `?compat=sg`） |
| HDFS | Kerberos 集群（NameNode VM 301） | HDFS | **runner VM 102（逐次批准）** | 运行根 `/tmp/data-mover-nightly/<run>/` | `LAB_HDFS_*`（`tests/lab/README.md`） |
| 性能 | node1 + m1-source 53M 树 | NFSv3 | node1 | 只读遍历 | 见 §4 |
| Local | WSL 本机 | — | WSL | 临时目录 | `make ci` / e2e-local |

**多版本 / 非多版本**：S3 的 ONTAP、RustFS、DXN、StorageGRID 都要在两种桶上各验证一遍（用户 2026-09-26）：ONTAP 用 `data-mover-test-ver` /
`data-mover-test`；RustFS、DXN 用 `data-mover-test` + 临时版本桶。NAS 桶不支持版本。

## 2. 按环境

### 2.1 ONTAP FAS2750（ONTAP 9.19.1，共享集群）

- LIF：`10.128.61.200`（node 01）、`10.128.61.201`（node 02）；管理 REST `https://10.128.61.20`（admin）。SVM `lizy`。
- **CIFS**：share `ontap_lisaauto_cifs`，本地用户 `lisauser`；匿名测试 share `dm_anon_share`（SVM 的 `guest-unix-user=pcuser` 自 2026-09-16 起保留）。
  QUERY_DIRECTORY 支持 `FileIdFullDirectoryInformation`，CREATE 回 `QFid` → 身份 `StableWithinBackend`。任何 CIFS 改动必须在这里过 e2e-cifs。
  符号链接夹具只能经 NFS 建（REST files API 不收 symlink）。
- **NFS**：卷 `ontap_lisaauto_nfs`（junction `/ontap_lisaauto_nfs`，unix，export policy default）。v3 / v4.0 / v4.1 都开着。
  - `mount_rootonly` 开着：**WSL 经 NAT 改写源端口，v3 MOUNT 被拒**（`AUTH_TOOWEAK` / “could not parse response due to bad status”）→ v3 在
    10.131.9.10 上以 root、特权端口跑（URL 不带 `noresvport`）。**不要改 SVM 设置**。v4.x 从 WSL 用 `noresvport=true`。
  - 同一导出 v3 比 v4.x 多一条：卷根的 `.snapshot`（2026-09-26 查清，P7 C7）。ONTAP 的 NFSv3 READDIRPLUS 列出它（未查 / 未改 SVM
    设置），但不带描述所需的属性 / 句柄，所以 role 遍历把它报成一个 `Protocol` 子项失败（listed 5400 = 5399 + 1 失败，下钻不进去）；
    NFSv4 的 READDIR 不列它。按名字 LOOKUP 两边都能进（目前列出来是空的）。除它之外 v3 / v4.1 的路径集合逐行相同。
    legacy `walkdir` 用 GETATTR 补属性，把它当普通目录（3545 个目录）。对这类项补一次 LOOKUP 的修法已写好未提交：
    补上之后 v3 会进入 `.snapshot`（卷上有快照时就是每个快照的整棵树），要不要进、要不要与 v4 一致地排除，待定。
- **S3**（SVM `lizy` 的 object store server `s3lizy.com`，`http://10.128.61.200:80`，path-style），用户 `data-mover-test`（只在 `e2e-s3/ontap/.env`）：
  - `data-mover-test`（无版本，FlexGroup `fg_oss_1790405106`）、`data-mover-test-ver`（开版本，`fg_oss_1790405136`）：两个卷是建桶时 ONTAP 新建的
    （FAS2750_01_SSD_1 × 3 成员，thin 100 GB），后台重删 + 压缩。我们的用户**不能建桶**，脚本要用已有桶、跑完清空。
  - `data-mover-nas`：NAS 桶，FlexVol `dm_s3_nas`（`/dm_s3_nas`，20 GB thin，unix 777，重删 + 压缩；S3 用户映射到 `pcuser`）卷每小时 :05 打快照 `.snapshot/hourly.*`，GB 级测试对象要在下个快照前删掉，否则删除后空间要等快照轮转才回来）。
  - 该用户也能**读** `ontap-lisa-auto-bucket`（那个桶的原有策略放行）——**只写我们的三个桶**。
  - **NAS 桶限制**（2026-09-26 curl + staged 矩阵实测，详见 `storage-s3.md`「ONTAP S3 NAS 桶实测行为」）：
    - PUT 回复的 `ETag` 是 MD5，但 HEAD / GET / 列举 / 拷贝回复给的是**另一个不透明值**（不是 MD5，同内容每写一次都变）
      → 读回校验报 `Verify … (Conflict)`「replaced after publication」（误导，存储性质）；
    - 条件 GET（`If-Match` / `If-None-Match`）**501 `NotImplemented`** → 钉住的读（读回、`--read-back` 时源端 BLAKE3）不可用；
    - `CreateMultipartUpload` **501**（不支持分段，不是权限）；`ListBucketMultipartUploads` / `ListMultipartUploadParts` 策略不能放行（403）
      → > 8 MiB 的 Checkpointed / AtomicReplace 在 Prepare 报 `Observe … (PermissionDenied)`，Direct > 8 MiB 报 `Write … (Unsupported)`；
    - 同桶 CopyObject 可用（要 `Content-Length: 0`，`994828c` 起带），对象桶 ↔ NAS 桶的拷贝 501；单次 PUT > 8 MiB 可用（9 MiB 实测）；
      条件 PUT `If-None-Match: *` 可用。
    - `a65ae18` 起 501 报 `Unsupported, Permanent`（之前是 `Connectivity, Transient`）。能写的只有**不读回的 ≤ 8 MiB**，外加读回关的同桶原生拷贝。
      NAS 桶目前**只适合作列举（无序）源**；要让它可写 / 可校验需要设计决定（能力探测或端点选项，待定）。
    - 删对象后空目录会留下，清理要按目录后序逐个 DELETE `<dir>/`（非空目录的 DELETE 回 403）。
  - **ONTAP CopyObject 不带 `Content-Length` → 411 MissingContentLength**（对象桶与 NAS 桶都是）：`994828c` 起所有拷贝请求都带
    `Content-Length: 0`（legacy 与 role），ONTAP staged / versioning 矩阵原生行全部通过。CopyObjectResult 里的 `ETag` 不带引号（DXN 同）。
  - 已知行为：对象桶列举是 UTF-8 字节序（重删 / 压缩扫描前中后都是，11,101 个 key 实测）；**NAS 桶平铺列举是目录遍历序**（`/` 最小，
    NetApp KB CONTAP-675913），带 delimiter 逐层有序；CommonPrefix 计入 max-keys；满页后跟一个空末页；不回显 `Delimiter`；
    **带 `delimiter=/` 的 ListObjectVersions 可能只给 `NextKeyMarker` 不给 `NextVersionIdMarker`，下一页静默跳过该 key 余下的版本**
    （P7 设计现存问题 10 / F4）；versionId = base64(纳秒.v1.2|6.秒)，同毫秒也严格有序；NAS 桶不支持 ListObjectVersions。
  - 撤销：清空桶 → 删三个桶 → 删卷 `fg_oss_1790405106`、`fg_oss_1790405136`、`dm_s3_nas` → 删 S3 用户（UUID 在 `lab-environments.md` §7）。

### 2.2 DXN-1（S3，Ceph RGW Octopus 15.2.13，MagnaScale 定制）

- S3 `http://10.131.7.201:8184`（HTTPS 8182 自签），三个 RGW + keepalived。SSH（storadmin，uid 0）只读查看。
- 我们的 RGW 用户 `data-mover-test`（上限 5 个桶），桶 `data-mover-test`（**永不开版本**）。建桶必须带 LocationConstraint
  `magnascale:tos_placement`。**用户 2026-09-26 长期批准建临时桶**（用完删光：版本、标记、未完成上传，再删桶；检查建桶返回 200）。
- 别人的桶（约 75 个，含门禁桶 `lisa_auto/test-agent-s3-bucket-*`）不碰。
- 已知差异（详见 `.claude/docs/storage-s3.md`「DXN」）：UploadPartCopy 必须带 `Content-Length: 0`（否则 411）；版本桶里 CopyObject 不回
  `x-amz-version-id`；legal hold 下普通 DELETE 也 403；Object Lock 不自动开版本；重试已完成的 Complete 回 500；SSE-KMS 要 key id；
  3 个网关各自打时间戳（同 key 并发写入 `LastModified` 会倒挂）；只有 null 版本的 key 重开版本后写入再普通 DELETE 会丢掉 null 版本；
  PUT ≤ 5 GiB、part ≥ 5 MiB、DeleteObjects ≤ 1000 key 且要 Content-MD5；UploadPartCopy 拒含空格 / % / ? / # / 非 ASCII 的源 key。
- DXN-1 也有 NFS 导出（`jay_nfs` 等，v3 / v4.0 可挂），**未批准写入**。

### 2.3 DXN-2（NAS，10.128.136.211）

- NFS 导出 `/nfv3`（只 v3）、`/nfv3_v4`（v3 + v4.0）、`/share1`、`/along_nfs`，都是别人的；另开着 SMB（445）与 Web（80/443）。
- **我们只写 `/nfv3_v4/data-mover-test/`**（用户 2026-09-26 批准，目录已建）。环境文件把导出写成 `/nfv3_v4:/data-mover-test`，
  URL 根就落在子目录里，`nfs_opt_dir` 之类的写操作不会越界。v3 与 v4.0 都从 WSL 用 `noresvport=true` 可挂。
- 用法：`set -a; . .claude/skills/e2e-nfs/dxn-v3/.env; set +a; python3 .claude/skills/e2e-nfs/scripts/run.py`（进程环境覆盖 skill 的 `.env`），v4.0 换 `dxn-v40`。

### 2.4 PVE 实验室（10.131.9.x，宿主 10.131.9.20）

- 看 VM：`qm guest exec <id>`（101 / 401 / 402 无 guest agent，510–514 禁 exec）。VM 只收公钥 SSH。
- **MinIO**（VM 102 容器 `milvus-minio`，RELEASE.2023-03-20）`http://10.131.9.11:9000`：桶 `data-mover-test`；**`a-bucket` 是 Milvus 数据，绝不写**；
  `data-mover-test` 不开版本，临时版本桶可建（用完删光）。已知：ListMultipartUploads 只按精确 key；不支持条件 PUT；
  DeleteObjects 要 Content-MD5；upload id 列举里是裸 uuid；最新已是删除标记时再 DELETE 不新建标记；stale upload 24 h 过期。
- **RustFS**（VM 103 / 104 / 105，单节点单盘，均 1.0.0，旧二进制 `/usr/local/bin/rustfs-<旧版>.bak`）：S3 API 绑业务网 `10.10.1.12-14:9000`（WSL 不通），
  **控制台端口 `10.131.9.12-14:9001` 在管理网上也提供完整 S3 API**（WSL 可直连）。数据可丢弃（用户 2026-09-26）。
  - 测试环境：VM 105 `http://10.131.9.14:9001`，IAM 用户 `data-mover-test`（`s3:*` 仅 `data-mover-test` 与 `data-mover-*` 桶，可建临时桶），`e2e-s3/rustfs/.env`。
  - 已知：ListObjectVersions 每页先全部 DeleteMarker 再全部 Version（不交错 → 历史标 `Inferred`，P7 现存问题 11）；支持条件 PUT（412）；
    ListMultipartUploads 支持前缀；DeleteObjects / legal hold 不要 Content-MD5；建锁桶自动开版本；`GET ?partNumber=N` 不回 parts-count；
    **暂停版本的桶里 PUT 不回 `x-amz-version-id: null`**（HEAD 回；MinIO 两者都回）—— `958a3f9` 起连接时是 Suspended 就按 `"null"` 记，
    指针按 `?versionId=null` 删，不再留 null 删除标记；
    admin API `/rustfs/admin/v3/*` 明文 JSON。升级 / 下载：VM 访问 GitHub 约 90 KB/s，大文件在 WSL 下载后经 PVE 临时 HTTP 中转。
- **HDFS**（Kerberos，`HDFS.LOCAL`）：NameNode 10.131.9.30（VM 301），DataNode 10.131.9.31/32，namenode2 10.131.9.33（VM 304）。keytab 与主机名映射
  只在 **runner = VM 102**：快照 tarball → MinIO 临时 key → VM 里下载 → `su - github-runner` 用 setsid nohup 构建运行 → 清理。
  环境：`LAB_HDFS_CONFIG_DIR=/var/lib/data-mover-lab/hadoop`、keytab `/var/lib/data-mover-lab/terrasync-runner.keytab`
  （`hdfs/terrasync-runner@HDFS.LOCAL`）、`KRB5_CONFIG=/var/lib/data-mover-lab/krb5.conf`。**每次使用都要用户批准。**
  已知：NameNode 按**有符号字节序**排名字；列举文件回文件自身；缺目录 `FileNotFound`。

### 2.5 StorageGRID 11.5.0

- S3 `http://10.131.7.236:10443`（端口像 HTTPS，实为明文 HTTP），`e2e-s3/storagegrid/.env`；管理（Grid Manager API v3）`https://10.131.7.236`（root，凭据在 lab 文件）。
- **我们的独立租户 `data-mover-test`**（2026-09-26 用户批准建，account id 76970436579964471308，配额 100 GB，S3 能力；租户 root 凭据在 lab 文件，
  S3 密钥只在 `.env`）：桶 `data-mover-test`（无版本）、`data-mover-test-ver`（开版本）；租户内可建临时 `data-mover-*` 桶，用完删光。
  其他 7 个租户（xuyh、shenxm、zy1、c1、ccma、zhangpeng、shenxm1）与旧 `storage-bucket1` 都是别人的，不碰。
- 已知：**CreateBucket 必须带 `Content-Length: 0`**（否则 411）；ListMultipartUploads 带前缀可用；最新已是删除标记时再 DELETE 每次新建标记（不合并）；ListObjectVersions 拒绝空的 `version-id-marker`（400）；
  版本顺序实测（原始 XML 交错、同毫秒并列）见 P7 设计 §10.2.1。
- 撤销：清空并删除两个桶（及残留临时桶）→ Grid Manager 删除租户 `data-mover-test`。

## 3. 跑测试的机器

| 机器 | 用途 | 说明 |
|---|---|---|
| WSL（本机） | 默认；`make ci`、绝大多数 e2e | 经 NAT 出去：ONTAP NFSv3 不行；访问 GitHub 约 690 KB/s |
| 10.131.9.10（VM 101，ubuntu-24c） | ONTAP NFSv3；能到 RustFS 业务网 10.10.1.x | root；glibc 2.39，**WSL 编的二进制直接跑**；工作目录 `/root/dm-test/`（用完清理）；自带 rustc 1.95 旧，别在上面编 |
| node1（10.131.5.223） | 大树 / 性能（m1-source 53M） | RHEL9、glibc 2.34、32 核、200G 口 `ens1f0np0` = 192.168.131.126/24（2026-09-26 恢复）；**已无 gcc**，WSL 编 + glibc234 shim；上不了 GitHub；有他人登录，跑重负载前确认 |
| runner VM 102（10.131.9.11） | HDFS 真机（逐次批准） | 与 MinIO 同一 VM；只经 PVE `qm guest exec` 进入 |
| PVE 宿主 10.131.9.20 | 管 VM、临时文件中转 | 别在宿主上跑负载 |

### 3.3 openfs（m1-source / m1-target）

- `nfs://10.131.5.221/m1-source:/<子目录>?uid=0&gid=0&noresvport=true`（v3，无 mount_rootonly 问题）；node1 走 200G 直连 `192.168.131.214`。
- m1-source：均匀 20 叉 5 层，3,368,421 目录 / 53,368,420 条，性能基线见 memory `m1-source-nfs-lab` 与 P7 设计 §12.7。
- SMB `smb://10.131.5.221/m1-source`：guest 空密码 legacy 与 role-based 都登不上（客户端问题，待查）。

## 4. 通用规则

- 每个测试命令都包 `timeout`（用户规则）。
- 凭据只经 `-K <(printf 'user = "%s:%s"\n' ...)` / 0600 临时文件传给 curl，不打印、不进 commit、不进 memory。
- 杀测试进程按 comm 名（`ps -eo pid,etime,comm | awk '$3 ~ /^data_mover-/{print $1}' | xargs -r kill`），**不用 `pkill -f` 带自己命令行里的模式**。
- 临时桶 / 目录：带 `data-mover-` 前缀与时间戳，结束时删光并确认；删不掉的在报告里列出。
- 不改共享设备的全局设置（ONTAP SVM 选项、DXN 配置、node1 网络除非用户要求）。
- 各功能要求的矩阵（例如 P7 的 S3 五套、NFS 五种）写在各自的设计文档里，引用本文件的环境名。
