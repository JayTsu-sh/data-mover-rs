# data-mover-rs

Storage abstraction layer supporting Local, NFS, S3, and SMB/CIFS backends.

## Storage URL Formats

| Backend | URL Format |
|---------|------------|
| Local | `/path/to/dir` |
| NFS v3 | `nfs://server:port/export/path:/prefix?uid=1000&gid=1000` |
| S3 | `s3://access_key:secret_key@bucket.host:port/prefix` |
| S3 (TLS) | `s3+https://access_key:secret_key@bucket.host/prefix` |
| SMB/CIFS | `smb://user:password@host[:port]/share[/sub/path][?smb2_only=false]` |

### SMB/CIFS URL Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `smb2_only` | `true` | `true`：直接发 SMB2 NegotiateRequest，跳过 SMB1 多协议探测帧，速度更快。`false`：先发 SMB1 探测帧再升级到 SMB2/3，兼容不接受直接 SMB2 握手的老设备或防火墙。 |

**示例：**

```
# 默认（modern server，直接 SMB2 协商）
smb://admin:password@nas01/shared
smb://admin:password@nas01:445/shared/data

# 显式关闭（兼容老设备，走 SMB1 多协议探测帧）
smb://admin:password@nas01/shared?smb2_only=false

# 匿名访问（空密码）
smb://guest:@nas01/public
```

> 路径中的反斜杠 `\` 需 percent-encode 为 `%5C`。