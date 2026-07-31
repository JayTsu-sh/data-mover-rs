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

### S3 Timeout Environment Variables

S3 client timeouts can be configured in seconds. The values are read when an S3
client is created and apply to both bucket listing and normal data operations.
Unset, invalid, or zero values use the documented defaults.

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_CONNECT_TIMEOUT` | `10` | Connection establishment timeout in seconds |
| `S3_OPERATION_TIMEOUT` | `30` | Timeout for the complete S3 operation in seconds |
| `S3_READ_TIMEOUT` | `20` | Timeout for an individual socket read in seconds |

### StorageGRID compatibility

Some StorageGRID versions reject the AWS SDK's informational `x-id` query
parameter. Enable the compatibility mode only for an affected StorageGRID
endpoint:

```bash
export AWS_S3_STORAGEGRID_COMPAT=true
```

`1` and any ASCII case variation of `true` enable the mode. The variable is
read when an S3 client is created; unset values and all other values leave the
mode disabled.

When enabled, data-mover removes the exact `x-id` query parameter immediately
before SigV4 signing. Other query parameters and their encoded bytes are
preserved. The setting applies to both bucket listing and normal S3 data
operations. It does not change the endpoint, credentials, path-style
addressing, TLS behavior, or checksum configuration.

Do not enable this workaround for standard AWS S3 or compatible storage that
already accepts `x-id`. After changing the variable, recreate the data-mover
process or S3 client so that the new setting takes effect.

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
