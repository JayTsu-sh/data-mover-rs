# data-mover-rs

Storage abstraction layer supporting Local, NFS, S3, and SMB/CIFS backends.

## Storage URL Formats

| Backend | URL Format |
|---------|------------|
| Local | `/path/to/dir` |
| NFS v3 | `nfs://server:port/export/path:/prefix?uid=1000&gid=1000` |
| S3 | `s3://access_key:secret_key@bucket.host:port/prefix` |
| S3 (TLS) | `s3+https://access_key:secret_key@bucket.host/prefix` |
| StorageGRID | `s3+sg://access_key:secret_key@bucket.host:port/prefix` |
| StorageGRID (TLS) | `s3+sg+https://access_key:secret_key@bucket.host/prefix` |
| DXN | `s3+dxn://access_key:secret_key@bucket.host:port/prefix` |
| DXN (TLS) | `s3+dxn+https://access_key:secret_key@bucket.host/prefix` |
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

### Transfer concurrency

Read and write pipelines are configured independently. Every value must be an
integer in `1..=16`; an invalid value fails storage creation with a configuration
error instead of being silently ignored.

| Backend | Read default | Write default |
|---------|--------------|---------------|
| Local | `4` | `8` |
| NFS | `4` | `8` |
| SMB/CIFS | `4` | `4` |
| S3 | `4` | `5` |

`DATA_MOVER_READ_INFLIGHT` and `DATA_MOVER_WRITE_INFLIGHT` set global values.
Use `DATA_MOVER_<BACKEND>_READ_INFLIGHT` or
`DATA_MOVER_<BACKEND>_WRITE_INFLIGHT` for a backend-specific override, where
`<BACKEND>` is `LOCAL`, `NFS`, `CIFS`, or `S3`. Each direction is resolved
independently: backend-specific variable, global variable, then protocol
default. The Rust API can override the resolved pair with
`TransferConcurrency` and `StorageEnum::with_transfer_concurrency` (or the
corresponding concrete-adapter builder).

### StorageGRID compatibility

Some StorageGRID versions reject the AWS SDK's informational `x-id` query
parameter. Select StorageGRID compatibility per endpoint with its storage URL:

```text
s3+sg://access_key:secret_key@bucket.host:port/prefix
s3+sg+https://access_key:secret_key@bucket.host/prefix
```

`s3+sg://` uses HTTP. `s3+sg+https://` uses HTTPS and, like the existing
`s3+https://` scheme, skips certificate verification for trusted private
deployments with self-signed certificates.

For clients created from either StorageGRID scheme, data-mover removes the
exact `x-id` query parameter immediately before SigV4 signing. Other query
parameters and their encoded bytes are preserved. The scheme applies to both
bucket listing and normal S3 data operations and does not change credentials,
path-style addressing, or general checksum configuration. For multi-object
delete, it also supplies the legacy signed `Content-MD5` required by older
StorageGRID releases.

Standard `s3://`, `s3+http://`, and `s3+https://` clients never enable this
workaround, so standard S3 and StorageGRID endpoints can safely coexist in one
process. There is no environment-variable override.

### DXN compatibility

Select DXN compatibility per endpoint with `s3+dxn://` for HTTP or
`s3+dxn+https://` for HTTPS. DXN clients add a body-matching, SigV4-signed
`Content-MD5` to multi-object delete requests. Unlike StorageGRID compatibility,
DXN compatibility does not remove the AWS SDK's `x-id` query parameter.

Standard S3 clients remain unchanged. The HTTPS form skips certificate
verification and is intended only for trusted private deployments with
self-signed certificates.

#### Known DXN limitations

- Multipart rename uses S3 `UploadPartCopy`. DXN returns `InvalidArgument`
  when the source object key contains characters such as spaces, `%`, `?`,
  `#`, or non-ASCII text. Small-object rename through `CopyObject` is not
  affected. Data-mover currently reports the operation error and preserves
  the source object; it does not fall back to client-side streaming.
- DXN does not preserve the `x-amz-tagging` value supplied to
  `CreateMultipartUpload`. A multipart rename can therefore copy object data,
  content type, and user metadata while losing object tags. Callers that
  require tag preservation should not use multipart rename on DXN until a
  verified `PutObjectTagging` compatibility path is implemented.

These limitations have been reproduced against the current DXN lab endpoint.
They do not change standard S3 or StorageGRID behavior.

### Integrity-check timestamp options

Integrity checks compare modification timestamps exactly by default. Callers
that need to accommodate endpoints with coarser timestamp resolution can use
`IntegrityCheckOptions` with `MtimePrecision::Auto` and an explicit
`mtime_tolerance`. The tolerance is inclusive and represented as a Rust
`Duration`; existing `IntegrityCheck` methods retain exact comparison.

The `storage_integrity_check` example exposes the same behavior through
`--mtime-auto-precision` and `--mtime-tolerance-ms`.

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
