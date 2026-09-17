# data-mover-rs

Storage abstraction layer supporting Local, NFS, S3, HDFS, and SMB/CIFS backends.
SMB/CIFS is available only through the role-based `storage::connect_backend` API;
it has no `StorageEnum` variant and no URL form.

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
| SMB/CIFS | No URL. Use `connect_backend(BackendConfig::Cifs(CifsBackendConfig { .. }))` |

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
integer in `1..=24`; an invalid value fails storage creation with a configuration
error instead of being silently ignored.

| Backend | Read default | Write default |
|---------|--------------|---------------|
| Local | `4` | `8` |
| NFS | `4` | `8` |
| SMB/CIFS | `8` | `8` |
| S3 | `4` | `5` |

`DATA_MOVER_INFLIGHT=8`, `16`, or `24` sets both read and write depths.
`DATA_MOVER_READ_INFLIGHT` and `DATA_MOVER_WRITE_INFLIGHT` override each direction.
Use `DATA_MOVER_<BACKEND>_READ_INFLIGHT` or
`DATA_MOVER_<BACKEND>_WRITE_INFLIGHT` for a backend-specific override, where
`<BACKEND>` is `LOCAL`, `NFS`, `CIFS`, or `S3`. Each direction is resolved
independently: backend-specific direction, global direction, shared
`DATA_MOVER_INFLIGHT`, then protocol default. The Rust API can override the resolved pair with
`TransferConcurrency` and `StorageEnum::with_transfer_concurrency` (or the
corresponding concrete-adapter builder).

The upper bound is intentional: lab measurements across the complete Local,
NFSv3, NFSv4.1, and S3 copy matrix showed that increasing inflight from 8 to 16
provided only a small aggregate throughput gain while CPU and peak memory grew
substantially. The configurable limit is now 24 for targeted single-stream measurements;
protocol defaults remain unchanged. Values above 24 are rejected rather than clamped so the effective
configuration never differs silently from what the operator requested.

Role-based callers can resolve the same environment rules with
`TransferConcurrency::from_env(BackendKind::Nfs, defaults)` and use that pair
for their backend configuration and `InflightLimits`. Explicitly supplied
transfer limits are not silently overwritten by environment settings.
The `nfs_mount_comparison` example does this for both transport paths.

The defaults are the recommended general-purpose settings. For a high-latency
NFS path with sufficient server session capacity, `read=8` and `write=16` can
improve throughput. S3 normally reaches its throughput knee around 4 to 8, and
Local reads normally reach it around 4; raising them to 16 is generally not a
good CPU/memory tradeoff. The role-based CIFS backend defaults to 8/8 and reads
the same `DATA_MOVER_CIFS_*` variables through `TransferConcurrency::from_env`.

### QoS semantics

`QosManager` limits source-side migration traffic. Payload bytes are charged
once when they are read from the source; destination writes do not charge the
same bytes again. Consequently, copying 1 GiB consumes 1 GiB of bandwidth
quota rather than 2 GiB.

```rust,ignore
// Soft average 100 MiB/s, hard peak 150 MiB/s. After 500 ms idle the
// derived credit capacity is (150 - 100) × 0.5 = 25 MiB.
let qos = QosManager::try_new_with_limits(
    "100MiB/s",
    "150MiB/s",
    std::time::Duration::from_millis(500),
    2 * 1024 * 1024,
    Some(500),
)?;

// Compatibility shorthand: soft and hard are both 8 MiB/s, with a 1 MiB
// source request cap. There is no soft burst credit in this strict form.
let strict = QosManager::try_new_with_burst("8MiB/s", 1024 * 1024, Some(500))?;
```

The soft limit is a Token Bucket refill rate. It controls long-term average
bandwidth and permits idle capacity to be used later. The hard limit is a Leaky
Bucket schedule with no accumulated hard credit and no catch-up after a missed
slot. Soft credit starts at zero and is capped at
`(hard - soft) × peak_duration`; every soft permit must still pass through the
hard schedule. `hard < soft` and a zero maximum IO size are invalid.

Hard traffic is shaped in 10 ms quanta. The actual non-streaming source request
is `min(remaining chunk, hard rate × 10 ms, max_io_bytes)`, rounded up to at
least one byte. A large storage chunk is therefore split before the real source
read. The packetized guarantee is that a measured interval cannot exceed
`hard_rate × interval` by more than one shaping quantum; application IO cannot
provide a meaningful zero-byte tolerance at an arbitrarily small time scale.

`try_new(bandwidth, peak_rate, iops)` remains as a compatibility shorthand. It
uses `bandwidth` as the soft rate, `bandwidth × peak_rate` as the hard rate and
a one-second peak duration. New callers should prefer
`try_new_with_limits` because its soft rate, hard rate and peak duration are
explicit.

Bandwidth permits and IOPS permits represent different resources. Bandwidth is
charged for payload slices; IOPS is charged once for each real source protocol
request. All clones of one `QosManager` share the same schedules and counters.
IOPS uses the same dual-rate model as bandwidth: soft IOPS accumulates bounded
idle credit, while every operation must pass through a governor GCRA hard
leaky bucket with a one-operation burst. `--qos-iops` alone remains a strict
single-rate shorthand. Add `--qos-hard-iops` and
`--qos-iops-peak-duration-ms` to configure the sustained and peak rates
explicitly; soft credit starts at zero and is capped at
`(hard_iops - soft_iops) × peak_duration`.

| Source backend | Bandwidth pacing | IOPS accounting |
|----------------|------------------|-----------------|
| Local | Split positional reads at the QoS grant | One per actual read syscall |
| NFS | Split reads below the negotiated `rsize` when required | One per NFS READ |
| SMB/CIFS | Split reads below the negotiated server maximum when required | One per SMB READ |
| HDFS | Pace reads from the open HDFS file stream | One per client read |
| S3 | Keep block-sized Range GETs and pace/slice each response `ByteStream` | One per GET/Range GET, not per in-memory slice |

For S3, a small burst therefore does not create thousands of small Range GETs.
The SDK response body is consumed incrementally; stopping body polling applies
backpressure while zero-copy `Bytes` slices are forwarded to the copy pipeline.
HTTP/TCP and SDK buffers may already contain a limited amount of in-flight data,
so the hard guarantee applies at the application stream/pipeline seam. It does
not claim that every microsecond observed at the network interface is below the
configured rate.

Very small non-zero bursts are honored, but Local/NFS/SMB/HDFS may perform many
small reads and therefore achieve less throughput when an IOPS limit is also
enabled. For S3, bandwidth slicing does not increase request IOPS.

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

The ArchitectureReady DXN adapter therefore reports tags as unsupported
without issuing an extra storage request. It still exposes conditional native
transfer for a connected DXN pair. Multipart recovery always re-observes the
service manifest after reconnect; callers resume from that latest durable
prefix because already-issued inflight requests may settle after an earlier
checkpoint observation.

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

### SMB/CIFS connection

SMB/CIFS has no URL form. Connect it explicitly through the role-based factory:

```rust
use data_mover::model::{BackendIdentity, BackendKind};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, connect_backend,
};

let storage = connect_backend(BackendConfig::Cifs(CifsBackendConfig {
    signing_policy: CifsSigningPolicy::default(), // WhenRequired
    guest_policy: CifsGuestPolicy::default(),     // Deny; AllowUnsigned for guest/anonymous shares
    server: "nas01".into(),
    share: "shared".into(),
    username: "admin".into(),
    password: "password".into(),
    root: Some("data/2024".into()),
    identity: BackendIdentity::new(BackendKind::Cifs, "nas01/shared")?,
})).await?;
```

Credentials are NTLM only. Anonymous or guest shares need
`guest_policy: CifsGuestPolicy::AllowUnsigned`: servers map unknown or password-less users
to a guest session that cannot be signed, so `Deny` (the default) refuses it. With
`AllowUnsigned` an empty `username` sends a placeholder identity for the server's guest
mapping. The legacy `smb://` URL and `smb2_only` option were removed together with the
legacy `CifsStorage` path (#150).
`examples/cifs_mount_comparison.rs` and `tests/cifs_policy_contract.rs` read
`CIFS_REAL_SERVER`, `CIFS_REAL_SECOND_SERVER`, `CIFS_REAL_SHARE`, `CIFS_REAL_USER`,
and `CIFS_REAL_PASS`.
