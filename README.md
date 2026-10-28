# data-mover-rs

Storage abstraction layer supporting Local, NFS, S3, HDFS, and SMB/CIFS backends.
All backends use `storage::connect_backend` and typed storage roles.
SMB/CIFS can be configured from an `smb://` URL or explicit config fields.

For replacements for the removed legacy APIs, see the
[migration inventory](docs/design/legacy-api-removal.md).

## Storage URL Formats

| Backend | URL Format |
|---------|------------|
| Local | `/path/to/dir` |
| NFS v3 | `nfs://server:port/export/path:/prefix?uid=1000&gid=1000` |
| HDFS | `hdfs://namenode:port/root` or `hdfs://nameservice/root` |
| S3 | `s3://access_key:secret_key@bucket.host:port/prefix` |
| S3 (TLS) | `s3+https://access_key:secret_key@bucket.host/prefix` |
| StorageGRID | `s3://access_key:secret_key@bucket.host:port/prefix?compat=sg` |
| DXN | `s3://access_key:secret_key@bucket.host:port/prefix?compat=dxn` |
| ONTAP S3 | `s3://access_key:secret_key@bucket.host/prefix?compat=ontap` |
| SMB/CIFS | `smb://username:password@server[:port]/share[/root]` |

Non-local URL paths accept raw UTF-8 or percent-encoded names. For example, `测试` and
`%E6%B5%8B%E8%AF%95`, `a b` and `a%20b`, or `100%` and `100%25` resolve to the same name.
Path names are decoded once at the backend boundary. Encode a literal `#` or `?` in URL paths;
S3 access and secret keys remain raw URL credential text, while SMB credentials may be encoded.
A literal `%20` in a path name is written `%2520` in its URL.

S3 URL options (the query; leave one out for its default):

| Option | Values | Default |
|--------|--------|---------|
| `compat` | `sg`, `dxn`, `ontap` | standard S3 |
| `tls_verify` | `false` (only on `s3+https://`) | certificates verified |
| `bucket_type` | `nas` (only with `compat=ontap`; not implemented yet, refused at connect) | object bucket |

Writing a default (`compat=standard`, `tls_verify=true`, `bucket_type=object`), an unknown,
repeated or empty option, `tls_verify` on `s3://` or `bucket_type=nas` without `compat=ontap`
is a `UrlParseError`. Schemes other than `s3://` and `s3+https://`, including older
`s3+sg://`, `s3+dxn://` and `s3+hcp://` spellings, are unsupported.
Options are not part of the endpoint identity.

`s3+https://` verifies the server certificate against the system roots, which honour
`SSL_CERT_FILE` / `SSL_CERT_DIR` (trust a private CA there). For a self-signed endpoint add
`tls_verify=false`: the certificate is then not verified, and every connection logs a warning.

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
| Local (example baseline; typed configuration is explicit) | `4` | `2` |
| NFS | `4` | `8` |
| SMB/CIFS | `8` | `8` |
| S3 | `2` | `4` |
| HDFS | `4` | `1` |

`DATA_MOVER_INFLIGHT=8`, `16`, or `24` sets both read and write depths.
`DATA_MOVER_READ_INFLIGHT` and `DATA_MOVER_WRITE_INFLIGHT` override each direction.
Use `DATA_MOVER_<BACKEND>_READ_INFLIGHT` or
`DATA_MOVER_<BACKEND>_WRITE_INFLIGHT` for a backend-specific override, where
`<BACKEND>` is `LOCAL`, `NFS`, `CIFS`, or `S3`. Each direction is resolved
independently: backend-specific direction, global direction, shared
`DATA_MOVER_INFLIGHT`, then protocol default. Use `TransferConcurrency::from_env`
to resolve the pair, then apply it to backend configuration. A transfer uses its source's
read window and its destination's write window independently; callers do not supply inflight limits.

The upper bound is intentional: lab measurements across the complete Local,
NFSv3, NFSv4.1, and S3 copy matrix showed that increasing inflight from 8 to 16
provided only a small aggregate throughput gain while CPU and peak memory grew
substantially. The configurable limit is now 24 for targeted single-stream measurements;
protocol defaults remain unchanged. Values above 24 are rejected rather than clamped so the effective
configuration never differs silently from what the operator requested.

Role-based callers can resolve the same environment rules with
`TransferConcurrency::from_env(BackendKind::Nfs, defaults)` and use that pair
for their backend configuration. Explicit backend concurrency overrides the environment.
The `nfs_mount_comparison` example does this for both transport paths.

These defaults are backend baselines, not universal optima. The decoupled pipeline's DXN and
StorageGRID 40/128 MiB sweeps support S3 2/4: lower CPU/RSS than 4/4, with higher throughput.
Local's example baseline is 4/2 after warm-XFS 128 MiB/1 GiB CPU/RSS measurements;
the factory still requires explicit windows. NFS 4/8 remains a compromise between FAS and
openfs; other baselines are retained pending broader workload coverage.
One-sample S3 1 GiB checks favor 4/4 slightly in throughput, so 2/4 is a resource/throughput
tradeoff, not a claim of higher throughput at every size. Historical symmetric-depth tests do not establish the
best independent read/write combination. The role-based CIFS backend defaults to 8/8 and reads
the same `DATA_MOVER_CIFS_*` variables through `TransferConcurrency::from_env`.

The source emits an ordered byte stream into a bounded four-chunk channel. A full destination
write window stops consuming input, so channel backpressure eventually stops source prefetch.
Each backend bounds its own buffers, including completed reads waiting for their predecessors
and payload retained for durability or retries. The channel bounds queued chunks, not total RSS.
Read and write chunk sizes need not match. S3 aggregates shared `Bytes` fragments into exact
multipart boundaries and replays those fragments for retries without merging payload buffers.
Native copies manage their own backend request window and do not use the payload channel.

`TransferRequest::new(source, path, destination, cancel)` and its observation constructor no
longer take `InflightLimits`; the expert source/destination constructors and `TarRequest` follow
the same rule. Local backend configuration accepts independent optional `read_chunk_bytes`
and `write_chunk_bytes`; NFS accepts an optional explicit `transfer_concurrency`. These are
backend settings shared by transfers, while each transfer retains its own bounded queues.
Default tuning and its workload limitations are documented in
[the backend defaults research](docs/research/backend-transfer-defaults.md).

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
s3://access_key:secret_key@bucket.host:port/prefix?compat=sg
s3+https://access_key:secret_key@bucket.host/prefix?compat=sg
```

For clients created with `compat=sg`, data-mover removes the
exact `x-id` query parameter immediately before SigV4 signing. Other query
parameters and their encoded bytes are preserved. The option applies to both
bucket listing and normal S3 data operations and does not change credentials,
path-style addressing, or general checksum configuration.

Clients without `compat=sg` never remove `x-id`,
so standard S3 and StorageGRID endpoints can safely coexist in one process.
There is no environment-variable override.

Every profile, standard included, adds a body-matching, SigV4-signed
`Content-MD5` to multi-object delete requests. The AWS SDK's own checksum for
that request is `x-amz-checksum-crc32`, which older S3 implementations ignore
and then reject the request: older StorageGRID releases, MinIO
`RELEASE.2023-03-20` and Ceph RGW Octopus (DXN) all answer `MissingContentMD5`.
Every S3 implementation accepts `Content-MD5`.

### DXN compatibility

Select DXN compatibility per endpoint with `?compat=dxn` (on `s3://` or
`s3+https://`). Like every profile, DXN clients add the signed
`Content-MD5` to multi-object delete requests. Unlike StorageGRID compatibility,
DXN compatibility does not remove the AWS SDK's `x-id` query parameter.

DXN's HTTPS endpoint presents a self-signed certificate: trust its CA through
`SSL_CERT_FILE`, or add `tls_verify=false` on a trusted network.

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

### Integrity-check inputs

`integrity::compare` accepts an `IntegrityRequest`. Its `source_path` and
`destination_path` can each be a `StoragePath`, `ObservedEntry`, or
`Box<ObservedEntry>`. A path compares the current entry. An observation is
revalidated against its connected storage before comparison; a stale or foreign
observation fails on that side. An observed versioned entry selects the version
that was current when it was observed.

`IntegrityRequest.plan` independently selects native ACL and xattr comparisons.
Its default omits both. Owner, group, and mode are selected automatically from
the facts each backend can supply: each comparable fact is checked, and each
unavailable or incompatible fact is reported as ignored. Permission observation is owned by
`Metadata::permissions` and remains bound to the selected version. Numeric and named
principals are never mapped implicitly. Kind, size, and modification time are
checked independently, while content depends on
`IntegrityOptions.mode`. Requesting ACLs or xattrs on a backend that cannot
report them adds an `IntegrityIgnored` fact. A DXN NFSv4 endpoint can use
`IntegrityPlan::default()` to avoid requesting its unsupported ACL and xattr
families.

### Integrity-check timestamp options

`IntegrityOptions` controls modification-time comparison.
The library defaults to `MtimePrecision::Coarsest`, which treats modification
times in the same tick of the coarser observed precision as equal. Set
`mtime_precision` to `MtimePrecision::Exact` to compare observed nanoseconds.
The default `mtime_tolerance` is `Duration::ZERO`; either precision mode also
accepts an absolute timestamp difference up to and including the configured
`Duration`.

Strict ACL checks are explicit: `CopiedMetadataRequest::with_required_acl()` requires native
preservation; `with_acl_mapping()` permits the proven POSIX/NFSv4 regular-file access subset
under shared numeric identities and group resolution. NFSv3 POSIX ACLs require the actual
NFSACL extension. `IntegrityPlan::with_required_acl()` and `with_acl_equivalence()` never
ignore missing required ACLs. `IntegrityReport::acl_scope()` names the proven scope.
See the [four-way validation report](docs/reports/2026-10-28-nfs-acl-four-way-validation.md).

The `storage_integrity_check` example defaults to exact precision. Pass
`--mtime-auto-precision` to use the coarser observed precision, and
`--mtime-tolerance-ns` to set an inclusive tolerance in nanoseconds (default `0`).
The example checks owner/group/mode; pass `--acl` or `--xattrs` to
request those optional families. Content checks use each backend's own read
chunk and concurrency limits.

### SMB/CIFS connection

Parse an `smb://` URL before connecting. The share and root path are percent-decoded once;
the URL may include percent-encoded credentials. The default signing and guest policies can be
changed on the returned config:

```rust
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy, connect_backend,
};

let mut config = CifsBackendConfig::from_url("smb://admin:password@nas01/shared/data/2024")?;
config.signing_policy = CifsSigningPolicy::default(); // WhenRequired
config.guest_policy = CifsGuestPolicy::default();     // Deny; AllowUnsigned for guest shares
let storage = connect_backend(BackendConfig::Cifs(config)).await?;
// storage.identity().stable_id() == "smb://nas01/shared/data/2024"
```

Every connected storage's identity is its canonical endpoint, derived by data-mover — protocol,
address, share or bucket and prefix, never credentials (`nfs://…`, `smb://…`, `s3://…`, `hdfs://…`,
`file:///…`). `data_mover::storage::endpoint_identity(&config)` derives the same value without
connecting. See `docs/adr/0006-destination-resident-recovery.md`.

Credentials are NTLM only. Anonymous or guest shares need
`guest_policy: CifsGuestPolicy::AllowUnsigned`: servers map unknown or password-less users
to a guest session that cannot be signed, so `Deny` (the default) refuses it. With
`AllowUnsigned` an empty `username` sends a placeholder identity for the server's guest
mapping. `smb2_only` and the legacy `CifsStorage` path remain removed (#150).
`tests/lab/tools/cifs_mount_comparison.rs` and `tests/cifs_policy_contract.rs` read
`CIFS_REAL_SERVER`, `CIFS_REAL_SECOND_SERVER`, `CIFS_REAL_SHARE`, `CIFS_REAL_USER`,
and `CIFS_REAL_PASS`.
