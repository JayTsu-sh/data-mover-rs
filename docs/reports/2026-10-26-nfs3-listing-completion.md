# NFSv3 READDIRPLUS entry completion

## Cause and ownership

The original traversal of `10.128.133.31:/nfs/lisa_long_nfs` listed two children,
`mig_hsxl` and `ccma`, but emitted neither: both were `Child Protocol` failures.
A read-only RPC capture confirmed that the successful READDIRPLUS response carried
`attributes_follow=0` and `handle_follow=0` for both children. This is a wire-level
omission, not evidence that nfs-rs discarded fields during decoding.

The installed nfs-rs 0.8.6 already represents these optional fields and exposes
handle-relative LOOKUP and GETATTR. No dependency upgrade, registry-source edits,
or implicit extra RPCs inside `readdirplus_page` are required for this fix.
The NFS backend completes the mandatory entry facts before publishing an observation.
The server-side reason for omitting fields remains unverified.

## Behavior

| Listed facts | Request before describing the child |
| --- | --- |
| Attributes and handle | None |
| Handle, no attributes | GETATTR of that handle |
| No handle, with or without attributes | LOOKUP of the basename using the current listing directory's handle |
| LOOKUP returns a handle without attributes | GETATTR of the returned handle |

A LOOKUP may observe a replacement: use its handle and attributes together rather
than combining the new handle with the old READDIRPLUS attributes. An empty LOOKUP
handle fails the child without a GETATTR of an empty handle.

Keep the existing NFSv3 `.snapshot` rule: only an entry originally missing attributes
is looked up for snapshot detection and hidden when it is a directory. An originally
described `.snapshot` stays an ordinary child. NFSv4 omissions retain their existing
failure behavior.

The existing listing recovery budget handles parent-handle staleness, connection
failures and throttling. Child NOENT/ACCES/stale errors retain their classification
and do not hide siblings. Before starting each child, check cancellation and replay
deduplication. Hidden transfer artifacts and unspellable names trigger no completion
RPC. In-flight completion may finish. No whole-directory buffering or global handle
cache insertion is introduced.

## Wire validation of the directory context

With configured root `/mig_hsxl`, the post-fix capture showed the following handle
hashes (Wireshark CRC-32, for comparison only; these are not source identities):

| Request | Directory/child handle hash |
| --- | --- |
| READDIRPLUS of `mig_hsxl` | `0x0b6340b7` |
| LOOKUP `data_1b1` | `0x0b6340b7` |
| READDIRPLUS of `data_1b1` | `0x0175ac9f` |
| LOOKUP `data_1b_file1` | `0x0175ac9f` |

Both LOOKUP replies were successful. The export root's hash was `0x0191eb07`,
distinct from both directory hashes. This verifies the MR !58 regression case:
child resolution uses the directory being read even below a configured sub-root.

## Automated and live validation

New unit tests exercise the actual READDIRPLUS drain/completion seam. Before the
fix, six of the initial seven tests failed. Coverage includes the request matrix,
nested names, replacement metadata, parent versus child failures, NOENT/ACCES,
throttling, cancellation, deduplication, artifacts and NFSv4 behavior.

The workspace test suite, strict Clippy for lib/tests/examples, formatting and the
architecture dependency guard passed. After the final boundary tests were added,
the listing test filter passed all 23 selected tests (including two S3 listing tests).
The live source
contract is opt-in and writes only to an automatically removed local temporary root:

```bash
NFS_REAL_URL='nfs://10.128.133.31/nfs/lisa_long_nfs?version=3&noresvport=true' \
NFS_LISTING_FILE='mig_hsxl/data_1b1/data_1b_file1' \
cargo test --locked --test nfs_listing_contract -- --ignored --nocapture
```

It enumerates the file's parent, passes the actual traversal observation to
`TransferRequest::from_observation`, copies to Local, and compares content and
permissions independently. It passed for the export root and for configured roots
`/mig_hsxl` and `/mig_hsxl/data_1b1` with adjusted relative paths. The existing file
is one byte; this validates enumeration-to-transfer correctness, not large-file throughput.

Other live listing results:

| Source, forced NFSv3 | Scope | Result |
| --- | --- | --- |
| `10.128.133.31:/nfs/lisa_long_nfs` | Root children, depth 1 | 2 entries, 0 failures; 2 directories depth-truncated intentionally |
| Same export | Cancel after 30 seconds, block order | 55,303 entries, 0 failures; cancelled, not a complete scan |
| Same export, path `mig_hsxl` | Complete subtree | 2 entries, 0 failures |
| Same export, configured root `/mig_hsxl/data_1b1` | Complete subtree | 1 entry, 0 failures |
| `10.128.133.31:/nfs/xuyh_nfs` | Cancel after 50 entries | 50 entries, 0 failures; cancelled, not a complete scan |
| `10.128.61.200:/nfstest` | Complete subtree | 15 entries, 0 failures; 1 hidden `.snapshot` |
| `10.128.136.211:/caohui` | Complete subtree | 6 entries, 0 failures |

Source shares were not modified. Large exports are not claimed fully scanned by
the depth-limited or cancelled checks. On servers that omit many entry fields,
the required extra LOOKUP/GETATTR requests add latency; bounded parallel completion
is a separate performance change if measurements justify it.
