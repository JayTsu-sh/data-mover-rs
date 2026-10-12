# Public usage examples

These examples use the public storage roles and transfer APIs. Run them with
`cargo run --example <name> -- <arguments>`; URL-based examples accept local roots too.

| Example | Demonstrates |
| --- | --- |
| `storage_copy` | Verified copy or guarded move, with optional source QoS |
| `storage_read` | Bounded reads tied to a described source, byte count and BLAKE3 |
| `storage_traverse` | Streaming traversal and explicit completion/failure handling |
| `storage_namespace` | Create, inspect and remove an isolated local directory tree |
| `storage_integrity_check` | Metadata or content comparison across endpoints |
| `storage_rename` | Rename within one connected namespace |
| `nfs_export` | NFS export discovery through `storage::list_exports` |

`support/` contains URL setup and verification shared with the lab tools.

Protocol contracts, fault injection, benchmarks, and the full role CLI live in
[`tests/lab/tools`](../tests/lab/tools). Their explicit Cargo example target names are
preserved so existing harness commands keep working. In particular, `transfer_resume`
emits JSON for the transfer matrices; `storage_resume` exercises a two-process interruption
and resume with a source-state guard. They test different contracts and remain separate.
