# Crate workspace and migration to 0.4

The workspace separates independently testable engines and protocol adapters. The root
`data-mover` package remains the consumer facade. Internal packages are versioned together;
this change does not introduce independently released libraries or a dynamic plugin ABI.
The existing storage/transfer architecture still defines operation and durability semantics.

## Ownership and dependency direction

| Package | Owns | Production workspace dependencies |
| --- | --- | --- |
| `data-mover-model` | Paths, identities, observations, capabilities, snapshots and codecs | none |
| `data-mover-acl` | Pure ACL parsing, encoding, mapping and comparison | model |
| `data-mover-storage-core` | Storage roles, capabilities, adapter assembly, config parsing, endpoint identity, recovery contracts, shared bounded I/O runtime | model |
| `data-mover-metadata` | Metadata policy compilation, staged application and verification | model, ACL, storage-core |
| `data-mover-integrity` | Data/metadata consistency checking | model, ACL, storage-core |
| `data-mover-traversal` | Scanning, filters, traversal scheduling and scan checkpoints | model, storage-core |
| `data-mover-transfer` | Transfer planning, copy orchestration, recovery, QoS and verification policy | model, storage-core, metadata |
| `data-mover-backend-local` | Local filesystem operations, platform ACL access and persistence barriers | model, storage-core, ACL |
| `data-mover-backend-nfs` | NFS adapter and native NFS ACL conversion | model, storage-core, ACL |
| `data-mover-backend-cifs` | SMB/CIFS adapter and security descriptors | model, storage-core, ACL |
| `data-mover-backend-s3` | S3 client, compatibility profiles, object history and multipart lifecycle | model, storage-core |
| `data-mover-backend-hdfs` | HDFS client, authentication and filesystem adapter | model, storage-core |
| `data-mover` | Feature selection, legacy import paths and connector dispatch | optional engines and adapters, model, storage-core |
| `data-mover-test-support` | Cross-engine test connection helpers | adapters and storage-core; never a production engine dependency |

Functional engines do not import concrete backends in production. Each protocol SDK belongs
in its adapter. The facade also declares optional SDK dependencies used by existing protocol
examples and integration contracts; these follow the corresponding backend feature. Test
code may deliberately exercise several adapters. Such dev-dependencies do not imply that
compiling the engine as a library compiles those adapters.

The shared read budget, inflight admission, checkpoint hash state, pointer discovery and
bounded write progress live in storage-core because both engines and adapters use their
contracts. Moving them into transfer would force adapters to depend on an engine. They are
not a new public scheduling framework. Adapter support modules and `#[doc(hidden)]` items
are internal workspace integration surfaces and can evolve with the lockstep version.

## Features and builds

The default remains the complete product: `default = ["full"]`. Existing default-feature
consumers retain their `data_mover::{storage, transfer, traversal, metadata, integrity, model}`
paths. The root uses re-exports, so the facade and direct crate imports refer to the same Rust
types rather than duplicate wrappers or codec implementations.

| Feature | Effect |
| --- | --- |
| none | model, configuration, storage interfaces, shared runtime; no protocol SDK |
| `acl` | Expose `data_mover::model::acl` |
| `metadata` | Metadata engine and ACL |
| `integrity` | Integrity engine and ACL |
| `traversal` | Traversal engine and filter interfaces |
| `transfer` | Transfer engine, metadata, ACL, probes and tar helpers |
| `backend-local`, `backend-nfs`, `backend-cifs`, `backend-s3`, `backend-hdfs` | Compile the selected connector; NFS also exposes native ACL support |
| `full` | All of the above |

For a local-only application:

```toml
[dependencies]
data-mover = { path = "../data-mover-rs", default-features = false, features = [
    "backend-local", "traversal", "transfer", "integrity"
] }
```

Features are additive and select compiled capability. They do not change the meaning of a
scan or transfer policy. With multiple consumers, Cargo may enable the union of their
features. Runtime behavior still follows explicit configuration and connected capabilities.
Do not use a feature to select overwrite, metadata safety, resumability or verification policy.

All backend configuration variants and stable backend identity tags remain available even
when a connector is omitted. `connect_backend` returns a `BackendConnectError` with
`is_not_compiled() == true` before attempting configuration validation or I/O for that connector.
Endpoint identities can still be derived without a protocol SDK; Local identity derivation
requires the configured root to exist, as before. Parsing configuration does not promise that
a connector was compiled or that the remote service is reachable.

Public examples declare their necessary engine features. URL setup works with partial backend
builds; raw artifact inspection additionally requires the relevant backend. Multi-backend lab
programs and broad integration suites retain `required-features = ["full"]`. A local example:

```sh
cargo run --no-default-features --features backend-local,transfer,integrity --example storage_copy -- --help
```

The workspace uses resolver 3, edition 2024 and an explicit Rust 1.95 baseline. Third-party
versions, platform dependency definitions and lints are centralized in the root manifest.
The published NFS 0.9.1 dependency and pinned SMB/HDFS revisions from main are retained.
The former local NFS vendor snapshot is not part of this workspace. Child crates are `publish = false`; publishing the
workspace separately requires an explicit release/dependency-version policy first.

`default-members = ["."]` keeps ordinary root builds focused on the facade. **Use
`cargo test --workspace` for the complete suite**: library unit tests now live with their owner,
so plain `cargo test` no longer runs all of those tests. A workspace build can unify features
from several members and their dev-dependencies; it is not sufficient evidence for minimal
consumer builds. `tests/feature_matrix.py` checks each facade feature independently, verifies
normal/build dependency graphs, compiles minimal examples, and runs disabled-connector,
snapshot and public adapter contracts. CI runs this in addition to workspace Clippy and tests.
Legacy module filters used by test gates are mapped to their new owning package by
`tests/run_cargo_test.py`; its empty-selection guard remains active.

## Recovery and compatibility

Crate version 0.4 reflects the new opt-in build surface and package topology. In particular,
`default-features = false` now means a minimal interface build; applications that previously
used that spelling while relying on every module must select features explicitly.

Crate versions and persistence format versions are independent. This refactor retains:

- Backend kind names and wire tags, endpoint canonicalization and transfer binding inputs.
- Observation snapshot encoding, including main's v10 NFS files, directory markers and S3 history
  segments; golden fixtures stay with model and public facade contract tests.
- Traversal checkpoint ordering, plan validation, directory resumption and token checks.
- Destination pointer/checkpoint codecs, source hash coverage, recovery ownership and the
  receiving/ready-to-publish/published state transitions.
- ACL error and policy semantics, metadata provenance and bounded observation validation.

Scan recovery belongs to traversal: it restores enumeration progress and enforces the scan
plan's compatibility rules. Transfer recovery belongs to transfer plus the storage destination
contract: it reconciles source identity, metadata binding, durable byte/hash progress and
publication evidence. Backend-specific leases, multipart upload identities, file handles and
persistence barriers stay inside adapters. A scan checkpoint is not a transfer resume token.
No generic “resume” crate is introduced because these lifecycles have different invariants.

`PreparedStage` keeps its fields and synchronization state private. Adapters and engines use
operations to retain claims, record progress, attach recovery evidence and inspect state;
locks are not part of the consumer representation. `MetadataObservationParts::build()`
validates assembled observations, including aggregate bounds, rather than exposing mutable
fields of the validated value. Existing decoded data must remain valid independently of
which backend features a consumer selected.

## Extending the project

Use `StorageBuilder` to assemble roles for another implementation of an existing storage
kind. It verifies declared role capabilities against actual supplied roles before lending
them to engines. `crates/storage-core/tests/adapter_contract.rs` implements an in-memory
source entirely through the public interface, without a concrete backend dependency.
Ordinary tag-copy behavior is explicit instance policy and requires both endpoints to opt in;
engines no longer identify S3 by inspecting a concrete `Storage` enum variant.

For new features, keep policy and scheduling with the owning engine, pure serializable facts
with model, protocol operations with the adapter, and shared role contracts in storage-core.
Prefer optional capability methods with conservative defaults when extending an existing
role. Keep error classifications, cancellation, buffer bounds and durable progress truthful.
A new ACL encoding should extend the pure ACL module, while fetching/applying its native
representation remains adapter work. OS-specific APIs should stay target-gated in adapters.

`BackendKind` deliberately remains a closed, stable wire domain. An unrelated new protocol
requires a coordinated model/config/codec/facade change and compatibility fixtures; it cannot
masquerade as another backend kind. This workspace does not promise arbitrary third-party
protocol plugins, Rust ABI stability or loading adapters at runtime. Opening that domain
should be a separate persistence-format design decision.

Before accepting an extension, run the owning crate's tests, the affected cross-engine
contracts, the dependency guard, the independent feature matrix, and the relevant platform
or real-service gate. Adding a feature must not accidentally bring its SDK into unrelated
builds. `test-support` exposes fake protocols for workspace tests only; production consumers
should not enable it. It must never change a production default such as checkpoint cadence.

## Real-service validation

The [2026-10-29 validation report](../reports/2026-10-29-crate-refactor-live-validation.md)
records the actual cross-backend copy/recovery matrices, scan checkpoint recovery, ACL
comparisons, feature-isolated runtime check, and server-specific limitations. Live testing
found an NFS single-attempt portmap discovery panic already fixed by main's published
NFS dependency, and corrected HDFS lab harness
error propagation/HA root construction. An observational harness exit code alone is not a
successful recovery verdict; verify reused durable bytes and independent final content.
