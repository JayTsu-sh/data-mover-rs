//! Optional real SMB read-only contract: a share the account may only read.
//!
//! `CIFS_REAL_RO_SHARE` names a share whose ACL grants the account `read` and nothing else, with
//! the fixture tree below already in it (seeded through a writable share onto the same
//! directory). A source only ever reads, so the whole tree must traverse and copy out, and every
//! write, delete, rename or mkdir must be refused as `PermissionDenied` — by the server, not by
//! luck — while `create_directory_all` of levels that already exist succeeds without a write.
//! Before smb-rs `37d9404` (PR #87, issue #85) every directory below the root was opened with
//! write and delete access and the listing failed; before `2649a0b` (PR #89, issue #88) every
//! existing file was, and the source read failed.
//!
//! Sessions are always NTLM with guest sessions refused: the read-only share is for a named account.
//!
//! The fixtures are never written by this test. FAS2750 set-up: `.claude/docs/test-environments.md`.
mod common;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::Path;

use data_mover::model::{EntryKind, FailureClass, StoragePath};
use data_mover::storage::{
    BackendConfig, CifsBackendConfig, CifsGuestPolicy, CifsSigningPolicy,
    CreateDirectoryAllFailure, LocalBackendConfig, Namespace, Storage, StorageRoleFailure,
    connect_backend, create_directory_all,
};
use data_mover::transfer::{
    ReadBackVerification, TransferFailure, TransferIdentity, TransferOutcome, TransferPhase,
    TransferRequest, TransferSide, transfer,
};
use data_mover::traversal::{
    StorageTraversalSource, TraversalItem, TraversalOutcome, TraversalRequest, TraversalSource as _,
};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

/// The fixture files: path, size, BLAKE3 (hex). `sub/deeper/deepest/d.bin` normally spans several
/// reads (9 MiB + 12345 against the negotiated read size).
const FILES: [(&str, u64, &str); 4] = [
    (
        "a.txt",
        38,
        "8c2dbc5b9c45c77296fd2bc6ded6791b70e54b86d89c7806cb15b4fcb14b490d",
    ),
    (
        "sub/b.bin",
        1_048_576,
        "ac4ff1a39bc52058c8d24756a28778c3a3e532fd5e6b4f0dfc50806007bc8129",
    ),
    (
        "sub/deeper/c.txt",
        61,
        "fb3d16b4220e60df62c79646cedf88e3b087d22934c3b02e57e095fb183904dc",
    ),
    (
        "sub/deeper/deepest/d.bin",
        9_449_529,
        "e36e3405ed4abd6e53ea880c019871ca4d9ba4e21c7e3a9d45cd19f969bf45c3",
    ),
];

/// The fixture directories below the share root.
const DIRECTORIES: [&str; 3] = ["sub", "sub/deeper", "sub/deeper/deepest"];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a CIFS share the account may only read (CIFS_REAL_RO_SHARE)"]
async fn real_readonly_share_traverses_reads_and_refuses_writes() -> Result {
    common::init_tracing();
    let storage = connect_readonly().await?;
    let scratch = tempfile::tempdir()?;
    let local = connect_local(scratch.path()).await?;
    let id = uuid::Uuid::new_v4().simple().to_string();

    assert_fixture_tree(&storage).await?;
    for (path, size, digest) in FILES {
        copy_out_and_verify(&storage, &local, scratch.path(), path, size, digest).await?;
    }
    refuse_writes(&storage, &local, scratch.path(), &id).await?;
    create_directory_all_passes_existing_levels(&storage).await?;
    // Nothing a refused call did may have changed the tree.
    assert_fixture_tree(&storage).await
}

/// Traverses the whole share and requires exactly the fixture tree with no failure.
async fn assert_fixture_tree(storage: &Storage) -> Result {
    let request = TraversalRequest::new(StoragePath::root())
        .max_inflight_operations(NonZeroUsize::new(4).ok_or("zero slots")?);
    let mut session = StorageTraversalSource::new(storage)?.traverse(request);
    let mut seen = BTreeMap::new();
    let mut failures = Vec::new();
    while let Some(item) = session.next_item().await {
        match item {
            TraversalItem::Entry(entry) => {
                // A directory's size is whatever the server says; only a file's is fixed.
                let size = (entry.kind() == EntryKind::File).then(|| entry.size());
                seen.insert(
                    entry.path().as_str().to_owned(),
                    (entry.kind(), size.flatten()),
                );
            }
            TraversalItem::Failure(failure) => failures.push(format!(
                "{:?} {} {:?}",
                failure.scope,
                failure.path,
                failure.failure.class()
            )),
            _ => {}
        }
    }
    let outcome = session.finish().await;
    eprintln!(
        "[readonly] traversal: {} entries, {} failures, {outcome:?}",
        seen.len(),
        failures.len()
    );
    assert!(failures.is_empty(), "traversal failures: {failures:?}");
    let TraversalOutcome::Completed(completion) = outcome.map_err(|e| format!("{e:?}"))? else {
        return Err("the traversal was cancelled".into());
    };
    assert!(completion.totals.is_balanced(), "{:?}", completion.totals);
    assert_eq!(
        seen,
        expected_tree(),
        "the read-only share holds the fixture tree"
    );
    Ok(())
}

fn expected_tree() -> BTreeMap<String, (EntryKind, Option<u64>)> {
    let files = FILES
        .iter()
        .map(|&(path, size, _)| (path.to_owned(), (EntryKind::File, Some(size))));
    let directories = DIRECTORIES
        .iter()
        .map(|&path| (path.to_owned(), (EntryKind::Directory, None)));
    files.chain(directories).collect()
}

/// Copies one fixture into the local scratch directory through the transfer engine and checks
/// both the digest the engine read back and the bytes on disk.
async fn copy_out_and_verify(
    storage: &Storage,
    local: &Storage,
    scratch: &Path,
    path: &str,
    size: u64,
    digest: &str,
) -> Result {
    if let Some((parent, _)) = path.rsplit_once('/') {
        create_directory_all(local, &StoragePath::new(parent)?).await?;
    }
    let copied = copy(storage, path, local, path).await?;
    assert_eq!(copied.transferred_bytes, size, "{path}: bytes read");
    let read_back = copied
        .blake3
        .map(|bytes| blake3::Hash::from(bytes).to_hex());
    assert_eq!(
        read_back.as_deref(),
        Some(digest),
        "{path}: read-back digest"
    );
    let on_disk = blake3::hash(&std::fs::read(scratch.join(path))?).to_hex();
    assert_eq!(on_disk.as_str(), digest, "{path}: copied bytes");
    eprintln!("[readonly] read {path}: {size} bytes, blake3 {digest}");
    Ok(())
}

/// Every mutation of the read-only share is refused as `PermissionDenied`.
async fn refuse_writes(storage: &Storage, local: &Storage, scratch: &Path, id: &str) -> Result {
    let namespace = storage.namespace()?;
    let refused = format!("dm-ro-refused-{id}");
    // Keep the mkdir first: on a share that is in fact writable it succeeds, the test fails here,
    // and the fixtures are never offered to the delete and rename calls below.
    expect_denied(
        namespace.create_directory(&path(&refused)?).await,
        "create_directory",
    )?;
    expect_denied(namespace.delete(&path("a.txt")?).await, "delete a.txt")?;
    // Not empty: the server checks the share ACL when the DELETE-access open is made, before any
    // emptiness question arises, so this is `PermissionDenied` rather than `Conflict`.
    expect_denied(
        namespace.delete(&path("sub/deeper/deepest")?).await,
        "delete a directory",
    )?;
    expect_denied(
        namespace
            .rename(&path("a.txt")?, &path(&format!("{refused}.txt"))?)
            .await,
        "rename a.txt",
    )?;
    expect_denied(
        namespace
            .rename(&path("sub/deeper")?, &path(&refused)?)
            .await,
        "rename a directory",
    )?;
    refuse_upload(namespace.as_ref(), storage, local, scratch, &refused).await
}

/// ONTAP checks the share ACL before the name, so `CREATE` of a directory that exists is
/// `PermissionDenied` here, not a collision. `create_directory_all` must still accept levels
/// that are already directories — a read-only root above a writable directory would otherwise
/// refuse everything below it — and still refuse the first level that is missing, naming it.
async fn create_directory_all_passes_existing_levels(storage: &Storage) -> Result {
    for existing in ["sub/deeper", "sub/deeper/deepest"] {
        create_directory_all(storage, &path(existing)?)
            .await
            .map_err(|error| format!("create_directory_all({existing}): {error:?}"))?;
        eprintln!("[readonly] create_directory_all {existing}: exists, accepted");
    }
    for (missing, refused) in [
        ("sub/new", "sub/new"),
        ("sub/deeper/new/x", "sub/deeper/new"),
    ] {
        let failure = match create_directory_all(storage, &path(missing)?).await {
            Err(CreateDirectoryAllFailure::Role(StorageRoleFailure::Entry(failure))) => failure,
            outcome => {
                return Err(format!("create_directory_all({missing}): {outcome:?}").into());
            }
        };
        eprintln!(
            "[readonly] create_directory_all {missing} refused: {:?} at {}",
            failure.class(),
            failure.path()
        );
        assert_eq!(failure.class(), FailureClass::PermissionDenied, "{missing}");
        assert_eq!(failure.path().as_str(), refused, "{missing}");
    }
    Ok(())
}

/// A transfer onto the read-only share fails while preparing the destination, as
/// `PermissionDenied`, before any stage exists or the final path changes.
async fn refuse_upload(
    namespace: &dyn Namespace,
    storage: &Storage,
    local: &Storage,
    scratch: &Path,
    refused: &str,
) -> Result {
    let target = format!("{refused}.bin");
    let local_path = scratch.join(&target);
    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(local_path, b"must not arrive")?;
    let error = match copy(local, &target, storage, &target).await {
        Ok(outcome) => return Err(format!("upload was accepted: {outcome:?}").into()),
        Err(error) => error,
    };
    let failure = error
        .downcast_ref::<TransferFailure>()
        .ok_or_else(|| format!("upload failed outside the transfer engine: {error}"))?;
    let class = role_class(failure);
    eprintln!("[readonly] upload refused: {class:?} ({failure})");
    assert_eq!(class, Some(FailureClass::PermissionDenied), "upload");
    assert_eq!(
        (failure.phase(), failure.side()),
        (TransferPhase::Prepare, TransferSide::Destination),
        "the destination refused the upload while preparing it"
    );
    assert!(!failure.final_destination_changed(), "final path touched");
    assert!(!failure.has_unpublished_stage(), "a stage was left behind");
    expect_class(
        namespace.stat(&path(&target)?).await,
        FailureClass::NotFound,
        "stat of the refused upload",
    )
}

async fn copy(
    source: &Storage,
    from: &str,
    destination: &Storage,
    to: &str,
) -> Result<TransferOutcome> {
    assert_eq!(from, to, "migration preserves relative paths");
    Ok(transfer(
        TransferRequest::new(
            source.clone(),
            StoragePath::new(from)?,
            destination.clone(),
            CancellationToken::new(),
        )
        .with_identity_override(TransferIdentity::from_label(format!("ro-{from}-{to}"))?)
        .with_read_back_verification(ReadBackVerification::Enabled),
    )
    .await?)
}

/// The class of the first storage-role failure in `error`'s source chain.
fn role_class(error: &(dyn std::error::Error + 'static)) -> Option<FailureClass> {
    let mut cause = Some(error);
    while let Some(current) = cause {
        if let Some(failure) = current.downcast_ref::<StorageRoleFailure>() {
            return Some(class(failure));
        }
        cause = current.source();
    }
    None
}

fn expect_denied<T: std::fmt::Debug>(
    outcome: std::result::Result<T, StorageRoleFailure>,
    what: &str,
) -> Result {
    if let Err(failure) = &outcome {
        eprintln!("[readonly] {what} refused: {:?}", class(failure));
    }
    expect_class(outcome, FailureClass::PermissionDenied, what)
}

fn class(failure: &StorageRoleFailure) -> FailureClass {
    match failure {
        StorageRoleFailure::Entry(failure) => failure.class(),
        StorageRoleFailure::Session(failure) => failure.class(),
    }
}

fn expect_class<T: std::fmt::Debug>(
    outcome: std::result::Result<T, StorageRoleFailure>,
    expected: FailureClass,
    what: &str,
) -> Result {
    match outcome {
        Err(failure) if class(&failure) == expected => Ok(()),
        Err(failure) => Err(format!(
            "{what}: expected {expected:?}, got {:?} ({failure:?})",
            class(&failure)
        )
        .into()),
        Ok(result) => Err(format!("{what}: expected {expected:?}, got {result:?}").into()),
    }
}

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

async fn connect_readonly() -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Cifs(CifsBackendConfig {
        signing_policy: CifsSigningPolicy::default(),
        guest_policy: CifsGuestPolicy::Deny,
        server: std::env::var("CIFS_REAL_SERVER")?,
        share: std::env::var("CIFS_REAL_RO_SHARE")?,
        root: None,
        ensure_dir: false,
        username: std::env::var("CIFS_REAL_USER")?,
        password: std::env::var("CIFS_REAL_PASS")?,
    }))
    .await?)
}

async fn connect_local(root: &Path) -> Result<Storage> {
    let slots = NonZeroUsize::new(8).ok_or("zero slots")?;
    Ok(connect_backend(BackendConfig::Local(LocalBackendConfig {
        rename_context: None,
        read_chunk_bytes: None,
        write_chunk_bytes: None,
        root: root.to_path_buf(),
        read_concurrency: slots,
        write_concurrency: slots,
    }))
    .await?)
}
