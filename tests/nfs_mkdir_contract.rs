//! Optional real NFS contract: `create_directory_all` through levels the caller may not write.
//!
//! `NFS_REAL_URL` names an NFS URL whose identity (normally `uid=0&gid=0`) may create and chmod
//! below its root. The test works only in its own `dm-mkdir-<nanos>` directory there:
//!
//! ```text
//! dm-mkdir-<nanos>/          0755, owned by the URL's identity
//!     ro/                    0555
//!         w/                 0777
//! ```
//!
//! then connects as `NFS_REAL_UNPRIVILEGED_ID` (uid and gid, default 4242; it and the next id,
//! used to probe, must not be the owner nor squashed onto it) and requires:
//! - `create_directory_all("…/ro/w/new")` succeeds: `dm-mkdir-<nanos>` and `ro` exist but are not
//!   writable, so the server may refuse their MKDIR before looking at the name (`ACCES`), and
//!   that refusal must not stop the levels below;
//! - `create_directory_all("…/ro/new")` fails `PermissionDenied` / Permanent at `…/ro/new`, and
//!   nothing is created.
//!
//! It prints what the server answers a one-level MKDIR of an existing directory in an
//! unwritable parent (`EXIST` first, or `ACCES` first), then removes its directory as the
//! URL's identity.
//!
//! `NFS_REAL_RO_PATH` (optional) names an existing directory on a read-only file system below
//! the test's directory, written `{scratch}/…` — on ONTAP `{scratch}/.snapshot`, the snapshot
//! directory every directory reaches — which is checked to exist and is never written:
//! `create_directory_all` of it must succeed and of a new name below it must fail
//! `PermissionDenied` / Permanent.
//!
//! ```text
//! NFS_REAL_URL='nfs://host/export:/sub?uid=0&gid=0' \
//!     cargo test --test nfs_mkdir_contract -- --ignored --nocapture
//! ```
mod common;

use std::num::NonZeroUsize;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use data_mover::NFSStorage;
use data_mover::model::{EntryKind, FailureClass, StoragePath, Transience};
use data_mover::storage::{
    BackendConfig, CreateDirectoryAllFailure, DeleteTreeOutcome, DeleteTreeRequest,
    NfsBackendConfig, PreflightPolicy, Storage, StorageRoleFailure, connect_backend,
    create_directory_all, delete_tree,
};
use tokio_util::sync::CancellationToken;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

const DEFAULT_UNPRIVILEGED_ID: u32 = 4242;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a writable NFS export (NFS_REAL_URL)"]
async fn real_nfs_create_directory_all_passes_existing_levels_it_may_not_write() -> Result {
    common::init_tracing();
    let url = std::env::var("NFS_REAL_URL")?;
    let owner = connect(&url).await?;
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let scratch = format!("dm-mkdir-{nanos}");
    let namespace = owner.namespace(&PreflightPolicy::production())?;
    // Never adopt an existing directory: everything this test deletes, it created.
    if namespace.stat(&path(&scratch)?).await.is_ok() {
        return Err(format!("{scratch} already exists").into());
    }
    let outcome = exercise(&url, &owner, &scratch).await;
    let removed = remove(&url, &owner, &scratch).await;
    outcome?;
    removed?;
    eprintln!("[nfs-mkdir] removed {scratch}");
    Ok(())
}

/// Everything between creating the test's directory and removing it. Fails by returning, never
/// by panicking, so the directory is always removed.
async fn exercise(url: &str, owner: &Storage, scratch: &str) -> Result {
    let ro = format!("{scratch}/ro");
    let writable = format!("{ro}/w");
    create_directory_all(owner, &path(&writable)?).await?;
    let legacy = NFSStorage::new(url, None).await?;
    chmod(&legacy, &writable, 0o777).await?;
    chmod(&legacy, &ro, 0o555).await?;
    chmod(&legacy, scratch, 0o755).await?;

    let id = std::env::var("NFS_REAL_UNPRIVILEGED_ID")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(DEFAULT_UNPRIVILEGED_ID);
    // What the server answers first, asked through an identity of its own: each NFS URL
    // identity keeps its own directory-handle cache, so neither run below is served from a
    // handle the other cached.
    let probe_id = id + 1;
    let probe = connect(&with_identity(url, probe_id)).await?;
    let probe = probe.namespace(&PreflightPolicy::production())?;
    for existing in [scratch, ro.as_str()] {
        let answer = probe.create_directory(&path(existing)?).await;
        eprintln!(
            "[nfs-mkdir] uid {probe_id}: one-level MKDIR of existing {existing}: {}",
            answer_of(answer)
        );
    }

    let caller = connect(&with_identity(url, id)).await?;
    let namespace = caller.namespace(&PreflightPolicy::production())?;

    let new_below_writable = format!("{writable}/new");
    create_directory_all(&caller, &path(&new_below_writable)?)
        .await
        .map_err(|error| format!("create_directory_all({new_below_writable}): {error:?}"))?;
    let created = namespace.stat(&path(&new_below_writable)?).await?;
    if created.kind != EntryKind::Directory {
        return Err(format!("{new_below_writable} is a {:?}", created.kind).into());
    }
    eprintln!("[nfs-mkdir] uid {id}: create_directory_all {new_below_writable}: created");

    let refused = format!("{ro}/new");
    expect_refused(&caller, &refused, &refused).await?;
    let absent = namespace.stat(&path(&refused)?).await;
    if !matches!(&absent, Err(StorageRoleFailure::Entry(failure)) if failure.class() == FailureClass::NotFound)
    {
        return Err(format!("{refused} must not exist: {absent:?}").into());
    }
    read_only_file_system(&caller, scratch).await
}

/// The optional read-only file system (`NFS_REAL_RO_PATH`): existing levels pass, a new one is
/// refused. Never writes: every MKDIR there is refused by the server.
async fn read_only_file_system(storage: &Storage, scratch: &str) -> Result {
    let Ok(existing) = std::env::var("NFS_REAL_RO_PATH") else {
        eprintln!("[nfs-mkdir] NFS_REAL_RO_PATH unset: read-only file system not exercised");
        return Ok(());
    };
    // Only below the test's own directory, and only an existing directory: were the path
    // writable after all, nothing outside what `remove` deletes could be created.
    if !existing.starts_with("{scratch}/") {
        return Err(format!("NFS_REAL_RO_PATH must start with {{scratch}}/: {existing}").into());
    }
    let existing = existing.replace("{scratch}", scratch);
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let found = namespace.stat(&path(&existing)?).await?;
    if found.kind != EntryKind::Directory {
        return Err(format!("{existing} is a {:?}, not a directory", found.kind).into());
    }
    let answer = namespace.create_directory(&path(&existing)?).await;
    eprintln!(
        "[nfs-mkdir] read-only: one-level MKDIR of existing {existing}: {}",
        answer_of(answer)
    );
    create_directory_all(storage, &path(&existing)?)
        .await
        .map_err(|error| format!("create_directory_all({existing}): {error:?}"))?;
    eprintln!("[nfs-mkdir] read-only: create_directory_all {existing}: exists, accepted");
    let new = format!("{existing}/dm-mkdir-new");
    expect_refused(storage, &new, &new).await
}

/// `create_directory_all(target)` fails `PermissionDenied` / Permanent at `level`.
async fn expect_refused(storage: &Storage, target: &str, level: &str) -> Result {
    let failure = match create_directory_all(storage, &path(target)?).await {
        Err(CreateDirectoryAllFailure::Role(StorageRoleFailure::Entry(failure))) => failure,
        outcome => return Err(format!("create_directory_all({target}): {outcome:?}").into()),
    };
    eprintln!(
        "[nfs-mkdir] create_directory_all {target} refused: {:?} / {:?} at {}: {}",
        failure.class(),
        failure.transience(),
        failure.path(),
        failure.diagnostic()
    );
    if failure.class() != FailureClass::PermissionDenied
        || failure.transience() != Transience::Permanent
        || failure.path().as_str() != level
    {
        return Err(format!("{target}: expected PermissionDenied / Permanent at {level}").into());
    }
    Ok(())
}

/// A one-level answer with the server's status, which the failure's `Debug` leaves out.
fn answer_of(answer: std::result::Result<(), StorageRoleFailure>) -> String {
    match answer {
        Ok(()) => "Ok (EXIST checked before access)".to_owned(),
        Err(StorageRoleFailure::Entry(failure)) => format!(
            "{:?} / {:?}: {} (access checked before the name)",
            failure.class(),
            failure.transience(),
            failure.diagnostic()
        ),
        Err(failure) => format!("{failure}"),
    }
}

async fn chmod(storage: &NFSStorage, path: &str, mode: u32) -> Result {
    storage
        .update_metadata(Path::new(path), None, None, None, None, Some(mode))
        .await?;
    Ok(())
}

/// Removes the test's directory as the URL's identity, after giving `ro` its write bit back
/// (a squashed root may need it).
async fn remove(url: &str, owner: &Storage, scratch: &str) -> Result {
    if let Ok(legacy) = NFSStorage::new(url, None).await {
        let _ = chmod(&legacy, &format!("{scratch}/ro"), 0o755).await;
    }
    let mut session = delete_tree(
        owner,
        DeleteTreeRequest {
            root: path(scratch)?,
            delete_root: true,
            max_inflight_operations: nonzero(4)?,
            max_buffered_items: nonzero(4)?,
            cancel: CancellationToken::new(),
        },
    )?;
    while session.next_item().await.is_some() {}
    match session.finish().await? {
        DeleteTreeOutcome::Completed(completion) if completion.entry_failures == 0 => Ok(()),
        outcome => Err(format!("removing {scratch}: {outcome:?}").into()),
    }
}

async fn connect(url: &str) -> Result<Storage> {
    Ok(connect_backend(BackendConfig::Nfs(NfsBackendConfig {
        url: url.to_owned(),
        block_size: None,
        ensure_dir: false,
    }))
    .await?)
}

/// `url` with its `uid` / `gid` replaced by `id`.
fn with_identity(url: &str, id: u32) -> String {
    let (base, query) = url.split_once('?').unwrap_or((url, ""));
    let mut pairs: Vec<String> = query
        .split('&')
        .filter(|pair| !pair.is_empty() && !pair.starts_with("uid=") && !pair.starts_with("gid="))
        .map(str::to_owned)
        .collect();
    pairs.push(format!("uid={id}"));
    pairs.push(format!("gid={id}"));
    format!("{base}?{}", pairs.join("&"))
}

fn path(value: &str) -> Result<StoragePath> {
    Ok(StoragePath::new(value)?)
}

fn nonzero(value: usize) -> Result<NonZeroUsize> {
    Ok(NonZeroUsize::new(value).ok_or("value must be non-zero")?)
}
