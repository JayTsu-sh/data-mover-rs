//! Optional real NFS contract: `create_directory_all`, writes and renames through levels the
//! caller may not write.
//!
//! `NFS_REAL_URL` names an NFS URL whose identity (normally `uid=0&gid=0`) may create and chmod
//! below its root. The test works only in its own `dm-mkdir-<nanos>` directory there:
//!
//! ```text
//! dm-mkdir-<nanos>/          0755, owned by the URL's identity
//!     ro/                    0555
//!         keep/              0755
//!         w/                 0777
//! ```
//!
//! then connects as `NFS_REAL_UNPRIVILEGED_ID` (uid and gid, default 4242; it and the next id,
//! used to probe, must not be the owner nor squashed onto it) and requires:
//! - `create_directory_all("…/ro/w/new")` succeeds: `dm-mkdir-<nanos>` and `ro` exist but are not
//!   writable, so the server may refuse their MKDIR before looking at the name (`ACCES`), and
//!   that refusal must not stop the levels below;
//! - `create_directory_all("…/ro/new")` fails `PermissionDenied` / Permanent at `…/ro/new`, and
//!   nothing is created;
//! - writing a file below a missing path in `ro` (`prepare_at_destination("…/ro/missing/sub/f")`,
//!   the role write path creating its parents) fails `PermissionDenied` / Permanent too, reported
//!   at `…/ro/missing`, the level that was refused;
//! - renaming `…/ro/keep` (in the unwritable `ro`) fails `PermissionDenied` / Permanent at
//!   `…/ro/keep`, and renaming a directory of `…/ro/w` into a missing parent in `ro`
//!   (`…/ro/gone/moved`) fails `PermissionDenied` / Permanent at `…/ro/gone`; neither moves
//!   anything;
//! - a directory this caller created (so its handle is cached) and the URL's identity then
//!   deleted is rebuilt by one call: `create_directory_all` of a path below it, and a file
//!   written into it.
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
    DestinationPrepareRequest, FinalDestination, LocalBackendConfig, NfsBackendConfig,
    PreflightPolicy, PrepareRequest, SourceDescriptor, Storage, StorageRoleFailure,
    connect_backend, create_directory_all, delete_tree,
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
    create_directory_all(owner, &path(&format!("{ro}/keep"))?).await?;
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
    let source = fixture().await?;
    write_below_refused_level(&caller, &source, &ro).await?;
    renames_refused_at_their_level(&caller, &ro, &writable).await?;
    rebuild_deleted_cached_directories(owner, &caller, &source, &writable).await?;
    read_only_file_system(&caller, scratch).await
}

/// A local one-byte file to prepare destinations for (the prepare only needs its descriptor).
async fn fixture() -> Result<SourceDescriptor> {
    let root = std::env::temp_dir().join(format!(
        "dm-mkdir-source-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    std::fs::create_dir_all(&root)?;
    let described = describe_local(&root).await;
    std::fs::remove_dir_all(&root)?;
    described
}

async fn describe_local(root: &Path) -> Result<SourceDescriptor> {
    std::fs::write(root.join("f"), b"x")?;
    let local = connect_backend(BackendConfig::Local(LocalBackendConfig {
        root: root.to_path_buf(),
        read_concurrency: nonzero(2)?,
        write_concurrency: nonzero(2)?,
    }))
    .await?;
    Ok(local
        .read_source(&PreflightPolicy::production())?
        .describe(&path("f")?)
        .await?)
}

fn prepare_request(source: &SourceDescriptor, target: &str) -> Result<DestinationPrepareRequest> {
    Ok(DestinationPrepareRequest::new(
        PrepareRequest {
            final_destination: FinalDestination::new(path(target)?),
            source: source.clone(),
            recovery_binding: [7; 32],
        },
        [8; 32],
    )
    .with_recoverable(false))
}

/// The role write path creates the missing parents of what it writes one level at a time: the
/// first missing level in the unwritable `ro` is refused, and the write fails `PermissionDenied` /
/// Permanent (the legacy recursive helper made it `Protocol` / Unknown, retryable).
async fn write_below_refused_level(
    caller: &Storage,
    source: &SourceDescriptor,
    ro: &str,
) -> Result {
    let target = format!("{ro}/missing/sub/f");
    let staged = caller.staged_destination(&PreflightPolicy::production())?;
    let failure = match staged
        .prepare_at_destination(prepare_request(source, &target)?)
        .await
    {
        Err(StorageRoleFailure::Entry(failure)) => failure,
        Ok(stage) => {
            let _ = staged.discard(stage).await;
            return Err(format!("writing {target} must be refused").into());
        }
        Err(failure) => return Err(format!("writing {target}: {failure}").into()),
    };
    eprintln!(
        "[nfs-mkdir] write {target} refused: {:?} / {:?}: {}",
        failure.class(),
        failure.transience(),
        failure.diagnostic()
    );
    let level = format!("{ro}/missing");
    if failure.class() != FailureClass::PermissionDenied
        || failure.transience() != Transience::Permanent
        || failure.path().as_str() != level
    {
        return Err(
            format!("writing {target}: expected PermissionDenied / Permanent at {level}").into(),
        );
    }
    Ok(())
}

/// The role rename classifies the server's refusal itself (the legacy rename made it `Protocol` /
/// Unknown, retryable), and a refused missing parent of the destination is reported at that
/// parent. Nothing is moved.
async fn renames_refused_at_their_level(caller: &Storage, ro: &str, writable: &str) -> Result {
    let namespace = caller.namespace(&PreflightPolicy::production())?;
    let keep = format!("{ro}/keep");
    let renamed = format!("{ro}/kept");
    let answer = namespace.rename(&path(&keep)?, &path(&renamed)?).await;
    expect_refused_at(answer, &format!("rename {keep} -> {renamed}"), &keep)?;

    let moving = format!("{writable}/moving");
    create_directory_all(caller, &path(&moving)?).await?;
    let gone = format!("{ro}/gone");
    let target = format!("{gone}/moved");
    let answer = namespace.rename(&path(&moving)?, &path(&target)?).await;
    let diagnostic = expect_refused_at(answer, &format!("rename {moving} -> {target}"), &gone)?;
    if !diagnostic.contains(&format!("`{target}`")) {
        return Err(format!("the refusal at {gone} must name {target}: {diagnostic}").into());
    }

    for (name, present) in [
        (keep, true),
        (renamed, false),
        (moving, true),
        (gone, false),
    ] {
        let found = match namespace.stat(&path(&name)?).await {
            Ok(_) => true,
            Err(StorageRoleFailure::Entry(failure))
                if failure.class() == FailureClass::NotFound =>
            {
                false
            }
            Err(failure) => return Err(format!("stat {name}: {failure}").into()),
        };
        if found != present {
            return Err(format!("after the refused renames {name} exists: {found}").into());
        }
    }
    Ok(())
}

/// A refused answer: `PermissionDenied` / Permanent at `level`; its diagnostic.
fn expect_refused_at(
    answer: std::result::Result<(), StorageRoleFailure>,
    what: &str,
    level: &str,
) -> Result<String> {
    let failure = match answer {
        Err(StorageRoleFailure::Entry(failure)) => failure,
        outcome => return Err(format!("{what}: {outcome:?}").into()),
    };
    eprintln!(
        "[nfs-mkdir] {what} refused: {:?} / {:?} at {}: {}",
        failure.class(),
        failure.transience(),
        failure.path(),
        failure.diagnostic()
    );
    if failure.class() != FailureClass::PermissionDenied
        || failure.transience() != Transience::Permanent
        || failure.path().as_str() != level
    {
        return Err(format!("{what}: expected PermissionDenied / Permanent at {level}").into());
    }
    Ok(failure.diagnostic().to_owned())
}

/// A directory the caller created (its handle cached) that the URL's identity then deletes: one
/// call below it rebuilds it, as the legacy helper did, for a directory and for a file.
async fn rebuild_deleted_cached_directories(
    owner: &Storage,
    caller: &Storage,
    source: &SourceDescriptor,
    writable: &str,
) -> Result {
    let cached = format!("{writable}/cached");
    create_directory_all(caller, &path(&format!("{cached}/d"))?).await?;
    delete_as_owner(owner, &cached).await?;
    let below = format!("{cached}/d/e");
    create_directory_all(caller, &path(&below)?)
        .await
        .map_err(|error| format!("create_directory_all({below}) after deletion: {error:?}"))?;
    let namespace = caller.namespace(&PreflightPolicy::production())?;
    if namespace.stat(&path(&below)?).await?.kind != EntryKind::Directory {
        return Err(format!("{below} is not a directory").into());
    }
    eprintln!(
        "[nfs-mkdir] cached {cached} deleted externally: create_directory_all {below} rebuilt it"
    );

    let cached = format!("{writable}/cached-file");
    create_directory_all(caller, &path(&cached)?).await?;
    delete_as_owner(owner, &cached).await?;
    let target = format!("{cached}/f");
    let staged = caller.staged_destination(&PreflightPolicy::production())?;
    let stage = staged
        .prepare_at_destination(prepare_request(source, &target)?)
        .await
        .map_err(|error| format!("writing {target} after deletion: {error}"))?;
    staged.discard(stage).await?;
    eprintln!("[nfs-mkdir] cached {cached} deleted externally: writing {target} rebuilt it");
    Ok(())
}

async fn delete_as_owner(owner: &Storage, directory: &str) -> Result {
    let mut session = delete_tree(
        owner,
        DeleteTreeRequest {
            root: path(directory)?,
            delete_root: true,
            max_inflight_operations: nonzero(4)?,
            max_buffered_items: nonzero(4)?,
            cancel: CancellationToken::new(),
        },
    )?;
    while session.next_item().await.is_some() {}
    match session.finish().await? {
        DeleteTreeOutcome::Completed(completion) if completion.entry_failures == 0 => Ok(()),
        outcome => Err(format!("deleting {directory}: {outcome:?}").into()),
    }
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
    delete_as_owner(owner, scratch).await
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
