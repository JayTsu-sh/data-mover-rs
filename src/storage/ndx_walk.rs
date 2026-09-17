//! NDX-paged depth-first traversal driven by the [`Namespace`] role.
//!
//! Replaces the legacy per-backend `walkdir_2`: directories are listed through
//! `NamespaceRequest::List` and handed to the shared DFS driver in [`crate::dir_tree`], which
//! owns the prefetch window, the depth-first order and the NDX / gap numbering. Only the
//! listing and the legacy-model conversion live here, so the adapter carries no
//! protocol-specific code.
//!
//! Semantics worth knowing before relying on it:
//!
//! - **Only CIFS can actually be walked today.** Every emitted entry needs a modification
//!   time, and the only source that costs no extra round trip is
//!   [`SourceDescriptor::inline_timestamps`], which only the CIFS namespace attaches
//!   (`backends/cifs/namespace.rs`). NFS and HDFS lend a namespace role, so preflight admits
//!   them, but each of their directories is then reported as an error rather than emitted with
//!   an epoch timestamp — an epoch would silently tell an incremental sync that every entry
//!   changed, and would disable every `modified` filter condition. Making them work is a
//!   matter of attaching the timestamps their listings already carry (NFS `readdirplus`
//!   returns the attributes and currently drops them), not of changing this adapter.
//! - **Local and S3 lend no namespace role**, so they fail preflight here rather than
//!   producing an empty walk.
//! - **Stopping needs the token.** `NdxWalkRequest::cancel` stops the reader pool at its next
//!   listing. Dropping the returned iterator is *not* reliable on its own: the driver only
//!   gives up when sending a non-empty page fails, and ignores send failures for error and
//!   completion events, so a subtree that is entirely empty or entirely failing keeps
//!   listing after the consumer is gone.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::{
    CapabilityUnavailable, Namespace, NamespaceRequest, NamespaceResult, PreflightPolicy,
    SourceDescriptor, Storage, StorageRoleFailure,
};
use crate::dir_tree::{DirHandle, ReadContext, ReadResult, SubdirEntry, run_dfs_driver};
use crate::error::StorageError;
use crate::filter::{FilterExpression, FilterInput, should_skip};
use crate::model::{BackendKind, EntryKind, FailureClass, StoragePath, StorageTimestamp};
use crate::traversal::relative_to;
use crate::{EntryEnum, NASEntry, TransferConcurrency};

/// One NDX-paged traversal request.
#[derive(Clone, Debug)]
pub struct NdxWalkRequest {
    /// Subtree to enumerate, relative to the connected backend root. Emitted entries carry
    /// paths relative to **this** root, matching what the legacy walkers produced.
    pub root: StoragePath,
    /// Maximum depth to enumerate; direct children of `root` are depth 1. `None` is unlimited.
    pub max_depth: Option<NonZeroUsize>,
    /// Admission expression; `None` admits everything.
    pub match_expressions: Option<FilterExpression>,
    /// Exclusion expression; `None` excludes nothing.
    pub exclude_expressions: Option<FilterExpression>,
    /// Reader pool size, clamped to `1..=TransferConcurrency::MAX`.
    pub concurrency: NonZeroUsize,
    /// Stops the reader pool at its next listing. The DFS driver has no cancellation channel,
    /// so it keeps asking for directories; the readers answer without touching the backend,
    /// which is what actually bounds the cost.
    pub cancel: CancellationToken,
}

/// Starts an NDX-paged traversal of `request.root` through `storage`'s namespace role.
///
/// Stop the walk by dropping the returned iterator.
///
/// # Errors
/// Returns the capability failure when the storage lends no namespace role under production
/// preflight policy, before any backend I/O.
pub fn ndx_walk(
    storage: &Storage,
    request: NdxWalkRequest,
) -> Result<crate::WalkDirAsyncIterator2, CapabilityUnavailable> {
    let namespace = storage.namespace(&PreflightPolicy::production())?;
    let concurrency = request.concurrency.get().min(TransferConcurrency::MAX);
    let (request_sender, request_receiver) =
        async_channel::bounded::<crate::dir_tree::ReadRequest>(concurrency.saturating_mul(2));
    let (output_sender, output_receiver) = async_channel::bounded(64);

    for _ in 0..concurrency {
        let namespace = Arc::clone(&namespace);
        let root = request.root.clone();
        let receiver = request_receiver.clone();
        let cancel = request.cancel.clone();
        tokio::spawn(async move {
            while let Ok(read) = receiver.recv().await {
                // Once the walk is over, answer without a backend call. The driver has no
                // cancellation channel and will keep requesting the remaining directories, so
                // this is what keeps a dead session or a cancelled walk from costing one
                // doomed round trip per directory.
                let result = if cancel.is_cancelled() {
                    Ok(errors_only(
                        &read.dir_path,
                        format!("cancelled before listing '{}'", read.dir_path),
                    ))
                } else {
                    read_dir(
                        namespace.as_ref(),
                        &root,
                        &read.dir_path,
                        &read.ctx,
                        &cancel,
                    )
                    .await
                };
                let _ = read.reply.send(result);
            }
        });
    }

    let context = ReadContext {
        match_expr: Arc::new(request.match_expressions),
        exclude_expr: Arc::new(request.exclude_expressions),
        current_depth: 0,
        max_depth: request.max_depth.map_or(0, NonZeroUsize::get),
        apply_filter: true,
        include_tags: false,
        is_versioned: false,
    };
    tokio::spawn(run_dfs_driver(
        request_sender,
        output_sender,
        PathBuf::new(),
        root_handle(storage.kind()),
        context,
    ));
    Ok(crate::AsyncReceiver::new(output_receiver))
}

/// Picks the handle variant for the connected backend.
///
/// `DirHandle` exists so a legacy reader can carry protocol-specific state (an NFS file
/// handle, a Local absolute path). This adapter needs none of it: every directory it reads is
/// identified by `ReadRequest::dir_path`, which the driver fills with the traversal root's
/// empty string for the root frame and with the child's scan-root-relative path below it. The
/// variant therefore only decides which `BackendKind` the driver derives, and that in turn
/// only selects which handle `extract_dir_handle` builds for a child — a value this adapter
/// ignores. Matching the connected backend keeps the value honest for anyone reading a trace.
fn root_handle(kind: BackendKind) -> DirHandle {
    match kind {
        BackendKind::Cifs => DirHandle::Cifs(String::new()),
        BackendKind::Hdfs => DirHandle::Hdfs(PathBuf::new()),
        BackendKind::Nfs => DirHandle::Nfs {
            fh: bytes::Bytes::new(),
            path: String::new(),
        },
        // Local and S3 lend no namespace role, so preflight already refused them.
        BackendKind::Local | BackendKind::S3 => DirHandle::Local(PathBuf::new()),
    }
}

/// Re-expresses a scan-root-relative path as the backend-relative path `List` expects.
fn join_root(root: &StoragePath, relative: &str) -> Result<StoragePath, StorageError> {
    let relative = relative.trim_start_matches('/');
    let joined = match (root.as_str(), relative) {
        ("", rest) => rest.to_owned(),
        (base, "") => base.to_owned(),
        (base, rest) => format!("{base}/{rest}"),
    };
    StoragePath::new(joined).map_err(|error| StorageError::InvalidPath(error.to_string()))
}

async fn read_dir(
    namespace: &dyn Namespace,
    root: &StoragePath,
    dir_path: &str,
    ctx: &ReadContext,
    cancel: &CancellationToken,
) -> crate::Result<ReadResult> {
    let target = join_root(root, dir_path)?;
    let descriptors = match namespace.execute(NamespaceRequest::List(target)).await {
        Ok(NamespaceResult::Entries(entries)) => entries,
        Ok(_) => return Err(StorageError::MismatchedType),
        Err(failure) => return listing_failure(dir_path, &failure, cancel),
    };
    Ok(build_read_result(root, dir_path, &descriptors, ctx))
}

/// Splits a listing failure into the driver's two channels, and ends the walk when the
/// failure means no later listing can succeed either.
///
/// An entry-scoped failure (one unreadable directory) stays soft so the rest of the tree keeps
/// paging, matching what the legacy `read_dir_sorted` did for a failed open. A session-scoped
/// failure and a cancellation both trip the token: the driver cannot be told to stop, so
/// without this every remaining directory would cost one more doomed round trip. `Cancelled`
/// is reported with a distinguishable prefix rather than as an I/O error, per R10.
fn listing_failure(
    dir_path: &str,
    failure: &StorageRoleFailure,
    cancel: &CancellationToken,
) -> crate::Result<ReadResult> {
    let class = match failure {
        StorageRoleFailure::Entry(error) => error.class(),
        StorageRoleFailure::Session(error) => error.class(),
    };
    if class == FailureClass::Cancelled {
        cancel.cancel();
        return Ok(errors_only(dir_path, format!("cancelled: {dir_path}")));
    }
    match failure {
        StorageRoleFailure::Entry(error) => Ok(errors_only(
            dir_path,
            format!("failed to list '{dir_path}': {error}"),
        )),
        StorageRoleFailure::Session(error) => {
            cancel.cancel();
            Err(StorageError::OperationError(format!(
                "namespace session failed while listing '{dir_path}': {error}"
            )))
        }
    }
}

fn errors_only(dir_path: &str, reason: String) -> ReadResult {
    ReadResult {
        dir_path: dir_path.to_owned(),
        files: Vec::new(),
        subdirs: Vec::new(),
        errors: vec![reason],
    }
}

fn build_read_result(
    root: &StoragePath,
    dir_path: &str,
    descriptors: &[SourceDescriptor],
    ctx: &ReadContext,
) -> ReadResult {
    let mut entries = Vec::with_capacity(descriptors.len());
    let mut undated = 0_usize;
    for descriptor in descriptors {
        match descriptor_to_nas(root, descriptor) {
            Some(entry) => entries.push(entry),
            None => undated += 1,
        }
    }
    if undated > 0 {
        // Aggregated rather than one error per entry: a backend attaches inline timestamps to
        // every listed entry or to none, so per-entry reporting would emit a line per file for
        // the whole tree. The count is carried anyway, because "or to none" is an observation
        // about today's backends, not a contract `SourceDescriptor` enforces.
        return errors_only(
            dir_path,
            format!(
                "{undated} of {} entries in '{dir_path}' carry no inline modification time;                  emitting them would report the epoch and tell an incremental sync that every                  entry changed, so the directory and its subtree are skipped",
                descriptors.len()
            ),
        );
    }
    let (files, subdirs) = partition_entries(entries, ctx);
    ReadResult {
        dir_path: dir_path.to_owned(),
        files,
        subdirs,
        errors: Vec::new(),
    }
}

/// Applies the legacy `should_skip` triple and sorts each bucket by name.
///
/// A directory at the depth limit is emitted as an entry rather than descended into, which is
/// what the legacy readers did; `visible: false` keeps a filtered-out directory out of the page
/// while still descending into it.
fn partition_entries(
    entries: Vec<NASEntry>,
    ctx: &ReadContext,
) -> (Vec<Arc<EntryEnum>>, Vec<SubdirEntry>) {
    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    for entry in entries {
        let (skip, continue_scan, need_filter) = if ctx.apply_filter {
            filter_decision(&entry, ctx)
        } else {
            (false, true, false)
        };
        let can_descend = entry.is_dir
            && (ctx.max_depth == 0 || ctx.current_depth.saturating_add(1) < ctx.max_depth);
        let entry = Arc::new(EntryEnum::NAS(entry));
        if skip {
            if can_descend && continue_scan {
                subdirs.push(SubdirEntry {
                    entry,
                    visible: false,
                    need_filter,
                });
            }
        } else if can_descend {
            subdirs.push(SubdirEntry {
                entry,
                visible: true,
                need_filter,
            });
        } else {
            files.push(entry);
        }
    }
    files.sort_by(|left, right| left.get_name().cmp(right.get_name()));
    subdirs.sort_by(|left, right| left.entry.get_name().cmp(right.entry.get_name()));
    (files, subdirs)
}

fn filter_decision(entry: &NASEntry, ctx: &ReadContext) -> (bool, bool, bool) {
    let path = entry.relative_path.to_string_lossy();
    let file_type = if entry.is_dir { "dir" } else { "file" };
    should_skip(
        ctx.match_expr.as_ref().as_ref(),
        ctx.exclude_expr.as_ref().as_ref(),
        FilterInput {
            file_name: Some(&entry.name),
            file_path: Some(&path),
            file_type: Some(file_type),
            modified_epoch: Some(entry.mtime / 1_000_000_000),
            size: Some(entry.size),
            extension: entry.extension.as_deref().or(Some("")),
        },
    )
}

/// Rebuilds the legacy enumeration entry from a neutral descriptor.
///
/// Returns `None` unless the listing carried a modification time. The whole record being
/// absent and the record being present with `modified: None` are the same thing here: either
/// way `mtime` would have to be the epoch, which is the outcome the module docs rule out.
/// `accessed` and `created` are allowed to be missing — nothing decides transfers on them.
fn descriptor_to_nas(root: &StoragePath, descriptor: &SourceDescriptor) -> Option<NASEntry> {
    let timestamps = descriptor.inline_timestamps()?;
    timestamps.modified?;
    let relative = relative_to(root, &descriptor.path);
    let name = relative
        .rsplit_once('/')
        .map_or(relative, |(_, name)| name)
        .to_owned();
    let is_dir = descriptor.kind == EntryKind::Directory;
    Some(NASEntry {
        extension: Path::new(&name)
            .extension()
            .map(|value| value.to_string_lossy().into_owned()),
        name,
        relative_path: PathBuf::from(relative),
        is_dir,
        size: if is_dir {
            0
        } else {
            descriptor.size.unwrap_or(0)
        },
        atime: unix_nanos(timestamps.accessed),
        ctime: unix_nanos(timestamps.created),
        mtime: unix_nanos(timestamps.modified),
        // Approximate bits when the listing could derive them; otherwise the conventional
        // default for the kind. A backend that observes real POSIX mode fills `inline_mode`.
        mode: descriptor
            .inline_mode()
            .unwrap_or(if is_dir { 0o755 } else { 0o644 }),
        // Reparse points are listed as files (the facade cannot read link targets), so no
        // listed entry is ever a link.
        is_symlink: false,
        // No protocol-neutral source: SMB link counts need a second per-file query, and the
        // domain facade exposes no file id at all, so rename detection falls back to paths.
        hard_links: None,
        file_handle: None,
        ino: None,
        uid: None,
        gid: None,
        acl: None,
        owner: None,
        owner_group: None,
        xattrs: None,
    })
}

fn unix_nanos(value: Option<StorageTimestamp>) -> i64 {
    value.map_or(0, |value| {
        i64::try_from(value.unix_nanos()).unwrap_or(i64::MAX)
    })
}

#[cfg(test)]
mod tests;
