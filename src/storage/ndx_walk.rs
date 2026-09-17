//! NDX-paged depth-first traversal driven by the [`Namespace`] role.
//!
//! Replaces the legacy per-backend `walkdir_2`: directories are listed through
//! `NamespaceRequest::List` and handed to the shared DFS driver in [`crate::dir_tree`], which
//! owns the prefetch window, the depth-first order and the NDX / gap numbering. Only the
//! listing and the legacy-model conversion live here, so any backend that lends a namespace
//! role gets NDX paging without protocol-specific code.
//!
//! Semantics worth knowing before relying on it:
//!
//! - **Timestamps must come from the listing.** A `SourceDescriptor` without
//!   [`SourceDescriptor::inline_timestamps`] would force either a per-entry metadata round trip
//!   (the N+1 this adapter exists to avoid) or an epoch timestamp, which silently tells an
//!   incremental sync that every entry changed. Such a directory is reported as an error
//!   instead. Today only CIFS attaches them.
//! - **Local and S3 lend no namespace role**, so they fail preflight here rather than
//!   producing an empty walk.
//! - **Stopping is by drop.** There is no cancellation token, matching the four legacy
//!   `walkdir_2` entry points: drop the returned iterator and the driver's next send fails,
//!   which unwinds the reader pool.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{
    CapabilityUnavailable, Namespace, NamespaceRequest, NamespaceResult, PreflightPolicy,
    SourceDescriptor, Storage, StorageRoleFailure,
};
use crate::dir_tree::{DirHandle, ReadContext, ReadResult, SubdirEntry, run_dfs_driver};
use crate::filter::{FilterExpression, FilterInput, should_skip};
use crate::error::StorageError;
use crate::model::{BackendKind, EntryKind, FailureClass, StoragePath};
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
        tokio::spawn(async move {
            while let Ok(read) = receiver.recv().await {
                let result = read_dir(namespace.as_ref(), &root, &read.dir_path, &read.ctx).await;
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
) -> crate::Result<ReadResult> {
    let target = join_root(root, dir_path)?;
    let descriptors = match namespace.execute(NamespaceRequest::List(target)).await {
        Ok(NamespaceResult::Entries(entries)) => entries,
        Ok(_) => return Err(StorageError::MismatchedType),
        Err(failure) => return listing_failure(dir_path, &failure),
    };
    Ok(build_read_result(root, dir_path, &descriptors, ctx))
}

/// Splits a listing failure into the driver's two channels.
///
/// An entry-scoped failure (one unreadable directory) becomes a soft error so the rest of the
/// tree keeps paging, matching what the legacy `read_dir_sorted` did for a failed open. A
/// session-scoped failure returns `Err`, which makes the driver drop that frame. `Cancelled`
/// is a signal rather than a failure (R10), so it is reported with a distinguishable prefix
/// and never looks like an I/O error to whoever reads the page stream.
fn listing_failure(dir_path: &str, failure: &StorageRoleFailure) -> crate::Result<ReadResult> {
    let cancelled = matches!(
        failure,
        StorageRoleFailure::Entry(error) if error.class() == FailureClass::Cancelled
    ) || matches!(
        failure,
        StorageRoleFailure::Session(error) if error.class() == FailureClass::Cancelled
    );
    if cancelled {
        return Ok(errors_only(dir_path, format!("cancelled: {dir_path}")));
    }
    match failure {
        StorageRoleFailure::Entry(error) => Ok(errors_only(
            dir_path,
            format!("failed to list '{dir_path}': {error}"),
        )),
        StorageRoleFailure::Session(error) => Err(StorageError::OperationError(format!(
            "namespace session failed while listing '{dir_path}': {error}"
        ))),
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
    for descriptor in descriptors {
        match descriptor_to_nas(root, descriptor) {
            Some(entry) => entries.push(entry),
            // One aggregated error rather than one per entry: a backend either attaches inline
            // timestamps to every listed entry or to none, so per-entry reporting would emit a
            // line per file for the whole tree.
            None => {
                return errors_only(
                    dir_path,
                    format!(
                        "listing of '{dir_path}' carries no inline timestamps, so NDX entries \
                         would report the epoch as the modification time"
                    ),
                );
            }
        }
    }

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
    ReadResult {
        dir_path: dir_path.to_owned(),
        files,
        subdirs,
        errors: Vec::new(),
    }
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
/// Returns `None` when the listing carried no timestamps; see the module docs.
fn descriptor_to_nas(root: &StoragePath, descriptor: &SourceDescriptor) -> Option<NASEntry> {
    let timestamps = descriptor.inline_timestamps()?;
    let relative = crate::traversal::relative_to(root, &descriptor.path);
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

fn unix_nanos(value: Option<crate::model::StorageTimestamp>) -> i64 {
    value.map_or(0, |value| {
        i64::try_from(value.unix_nanos()).unwrap_or(i64::MAX)
    })
}

#[cfg(test)]
mod tests;
