//! Current-name LOOKUPs; the normal cache-backed path does not enter this module.
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use nfs_rs::{Attr, Mount, NfsError, ObjRes};

use super::{
    FType3, NFSStorage, NfsProtocolFailure, invalidate_subtree_cache, reconcile_namespace_cache,
};
use crate::model::{FailureClass, StoragePath, Transience};
use crate::storage::backends::nfs::protocol::classify_error;

/// Only the three SDK operations used by the production strong lookup.
#[async_trait]
pub(super) trait Calls: Send + Sync {
    async fn root(&self, path: &str) -> nfs_rs::Result<ObjRes>;
    async fn child(&self, parent: Bytes, name: &str) -> nfs_rs::Result<ObjRes>;
    async fn attrs(&self, handle: Bytes) -> nfs_rs::Result<Attr>;
}

#[async_trait]
impl Calls for dyn Mount {
    async fn root(&self, path: &str) -> nfs_rs::Result<ObjRes> {
        self.lookup_path(path).await
    }
    async fn child(&self, parent: Bytes, name: &str) -> nfs_rs::Result<ObjRes> {
        self.lookup(parent, name).await
    }
    async fn attrs(&self, handle: Bytes) -> nfs_rs::Result<Attr> {
        self.getattr(handle).await
    }
}

/// One caller-owned deadline/cancellation window surrounds this entire operation. This is a
/// current-name observation, not a lock against an external rename after any LOOKUP.
pub(super) async fn lookup<C: Calls + ?Sized>(
    calls: &C,
    root: &str,
    bound: &Bytes,
    scope: &Bytes,
    path: &StoragePath,
) -> Result<(Bytes, Attr), NfsProtocolFailure> {
    let root_components = NFSStorage::collect_components(Path::new(root))
        .map_err(|_| NfsProtocolFailure::protocol())?;
    let canonical_root = root_components.join("/");
    let canonical_root = if canonical_root.is_empty() {
        "/"
    } else {
        &canonical_root
    };
    let mut object = calls
        .root(canonical_root)
        .await
        .map_err(|error| failed_root_lookup(error, scope))?;
    check_root(bound, scope, &object.fh)?;
    // Deliberately start from the original held root; the guard never rebinds it. Do not
    // shortcut through GLOBAL_CACHE: every ancestor must be confirmed by this observation.
    let mut parent = bound.clone();
    let mut relative = PathBuf::new();
    let components = NFSStorage::collect_components(Path::new(path.as_str()))
        .map_err(|_| NfsProtocolFailure::protocol())?;
    for name in components {
        relative.push(&name);
        object = calls
            .child(parent, &name)
            .await
            .map_err(|error| failed_lookup(error, scope, relative.clone()))?;
        if object.attr.is_none() {
            object.attr = Some(
                calls
                    .attrs(object.fh.clone())
                    .await
                    .map_err(|error| failed_lookup(error, scope, relative.clone()))?,
            );
        }
        let directory = object
            .attr
            .as_ref()
            .is_some_and(|attrs| attrs.type_ == FType3::NF3DIR as u32);
        reconcile_namespace_cache(
            relative.clone(),
            scope.clone(),
            bound,
            &object.fh,
            directory,
        );
        parent = object.fh.clone();
    }
    let attrs = match object.attr {
        Some(attrs) => attrs,
        None => calls
            .attrs(object.fh.clone())
            .await
            .map_err(|error| failed_root_lookup(error, scope))?,
    };
    Ok((object.fh, attrs))
}

pub(super) fn check_root(
    bound: &Bytes,
    scope: &Bytes,
    observed: &Bytes,
) -> Result<(), NfsProtocolFailure> {
    if reconcile_namespace_cache(PathBuf::new(), scope.clone(), bound, observed, true) {
        Ok(())
    } else {
        Err(NfsProtocolFailure::new(
            FailureClass::Conflict,
            Transience::Permanent,
        ))
    }
}

pub(super) fn failed_root_lookup(error: NfsError, scope: &Bytes) -> NfsProtocolFailure {
    failed_lookup(error, scope, PathBuf::new())
}

fn failed_lookup(error: NfsError, scope: &Bytes, relative: PathBuf) -> NfsProtocolFailure {
    let mut failure = classify_error(error);
    if failure.class == FailureClass::NotFound {
        let root = relative.as_os_str().is_empty();
        invalidate_subtree_cache(relative, scope.clone());
        if root {
            // A missing configured root is a lost binding, never absence of a child.
            failure.class = FailureClass::Conflict;
            failure.transience = Transience::Permanent;
        }
    }
    failure
}
