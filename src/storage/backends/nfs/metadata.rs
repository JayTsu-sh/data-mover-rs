use std::sync::Arc;

use async_trait::async_trait;

use super::acl;
use super::source::NfsProtocolFailure;
use crate::model::{
    ExtendedAttribute, MetadataObservation, MetadataObservations, MetadataProvenance,
    ObservationMode, ObservationPlan, OwnershipMode, StoragePath, StorageTimestamp, TimePrecision,
    TimestampMetadata,
};
use crate::storage::{CopiedMetadataObservation, Metadata, MetadataMutation, StorageRoleFailure};

pub(crate) struct NfsMetadataAdapter {
    protocol: Arc<dyn NfsMetadataProtocol>,
    identity: crate::model::BackendIdentity,
}

pub(crate) struct NfsMetadataInline {
    pub(crate) file_handle: bytes::Bytes,
    pub(crate) symlink: bool,
    pub(crate) uid: Option<u32>,
    pub(crate) gid: Option<u32>,
    pub(crate) mode: u32,
    pub(crate) atime: i64,
    pub(crate) mtime: i64,
    pub(crate) ctime: i64,
}

#[async_trait]
pub(crate) trait NfsMetadataProtocol: Send + Sync {
    async fn stat(&self, path: &StoragePath) -> Result<NfsMetadataInline, NfsProtocolFailure>;
    fn supports_acl(&self) -> bool;
    fn supports_xattrs(&self) -> bool;
    async fn get_acl(&self, path: &StoragePath) -> Result<nfs_rs::Acl, NfsProtocolFailure>;
    async fn get_xattrs(
        &self,
        path: &StoragePath,
    ) -> Result<Vec<ExtendedAttribute>, NfsProtocolFailure>;
    async fn set_acl(
        &self,
        path: &StoragePath,
        acl: &nfs_rs::Acl,
    ) -> Result<(), NfsProtocolFailure>;
    async fn set_xattr(
        &self,
        path: &StoragePath,
        value: &ExtendedAttribute,
    ) -> Result<(), NfsProtocolFailure>;
    async fn set_numeric_ownership(
        &self,
        path: &StoragePath,
        value: OwnershipMode,
    ) -> Result<(), NfsProtocolFailure>;
    /// Change permission bits without synthesizing or rewriting numeric ownership.
    async fn set_mode(&self, path: &StoragePath, mode: u32) -> Result<(), NfsProtocolFailure> {
        let _ = (path, mode);
        Err(NfsProtocolFailure::new(
            crate::model::FailureClass::Unsupported,
            crate::model::Transience::Permanent,
        ))
    }
    async fn set_timestamps(
        &self,
        path: &StoragePath,
        value: TimestampMetadata,
    ) -> Result<(), NfsProtocolFailure>;
}

impl NfsMetadataAdapter {
    pub(crate) fn new(
        protocol: Arc<dyn NfsMetadataProtocol>,
        identity: crate::model::BackendIdentity,
    ) -> Self {
        Self { protocol, identity }
    }

    async fn observe_entry(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
        entry: NfsMetadataInline,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        if plan.ownership_mode() == ObservationMode::Required
            && (entry.uid.is_none() || entry.gid.is_none())
        {
            return Err(unsupported(path));
        }
        let (acl, xattrs) = self
            .observe_optional_families(path, plan, entry.symlink)
            .await?;
        let ownership = ownership(plan.ownership_mode(), &entry);
        let timestamps = inline(
            plan.timestamps(),
            TimestampMetadata {
                accessed: timestamp(entry.atime),
                modified: timestamp(entry.mtime),
                created: timestamp(entry.ctime),
            },
        );
        MetadataObservations::new(
            acl,
            xattrs,
            optional_not_applicable(plan.tags()),
            ownership,
            timestamps,
        )
        .map_err(|_| {
            super::source::entry_failure(
                path,
                crate::model::Operation::Metadata,
                crate::model::FailureClass::Protocol,
                crate::model::Transience::Permanent,
            )
        })
    }

    /// ACL and xattrs: never for a symlink, otherwise one extra call each when asked for and the
    /// mount negotiated them.
    async fn observe_optional_families(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
        symlink: bool,
    ) -> Result<
        (
            MetadataObservation<crate::model::AclMetadata>,
            MetadataObservation<Vec<ExtendedAttribute>>,
        ),
        StorageRoleFailure,
    > {
        if symlink {
            return Ok((
                optional_not_applicable(plan.acl()),
                optional_not_applicable(plan.xattrs()),
            ));
        }
        let acl = observe_optional(path, plan.acl(), self.protocol.supports_acl(), || async {
            let value = self.protocol.get_acl(path).await?;
            acl::encode(&value).map_err(|_| super::source::NfsProtocolFailure::protocol())
        })
        .await?;
        let xattrs = observe_optional(
            path,
            plan.xattrs(),
            self.protocol.supports_xattrs(),
            || async { self.protocol.get_xattrs(path).await },
        )
        .await?;
        Ok((acl, xattrs))
    }

    /// One stat of `path`, refused unless it is still the object `expected` describes.
    async fn bound_entry(
        &self,
        path: &StoragePath,
        expected: &crate::model::SourceIdentity,
    ) -> Result<NfsMetadataInline, StorageRoleFailure> {
        let entry = self.protocol.stat(path).await.map_err(|error| {
            super::source::role_failure(path, crate::model::Operation::Metadata, error)
        })?;
        let observed = crate::model::SourceIdentity::new(
            self.identity.clone(),
            crate::model::IdentityStrength::StableWithinBackend,
            &entry.file_handle,
        )
        .map_err(|_| {
            super::source::entry_failure(
                path,
                crate::model::Operation::Metadata,
                crate::model::FailureClass::Protocol,
                crate::model::Transience::Permanent,
            )
        })?;
        if observed != *expected {
            return Err(super::source::entry_failure(
                path,
                crate::model::Operation::Metadata,
                crate::model::FailureClass::Conflict,
                crate::model::Transience::Permanent,
            ));
        }
        Ok(entry)
    }
}

#[async_trait]
impl Metadata for NfsMetadataAdapter {
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        Some(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::Required)
                .with_timestamps(ObservationMode::Required),
        )
    }

    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &crate::model::SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let entry = self.bound_entry(path, expected).await?;
        self.observe_entry(path, plan, entry).await
    }

    /// An owner or group nfs-rs could not map is copied the way a mode-only source is: the mode is
    /// carried, and owner and group are recorded as dropped — never as a made-up id, and never as a
    /// failure, because the destination can still take everything else.
    async fn observe_copy_bound(
        &self,
        path: &StoragePath,
        expected: &crate::model::SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<CopiedMetadataObservation, StorageRoleFailure> {
        let entry = self.bound_entry(path, expected).await?;
        let mode_without_ownership =
            (entry.uid.is_none() || entry.gid.is_none()).then_some(entry.mode & 0o7777);
        let plan = if mode_without_ownership.is_some() {
            plan.with_ownership_mode(ObservationMode::Omit)
        } else {
            plan
        };
        Ok(CopiedMetadataObservation {
            observations: self.observe_entry(path, plan, entry).await?,
            owner_names_unmapped: mode_without_ownership.is_some(),
            mode_without_ownership,
        })
    }

    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let entry = self.protocol.stat(path).await.map_err(|error| {
            super::source::role_failure(path, crate::model::Operation::Metadata, error)
        })?;
        self.observe_entry(path, plan, entry).await
    }

    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(super::source::cancelled(path));
        }
        let result = match mutation {
            MetadataMutation::Acl(value) => {
                // The xattr branch below has always checked this. Without the same check here a
                // mount that never negotiated ACL support still sends a SETACL, and the caller
                // gets whatever protocol error comes back instead of the typed refusal that says
                // the destination cannot do this at all.
                if !self.protocol.supports_acl() {
                    return Err(unsupported(path));
                }
                let value = acl::decode(&value).map_err(|_| {
                    super::source::entry_failure(
                        path,
                        crate::model::Operation::Metadata,
                        crate::model::FailureClass::InvalidInput,
                        crate::model::Transience::Permanent,
                    )
                })?;
                self.protocol.set_acl(path, &value).await
            }
            MetadataMutation::Xattrs(values) => {
                if !self.protocol.supports_xattrs() {
                    return Err(unsupported(path));
                }
                for value in values {
                    if cancel.is_cancelled() {
                        return Err(super::source::cancelled(path));
                    }
                    self.protocol
                        .set_xattr(path, &value)
                        .await
                        .map_err(|error| {
                            super::source::role_failure(
                                path,
                                crate::model::Operation::Metadata,
                                error,
                            )
                        })?;
                }
                return Ok(());
            }
            MetadataMutation::NumericOwnership(value) => {
                self.protocol.set_numeric_ownership(path, value).await
            }
            MetadataMutation::Mode(mode) => self.protocol.set_mode(path, mode).await,
            MetadataMutation::Timestamps(value) => self.protocol.set_timestamps(path, value).await,
            MetadataMutation::Tags(_) | MetadataMutation::MappedOwnership(_) => {
                return Err(super::source::entry_failure(
                    path,
                    crate::model::Operation::Metadata,
                    crate::model::FailureClass::Unsupported,
                    crate::model::Transience::Permanent,
                ));
            }
        };
        result.map_err(|error| {
            super::source::role_failure(path, crate::model::Operation::Metadata, error)
        })
    }
}

fn unsupported(path: &StoragePath) -> StorageRoleFailure {
    super::source::entry_failure(
        path,
        crate::model::Operation::Metadata,
        crate::model::FailureClass::Unsupported,
        crate::model::Transience::Permanent,
    )
}

pub(crate) fn timestamp(value: i64) -> Option<StorageTimestamp> {
    StorageTimestamp::new(i128::from(value), TimePrecision::Nanoseconds).ok()
}

fn inline<T>(mode: ObservationMode, value: T) -> MetadataObservation<T> {
    match mode {
        ObservationMode::Omit => MetadataObservation::NotRequested,
        ObservationMode::InlineOnly | ObservationMode::BestEffort | ObservationMode::Required => {
            MetadataObservation::Value {
                value,
                provenance: MetadataProvenance::Inline,
            }
        }
    }
}

/// An owner or group the server named and nfs-rs could not map has no number to report, and one
/// made up would be written as the owner (see `nfs::owner_id`).
fn ownership(
    mode: ObservationMode,
    entry: &NfsMetadataInline,
) -> MetadataObservation<OwnershipMode> {
    match (entry.uid, entry.gid) {
        (Some(uid), Some(gid)) => inline(
            mode,
            OwnershipMode {
                uid,
                gid,
                mode: entry.mode,
            },
        ),
        _ => unmapped(mode),
    }
}

/// No value to give: the observation says so instead of carrying a made-up one.
fn unmapped<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::Unsupported
    }
}

fn optional_not_applicable<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::NotApplicable
    }
}

async fn observe_optional<T, F, Fut>(
    path: &StoragePath,
    mode: ObservationMode,
    supported: bool,
    operation: F,
) -> Result<MetadataObservation<T>, StorageRoleFailure>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T, super::source::NfsProtocolFailure>>,
{
    if mode == ObservationMode::Omit {
        return Ok(MetadataObservation::NotRequested);
    }
    if mode == ObservationMode::InlineOnly {
        return Ok(MetadataObservation::NotRequested);
    }
    if !supported {
        return Ok(MetadataObservation::Unsupported);
    }
    match operation().await {
        Ok(value) => Ok(MetadataObservation::Value {
            value,
            provenance: MetadataProvenance::AdditionalCall,
        }),
        Err(error) if mode == ObservationMode::BestEffort => Ok(MetadataObservation::Failed {
            class: error.class,
            transience: error.transience,
        }),
        Err(error) => Err(super::source::entry_scoped(
            path,
            crate::model::Operation::Metadata,
            error,
        )),
    }
}

#[cfg(test)]
#[path = "metadata_tests.rs"]
mod tests;
