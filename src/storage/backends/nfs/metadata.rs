use std::sync::Arc;

use async_trait::async_trait;

use super::namespace::inline_ownership;
use super::source::NfsProtocolFailure;
use crate::model::{
    EntryKind, ExtendedAttribute, MetadataCapabilities, MetadataFamily as F, MetadataObservation,
    MetadataObservations, MetadataProvenance, ObservationMode, ObservationPlan,
    ObservationSupport as S, OwnershipMode, StoragePath, StorageTimestamp, TimePrecision,
    TimestampMetadata,
};
use crate::storage::{
    CopiedMetadataObservation, InlineOwnership, Metadata, MetadataMutation, SourceDescriptor,
    StorageRoleFailure,
};

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
    fn acl_encoding(&self) -> Option<crate::model::AclEncoding> {
        self.supports_acl()
            .then_some(crate::model::AclEncoding::NfsV4)
    }
    async fn get_native_acl(
        &self,
        path: &StoragePath,
    ) -> Result<crate::model::AclMetadata, NfsProtocolFailure> {
        let value = self.get_acl(path).await?;
        super::acl::encode(&value).map_err(|_| NfsProtocolFailure::protocol())
    }
    async fn set_native_acl(
        &self,
        path: &StoragePath,
        value: &crate::model::AclMetadata,
    ) -> Result<(), NfsProtocolFailure> {
        let value = super::acl::decode(value).map_err(|_| {
            NfsProtocolFailure::new(
                crate::model::FailureClass::InvalidInput,
                crate::model::Transience::Permanent,
            )
        })?;
        self.set_acl(path, &value).await
    }
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
    async fn remove_xattr(
        &self,
        _path: &StoragePath,
        _name: &str,
    ) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::new(
            crate::model::FailureClass::Unsupported,
            crate::model::Transience::Permanent,
        ))
    }
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

    /// A successful SET is insufficient: servers may rewrite or drop ACL entries.
    async fn apply_acl(
        &self,
        path: &StoragePath,
        value: &crate::model::AclMetadata,
    ) -> Result<(), NfsProtocolFailure> {
        let Some(encoding) = self.protocol.acl_encoding() else {
            return Err(NfsProtocolFailure::new(
                crate::model::FailureClass::Unsupported,
                crate::model::Transience::Permanent,
            ));
        };
        if value.encoding() != encoding {
            return Err(NfsProtocolFailure::new(
                crate::model::FailureClass::InvalidInput,
                crate::model::Transience::Permanent,
            ));
        }
        crate::model::acl::equivalent_native(value, value).map_err(|_| {
            NfsProtocolFailure::new(
                crate::model::FailureClass::InvalidInput,
                crate::model::Transience::Permanent,
            )
        })?;
        let value = self.materialize_acl(path, value).await?;
        self.protocol.set_native_acl(path, &value).await?;
        let actual = self.protocol.get_native_acl(path).await?;
        if !crate::model::acl::equivalent_native(&value, &actual)
            .map_err(|_| NfsProtocolFailure::protocol())?
        {
            return Err(NfsProtocolFailure::protocol().with_status("ACL_READBACK_MISMATCH"));
        }
        Ok(())
    }

    /// POSIX xattrs can report absence for a basic ACL; NFSACL SET needs explicit mode entries.
    async fn materialize_acl(
        &self,
        path: &StoragePath,
        value: &crate::model::AclMetadata,
    ) -> Result<crate::model::AclMetadata, NfsProtocolFailure> {
        if value.encoding() != crate::model::AclEncoding::Posix {
            return Ok(value.clone());
        }
        let mut acl =
            super::acl::decode_posix(value).map_err(|_| NfsProtocolFailure::protocol())?;
        if !acl.access.is_empty() {
            return Ok(value.clone());
        }
        acl.access = super::acl::posix_from_mode(self.protocol.stat(path).await?.mode);
        super::acl::encode_posix(&acl).map_err(|_| NfsProtocolFailure::protocol())
    }

    async fn observe_entry(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
        entry: NfsMetadataInline,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let owner = inline_ownership(entry.uid, entry.gid, entry.mode);
        refuse_unmapped(path, plan, owner)?;
        let (acl, xattrs) = self
            .observe_optional_families(path, plan, entry.symlink)
            .await?;
        let timestamps = TimestampMetadata {
            accessed: timestamp(entry.atime),
            modified: timestamp(entry.mtime),
            created: timestamp(entry.ctime),
        };
        observations(path, plan, owner, timestamps, (acl, xattrs))
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
            self.protocol.get_native_acl(path).await
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

/// A plan that requires ownership cannot be met by an owner or group that maps to no number.
fn refuse_unmapped(
    path: &StoragePath,
    plan: ObservationPlan,
    owner: InlineOwnership,
) -> Result<(), StorageRoleFailure> {
    if plan.ownership_mode() == ObservationMode::Required && owner == InlineOwnership::Unmapped {
        return Err(unsupported(path));
    }
    Ok(())
}

type OptionalFamilies = (
    MetadataObservation<crate::model::AclMetadata>,
    MetadataObservation<Vec<ExtendedAttribute>>,
);

/// Everything one attribute set says under `plan`, whether it came from a GETATTR or from the
/// READDIRPLUS entry that listed the child: both go through here, so they cannot disagree.
fn observations(
    path: &StoragePath,
    plan: ObservationPlan,
    owner: InlineOwnership,
    timestamps: TimestampMetadata,
    (acl, xattrs): OptionalFamilies,
) -> Result<MetadataObservations, StorageRoleFailure> {
    // An owner or group the server named and nfs-rs could not map has no number to report, and
    // one made up would be written as the owner (see `nfs::owner_id`).
    let ownership = match owner {
        InlineOwnership::Known(value) => inline(plan.ownership_mode(), value),
        InlineOwnership::Unmapped => unmapped(plan.ownership_mode()),
    };
    MetadataObservations::new(
        acl,
        xattrs,
        optional_not_applicable(plan.tags()),
        ownership,
        inline(plan.timestamps(), timestamps),
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

/// ACL or xattrs of a listed child when answering takes no call: what
/// [`NfsMetadataAdapter::observe_optional_families`] reports without reaching the server.
fn listed_optional<T>(
    mode: ObservationMode,
    symlink: bool,
    supported: bool,
) -> Option<MetadataObservation<T>> {
    if symlink {
        return Some(optional_not_applicable(mode));
    }
    match mode {
        ObservationMode::Omit | ObservationMode::InlineOnly => {
            Some(MetadataObservation::NotRequested)
        }
        ObservationMode::BestEffort | ObservationMode::Required if !supported => {
            Some(MetadataObservation::Unsupported)
        }
        ObservationMode::BestEffort | ObservationMode::Required => None,
    }
}

#[async_trait]
impl Metadata for NfsMetadataAdapter {
    async fn permissions(
        &self,
        descriptor: &SourceDescriptor,
    ) -> Result<crate::storage::PermissionFacts, StorageRoleFailure> {
        use crate::storage::{PermissionFacts, PrincipalIdentity};
        if let Some(InlineOwnership::Known(value)) = descriptor.inline_ownership {
            return Ok(PermissionFacts {
                owner: Some(PrincipalIdentity::Numeric(value.uid)),
                group: Some(PrincipalIdentity::Numeric(value.gid)),
                mode: Some(value.mode & 0o7777),
            });
        }
        let entry = self
            .bound_entry(&descriptor.path, &descriptor.source_identity)
            .await?;
        Ok(PermissionFacts {
            owner: entry.uid.map(PrincipalIdentity::Numeric),
            group: entry.gid.map(PrincipalIdentity::Numeric),
            mode: Some(entry.mode & 0o7777),
        })
    }

    fn observation_capabilities(&self) -> MetadataCapabilities {
        MetadataCapabilities::default()
            .with(
                F::Acl,
                if self.protocol.supports_acl() {
                    S::Supported
                } else {
                    S::Unsupported
                },
            )
            .with(
                F::Xattrs,
                if self.protocol.supports_xattrs() {
                    S::Supported
                } else {
                    S::Unsupported
                },
            )
            .with(F::Tags, S::NotApplicable)
            .with(F::OwnershipMode, S::Supported)
            .with(F::Timestamps, S::Supported)
    }

    async fn remove_xattr(
        &self,
        path: &StoragePath,
        name: &[u8],
    ) -> Result<(), StorageRoleFailure> {
        if !self.protocol.supports_xattrs() {
            return Err(unsupported(path));
        }
        let invalid = || {
            super::source::entry_failure(
                path,
                crate::model::Operation::Metadata,
                crate::model::FailureClass::InvalidInput,
                crate::model::Transience::Permanent,
            )
        };
        if name.is_empty() || name.contains(&0) || name.len() > crate::model::MAX_MODEL_FIELD_BYTES
        {
            return Err(invalid());
        }
        let name = std::str::from_utf8(name).map_err(|_| invalid())?;
        self.protocol
            .remove_xattr(path, name)
            .await
            .map_err(|failure| {
                super::source::role_failure(path, crate::model::Operation::Metadata, failure)
            })
    }

    /// READDIRPLUS carries the same attributes a GETATTR would return (`NFSv4` asks for the same
    /// bitmap in both), so ownership and timestamps of a listed child need no second round
    /// trip; only an ACL or xattrs the plan really wants do.
    fn observe_listed(
        &self,
        descriptor: &SourceDescriptor,
        plan: ObservationPlan,
    ) -> Option<Result<MetadataObservations, StorageRoleFailure>> {
        let symlink = descriptor.kind == EntryKind::Symlink;
        let acl = listed_optional(plan.acl(), symlink, self.protocol.supports_acl())?;
        let xattrs = listed_optional(plan.xattrs(), symlink, self.protocol.supports_xattrs())?;
        let owner = descriptor.inline_ownership?;
        let timestamps = descriptor.inline_timestamps?;
        let path = &descriptor.path;
        Some(
            refuse_unmapped(path, plan, owner)
                .and_then(|()| observations(path, plan, owner, timestamps, (acl, xattrs))),
        )
    }

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
            MetadataMutation::Acl(value) => self.apply_acl(path, &value).await,
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
