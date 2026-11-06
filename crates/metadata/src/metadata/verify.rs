//! 核验已经发布的目标元数据；读取目标，不重新读取源端。

use std::{error::Error, fmt};

use tokio_util::sync::CancellationToken;

use super::MetadataPlan;
use crate::model::acl::{AclError, equivalent_native};
use crate::model::{
    MetadataFamily, MetadataObservationFailure, MetadataObservations, ObservationMode,
    ObservationPlan, PermissionFacts, PrincipalIdentity, SourceVersion, StoragePath,
    StorageTimestamp,
};
use crate::storage::{
    CapabilityUnavailable, MetadataMutation, SourceDescriptor, Storage, StorageRoleFailure,
};

#[derive(Debug)]
pub enum MetadataVerificationError {
    Capability(CapabilityUnavailable),
    Read(StorageRoleFailure),
    Acl(AclError),
    Cancelled,
    Mismatch {
        path: StoragePath,
        families: Vec<MetadataFamily>,
        observation_failures: Vec<MetadataObservationFailure>,
    },
    VersionChanged {
        path: StoragePath,
    },
}

impl fmt::Display for MetadataVerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capability(error) => write!(f, "metadata verification capability: {error}"),
            Self::Read(error) => write!(f, "target metadata verification read failed: {error}"),
            Self::Acl(error) => write!(f, "target ACL verification failed: {error}"),
            Self::Cancelled => f.write_str("metadata verification cancelled"),
            Self::Mismatch {
                path,
                families,
                observation_failures,
            } => write!(
                f,
                "target metadata verification failed at {path}: {families:?}; observations={observation_failures:?}"
            ),
            Self::VersionChanged { path } => write!(
                f,
                "target version changed before metadata verification at {path}"
            ),
        }
    }
}

impl Error for MetadataVerificationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Capability(error) => Some(error),
            Self::Read(error) => Some(error),
            Self::Acl(error) => Some(error),
            _ => None,
        }
    }
}

impl MetadataPlan {
    /// Checks the final target against this mapped plan, returning its fresh description so the
    /// caller can also verify type and size. ACLs compare native semantics; tags/xattrs compare
    /// complete sets. `expected_version` pins the publication of a versioned destination.
    ///
    /// # Errors
    /// Reports unreadable/mismatched families and retains typed target read failures.
    pub async fn verify_published(
        &self,
        target: &Storage,
        path: &StoragePath,
        expected_version: Option<&str>,
        cancel: CancellationToken,
    ) -> Result<SourceDescriptor, MetadataVerificationError> {
        if cancel.is_cancelled() {
            return Err(MetadataVerificationError::Cancelled);
        }
        // namespace 的目录投影不承诺绑定对象版本；版本化发布使用读取角色的 HEAD 描述。
        let descriptor = if expected_version.is_some() {
            target
                .read_source()
                .map_err(MetadataVerificationError::Capability)?
                .describe(path)
                .await
        } else {
            target
                .namespace()
                .map_err(MetadataVerificationError::Capability)?
                .stat(path)
                .await
        }
        .map_err(MetadataVerificationError::Read)?;
        if expected_version
            .is_some_and(|expected| descriptor.version != SourceVersion::Id(expected.to_owned()))
        {
            return Err(MetadataVerificationError::VersionChanged { path: path.clone() });
        }
        if self.mutations.is_empty() {
            return Ok(descriptor);
        }
        let (facts, permissions) = self.verification_facts(target, &descriptor).await?;
        let mut families = Vec::new();
        for (family, expected) in &self.mutations {
            if cancel.is_cancelled() {
                return Err(MetadataVerificationError::Cancelled);
            }
            if !matches_mutation(expected, &facts, &permissions)? && !families.contains(family) {
                families.push(*family);
            }
        }
        if families.is_empty() {
            Ok(descriptor)
        } else {
            Err(MetadataVerificationError::Mismatch {
                path: path.clone(),
                families,
                observation_failures: facts.failures().collect(),
            })
        }
    }

    async fn verification_facts(
        &self,
        target: &Storage,
        descriptor: &SourceDescriptor,
    ) -> Result<(MetadataObservations, PermissionFacts), MetadataVerificationError> {
        let metadata = target
            .metadata()
            .map_err(MetadataVerificationError::Capability)?;
        let plan = self
            .mutations
            .iter()
            .fold(ObservationPlan::default(), |plan, (family, _)| {
                plan.with(*family, ObservationMode::BestEffort)
            });
        let facts = match metadata.observe_listed(descriptor, plan) {
            Some(facts) => facts.map_err(MetadataVerificationError::Read)?,
            None => {
                metadata
                    .observe_copy_bound_version(
                        &descriptor.path,
                        &descriptor.source_identity,
                        &descriptor.version,
                        plan,
                    )
                    .await
                    .map_err(MetadataVerificationError::Read)?
                    .observations
            }
        };
        let permissions = if self
            .mutations
            .iter()
            .any(|(family, _)| *family == MetadataFamily::OwnershipMode)
        {
            match &descriptor.permissions {
                Some(permissions) => (**permissions).clone(),
                None => metadata
                    .permissions(descriptor)
                    .await
                    .map_err(MetadataVerificationError::Read)?,
            }
        } else {
            PermissionFacts::default()
        };
        Ok((facts, permissions))
    }
}

fn matches_mutation(
    expected: &MetadataMutation,
    facts: &MetadataObservations,
    permissions: &PermissionFacts,
) -> Result<bool, MetadataVerificationError> {
    Ok(match expected {
        MetadataMutation::Mode(mode) => permissions.mode == Some(*mode & 0o7777),
        MetadataMutation::NumericOwnership(owner) => {
            permissions.owner == Some(PrincipalIdentity::Numeric(owner.uid))
                && permissions.group == Some(PrincipalIdentity::Numeric(owner.gid))
                && permissions.mode == Some(owner.mode & 0o7777)
        }
        MetadataMutation::MappedOwnership(owner) => {
            permissions.owner == Some(PrincipalIdentity::Named(owner.owner().into()))
                && permissions.group == Some(PrincipalIdentity::Named(owner.group().into()))
                && permissions.mode == Some(owner.mode & 0o7777)
        }
        MetadataMutation::Timestamps(expected) => {
            facts.timestamps().value().is_some_and(|actual| {
                [
                    expected.accessed.zip(actual.accessed),
                    expected.modified.zip(actual.modified),
                    expected.created.zip(actual.created),
                ]
                .into_iter()
                .zip([expected.accessed, expected.modified, expected.created])
                .all(|(pair, expected)| {
                    expected.is_none() || pair.is_some_and(|(a, b)| same_time(a, b))
                })
            })
        }
        MetadataMutation::Tags(expected) => facts.tags().value().is_some_and(|actual| {
            let mut expected: Vec<_> = expected
                .iter()
                .map(|tag| (tag.key(), tag.value()))
                .collect();
            let mut actual: Vec<_> = actual.iter().map(|tag| (tag.key(), tag.value())).collect();
            expected.sort_unstable();
            actual.sort_unstable();
            expected == actual
        }),
        MetadataMutation::Xattrs(expected) => facts.xattrs().value().is_some_and(|actual| {
            let mut expected: Vec<_> = expected
                .iter()
                .map(|attr| (attr.name(), attr.value()))
                .collect();
            let mut actual: Vec<_> = actual
                .iter()
                .map(|attr| (attr.name(), attr.value()))
                .collect();
            expected.sort_unstable();
            actual.sort_unstable();
            expected == actual
        }),
        MetadataMutation::Acl(expected) => match facts.acl().value() {
            Some(actual) if actual == expected => true,
            Some(actual) => {
                equivalent_native(expected, actual).map_err(MetadataVerificationError::Acl)?
            }
            None => false,
        },
    })
}

fn same_time(a: StorageTimestamp, b: StorageTimestamp) -> bool {
    let precision = a
        .precision()
        .quantum_nanos()
        .max(b.precision().quantum_nanos());
    a.unix_nanos().div_euclid(precision) == b.unix_nanos().div_euclid(precision)
}
