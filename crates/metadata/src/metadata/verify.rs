//! 核验已经发布的目标元数据；读取目标，不重新读取源端。

use std::{error::Error, fmt};

use tokio_util::sync::CancellationToken;

use super::MetadataPlan;
use crate::model::acl::{AclError, equivalent_native};
use crate::model::{
    EntryKind, IdentityStrength, MetadataFamily, MetadataObservationFailure, MetadataObservations,
    ObservationMode, ObservationPlan, PermissionFacts, PrincipalIdentity, SourceVersion,
    StoragePath, StorageTimestamp,
};
use crate::storage::{
    CapabilityUnavailable, Metadata, MetadataMutation, SourceDescriptor, Storage,
    StorageRoleFailure,
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
    /// 核验当前目标或指定历史版本的已捕获元数据计划，返回新描述供调用方核对类型和大小。
    /// Current 且无发布约束时保留 namespace 的目录语义；Id 精确读取历史版本。
    /// `expected_current_version` 独立约束当前发布版本，不会被历史版本读取替代。
    /// `current_anchor` 只接受无当前版本约束的 Current 稳定目录描述；非法组合或身份／
    /// 类型变化返回 `VersionChanged`。有锚点时所有额外属性读取后都实际 fresh stat 并核对
    /// 稳定身份，不比较会因本轮属性写入变化的 `content_version`；这不是原子命名空间锁。
    /// 旧描述仅是内联资格提示，不保证最少读取次数。OwnershipMode 缺少 authoritative
    /// permissions 时保守完整回退，不从 inline ownership/mode 猜测或补齐权限。
    /// 最终 fresh 描述信息不足时重新绑定回读并再次 fresh；fresh 失败不会降级为缓存读取。
    /// None 保持原有当前／历史版本和权限读取语义。
    /// 取消会丢弃在途只读请求，不再等待其返回或启动后续读取。
    ///
    /// # Errors
    /// 保留不可读属性、版本变化、元数据不一致和取消的有类型错误。
    pub async fn verify_published(
        &self,
        target: &Storage,
        path: &StoragePath,
        version: &SourceVersion,
        expected_current_version: Option<&str>,
        current_anchor: Option<&SourceDescriptor>,
        cancel: CancellationToken,
    ) -> Result<SourceDescriptor, MetadataVerificationError> {
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(MetadataVerificationError::Cancelled),
            result = self.verify_selected(target, path, version, expected_current_version, current_anchor, cancel.clone()) => {
                match result {
                    Ok(_) if cancel.is_cancelled() => Err(MetadataVerificationError::Cancelled),
                    result => result,
                }
            }
        }
    }

    async fn verify_selected(
        &self,
        target: &Storage,
        path: &StoragePath,
        version: &SourceVersion,
        expected_current_version: Option<&str>,
        current_anchor: Option<&SourceDescriptor>,
        cancel: CancellationToken,
    ) -> Result<SourceDescriptor, MetadataVerificationError> {
        if let Some(anchor) = current_anchor {
            check_anchor_request(path, version, expected_current_version, anchor)?;
            return self
                .verify_anchored_directory(target, path, anchor, &cancel)
                .await;
        }
        let current = if let Some(expected) = expected_current_version {
            let descriptor = target
                .read_source()
                .map_err(MetadataVerificationError::Capability)?
                .describe(path)
                .await
                .map_err(MetadataVerificationError::Read)?;
            if descriptor.version != SourceVersion::Id(expected.to_owned()) {
                return Err(MetadataVerificationError::VersionChanged { path: path.clone() });
            }
            Some(descriptor)
        } else {
            None
        };
        let descriptor = match version {
            SourceVersion::Current => match current {
                Some(descriptor) => descriptor,
                None => target
                    .namespace()
                    .map_err(MetadataVerificationError::Capability)?
                    .stat(path, false)
                    .await
                    .map_err(MetadataVerificationError::Read)?,
            },
            SourceVersion::Id(_) => {
                let descriptor = target
                    .read_source()
                    .map_err(MetadataVerificationError::Capability)?
                    .describe_version(path, version)
                    .await
                    .map_err(MetadataVerificationError::Read)?;
                if &descriptor.version != version {
                    return Err(MetadataVerificationError::VersionChanged { path: path.clone() });
                }
                descriptor
            }
        };
        self.verify_descriptor(target, descriptor, cancel).await
    }

    async fn verify_descriptor(
        &self,
        target: &Storage,
        descriptor: SourceDescriptor,
        cancel: CancellationToken,
    ) -> Result<SourceDescriptor, MetadataVerificationError> {
        if self.mutations.is_empty() {
            return Ok(descriptor);
        }
        let (facts, permissions) = self.verification_facts(target, &descriptor, None).await?;
        self.verify_facts(descriptor, &facts, &permissions, &cancel)
    }

    async fn verify_anchored_directory(
        &self,
        target: &Storage,
        path: &StoragePath,
        anchor: &SourceDescriptor,
        cancel: &CancellationToken,
    ) -> Result<SourceDescriptor, MetadataVerificationError> {
        if self.mutations.is_empty() {
            return read_anchored_directory(target, path, anchor, true, cancel).await;
        }
        let metadata = target
            .metadata()
            .map_err(MetadataVerificationError::Capability)?;
        // 旧描述只判断是否值得尝试纯内联；其值和错误都不是最终核验结果。
        let inline = self
            .inline_verification_facts(metadata.as_ref(), anchor)
            .is_some_and(|facts| facts.is_ok());
        let descriptor = read_anchored_directory(target, path, anchor, inline, cancel).await?;
        if inline
            && let Some(facts) = self.inline_verification_facts(metadata.as_ref(), &descriptor)
        {
            let (facts, permissions) = facts?;
            return self.verify_facts(descriptor, &facts, &permissions, cancel);
        }
        let (facts, permissions) = self
            .verification_facts(target, &descriptor, Some(cancel))
            .await?;
        let actual = read_anchored_directory(target, path, anchor, true, cancel).await?;
        self.verify_facts(actual, &facts, &permissions, cancel)
    }

    fn inline_verification_facts(
        &self,
        metadata: &dyn Metadata,
        descriptor: &SourceDescriptor,
    ) -> Option<Result<(MetadataObservations, PermissionFacts), MetadataVerificationError>> {
        let permissions = if self
            .mutations
            .iter()
            .any(|(family, _)| *family == MetadataFamily::OwnershipMode)
        {
            descriptor.permissions.as_deref()?.clone()
        } else {
            PermissionFacts::default()
        };
        let facts = match metadata.observe_listed(descriptor, self.verification_plan())? {
            Ok(facts) => facts,
            Err(source) => return Some(Err(MetadataVerificationError::Read(source))),
        };
        self.mutations
            .iter()
            .all(|(_, expected)| complete_inline_facts(expected, &facts, &permissions))
            .then_some(Ok((facts, permissions)))
    }

    fn verification_plan(&self) -> ObservationPlan {
        self.mutations
            .iter()
            .fold(ObservationPlan::default(), |plan, (family, _)| {
                plan.with(*family, ObservationMode::BestEffort)
            })
    }

    fn verify_facts(
        &self,
        descriptor: SourceDescriptor,
        facts: &MetadataObservations,
        permissions: &PermissionFacts,
        cancel: &CancellationToken,
    ) -> Result<SourceDescriptor, MetadataVerificationError> {
        let mut families = Vec::new();
        for (family, expected) in &self.mutations {
            if cancel.is_cancelled() {
                return Err(MetadataVerificationError::Cancelled);
            }
            if !matches_mutation(expected, facts, permissions)? && !families.contains(family) {
                families.push(*family);
            }
        }
        if families.is_empty() {
            Ok(descriptor)
        } else {
            Err(MetadataVerificationError::Mismatch {
                path: descriptor.path.clone(),
                families,
                observation_failures: facts.failures().collect(),
            })
        }
    }

    async fn verification_facts(
        &self,
        target: &Storage,
        descriptor: &SourceDescriptor,
        cancel: Option<&CancellationToken>,
    ) -> Result<(MetadataObservations, PermissionFacts), MetadataVerificationError> {
        check_cancel(cancel)?;
        let metadata = target
            .metadata()
            .map_err(MetadataVerificationError::Capability)?;
        let plan = self.verification_plan();
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
        check_cancel(cancel)?;
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
        check_cancel(cancel)?;
        Ok((facts, permissions))
    }
}

fn check_cancel(cancel: Option<&CancellationToken>) -> Result<(), MetadataVerificationError> {
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        Err(MetadataVerificationError::Cancelled)
    } else {
        Ok(())
    }
}

fn check_anchor_request(
    path: &StoragePath,
    version: &SourceVersion,
    expected_current_version: Option<&str>,
    anchor: &SourceDescriptor,
) -> Result<(), MetadataVerificationError> {
    if version != &SourceVersion::Current
        || expected_current_version.is_some()
        || &anchor.path != path
        || anchor.version != SourceVersion::Current
        || anchor.kind != EntryKind::Directory
        || anchor.source_identity.strength() != IdentityStrength::StableWithinBackend
    {
        return Err(MetadataVerificationError::VersionChanged { path: path.clone() });
    }
    Ok(())
}

async fn read_anchored_directory(
    target: &Storage,
    path: &StoragePath,
    anchor: &SourceDescriptor,
    fresh: bool,
    cancel: &CancellationToken,
) -> Result<SourceDescriptor, MetadataVerificationError> {
    check_cancel(Some(cancel))?;
    let descriptor = target
        .namespace()
        .map_err(MetadataVerificationError::Capability)?
        .stat(path, fresh)
        .await
        .map_err(MetadataVerificationError::Read)?;
    check_cancel(Some(cancel))?;
    if &descriptor.path != path
        || descriptor.kind != EntryKind::Directory
        || descriptor.version != SourceVersion::Current
        || descriptor.source_identity.strength() != IdentityStrength::StableWithinBackend
        || descriptor.source_identity.identity_key() != anchor.source_identity.identity_key()
    {
        return Err(MetadataVerificationError::VersionChanged { path: path.clone() });
    }
    Ok(descriptor)
}

fn complete_inline_facts(
    expected: &MetadataMutation,
    facts: &MetadataObservations,
    permissions: &PermissionFacts,
) -> bool {
    match expected {
        MetadataMutation::Mode(_) => permissions.mode.is_some(),
        MetadataMutation::NumericOwnership(_) | MetadataMutation::MappedOwnership(_) => {
            permissions.mode.is_some() && permissions.owner.is_some() && permissions.group.is_some()
        }
        MetadataMutation::Timestamps(expected) => {
            facts.timestamps().value().is_some_and(|actual| {
                expected.accessed.is_none_or(|_| actual.accessed.is_some())
                    && expected.modified.is_none_or(|_| actual.modified.is_some())
                    && expected.created.is_none_or(|_| actual.created.is_some())
            })
        }
        MetadataMutation::Tags(_) => facts.tags().value().is_some(),
        MetadataMutation::Xattrs(_) => facts.xattrs().value().is_some(),
        MetadataMutation::Acl(_) => facts.acl().value().is_some(),
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
