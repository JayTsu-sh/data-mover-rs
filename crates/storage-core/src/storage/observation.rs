//! 将一次 namespace 描述与显式请求的元数据组成原生观察。

use thiserror::Error;

use super::{CapabilityUnavailable, SourceDescriptor, Storage, StorageRoleFailure};
use crate::model::{
    EntryKind, FailureClass, MetadataCapabilities, MetadataFamily, MetadataObservation,
    MetadataObservationParts, MetadataObservations, ModelValueError, ObservationMode,
    ObservationPlan, ObservedEntry, SourceVersion, StoragePath, Transience,
};

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;

#[derive(Debug, Error)]
pub enum EntryObservationFailure {
    #[error("current point observation cannot expand version or listing facts at {path}")]
    UnsupportedDescriptor { path: StoragePath },
    #[error(transparent)]
    Capability(#[from] CapabilityUnavailable),
    #[error(transparent)]
    Role(#[from] StorageRoleFailure),
    #[error(transparent)]
    ModelValue(#[from] ModelValueError),
}

/// 从当前条目的描述捕获事实，不枚举历史或展开目录。
/// 仅接受 `SourceVersion::Current`，拒绝历史、marker 和不可独立观察的文件描述。
/// 当前虚拟目录只投影已知内联事实，不对不存在的目录对象发起元数据请求。
///
/// 优先使用描述自带的元数据；缺少时调用 metadata role，软链接再读取精确目标。
/// 调用方显式选择观察计划，本函数不决定复制或任务策略，也不发起恢复或后台任务。
/// 后端事实的 `Bytes` 所有权直接移入观察；本操作不提供跨多次请求的原子快照。
///
/// # Errors
/// 保留角色能力缺失、后端操作失败及模型校验的有类型原因。IO 的超时由 backend
/// 约束；需要扫描取消与恢复时，调用方使用已有的 traversal 契约。
pub async fn observe_descriptor(
    storage: &Storage,
    descriptor: SourceDescriptor,
    plan: ObservationPlan,
) -> Result<ObservedEntry, EntryObservationFailure> {
    if !matches!(descriptor.version, SourceVersion::Current)
        || descriptor.listing.history.is_some()
        || descriptor.listing.directory_marker.is_some()
        || (descriptor.listing.listing_only && descriptor.kind != EntryKind::Directory)
    {
        return Err(EntryObservationFailure::UnsupportedDescriptor {
            path: descriptor.path,
        });
    }
    let metadata = if descriptor.listing.listing_only {
        descriptor.inline_observations(plan, MetadataCapabilities::default())
    } else {
        let metadata_role = storage.metadata()?;
        let result = match metadata_role.observe_listed(&descriptor, plan) {
            Some(result) => result,
            None => metadata_role.observe(&descriptor.path, plan).await,
        };
        match result {
            Ok(facts) => facts,
            Err(StorageRoleFailure::Entry(error)) if optional_failure(plan, error.class()) => {
                failed_optional_observations(
                    &descriptor,
                    plan,
                    metadata_role.observation_capabilities(),
                    error.class(),
                    error.transience(),
                )?
            }
            Err(error) => return Err(error.into()),
        }
    };
    let modified = metadata
        .timestamps()
        .value()
        .and_then(|times| times.modified)
        .or(descriptor
            .inline_timestamps
            .and_then(|times| times.modified));
    let mut entry = if descriptor.kind == EntryKind::Symlink {
        let target = storage.namespace()?.read_link(&descriptor.path).await?;
        ObservedEntry::new_symlink(
            descriptor.path,
            modified,
            descriptor.source_identity,
            target,
        )?
    } else {
        ObservedEntry::new(
            descriptor.path,
            descriptor.kind,
            descriptor.size,
            modified,
            descriptor.source_identity,
        )?
    }
    .with_permissions(descriptor.permissions)?
    .with_metadata(metadata)
    .with_rename_evidence(descriptor.rename_evidence);
    if let Some(facts) = descriptor.backend_fact {
        entry = entry.with_backend_fact_bytes(facts)?;
    }
    Ok(entry)
}

fn optional_failure(plan: ObservationPlan, class: FailureClass) -> bool {
    class != FailureClass::Cancelled
        && !MetadataFamily::ALL
            .into_iter()
            .any(|family| plan.mode(family) == ObservationMode::Required)
        && MetadataFamily::ALL
            .into_iter()
            .any(|family| plan.mode(family) == ObservationMode::BestEffort)
}

fn failed_optional_observations(
    descriptor: &SourceDescriptor,
    plan: ObservationPlan,
    capabilities: MetadataCapabilities,
    class: FailureClass,
    transience: Transience,
) -> Result<MetadataObservations, ModelValueError> {
    let inline: MetadataObservationParts =
        descriptor.inline_observations(plan, capabilities).into();
    let failed = |mode| mode == ObservationMode::BestEffort;
    // 保留 stat/listing 已证明的事实；只给需要额外读取的 family 记录失败，不伪造空值。
    MetadataObservationParts {
        acl: if failed(plan.acl()) {
            MetadataObservation::Failed { class, transience }
        } else {
            inline.acl
        },
        xattrs: if failed(plan.xattrs()) {
            MetadataObservation::Failed { class, transience }
        } else {
            inline.xattrs
        },
        tags: if failed(plan.tags()) {
            MetadataObservation::Failed { class, transience }
        } else {
            inline.tags
        },
        ownership_mode: if failed(plan.ownership_mode()) && inline.ownership_mode.value().is_none()
        {
            MetadataObservation::Failed { class, transience }
        } else {
            inline.ownership_mode
        },
        timestamps: if failed(plan.timestamps()) && inline.timestamps.value().is_none() {
            MetadataObservation::Failed { class, transience }
        } else {
            inline.timestamps
        },
        ..MetadataObservationParts::default()
    }
    .build()
}
