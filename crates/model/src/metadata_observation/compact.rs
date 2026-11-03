//! 可选的大块元数据与普通列举的内联事实分开存放，公开观察状态保持完整。

use std::fmt;

use super::{
    AclMetadata, ExtendedAttribute, MetadataObservation, MetadataObservationFailure,
    MetadataObservationParts, MetadataObservations, ObjectTag,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ColdMetadata {
    pub(super) retained_failures: Option<Box<[MetadataObservationFailure]>>,
    acl: MetadataObservation<AclMetadata>,
    xattrs: MetadataObservation<Vec<ExtendedAttribute>>,
    tags: MetadataObservation<Vec<ObjectTag>>,
}

impl Default for ColdMetadata {
    fn default() -> Self {
        Self {
            retained_failures: None,
            acl: MetadataObservation::NotRequested,
            xattrs: MetadataObservation::NotRequested,
            tags: MetadataObservation::NotRequested,
        }
    }
}

impl ColdMetadata {
    pub(super) fn pack(
        acl: MetadataObservation<AclMetadata>,
        xattrs: MetadataObservation<Vec<ExtendedAttribute>>,
        tags: MetadataObservation<Vec<ObjectTag>>,
    ) -> Option<Box<Self>> {
        // 仅未请求状态省略；空 Value、失败和不适用等状态仍须原样保留。
        if matches!(acl, MetadataObservation::NotRequested)
            && matches!(xattrs, MetadataObservation::NotRequested)
            && matches!(tags, MetadataObservation::NotRequested)
        {
            None
        } else {
            Some(Box::new(Self {
                retained_failures: None,
                acl,
                xattrs,
                tags,
            }))
        }
    }
}

impl MetadataObservations {
    pub(super) fn cold_mut(&mut self) -> &mut ColdMetadata {
        self.cold.get_or_insert_with(Box::default)
    }

    #[must_use]
    pub const fn acl(&self) -> &MetadataObservation<AclMetadata> {
        match &self.cold {
            Some(cold) => &cold.acl,
            None => &MetadataObservation::NotRequested,
        }
    }

    #[must_use]
    pub const fn xattrs(&self) -> &MetadataObservation<Vec<ExtendedAttribute>> {
        match &self.cold {
            Some(cold) => &cold.xattrs,
            None => &MetadataObservation::NotRequested,
        }
    }

    #[must_use]
    pub const fn tags(&self) -> &MetadataObservation<Vec<ObjectTag>> {
        match &self.cold {
            Some(cold) => &cold.tags,
            None => &MetadataObservation::NotRequested,
        }
    }
}

impl From<MetadataObservations> for MetadataObservationParts {
    fn from(value: MetadataObservations) -> Self {
        let cold = value.cold.map(|cold| *cold).unwrap_or_default();
        Self {
            retained_failures: cold.retained_failures,
            acl: cold.acl,
            xattrs: cold.xattrs,
            tags: cold.tags,
            ownership_mode: value.ownership_mode,
            timestamps: value.timestamps,
        }
    }
}

impl fmt::Debug for MetadataObservations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataObservations")
            .field(
                "retained_failures",
                &self
                    .cold
                    .as_ref()
                    .and_then(|cold| cold.retained_failures.as_ref()),
            )
            .field("acl", self.acl())
            .field("xattrs", self.xattrs())
            .field("tags", self.tags())
            .field("ownership_mode", &self.ownership_mode)
            .field("timestamps", &self.timestamps)
            .finish()
    }
}
