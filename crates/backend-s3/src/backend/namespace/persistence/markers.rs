//! 已选删除标记按精确 HEAD 复核；Current 选择另外要求它仍是唯一当前记录。
use super::super::super::histories::record;
use super::super::super::paging::Paging;
use super::super::super::source::{marker_descriptor, role_failure};
use super::super::purge::same_modified;
use super::super::{S3Namespace, S3Protocol, refuse_artifact};
use super::rejected;
use crate::model::{EntryKind, FailureClass, Operation, SourceVersion, StoragePath, VersionRecord};
use crate::storage::{SourceDescriptor, StorageRoleFailure};

pub(super) fn selected(expected: &SourceDescriptor) -> Option<&VersionRecord> {
    let history = expected.listing.history.as_ref()?;
    (expected.listing.listing_only && expected.size.is_none() && history.records().len() == 1)
        .then(|| &history.records()[0])
        .filter(|record| record.is_delete_marker())
}

impl<P: S3Protocol + 'static> S3Namespace<P> {
    pub(super) async fn confirm_marker(
        &self,
        expected: &SourceDescriptor,
    ) -> Result<(), StorageRoleFailure> {
        let marker = self.validate_marker(expected)?;
        let id = marker.version_id().unwrap_or("null");
        let facts = self
            .protocol
            .head_version(expected.path.as_str(), id)
            .await
            .map_err(|cause| role_failure(&expected.path, Operation::Publish, cause))?;
        if !facts.delete_marker || facts.version_id.as_deref() != Some(id) {
            return Err(rejected(
                &expected.path,
                FailureClass::Conflict,
                "selected target is not the bound delete marker",
            ));
        }
        let actual = marker_descriptor(&self.identity, &expected.path, &facts)?;
        if actual.source_identity != expected.source_identity
            || (id == "null" && !same_modified(facts.last_modified, marker.modified()))
        {
            return Err(rejected(
                &expected.path,
                FailureClass::Conflict,
                "target marker changed after verification",
            ));
        }
        if matches!(expected.version, SourceVersion::Current) {
            self.confirm_latest_marker(expected, marker).await?;
        }
        Ok(())
    }

    fn validate_marker<'a>(
        &self,
        expected: &'a SourceDescriptor,
    ) -> Result<&'a VersionRecord, StorageRoleFailure> {
        refuse_artifact(&expected.path)?;
        let marker = selected(expected).ok_or_else(|| {
            rejected(
                &expected.path,
                FailureClass::Unsupported,
                "no selected marker evidence",
            )
        })?;
        let id = marker.version_id().unwrap_or("null");
        let capabilities = self.protocol.bucket_capabilities();
        if !capabilities.versions
            || !capabilities.durable_object_publication
            || expected.path.as_str().is_empty()
            || expected.kind != EntryKind::File
            || expected.content_version.is_some()
            || expected.source_identity.backend() != &self.identity
            || marker.identity() != &expected.source_identity
            || expected
                .version
                .version_id()
                .is_some_and(|requested| requested != id)
            || (id == "null" && marker.modified().is_none())
        {
            return Err(rejected(
                &expected.path,
                FailureClass::Unsupported,
                "marker persistence requires a certified bucket and bound marker identity; null also requires modification evidence",
            ));
        }
        Ok(marker)
    }

    async fn confirm_latest_marker(
        &self,
        expected: &SourceDescriptor,
        wanted: &VersionRecord,
    ) -> Result<(), StorageRoleFailure> {
        let mut paging = Paging::new(self.limits);
        let mut cursor = None;
        let mut proof = CurrentMarkerProof::default();
        loop {
            let page = self
                .protocol
                .list_versions_page(expected.path.as_str(), cursor.as_ref())
                .await
                .map_err(|cause| role_failure(&expected.path, Operation::Publish, cause))?;
            if !page.rejected.is_empty() {
                return Err(rejected(
                    &expected.path,
                    FailureClass::Protocol,
                    "undecodable record in current-marker confirmation",
                ));
            }
            let listed = page.versions.len() + page.markers.len() + page.prefixes.len();
            for item in page
                .versions
                .into_iter()
                .chain(page.markers)
                .filter(|item| item.key == expected.path.as_str())
            {
                let current = record(&self.identity, &expected.path, item)
                    .map_err(StorageRoleFailure::Entry)?;
                proof.observe(&expected.path, wanted, current)?;
            }
            // continuation 是不透明的服务端 token；只有真实 EOF 才能结束证明。
            cursor = paging.advance_holding(
                &expected.path,
                listed,
                usize::from(proof.latest.is_some()),
                page.next,
            )?;
            if cursor.is_none() {
                break;
            }
        }
        proof.confirm(&expected.path, wanted)
    }
}

/// 只保留一个当前记录与冲突事实，不随 key 历史长度增长。
#[derive(Default)]
struct CurrentMarkerProof {
    latest: Option<VersionRecord>,
    historical: bool,
}

impl CurrentMarkerProof {
    fn observe(
        &mut self,
        path: &StoragePath,
        wanted: &VersionRecord,
        current: VersionRecord,
    ) -> Result<(), StorageRoleFailure> {
        self.historical |= !current.is_latest() && current.version_id() == wanted.version_id();
        if current.is_latest() {
            if self.latest.as_ref().is_some_and(|seen| seen != &current) {
                return Err(rejected(
                    path,
                    FailureClass::Protocol,
                    "multiple current records in marker confirmation",
                ));
            }
            self.latest = Some(current);
        }
        Ok(())
    }

    fn confirm(self, path: &StoragePath, wanted: &VersionRecord) -> Result<(), StorageRoleFailure> {
        let actual = self.latest.ok_or_else(|| {
            rejected(path, FailureClass::NotFound, "current marker was not found")
        })?;
        if self.historical && actual.version_id() == wanted.version_id() {
            return Err(rejected(
                path,
                FailureClass::Protocol,
                "marker is both current and historical",
            ));
        }
        if !actual.is_delete_marker()
            || actual.version_id() != wanted.version_id()
            || actual.identity() != wanted.identity()
            || (wanted.version_id().is_none()
                && !same_modified(actual.modified(), wanted.modified()))
        {
            return Err(rejected(
                path,
                FailureClass::Conflict,
                "published current marker no longer matches the bound version",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::storage::backends::s3::{S3ListedVersion, S3VersionFacts, tests::identity};

    fn marker(latest: bool, id: &str) -> VersionRecord {
        record(
            &identity(),
            &StoragePath::new("key").unwrap(),
            S3ListedVersion {
                key: "key".into(),
                facts: S3VersionFacts {
                    version_id: id.into(),
                    is_latest: latest,
                    delete_marker: true,
                    size: 0,
                    etag: String::new(),
                },
                last_modified: None,
                position: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn contradictory_current_history_and_multiple_current_fail_in_either_order() {
        let path = StoragePath::new("key").unwrap();
        let wanted = marker(false, "v1");
        for records in [
            [marker(true, "v1"), marker(false, "v1")],
            [marker(false, "v1"), marker(true, "v1")],
        ] {
            let mut proof = CurrentMarkerProof::default();
            for current in records {
                proof.observe(&path, &wanted, current).unwrap();
            }
            assert!(
                matches!(proof.confirm(&path, &wanted), Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol)
            );
        }
        let mut proof = CurrentMarkerProof::default();
        proof.observe(&path, &wanted, marker(true, "v1")).unwrap();
        assert!(
            matches!(proof.observe(&path, &wanted, marker(true, "v2")), Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol)
        );
    }
}
