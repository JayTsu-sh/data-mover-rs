use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use binrw::{BinRead as _, BinWrite as _};

use super::source::{CifsSourceFacts, classify, descriptor_from_facts, entry_failure};
use crate::model::{
    AclEncoding, AclMetadata, BackendIdentity, FailureClass, MetadataObservation,
    MetadataObservations, MetadataProvenance, ObservationMode, ObservationPlan, Operation,
    StoragePath, StorageTimestamp, TimePrecision, TimestampMetadata,
};
use crate::storage::{Metadata, MetadataMutation, StorageRoleFailure};

#[derive(Clone)]
pub(super) struct CifsInlineMetadata {
    pub(super) facts: CifsSourceFacts,
    pub(super) accessed: std::time::SystemTime,
    pub(super) modified: std::time::SystemTime,
    pub(super) created: std::time::SystemTime,
    /// `FILE_ATTRIBUTE_READONLY`, when the operation that produced this record carried it.
    ///
    /// Both `QUERY_DIRECTORY` records and the `CREATE` response snapshot carry the attribute,
    /// so `list` and `metadata` fill it. It stays optional for protocols whose records omit it,
    /// so an unknown attribute is never reported as a writable entry.
    pub(super) readonly: Option<bool>,
    /// `FILE_ATTRIBUTE_REPARSE_POINT` (symbolic link, junction, …) as the record reported it;
    /// `false` when it carried no attributes. A recursive delete never descends into one.
    pub(super) reparse_point: bool,
}

#[async_trait]
pub(super) trait CifsMetadataProtocol: Send + Sync {
    async fn metadata(&self, path: &StoragePath) -> smb_domain::Result<CifsInlineMetadata>;
    async fn set_timestamps(
        &self,
        path: &StoragePath,
        value: TimestampMetadata,
    ) -> smb_domain::Result<()>;
    async fn get_acl(
        &self,
        path: &StoragePath,
    ) -> smb_domain::Result<smb_domain::SecurityDescriptor>;
    async fn set_acl(
        &self,
        path: &StoragePath,
        descriptor: smb_domain::SecurityDescriptor,
    ) -> smb_domain::Result<()>;
}

pub(super) struct CifsMetadata {
    protocol: Arc<dyn CifsMetadataProtocol>,
    identity: BackendIdentity,
}

impl CifsMetadata {
    pub(super) fn new<P>(protocol: Arc<P>, identity: BackendIdentity) -> Self
    where
        P: CifsMetadataProtocol + 'static,
    {
        Self { protocol, identity }
    }
}

impl CifsMetadata {
    async fn observe_inline(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
        inline: CifsInlineMetadata,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let acl = observe_acl(self.protocol.as_ref(), path, plan.acl()).await?;
        let timestamps = match plan.timestamps() {
            ObservationMode::Omit => MetadataObservation::NotRequested,
            _ => MetadataObservation::Value {
                value: TimestampMetadata {
                    accessed: timestamp(inline.accessed),
                    modified: timestamp(inline.modified),
                    created: timestamp(inline.created),
                },
                provenance: MetadataProvenance::Inline,
            },
        };
        MetadataObservations::new(
            acl,
            cannot_read(plan.xattrs()),
            not_applicable(plan.tags()),
            not_applicable(plan.ownership_mode()),
            timestamps,
        )
        .map_err(|_| entry_failure(path, Operation::Metadata, FailureClass::Protocol))
    }
}

#[async_trait]
impl Metadata for CifsMetadata {
    fn copied_metadata_observation_plan(&self) -> Option<ObservationPlan> {
        Some(
            ObservationPlan::default()
                .with_ownership_mode(ObservationMode::InlineOnly)
                .with_timestamps(ObservationMode::Required),
        )
    }

    async fn observe_bound(
        &self,
        path: &StoragePath,
        expected: &crate::model::SourceIdentity,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let inline = self
            .protocol
            .metadata(path)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))?;
        let observed =
            descriptor_from_facts(&self.identity, path, &inline.facts, Operation::Metadata)?;
        if observed.source_identity != *expected {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Conflict,
            ));
        }
        self.observe_inline(path, plan, inline).await
    }

    async fn observe(
        &self,
        path: &StoragePath,
        plan: ObservationPlan,
    ) -> Result<MetadataObservations, StorageRoleFailure> {
        let inline = self
            .protocol
            .metadata(path)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))?;
        self.observe_inline(path, plan, inline).await
    }

    async fn apply(
        &self,
        path: &StoragePath,
        mutation: MetadataMutation,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), StorageRoleFailure> {
        if cancel.is_cancelled() {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Cancelled,
            ));
        }
        if let MetadataMutation::Timestamps(value) = mutation {
            return self
                .protocol
                .set_timestamps(path, value)
                .await
                .map_err(|error| classify(path, Operation::Metadata, &error));
        }
        let MetadataMutation::Acl(value) = mutation else {
            return Err(entry_failure(
                path,
                Operation::Metadata,
                FailureClass::Unsupported,
            ));
        };
        let source = decode_acl(path, &value)?;
        let target = self
            .protocol
            .get_acl(path)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))?;
        let Some(merged) = merge_dacl(&source, target) else {
            return Ok(());
        };
        self.protocol
            .set_acl(path, merged)
            .await
            .map_err(|error| classify(path, Operation::Metadata, &error))
    }
}

/// Keeps only explicit DACL entries plus the inheritance-protection bit, mirroring the
/// Windows `acl::copy_acl` contract: inherited ACEs belong to the destination tree.
fn explicit_dacl(mut descriptor: smb_domain::SecurityDescriptor) -> smb_domain::SecurityDescriptor {
    if let Some(dacl) = descriptor.dacl.as_mut() {
        dacl.ace.retain(|ace| !ace.ace_flags.inherited());
    }
    descriptor
}

/// Builds the descriptor to apply: source explicit ACEs first, then the target's inherited
/// ACEs (canonical order) unless the source DACL is protected, since `SE_DACL_PROTECTED`
/// means the object stops inheriting. Stale explicit ACEs on the target are dropped so
/// the result mirrors the source. Returns `None` when there is nothing to apply: the
/// source has a NULL DACL (no ACL information, everyone-allowed on NTFS), or the target
/// already matches (no explicit ACEs on either side and the same protection bit), so an
/// unchanged file costs no `SET_INFO`.
fn merge_dacl(
    source: &smb_domain::SecurityDescriptor,
    mut target: smb_domain::SecurityDescriptor,
) -> Option<smb_domain::SecurityDescriptor> {
    source.dacl.as_ref()?;
    let source_protected = source.control.dacl_protected();
    let explicit: Vec<_> = source
        .dacl
        .iter()
        .flat_map(|dacl| dacl.ace.iter())
        .filter(|ace| !ace.ace_flags.inherited())
        .cloned()
        .collect();
    let target_has_explicit = target
        .dacl
        .as_ref()
        .is_some_and(|dacl| dacl.ace.iter().any(|ace| !ace.ace_flags.inherited()));
    if source_protected == target.control.dacl_protected()
        && explicit.is_empty()
        && !target_has_explicit
    {
        return None;
    }
    let revision = source
        .dacl
        .as_ref()
        .or(target.dacl.as_ref())
        .map_or(smb_domain::protocol::AclRevision::Nt4, |dacl| {
            dacl.acl_revision
        });
    let mut ace = explicit;
    match target.dacl.take() {
        Some(dacl) if !source_protected => {
            ace.extend(dacl.ace.into_iter().filter(|ace| ace.ace_flags.inherited()));
        }
        _ => {}
    }
    target.dacl = Some(smb_domain::protocol::ACL {
        acl_revision: revision,
        ace,
    });
    target.control = target
        .control
        .with_dacl_present(true)
        .with_dacl_protected(source_protected);
    Some(target)
}

fn decode_acl(
    path: &StoragePath,
    value: &AclMetadata,
) -> Result<smb_domain::SecurityDescriptor, StorageRoleFailure> {
    if value.encoding() != AclEncoding::WindowsSecurityDescriptor {
        return Err(entry_failure(
            path,
            Operation::Metadata,
            FailureClass::InvalidInput,
        ));
    }
    let bytes = value
        .access()
        .ok_or_else(|| entry_failure(path, Operation::Metadata, FailureClass::InvalidInput))?;
    smb_domain::SecurityDescriptor::read_le(&mut Cursor::new(bytes))
        .map_err(|_| entry_failure(path, Operation::Metadata, FailureClass::InvalidInput))
}

async fn observe_acl(
    protocol: &dyn CifsMetadataProtocol,
    path: &StoragePath,
    mode: ObservationMode,
) -> Result<MetadataObservation<AclMetadata>, StorageRoleFailure> {
    if matches!(mode, ObservationMode::Omit | ObservationMode::InlineOnly) {
        return Ok(MetadataObservation::NotRequested);
    }
    match protocol.get_acl(path).await {
        Ok(descriptor) => {
            let mut output = Cursor::new(Vec::new());
            explicit_dacl(descriptor)
                .write_le(&mut output)
                .map_err(|_| entry_failure(path, Operation::Metadata, FailureClass::Protocol))?;
            let value =
                AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, output.into_inner())
                    .map_err(|_| {
                        entry_failure(path, Operation::Metadata, FailureClass::Protocol)
                    })?;
            Ok(MetadataObservation::Value {
                value,
                provenance: MetadataProvenance::AdditionalCall,
            })
        }
        Err(error) => match classify(path, Operation::Metadata, &error) {
            StorageRoleFailure::Entry(error) if mode == ObservationMode::BestEffort => {
                Ok(MetadataObservation::Failed {
                    class: error.class(),
                    transience: error.transience(),
                })
            }
            failure => Err(failure),
        },
    }
}

/// A family this adapter cannot read at all — extended attributes: SMB carries them, but the
/// smb-rs domain API exposes no EA query (upstream request S2). Saying `NotApplicable` instead
/// would claim the file has none, and a copy that asked for them would drop them without a reason.
fn cannot_read<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::Unsupported
    }
}

fn not_applicable<T>(mode: ObservationMode) -> MetadataObservation<T> {
    if mode == ObservationMode::Omit {
        MetadataObservation::NotRequested
    } else {
        MetadataObservation::NotApplicable
    }
}

pub(super) fn timestamp(value: std::time::SystemTime) -> Option<StorageTimestamp> {
    let nanos = match value.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i128::try_from(duration.as_nanos()).ok()?,
        Err(error) => -i128::try_from(error.duration().as_nanos()).ok()?,
    };
    StorageTimestamp::new(nanos, TimePrecision::HundredNanoseconds).ok()
}

pub(super) fn timestamp_update(
    value: TimestampMetadata,
) -> smb_domain::Result<smb_domain::MetadataUpdate> {
    Ok(smb_domain::MetadataUpdate {
        accessed: system_time(value.accessed)?,
        written: system_time(value.modified)?,
        created: system_time(value.created)?,
    })
}

fn system_time(
    value: Option<StorageTimestamp>,
) -> smb_domain::Result<Option<std::time::SystemTime>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let nanos = value.unix_nanos();
    let magnitude = nanos.unsigned_abs();
    let seconds = u64::try_from(magnitude / 1_000_000_000)
        .map_err(|_| smb_domain::Error::InvalidArgument("timestamp is out of range".into()))?;
    let fraction = u32::try_from(magnitude % 1_000_000_000)
        .map_err(|_| smb_domain::Error::InvalidArgument("invalid timestamp fraction".into()))?;
    let duration = std::time::Duration::new(seconds, fraction);
    let time = if nanos < 0 {
        std::time::UNIX_EPOCH.checked_sub(duration)
    } else {
        std::time::UNIX_EPOCH.checked_add(duration)
    };
    time.map(Some)
        .ok_or_else(|| smb_domain::Error::InvalidArgument("timestamp is out of range".into()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct ProbeProtocol {
        acl_calls: AtomicUsize,
        sets: std::sync::Mutex<Vec<TimestampMetadata>>,
    }

    #[async_trait]
    impl CifsMetadataProtocol for ProbeProtocol {
        async fn metadata(&self, _path: &StoragePath) -> smb_domain::Result<CifsInlineMetadata> {
            Ok(CifsInlineMetadata {
                facts: CifsSourceFacts {
                    kind: crate::model::EntryKind::File,
                    size: 4,
                    identity: bytes::Bytes::from_static(b"source-version"),
                    file_id: None,
                    maximum_read_chunk: 1024,
                },
                accessed: std::time::UNIX_EPOCH,
                modified: std::time::UNIX_EPOCH,
                created: std::time::UNIX_EPOCH,
                readonly: None,
                reparse_point: false,
            })
        }

        async fn set_timestamps(
            &self,
            _path: &StoragePath,
            value: TimestampMetadata,
        ) -> smb_domain::Result<()> {
            self.sets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(value);
            Ok(())
        }

        async fn get_acl(
            &self,
            _path: &StoragePath,
        ) -> smb_domain::Result<smb_domain::SecurityDescriptor> {
            self.acl_calls.fetch_add(1, Ordering::SeqCst);
            Err(smb_domain::Error::InvalidMessage(
                "scripted ACL failure".into(),
            ))
        }

        async fn set_acl(
            &self,
            _path: &StoragePath,
            _descriptor: smb_domain::SecurityDescriptor,
        ) -> smb_domain::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn copied_timestamps_are_bound_to_the_observed_source_version()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(ProbeProtocol::default());
        let identity = BackendIdentity::new(crate::model::BackendKind::Cifs, "test")?;
        let adapter = CifsMetadata::new(protocol.clone(), identity.clone());
        let path = StoragePath::new("source")?;
        let mut inline = protocol.metadata(&path).await?;
        let plan = adapter
            .copied_metadata_observation_plan()
            .ok_or("missing plan")?;
        let descriptor =
            descriptor_from_facts(&identity, &path, &inline.facts, Operation::Metadata)?;
        let observed = adapter
            .observe_bound(&path, &descriptor.source_identity, plan)
            .await?;
        assert!(matches!(
            observed.timestamps(),
            MetadataObservation::Value { .. }
        ));
        assert_eq!(
            observed.ownership_mode(),
            &MetadataObservation::NotApplicable
        );
        assert_eq!(protocol.acl_calls.load(Ordering::SeqCst), 0);
        inline.facts.identity = bytes::Bytes::from_static(b"different-version");
        let changed = descriptor_from_facts(&identity, &path, &inline.facts, Operation::Metadata)?;
        assert!(
            matches!(adapter.observe_bound(&path, &changed.source_identity, plan).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Conflict)
        );
        Ok(())
    }

    #[tokio::test]
    async fn mtime_only_application_and_cancellation_preserve_unrequested_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let protocol = Arc::new(ProbeProtocol::default());
        let adapter = CifsMetadata::new(
            protocol.clone(),
            BackendIdentity::new(crate::model::BackendKind::Cifs, "test")?,
        );
        let path = StoragePath::new("target")?;
        let value = TimestampMetadata {
            accessed: None,
            modified: Some(StorageTimestamp::new(
                1_700_000_001_123_456_700,
                TimePrecision::HundredNanoseconds,
            )?),
            created: None,
        };
        let cancel = tokio_util::sync::CancellationToken::new();
        adapter
            .apply(&path, MetadataMutation::Timestamps(value), cancel.clone())
            .await?;
        let update = timestamp_update(value)?;
        assert_eq!(update.accessed, None);
        assert_eq!(update.created, None);
        assert_eq!(update.written.and_then(timestamp), value.modified);
        cancel.cancel();
        assert!(
            matches!(adapter.apply(&path, MetadataMutation::Timestamps(value), cancel).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Cancelled && error.transience() == crate::model::Transience::Transient)
        );
        assert_eq!(
            *protocol
                .sets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![value]
        );
        Ok(())
    }

    #[test]
    fn timestamp_conversion_handles_pre_epoch_and_rejects_overflow()
    -> Result<(), Box<dyn std::error::Error>> {
        let before = StorageTimestamp::new(-100, TimePrecision::HundredNanoseconds)?;
        assert_eq!(system_time(Some(before))?.and_then(timestamp), Some(before));
        assert!(
            system_time(Some(StorageTimestamp::new(
                i128::MAX,
                TimePrecision::Nanoseconds
            )?))
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn xattrs_asked_for_are_unreadable_not_absent() -> Result<(), Box<dyn std::error::Error>>
    {
        let metadata = CifsMetadata::new(
            Arc::new(ProbeProtocol::default()),
            BackendIdentity::new(crate::model::BackendKind::Cifs, "test")?,
        );
        let path = StoragePath::new("file")?;
        let omitted = metadata.observe(&path, ObservationPlan::default()).await?;
        assert_eq!(omitted.xattrs(), &MetadataObservation::NotRequested);
        for mode in [ObservationMode::BestEffort, ObservationMode::Required] {
            let asked = metadata
                .observe(&path, ObservationPlan::default().with_xattrs(mode))
                .await?;
            assert_eq!(
                asked.xattrs(),
                &MetadataObservation::Unsupported,
                "{mode:?}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn acl_requires_explicit_additional_call_mode() -> Result<(), Box<dyn std::error::Error>>
    {
        let protocol = Arc::new(ProbeProtocol::default());
        let metadata = CifsMetadata::new(
            Arc::clone(&protocol),
            BackendIdentity::new(crate::model::BackendKind::Cifs, "test")?,
        );
        let path = StoragePath::new("file")?;

        for plan in [
            ObservationPlan::default(),
            ObservationPlan::default().with_acl(ObservationMode::InlineOnly),
        ] {
            let observed = metadata.observe(&path, plan).await?;
            assert_eq!(observed.acl(), &MetadataObservation::NotRequested);
        }
        assert_eq!(protocol.acl_calls.load(Ordering::SeqCst), 0);

        let observed = metadata
            .observe(
                &path,
                ObservationPlan::default().with_acl(ObservationMode::BestEffort),
            )
            .await?;
        assert!(matches!(
            observed.acl(),
            MetadataObservation::Failed {
                class: FailureClass::Protocol,
                ..
            }
        ));
        assert_eq!(protocol.acl_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}

#[cfg(test)]
mod acl_tests {
    use std::str::FromStr as _;

    use smb_domain::protocol::{
        ACE, ACL, AccessAce, AccessMask, AceFlags, AceValue, AclRevision, SID, SecurityDescriptor,
        SecurityDescriptorControl,
    };

    use super::{explicit_dacl, merge_dacl};

    type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    fn ace(sid: &str, inherited: bool) -> Result<ACE> {
        Ok(ACE {
            ace_flags: AceFlags::new().with_inherited(inherited),
            value: AceValue::AccessAllowed(AccessAce {
                access_mask: AccessMask::new().with_generic_read(true),
                sid: SID::from_str(sid)?,
            }),
        })
    }

    fn descriptor(aces: Vec<ACE>, protected: bool) -> SecurityDescriptor {
        SecurityDescriptor {
            sbz1: 0,
            control: SecurityDescriptorControl::new()
                .with_self_relative(true)
                .with_dacl_present(true)
                .with_dacl_protected(protected),
            owner_sid: None,
            group_sid: None,
            sacl: None,
            dacl: Some(ACL {
                acl_revision: AclRevision::Nt4,
                ace: aces,
            }),
        }
    }

    fn sids(descriptor: &SecurityDescriptor) -> Vec<(String, bool)> {
        descriptor
            .dacl
            .iter()
            .flat_map(|dacl| dacl.ace.iter())
            .map(|ace| {
                let sid = match &ace.value {
                    AceValue::AccessAllowed(value) => value.sid.to_string(),
                    other => format!("{other:?}"),
                };
                (sid, ace.ace_flags.inherited())
            })
            .collect()
    }

    #[test]
    fn observation_keeps_only_explicit_aces_and_the_protection_bit() -> Result {
        let observed = explicit_dacl(descriptor(
            vec![ace("S-1-1-0", true)?, ace("S-1-5-32-545", false)?],
            true,
        ));
        assert_eq!(sids(&observed), vec![("S-1-5-32-545".to_owned(), false)]);
        assert!(observed.control.dacl_protected());
        assert!(observed.control.dacl_present());
        Ok(())
    }

    #[test]
    fn unchanged_target_costs_no_set_info() -> Result {
        let source = descriptor(Vec::new(), false);
        let target = descriptor(vec![ace("S-1-1-0", true)?], false);
        assert!(merge_dacl(&source, target).is_none());
        Ok(())
    }

    #[test]
    fn merge_puts_source_explicit_aces_before_target_inherited_ones() -> Result {
        let source = descriptor(vec![ace("S-1-5-32-545", false)?], false);
        let target = descriptor(
            vec![ace("S-1-5-32-544", false)?, ace("S-1-1-0", true)?],
            false,
        );
        let merged = merge_dacl(&source, target).ok_or("merge expected")?;
        assert_eq!(
            sids(&merged),
            vec![
                ("S-1-5-32-545".to_owned(), false),
                ("S-1-1-0".to_owned(), true),
            ]
        );
        assert!(!merged.control.dacl_protected());
        assert!(merged.control.dacl_present());
        Ok(())
    }

    #[test]
    fn protected_source_stops_inheritance_on_the_target() -> Result {
        let source = descriptor(vec![ace("S-1-5-32-545", false)?], true);
        let target = descriptor(
            vec![ace("S-1-5-32-544", false)?, ace("S-1-1-0", true)?],
            false,
        );
        let merged = merge_dacl(&source, target).ok_or("merge expected")?;
        assert_eq!(sids(&merged), vec![("S-1-5-32-545".to_owned(), false)]);
        assert!(merged.control.dacl_protected());
        Ok(())
    }

    #[test]
    fn null_dacl_source_applies_nothing() -> Result {
        let mut source = descriptor(Vec::new(), false);
        source.dacl = None;
        source.control = source.control.with_dacl_present(false);
        let target = descriptor(vec![ace("S-1-5-32-544", false)?], true);
        assert!(merge_dacl(&source, target).is_none());
        Ok(())
    }

    #[test]
    fn stale_explicit_target_aces_are_cleared_when_source_has_none() -> Result {
        let source = descriptor(Vec::new(), false);
        let target = descriptor(
            vec![ace("S-1-5-32-544", false)?, ace("S-1-1-0", true)?],
            false,
        );
        let merged = merge_dacl(&source, target).ok_or("merge expected")?;
        assert_eq!(sids(&merged), vec![("S-1-1-0".to_owned(), true)]);
        Ok(())
    }

    #[test]
    fn protection_bit_change_alone_triggers_an_update() -> Result {
        let source = descriptor(Vec::new(), true);
        let target = descriptor(vec![ace("S-1-1-0", true)?], false);
        let merged = merge_dacl(&source, target).ok_or("merge expected")?;
        assert!(merged.control.dacl_protected());
        assert!(merged.control.dacl_present());
        assert!(
            sids(&merged).is_empty(),
            "protected source without ACEs is deny-all"
        );
        Ok(())
    }
}
