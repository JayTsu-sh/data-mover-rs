#![cfg_attr(test, allow(clippy::unwrap_used))]

use super::*;
use crate::model::{AclEncoding, AclMetadata};
use std::sync::Mutex;

struct NativeProtocol {
    base: CancellingProtocol,
    encoding: Option<AclEncoding>,
    acl: Mutex<AclMetadata>,
    writes: AtomicUsize,
    reads: Mutex<Vec<String>>,
    read_failure: Option<FailureClass>,
    write_failure: Option<FailureClass>,
    drop_write: bool,
}

#[async_trait]
impl NfsMetadataProtocol for NativeProtocol {
    async fn stat(&self, path: &StoragePath) -> Result<NfsMetadataInline, NfsProtocolFailure> {
        self.base.stat(path).await
    }
    fn supports_acl(&self) -> bool {
        self.encoding.is_some()
    }
    fn acl_encoding(&self) -> Option<AclEncoding> {
        self.encoding
    }
    fn supports_xattrs(&self) -> bool {
        false
    }
    async fn get_native_acl(&self, path: &StoragePath) -> Result<AclMetadata, NfsProtocolFailure> {
        self.reads.lock().unwrap().push(path.as_str().to_owned());
        if let Some(class) = self.read_failure {
            return Err(NfsProtocolFailure::new(class, Transience::Permanent));
        }
        Ok(self.acl.lock().unwrap().clone())
    }
    async fn set_native_acl(
        &self,
        _: &StoragePath,
        value: &AclMetadata,
    ) -> Result<(), NfsProtocolFailure> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        if let Some(class) = self.write_failure {
            return Err(NfsProtocolFailure::new(class, Transience::Permanent));
        }
        if !self.drop_write {
            *self.acl.lock().unwrap() = value.clone();
        }
        Ok(())
    }
    async fn get_acl(&self, _: &StoragePath) -> Result<nfs_rs::Acl, NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }
    async fn set_acl(&self, _: &StoragePath, _: &nfs_rs::Acl) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }
    async fn get_xattrs(
        &self,
        _: &StoragePath,
    ) -> Result<Vec<ExtendedAttribute>, NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }
    async fn set_xattr(
        &self,
        _: &StoragePath,
        _: &ExtendedAttribute,
    ) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }
    async fn set_numeric_ownership(
        &self,
        _: &StoragePath,
        _: OwnershipMode,
    ) -> Result<(), NfsProtocolFailure> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Err(NfsProtocolFailure::protocol())
    }
    async fn set_timestamps(
        &self,
        _: &StoragePath,
        _: TimestampMetadata,
    ) -> Result<(), NfsProtocolFailure> {
        Err(NfsProtocolFailure::protocol())
    }
}

fn posix(default: bool, named_permissions: u32) -> AclMetadata {
    use crate::model::acl::{PosixAcl, PosixEntry, encode_posix};
    let entry = |tag, id, permissions| PosixEntry {
        tag,
        id,
        permissions,
    };
    let access = vec![
        entry(1, u32::MAX, 6),
        entry(2, 42421, named_permissions),
        entry(4, u32::MAX, 4),
        entry(16, u32::MAX, 4),
        entry(32, u32::MAX, 0),
    ];
    encode_posix(&PosixAcl {
        default: if default { access.clone() } else { vec![] },
        access,
    })
    .unwrap()
}

fn protocol(value: AclMetadata) -> NativeProtocol {
    NativeProtocol {
        base: CancellingProtocol {
            cancel: tokio_util::sync::CancellationToken::new(),
            sets: AtomicUsize::new(0),
            xattrs_supported: false,
            owner_unmapped: false,
        },
        encoding: Some(value.encoding()),
        acl: Mutex::new(value),
        writes: AtomicUsize::new(0),
        reads: Mutex::new(Vec::new()),
        read_failure: None,
        write_failure: None,
        drop_write: false,
    }
}

fn adapter(value: NativeProtocol) -> (NfsMetadataAdapter, Arc<NativeProtocol>) {
    let protocol = Arc::new(value);
    let identity =
        crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "native-acl").unwrap();
    (
        NfsMetadataAdapter::new(protocol.clone(), identity),
        protocol,
    )
}

async fn apply(adapter: &NfsMetadataAdapter, value: AclMetadata) -> Result<(), StorageRoleFailure> {
    adapter
        .apply(
            &StoragePath::new("file").unwrap(),
            MetadataMutation::Acl(value),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
}

#[tokio::test]
async fn posix_observation_preserves_masked_bits_and_default_acl() {
    let expected = posix(true, 7);
    let (adapter, _) = adapter(protocol(expected.clone()));
    assert_eq!(
        adapter.observation_capabilities().support(F::Acl),
        S::Supported
    );
    let observed = adapter
        .observe(
            &StoragePath::new("dir").unwrap(),
            ObservationPlan::default().with_acl(ObservationMode::Required),
        )
        .await
        .unwrap();
    assert!(
        matches!(observed.acl(), MetadataObservation::Value { value, .. } if value == &expected)
    );
}

#[tokio::test]
async fn posix_application_clears_default_acl_and_reads_back_complete_configuration() {
    let (adapter, protocol) = adapter(protocol(posix(true, 7)));
    let expected = posix(false, 7);
    apply(&adapter, expected.clone()).await.unwrap();
    assert_eq!(*protocol.acl.lock().unwrap(), expected);
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn posix_readback_detects_loss_of_masked_raw_permissions() {
    let mut protocol = protocol(posix(false, 4));
    protocol.drop_write = true;
    let (adapter, protocol) = adapter(protocol);
    assert!(matches!(apply(&adapter, posix(false, 7)).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol));
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn wrong_model_and_malformed_acl_are_refused_before_any_set() {
    let (adapter, protocol) = adapter(protocol(posix(false, 7)));
    for invalid in [
        acl::encode(&nfs_rs::Acl::default()).unwrap(),
        AclMetadata::new(AclEncoding::Posix, vec![1]).unwrap(),
    ] {
        assert!(matches!(apply(&adapter, invalid).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::InvalidInput));
    }
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unsupported_capability_refuses_application_before_any_set() {
    let mut protocol = protocol(posix(false, 7));
    protocol.encoding = None;
    let (adapter, protocol) = adapter(protocol);
    assert!(matches!(apply(&adapter, posix(false, 7)).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Unsupported));
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn advertised_posix_acl_read_failure_cannot_become_empty_or_successful_acl() {
    for class in [FailureClass::Unsupported, FailureClass::PermissionDenied] {
        let mut protocol = protocol(posix(false, 7));
        protocol.read_failure = Some(class);
        let (adapter, _) = adapter(protocol);
        let path = StoragePath::new("file").unwrap();
        let plan = ObservationPlan::default().with_acl(ObservationMode::Required);
        assert!(matches!(adapter.observe(&path, plan).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == class));
        let observed = adapter
            .observe(&path, plan.with_acl(ObservationMode::BestEffort))
            .await
            .unwrap();
        assert!(
            matches!(observed.acl(), MetadataObservation::Failed { class: got, .. } if *got == class)
        );
        assert!(matches!(apply(&adapter, posix(false, 7)).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == class));
    }
}

#[tokio::test]
async fn refused_posix_set_is_a_failure_with_the_original_class() {
    for class in [FailureClass::Unsupported, FailureClass::PermissionDenied] {
        let mut protocol = protocol(posix(false, 7));
        protocol.write_failure = Some(class);
        let (adapter, protocol) = adapter(protocol);
        assert!(matches!(apply(&adapter, posix(false, 7)).await,
            Err(StorageRoleFailure::Entry(error)) if error.class() == class));
        assert_eq!(protocol.writes.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn native_nfs4_application_keeps_order_and_requires_readback() {
    let value = acl::encode(&nfs_rs::Acl {
        aces: vec![nfs_rs::NfsAce {
            ace_type: nfs_rs::AceType::AccessAllowed,
            flags: nfs_rs::AceFlags(0),
            access_mask: nfs_rs::AceMask(1),
            who: "42421".into(),
        }],
    })
    .unwrap();
    let (adapter, protocol) = adapter(protocol(value.clone()));
    apply(&adapter, value.clone()).await.unwrap();
    assert_eq!(*protocol.acl.lock().unwrap(), value);
}

#[test]
fn staged_destination_advertises_the_negotiated_native_acl_model() {
    use crate::storage::backends::nfs::staged::{NfsStagedDestinationAdapter, tests::FakeProtocol};
    use crate::storage::{CopiedAclTarget, StagedDestination};
    for encoding in [Some(AclEncoding::Posix), Some(AclEncoding::NfsV4), None] {
        let mut staged_protocol = FakeProtocol::default();
        staged_protocol.acl_encoding = encoding;
        let (metadata, _) = adapter(protocol(posix(false, 7)));
        let identity =
            crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "native-acl")
                .unwrap();
        let staged = NfsStagedDestinationAdapter::new(Arc::new(staged_protocol), identity)
            .with_metadata(Arc::new(metadata));
        assert_eq!(
            staged.copied_metadata_target().unwrap().acl,
            encoding.map_or(CopiedAclTarget::Unsupported, CopiedAclTarget::Encoding)
        );
    }
}

#[tokio::test]
async fn posix_readback_detects_default_loss_with_identical_access_acl() {
    let mut protocol = protocol(posix(false, 7));
    protocol.drop_write = true;
    let (adapter, _) = adapter(protocol);
    assert!(matches!(apply(&adapter, posix(true, 7)).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol));
}

#[tokio::test]
async fn native_nfs4_readback_rejects_changed_ace_order() {
    let allow = nfs_rs::NfsAce {
        ace_type: nfs_rs::AceType::AccessAllowed,
        flags: nfs_rs::AceFlags(0),
        access_mask: nfs_rs::AceMask(1),
        who: "42421".into(),
    };
    let deny = nfs_rs::NfsAce {
        ace_type: nfs_rs::AceType::AccessDenied,
        ..allow.clone()
    };
    let expected = acl::encode(&nfs_rs::Acl {
        aces: vec![deny.clone(), allow.clone()],
    })
    .unwrap();
    let changed = acl::encode(&nfs_rs::Acl {
        aces: vec![allow, deny],
    })
    .unwrap();
    let mut protocol = protocol(changed);
    protocol.drop_write = true;
    let (adapter, _) = adapter(protocol);
    assert!(matches!(apply(&adapter, expected).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol));
}

#[tokio::test]
async fn native_nfs4_readback_accepts_only_protocol_defined_special_group_flag_change() {
    let entry = nfs_rs::NfsAce {
        ace_type: nfs_rs::AceType::AccessAllowed,
        flags: nfs_rs::AceFlags(0),
        access_mask: nfs_rs::AceMask(1),
        who: "GROUP@".into(),
    };
    let expected = acl::encode(&nfs_rs::Acl {
        aces: vec![entry.clone()],
    })
    .unwrap();
    let actual = acl::encode(&nfs_rs::Acl {
        aces: vec![nfs_rs::NfsAce {
            flags: nfs_rs::AceFlags(0x40),
            ..entry
        }],
    })
    .unwrap();
    let mut protocol = protocol(actual);
    protocol.drop_write = true;
    let (adapter, _) = adapter(protocol);
    apply(&adapter, expected).await.unwrap();
}

fn recovered_stage(identity: crate::model::BackendIdentity) -> crate::storage::PreparedStage {
    use crate::storage::artifacts::ArtifactKind;
    use crate::storage::backends::nfs::at_destination::artifact_path;
    let path = StoragePath::new("final.bin").unwrap();
    let token = artifact_path(&path, ArtifactKind::Stage, false).unwrap();
    let mut stage = crate::storage::PreparedStage::new(
        identity,
        crate::storage::FinalDestination::new(path),
        bytes::Bytes::from(token.as_str().to_owned()),
        [7; 32],
        0,
        None,
    );
    stage.mark_at_destination(crate::storage::PrepareFact::Fresh);
    stage
}

#[tokio::test]
async fn recovered_acl_validation_reads_final_and_never_reapplies_acl() {
    use crate::storage::StagedDestination;
    use crate::storage::backends::nfs::staged::{NfsStagedDestinationAdapter, tests::FakeProtocol};
    let expected = posix(false, 7);
    let (metadata, protocol) = adapter(protocol(expected.clone()));
    let identity =
        crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "recover-acl").unwrap();
    let staged =
        NfsStagedDestinationAdapter::new(Arc::new(FakeProtocol::default()), identity.clone())
            .with_metadata(Arc::new(metadata));
    let stage = recovered_stage(identity);
    staged.claim_authority(stage.token().clone());
    staged
        .validate_recovered_metadata(
            &stage,
            vec![MetadataMutation::Acl(expected)],
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(matches!(staged.validate_recovered_metadata(&stage,
        vec![MetadataMutation::Acl(posix(false, 4))], tokio_util::sync::CancellationToken::new()).await,
        Err(StorageRoleFailure::Entry(error)) if error.class() == FailureClass::Protocol));
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        *protocol.reads.lock().unwrap(),
        vec!["final.bin", "final.bin"]
    );
}

#[tokio::test]
async fn absent_posix_access_is_materialized_from_mode_and_clears_extended_acl() {
    let (adapter, protocol) = adapter(protocol(posix(true, 7)));
    let absent = crate::model::acl::encode_posix(&crate::model::acl::PosixAcl::default()).unwrap();
    apply(&adapter, absent).await.unwrap();
    let actual = crate::model::acl::decode_posix(&protocol.acl.lock().unwrap()).unwrap();
    assert!(actual.default.is_empty());
    assert_eq!(
        actual
            .access
            .into_iter()
            .map(|entry| (entry.tag, entry.permissions))
            .collect::<Vec<_>>(),
        vec![(1, 6), (4, 4), (32, 0)]
    );
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 1);
}

fn recovery_fixture(
    native: NativeProtocol,
) -> (
    crate::storage::backends::nfs::staged::NfsStagedDestinationAdapter,
    crate::storage::PreparedStage,
    Arc<NativeProtocol>,
) {
    use crate::storage::backends::nfs::staged::{NfsStagedDestinationAdapter, tests::FakeProtocol};
    let (metadata, protocol) = adapter(native);
    let identity =
        crate::model::BackendIdentity::new(crate::model::BackendKind::Nfs, "recover-owner")
            .unwrap();
    let staged =
        NfsStagedDestinationAdapter::new(Arc::new(FakeProtocol::default()), identity.clone())
            .with_metadata(Arc::new(metadata));
    let stage = recovered_stage(identity);
    staged.claim_authority(stage.token().clone());
    (staged, stage, protocol)
}

fn recovery_acl() -> AclMetadata {
    acl::encode(&nfs_rs::Acl {
        aces: vec![nfs_rs::NfsAce {
            ace_type: nfs_rs::AceType::AccessAllowed,
            flags: nfs_rs::AceFlags(0),
            access_mask: nfs_rs::AceMask(7),
            who: "OWNER@".into(),
        }],
    })
    .unwrap()
}

const RECOVERED_OWNER: OwnershipMode = OwnershipMode {
    uid: 12345,
    gid: 12346,
    mode: 0o640,
};

#[tokio::test]
async fn acl_recovery_rejects_changed_owner_or_group_with_unchanged_acl() {
    use crate::storage::StagedDestination;
    for expected in [
        OwnershipMode {
            uid: 54321,
            ..RECOVERED_OWNER
        },
        OwnershipMode {
            gid: 54322,
            ..RECOVERED_OWNER
        },
    ] {
        let acl = recovery_acl();
        let (staged, stage, protocol) = recovery_fixture(protocol(acl.clone()));
        let result = staged
            .validate_recovered_metadata(
                &stage,
                vec![
                    MetadataMutation::Acl(acl),
                    MetadataMutation::NumericOwnership(expected),
                ],
                tokio_util::sync::CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageRoleFailure::Entry(ref error))
            if error.class() == FailureClass::Protocol),
            "{result:?}"
        );
        assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn acl_recovery_rejects_changed_special_mode_bits_with_unchanged_acl() {
    use crate::storage::StagedDestination;
    for bits in [0o4000, 0o2000, 0o1000] {
        let acl = recovery_acl();
        let (staged, stage, protocol) = recovery_fixture(protocol(acl.clone()));
        let expected = OwnershipMode {
            mode: bits | RECOVERED_OWNER.mode,
            ..RECOVERED_OWNER
        };
        let result = staged
            .validate_recovered_metadata(
                &stage,
                vec![
                    MetadataMutation::NumericOwnership(expected),
                    MetadataMutation::Acl(acl),
                ],
                tokio_util::sync::CancellationToken::new(),
            )
            .await;
        assert!(
            matches!(result, Err(StorageRoleFailure::Entry(ref error))
            if error.class() == FailureClass::Protocol),
            "{result:?}"
        );
        assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn acl_recovery_accepts_matching_owner_and_acl_proven_low_mode_difference() {
    use crate::storage::StagedDestination;
    for mode in [0o640, 0o600] {
        for acl in [recovery_acl(), posix(false, 7)] {
            let (staged, stage, protocol) = recovery_fixture(protocol(acl.clone()));
            staged
                .validate_recovered_metadata(
                    &stage,
                    vec![
                        MetadataMutation::Acl(acl),
                        MetadataMutation::NumericOwnership(OwnershipMode {
                            mode,
                            ..RECOVERED_OWNER
                        }),
                    ],
                    tokio_util::sync::CancellationToken::new(),
                )
                .await
                .unwrap();
            assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
            assert_eq!(*protocol.reads.lock().unwrap(), vec!["final.bin"]);
        }
    }
}

#[tokio::test]
async fn acl_recovery_keeps_numeric_only_metadata_a_noop() {
    use crate::storage::StagedDestination;
    let (staged, stage, protocol) = recovery_fixture(protocol(recovery_acl()));
    staged
        .validate_recovered_metadata(
            &stage,
            vec![MetadataMutation::NumericOwnership(OwnershipMode {
                uid: 54321,
                ..RECOVERED_OWNER
            })],
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
    assert!(protocol.reads.lock().unwrap().is_empty());
    assert_eq!(protocol.base.sets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn acl_recovery_requires_observable_owner_only_when_numeric_owner_was_expected() {
    use crate::storage::StagedDestination;
    for expected_owner in [false, true] {
        let acl = recovery_acl();
        let mut native = protocol(acl.clone());
        native.base.owner_unmapped = true;
        let (staged, stage, protocol) = recovery_fixture(native);
        let mut mutations = vec![MetadataMutation::Acl(acl)];
        if expected_owner {
            mutations.push(MetadataMutation::NumericOwnership(RECOVERED_OWNER));
        }
        let result = staged
            .validate_recovered_metadata(
                &stage,
                mutations,
                tokio_util::sync::CancellationToken::new(),
            )
            .await;
        if expected_owner {
            assert!(
                matches!(result, Err(StorageRoleFailure::Entry(ref error))
                if error.class() == FailureClass::Protocol),
                "{result:?}"
            );
        } else {
            result.unwrap();
        }
        assert_eq!(protocol.writes.load(Ordering::SeqCst), 0);
    }
}
