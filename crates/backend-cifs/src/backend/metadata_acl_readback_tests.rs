use super::acl_tests::{ace, descriptor};
use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::model::Transience;
use crate::model::acl::safe_deny_all_acl;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

enum Readback {
    Applied,
    Value(smb_domain::SecurityDescriptor),
    Denied,
    Disconnected,
}

struct Protocol {
    current: Mutex<smb_domain::SecurityDescriptor>,
    readback: Readback,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

#[async_trait]
impl CifsMetadataProtocol for Protocol {
    async fn metadata(&self, _: &StoragePath) -> smb_domain::Result<CifsInlineMetadata> {
        unreachable!("ACL-only fixture")
    }
    async fn set_timestamps(
        &self,
        _: &StoragePath,
        _: TimestampMetadata,
    ) -> smb_domain::Result<()> {
        unreachable!("ACL-only fixture")
    }
    async fn get_acl(&self, _: &StoragePath) -> smb_domain::Result<smb_domain::SecurityDescriptor> {
        if self.reads.fetch_add(1, Ordering::SeqCst) != 0 {
            match &self.readback {
                Readback::Applied => {}
                Readback::Value(value) => return Ok(value.clone()),
                Readback::Denied => {
                    return Err(smb_domain::Error::UnexpectedMessageStatus(0xc000_0022));
                }
                Readback::Disconnected => return Err(smb_domain::Error::ConnectionStopped),
            }
        }
        Ok(self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone())
    }
    async fn set_acl(
        &self,
        _: &StoragePath,
        value: smb_domain::SecurityDescriptor,
    ) -> smb_domain::Result<()> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        *self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = value;
        Ok(())
    }
}

fn adapter(readback: Readback) -> Result<(CifsMetadata, Arc<Protocol>)> {
    let protocol = Arc::new(Protocol {
        current: Mutex::new(descriptor(vec![ace("S-1-1-0", true)?], false)),
        readback,
        reads: AtomicUsize::new(0),
        writes: AtomicUsize::new(0),
    });
    Ok((
        CifsMetadata::new(
            protocol.clone(),
            BackendIdentity::new(crate::model::BackendKind::Cifs, "readback")?,
        ),
        protocol,
    ))
}

async fn apply(metadata: &CifsMetadata) -> std::result::Result<(), StorageRoleFailure> {
    let value = safe_deny_all_acl(AclEncoding::WindowsSecurityDescriptor)
        .unwrap_or_else(|error| panic!("valid fallback: {error}"));
    metadata
        .apply(
            &StoragePath::new("file").unwrap_or_else(|_| unreachable!()),
            MetadataMutation::Acl(value),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
}

#[tokio::test]
async fn acknowledged_fallback_set_must_not_succeed_when_server_keeps_grants() -> Result {
    let (metadata, protocol) = adapter(Readback::Value(descriptor(
        vec![ace("S-1-1-0", true)?],
        false,
    )))?;
    assert!(
        matches!(apply(&metadata).await, Err(StorageRoleFailure::Entry(error))
        if error.class() == FailureClass::Protocol && error.transience() == Transience::Permanent)
    );
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 1);
    assert_eq!(protocol.reads.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn empty_acl_readback_must_preserve_protection_presence_and_non_null_state() -> Result {
    let empty_unprotected = descriptor(Vec::new(), false);
    let mut null = descriptor(Vec::new(), true);
    null.dacl = None;
    let absent = descriptor(Vec::new(), true);
    let absent = smb_domain::SecurityDescriptor {
        control: absent.control.with_dacl_present(false),
        ..absent
    };
    for actual in [empty_unprotected, null, absent] {
        let (metadata, _) = adapter(Readback::Value(actual))?;
        assert!(
            matches!(apply(&metadata).await, Err(StorageRoleFailure::Entry(error))
            if error.class() == FailureClass::Protocol
                && error.diagnostic().contains("SET readback mismatch"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn preserved_empty_dacl_passes_only_after_a_successful_readback() -> Result {
    let (metadata, protocol) = adapter(Readback::Applied)?;
    apply(&metadata).await?;
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 1);
    assert_eq!(protocol.reads.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn readback_errors_keep_their_class_transience_and_session_scope() -> Result {
    let (metadata, protocol) = adapter(Readback::Denied)?;
    assert!(
        matches!(apply(&metadata).await, Err(StorageRoleFailure::Entry(error))
        if error.class() == FailureClass::PermissionDenied && error.transience() == Transience::Permanent)
    );
    assert_eq!(protocol.writes.load(Ordering::SeqCst), 1);
    let (metadata, _) = adapter(Readback::Disconnected)?;
    assert!(
        matches!(apply(&metadata).await, Err(StorageRoleFailure::Session(error))
        if error.class() == FailureClass::Connectivity)
    );
    Ok(())
}

#[tokio::test]
async fn native_dacl_readback_detects_changed_ace_order_rights_and_flags() -> Result {
    use binrw::BinWrite as _;
    let expected = descriptor(vec![ace("S-1-1-0", false)?, ace("S-1-5-18", false)?], true);
    let mut encoded = Cursor::new(Vec::new());
    expected.write_le(&mut encoded)?;
    let value = AclMetadata::new(AclEncoding::WindowsSecurityDescriptor, encoded.into_inner())?;
    for case in 0..5 {
        let mut actual = expected.clone();
        let aces = &mut actual.dacl.as_mut().ok_or("missing DACL")?.ace;
        match case {
            0 => {}
            1 => aces.reverse(),
            2 => aces[0].ace_flags = smb_domain::AceFlags::new().with_inherited(true),
            3 => aces[0] = ace("S-1-5-21-1-2-3-1000", false)?,
            _ => {
                let smb_domain::AceValue::AccessAllowed(value) = &mut aces[0].value else {
                    unreachable!("access-allowed fixture")
                };
                value.access_mask = smb_domain::AccessMask::new().with_generic_write(true);
            }
        }
        let (metadata, _) = adapter(Readback::Value(actual))?;
        let result = metadata
            .apply(
                &StoragePath::new("file")?,
                MetadataMutation::Acl(value.clone()),
                tokio_util::sync::CancellationToken::new(),
            )
            .await;
        assert_eq!(result.is_ok(), case == 0);
    }
    Ok(())
}
