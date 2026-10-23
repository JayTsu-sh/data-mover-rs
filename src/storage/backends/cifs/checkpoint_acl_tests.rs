use super::*;
use crate::model::TimestampMetadata;
use crate::storage::backends::cifs::metadata::CifsInlineMetadata;
use async_trait::async_trait;
use smb_domain::{
    SecurityDescriptor,
    protocol::{ACL, AclRevision, SecurityDescriptorControl},
};
use std::sync::Mutex;

fn descriptor(dacl: bool) -> SecurityDescriptor {
    SecurityDescriptor {
        sbz1: 0,
        control: SecurityDescriptorControl::new()
            .with_self_relative(true)
            .with_dacl_present(dacl),
        owner_sid: None,
        group_sid: None,
        sacl: None,
        dacl: dacl.then(|| ACL {
            acl_revision: AclRevision::Nt4,
            ace: Vec::new(),
        }),
    }
}

struct Protocol {
    current: Mutex<SecurityDescriptor>,
    ignore_set: bool,
    calls: Mutex<Vec<&'static str>>,
}

#[async_trait]
impl CifsMetadataProtocol for Protocol {
    async fn metadata(&self, _: &StoragePath) -> smb_domain::Result<CifsInlineMetadata> {
        unreachable!("security-only fixture")
    }
    async fn set_timestamps(
        &self,
        _: &StoragePath,
        _: TimestampMetadata,
    ) -> smb_domain::Result<()> {
        unreachable!("security-only fixture")
    }
    async fn get_acl(&self, path: &StoragePath) -> smb_domain::Result<SecurityDescriptor> {
        let payload = path.as_str() == "payload";
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(if payload {
                "read payload"
            } else {
                "read pointer"
            });
        Ok(if payload {
            descriptor(true)
        } else {
            self.current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        })
    }
    async fn set_acl(
        &self,
        _: &StoragePath,
        descriptor: SecurityDescriptor,
    ) -> smb_domain::Result<()> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("restrict pointer");
        if !self.ignore_set {
            *self
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = descriptor;
        }
        Ok(())
    }
}

#[tokio::test]
async fn clones_payload_dacl_with_protection_and_verifies_server_applied_it()
-> Result<(), Box<dyn std::error::Error>> {
    for ignore_set in [false, true] {
        let protocol = Protocol {
            current: Mutex::new(descriptor(false)),
            ignore_set,
            calls: Mutex::new(Vec::new()),
        };
        let result = copy(
            &protocol,
            &StoragePath::new("pointer")?,
            &StoragePath::new("payload")?,
        )
        .await;
        assert_eq!(result.is_err(), ignore_set);
        assert_eq!(
            *protocol
                .calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ["read payload", "restrict pointer", "read pointer"]
        );
        if !ignore_set {
            let current = protocol
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(current.control.dacl_protected());
            assert_eq!(current.dacl, descriptor(true).dacl);
        }
    }
    Ok(())
}
