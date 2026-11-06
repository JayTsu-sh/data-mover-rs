use super::*;
use crate::model::{IdentityStrength, SourceIdentity};
use crate::storage::backends::local::test_destination_storage_with_role;

#[tokio::test]
async fn expert_descriptor_keeps_the_observed_backend_payload_allocation()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = Bytes::from(vec![7u8; 128]);
    let pointer = bytes.as_ptr();
    let root = tempfile::tempdir()?;
    let (storage, _) = test_destination_storage_with_role(root.path(), "shared-expert-facts")?;
    let source = SourceIdentity::new(
        storage.identity().clone(),
        IdentityStrength::StableWithinBackend,
        b"file-handle",
    )?;
    let entry = ObservedEntry::new(
        StoragePath::new("file")?,
        EntryKind::File,
        Some(8),
        None,
        source,
    )?
    .with_backend_fact_bytes(bytes)?;
    let descriptor = descriptor_from_observation(&entry)?;
    let fact = descriptor.backend_fact.ok_or("backend fact missing")?;
    assert_eq!(fact.as_ptr(), pointer);
    assert_eq!(fact.as_ref(), &[7u8; 128]);
    Ok(())
}
