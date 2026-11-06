use super::*;
use crate::model::observation::PrivateBackendEntryFacts;
use crate::model::{IdentityStrength, SourceIdentity};
use crate::storage::backends::local::test_destination_storage_with_role;
use bytes::Bytes;

#[tokio::test]
async fn assembling_a_listing_keeps_its_backend_payload_allocation() {
    let bytes = Bytes::from(vec![9u8; 128]);
    let pointer = bytes.as_ptr();
    let root = tempfile::tempdir().unwrap();
    let (storage, _) =
        test_destination_storage_with_role(root.path(), "shared-listed-facts").unwrap();
    let source = SourceIdentity::new(
        storage.identity().clone(),
        IdentityStrength::StableWithinBackend,
        b"file-handle",
    )
    .unwrap();
    let descriptor = SourceDescriptor::new(
        StoragePath::new("file").unwrap(),
        EntryKind::File,
        Some(8),
        source,
    )
    .with_backend_fact(bytes);
    let entry = assemble_entry(descriptor, MetadataObservations::default(), None).unwrap();
    let PrivateBackendEntryFacts::Local(fact) = entry.backend_facts() else {
        panic!("backend fact missing")
    };
    assert_eq!(fact.as_ptr(), pointer);
    assert_eq!(fact.as_ref(), &[9u8; 128]);
}
