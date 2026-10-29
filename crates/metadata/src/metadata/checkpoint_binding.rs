//! Stable binding of actual planned writes, never their potentially redacted Debug output.

use blake3::Hasher;

use super::MetadataPlan;
use crate::model::{AclEncoding, AclMetadata, ExtendedAttribute, ObjectTag, StorageTimestamp};
use crate::storage::MetadataMutation;

impl MetadataPlan {
    /// A publication-ready checkpoint must not silently accept a different metadata plan on
    /// retry. Request selectors are bound separately by the transfer engine.
    #[doc(hidden)]
    #[must_use]
    pub fn checkpoint_binding(&self) -> [u8; 32] {
        let mut hash = Hasher::new();
        hash.update(b"data-mover planned metadata writes v1\0");
        if let Some(binding) = self.acl_source_binding {
            hash.update(b"partial ACL source v1\0");
            hash.update(&binding);
        }
        count(&mut hash, self.mutations.len());
        for (_, mutation) in &self.mutations {
            observe(&mut hash, mutation);
        }
        *hash.finalize().as_bytes()
    }
}

fn observe(hash: &mut Hasher, mutation: &MetadataMutation) {
    match mutation {
        MetadataMutation::Acl(value) => {
            hash.update(&[0]);
            acl(hash, value);
        }
        MetadataMutation::Xattrs(values) => {
            hash.update(&[1]);
            attributes(hash, values);
        }
        MetadataMutation::Tags(values) => {
            hash.update(&[2]);
            tags(hash, values);
        }
        MetadataMutation::NumericOwnership(value) => {
            hash.update(&[3]);
            for number in [value.uid, value.gid, value.mode] {
                hash.update(&number.to_le_bytes());
            }
        }
        MetadataMutation::MappedOwnership(value) => {
            hash.update(&[4]);
            field(hash, value.owner().as_bytes());
            field(hash, value.group().as_bytes());
            hash.update(&value.mode.to_le_bytes());
        }
        MetadataMutation::Mode(mode) => {
            hash.update(&[5]);
            hash.update(&mode.to_le_bytes());
        }
        MetadataMutation::Timestamps(value) => {
            hash.update(&[6]);
            for time in [value.accessed, value.modified, value.created] {
                timestamp(hash, time);
            }
        }
    }
}

fn attributes(hash: &mut Hasher, values: &[ExtendedAttribute]) {
    let mut sorted: Vec<_> = values.iter().collect();
    sorted.sort_by_key(|value| value.name());
    // Unique keys commute. If malformed input repeats a key, keep the actual execution order
    // instead of treating two different last-write-wins plans as equivalent.
    if sorted
        .windows(2)
        .any(|pair| pair[0].name() == pair[1].name())
    {
        sorted = values.iter().collect();
    }
    count(hash, sorted.len());
    for value in sorted {
        field(hash, value.name());
        field(hash, value.value());
    }
}

fn tags(hash: &mut Hasher, values: &[ObjectTag]) {
    let mut sorted: Vec<_> = values.iter().collect();
    sorted.sort_by_key(|value| value.key());
    if sorted.windows(2).any(|pair| pair[0].key() == pair[1].key()) {
        sorted = values.iter().collect();
    }
    count(hash, sorted.len());
    for value in sorted {
        field(hash, value.key().as_bytes());
        field(hash, value.value().as_bytes());
    }
}

pub(super) fn acl(hash: &mut Hasher, value: &AclMetadata) {
    hash.update(&[match value.encoding() {
        AclEncoding::Posix => 0,
        AclEncoding::NfsV4 => 1,
        AclEncoding::WindowsSecurityDescriptor => 2,
    }]);
    for part in [value.access(), value.default_acl()] {
        hash.update(&[u8::from(part.is_some())]);
        if let Some(bytes) = part {
            field(hash, bytes);
        }
    }
}

fn timestamp(hash: &mut Hasher, time: Option<StorageTimestamp>) {
    hash.update(&[u8::from(time.is_some())]);
    if let Some(time) = time {
        hash.update(&time.unix_nanos().to_le_bytes());
        hash.update(&time.precision().quantum_nanos().to_le_bytes());
    }
}

fn count(hash: &mut Hasher, value: usize) {
    hash.update(&(value as u64).to_le_bytes());
}

fn field(hash: &mut Hasher, bytes: &[u8]) {
    count(hash, bytes.len());
    hash.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{LossReport, MetadataFamily};
    use crate::model::{
        MappedOwnership, ObjectTag, OwnershipMode, TimePrecision, TimestampMetadata,
    };

    fn plan(mutation: MetadataMutation) -> MetadataPlan {
        MetadataPlan {
            mappings: Vec::new(),
            mutations: vec![(MetadataFamily::Tags, mutation)],
            losses: LossReport::default(),
            skipped: Vec::new(),
            acl_copy: None,
            acl_source_binding: None,
        }
    }

    #[test]
    fn redacted_tag_and_principal_values_are_part_of_the_binding()
    -> Result<(), Box<dyn std::error::Error>> {
        let tag = |value| {
            Ok::<_, Box<dyn std::error::Error>>(plan(MetadataMutation::Tags(vec![ObjectTag::new(
                "key", value,
            )?])))
        };
        assert_ne!(
            tag("first")?.checkpoint_binding(),
            tag("second")?.checkpoint_binding()
        );
        let owner = |value| {
            Ok::<_, Box<dyn std::error::Error>>(plan(MetadataMutation::MappedOwnership(
                MappedOwnership::new(value, "group", 0o600)?,
            )))
        };
        assert_ne!(
            owner("alice")?.checkpoint_binding(),
            owner("bob")?.checkpoint_binding()
        );
        Ok(())
    }

    #[test]
    fn mutation_types_acl_absence_and_time_precision_are_distinct()
    -> Result<(), Box<dyn std::error::Error>> {
        let mode = plan(MetadataMutation::Mode(0o600));
        assert_eq!(mode.checkpoint_binding(), mode.clone().checkpoint_binding());
        let owner = plan(MetadataMutation::NumericOwnership(OwnershipMode {
            uid: 0,
            gid: 0,
            mode: 0o600,
        }));
        assert_ne!(mode.checkpoint_binding(), owner.checkpoint_binding());
        let acl = |part| {
            Ok::<_, Box<dyn std::error::Error>>(plan(MetadataMutation::Acl(
                AclMetadata::new_posix(part, None)?,
            )))
        };
        assert_ne!(
            acl(None)?.checkpoint_binding(),
            acl(Some(Vec::new()))?.checkpoint_binding()
        );
        let time = |precision| {
            Ok::<_, Box<dyn std::error::Error>>(plan(MetadataMutation::Timestamps(
                TimestampMetadata {
                    accessed: None,
                    modified: Some(StorageTimestamp::new(0, precision)?),
                    created: None,
                },
            )))
        };
        assert_ne!(
            time(TimePrecision::Seconds)?.checkpoint_binding(),
            time(TimePrecision::Nanoseconds)?.checkpoint_binding()
        );
        Ok(())
    }

    #[test]
    fn unique_metadata_keys_are_order_independent_but_duplicate_keys_are_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = ObjectTag::new("a", "first")?;
        let second = ObjectTag::new("b", "second")?;
        let tags = |values| plan(MetadataMutation::Tags(values)).checkpoint_binding();
        assert_eq!(
            tags(vec![first.clone(), second.clone()]),
            tags(vec![second, first.clone()])
        );
        let duplicate = ObjectTag::new("a", "last")?;
        assert_ne!(
            tags(vec![first.clone(), duplicate.clone()]),
            tags(vec![duplicate, first])
        );
        let a = ExtendedAttribute::new(b"a".to_vec(), vec![1])?;
        let b = ExtendedAttribute::new(b"b".to_vec(), vec![2])?;
        let attrs = |values| plan(MetadataMutation::Xattrs(values)).checkpoint_binding();
        assert_eq!(attrs(vec![a.clone(), b.clone()]), attrs(vec![b, a]));
        Ok(())
    }
}
