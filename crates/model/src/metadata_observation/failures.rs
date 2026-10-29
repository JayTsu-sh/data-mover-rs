//! Bounded failures retained even when fallback facts make the final observation usable.

use super::{
    Cursor, FailureClass, MetadataObservation, MetadataObservations, SnapshotDecodeError,
    Transience, failure_from_tag, failure_tag, transience_from_tag, transience_tag,
};
use crate::model::MetadataFamily as F;

/// One metadata family that failed while an entry or version was observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataObservationFailure {
    pub family: F,
    pub class: FailureClass,
    pub transience: Transience,
}

impl MetadataObservations {
    /// Iterates failed families, including failed reads replaced by successful fallback facts.
    /// A family appears once. Inspect history records separately to retain their version identity.
    pub fn failures(&self) -> impl Iterator<Item = MetadataObservationFailure> + '_ {
        F::ALL.into_iter().filter_map(|family| {
            self.current_failure(family).or_else(|| {
                self.retained_failures.as_deref().and_then(|failures| {
                    failures
                        .iter()
                        .find(|failure| failure.family == family)
                        .copied()
                })
            })
        })
    }

    /// Preserve a failed attempt before replacing its final state with a usable fallback value.
    pub fn retain_failure(&mut self, family: F) {
        let Some(failure) = self.current_failure(family) else {
            return;
        };
        self.retain_failure_summary(failure);
    }

    pub fn retain_failure_summary(&mut self, failure: MetadataObservationFailure) {
        retain_summary(&mut self.retained_failures, failure);
    }

    fn current_failure(&self, family: F) -> Option<MetadataObservationFailure> {
        match family {
            F::Acl => failure(family, &self.acl),
            F::Xattrs => failure(family, &self.xattrs),
            F::Tags => failure(family, &self.tags),
            F::OwnershipMode => failure(family, &self.ownership_mode),
            F::Timestamps => failure(family, &self.timestamps),
        }
    }
}

fn failure<T>(family: F, value: &MetadataObservation<T>) -> Option<MetadataObservationFailure> {
    if let MetadataObservation::Failed { class, transience } = value {
        Some(MetadataObservationFailure {
            family,
            class: *class,
            transience: *transience,
        })
    } else {
        None
    }
}

pub(super) fn encode(metadata: &MetadataObservations, output: &mut Vec<u8>) {
    let failures = metadata.retained_failures.as_deref().unwrap_or_default();
    output.push(u8::try_from(failures.len()).unwrap_or(u8::MAX));
    for failure in failures {
        output.extend_from_slice(&[
            failure.family as u8,
            failure_tag(failure.class),
            transience_tag(failure.transience),
        ]);
    }
}

pub(super) fn decode(
    cursor: &mut Cursor<'_>,
) -> Result<Option<Box<[MetadataObservationFailure]>>, SnapshotDecodeError> {
    let count = usize::from(cursor.byte()?);
    if count > F::ALL.len() {
        return Err(SnapshotDecodeError::FieldTooLarge);
    }
    if count == 0 {
        return Ok(None);
    }
    let mut failures = Vec::with_capacity(count);
    for _ in 0..count {
        let family = F::ALL
            .get(usize::from(cursor.byte()?))
            .copied()
            .ok_or(SnapshotDecodeError::Malformed)?;
        if failures
            .iter()
            .any(|failure: &MetadataObservationFailure| failure.family == family)
        {
            return Err(SnapshotDecodeError::Malformed);
        }
        let class = failure_from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?;
        let transience =
            transience_from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?;
        failures.push(MetadataObservationFailure {
            family,
            class,
            transience,
        });
    }
    Ok(Some(failures.into_boxed_slice()))
}

pub(super) fn retain_summary(
    slot: &mut Option<Box<[MetadataObservationFailure]>>,
    failure: MetadataObservationFailure,
) {
    let mut retained = slot
        .take()
        .map_or_else(Vec::new, <[MetadataObservationFailure]>::into_vec);
    if !retained.iter().any(|value| value.family == failure.family) {
        retained.push(failure);
    }
    *slot = Some(retained.into_boxed_slice());
}
