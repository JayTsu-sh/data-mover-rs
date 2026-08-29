use super::observation::{Cursor, SnapshotDecodeError, decode_time, encode_time, put_bytes};
use super::{FailureClass, MAX_MODEL_FIELD_BYTES, ModelValueError, StorageTimestamp, Transience};

const MAX_XATTR_COUNT: usize = 4_096;
const MAX_TAG_COUNT: usize = 1_024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ObservationMode {
    #[default]
    Omit,
    InlineOnly,
    BestEffort,
    Required,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ObservationPlan {
    acl: ObservationMode,
    xattrs: ObservationMode,
    tags: ObservationMode,
    ownership_mode: ObservationMode,
    timestamps: ObservationMode,
}

impl ObservationPlan {
    #[must_use]
    pub const fn acl(self) -> ObservationMode {
        self.acl
    }
    #[must_use]
    pub const fn xattrs(self) -> ObservationMode {
        self.xattrs
    }
    #[must_use]
    pub const fn tags(self) -> ObservationMode {
        self.tags
    }
    #[must_use]
    pub const fn ownership_mode(self) -> ObservationMode {
        self.ownership_mode
    }
    #[must_use]
    pub const fn timestamps(self) -> ObservationMode {
        self.timestamps
    }
    #[must_use]
    pub const fn with_acl(mut self, mode: ObservationMode) -> Self {
        self.acl = mode;
        self
    }
    #[must_use]
    pub const fn with_xattrs(mut self, mode: ObservationMode) -> Self {
        self.xattrs = mode;
        self
    }
    #[must_use]
    pub const fn with_tags(mut self, mode: ObservationMode) -> Self {
        self.tags = mode;
        self
    }
    #[must_use]
    pub const fn with_ownership_mode(mut self, mode: ObservationMode) -> Self {
        self.ownership_mode = mode;
        self
    }
    #[must_use]
    pub const fn with_timestamps(mut self, mode: ObservationMode) -> Self {
        self.timestamps = mode;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataProvenance {
    Inline,
    AdditionalCall,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetadataObservation<T> {
    Value {
        value: T,
        provenance: MetadataProvenance,
    },
    NotRequested,
    NotApplicable,
    Unsupported,
    Failed {
        class: FailureClass,
        transience: Transience,
    },
}

impl<T> MetadataObservation<T> {
    #[must_use]
    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Value { value, .. } => Some(value),
            Self::NotRequested | Self::NotApplicable | Self::Unsupported | Self::Failed { .. } => {
                None
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AclMetadata {
    encoding: AclEncoding,
    access: Option<Vec<u8>>,
    default: Option<Vec<u8>>,
}

impl AclMetadata {
    /// Creates a bounded native ACL payload.
    ///
    /// # Errors
    /// Returns an error when the payload exceeds the model field limit.
    pub fn new(encoding: AclEncoding, bytes: Vec<u8>) -> Result<Self, ModelValueError> {
        Self::new_parts(encoding, Some(bytes), None)
    }

    /// Creates lossless POSIX access and default ACL observations.
    ///
    /// # Errors
    /// Returns an error when their combined payload exceeds the model field limit.
    pub fn new_posix(
        access: Option<Vec<u8>>,
        default: Option<Vec<u8>>,
    ) -> Result<Self, ModelValueError> {
        Self::new_parts(AclEncoding::Posix, access, default)
    }

    fn new_parts(
        encoding: AclEncoding,
        access: Option<Vec<u8>>,
        default: Option<Vec<u8>>,
    ) -> Result<Self, ModelValueError> {
        let size = access
            .as_ref()
            .map_or(0, Vec::len)
            .checked_add(default.as_ref().map_or(0, Vec::len));
        if size.is_none_or(|size| size > MAX_MODEL_FIELD_BYTES) {
            return Err(ModelValueError::new("acl", "exceeds model field limit"));
        }
        Ok(Self {
            encoding,
            access,
            default,
        })
    }
    #[must_use]
    pub const fn encoding(&self) -> AclEncoding {
        self.encoding
    }
    #[must_use]
    pub fn access(&self) -> Option<&[u8]> {
        self.access.as_deref()
    }
    #[must_use]
    pub fn default_acl(&self) -> Option<&[u8]> {
        self.default.as_deref()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AclEncoding {
    Posix,
    NfsV4,
    WindowsSecurityDescriptor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtendedAttribute {
    name: Vec<u8>,
    value: Vec<u8>,
}

/// One bounded UTF-8 object tag.
#[derive(Clone, Eq, PartialEq)]
pub struct ObjectTag {
    key: String,
    value: String,
}

impl ObjectTag {
    /// Creates one tag without exposing its value through `Debug`.
    ///
    /// # Errors
    /// Rejects empty or unbounded keys and unbounded values.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Result<Self, ModelValueError> {
        let key = key.into();
        let value = value.into();
        if key.is_empty()
            || key.contains('\0')
            || value.contains('\0')
            || key.len() > MAX_MODEL_FIELD_BYTES
            || value.len() > MAX_MODEL_FIELD_BYTES
        {
            return Err(ModelValueError::new(
                "tag",
                "key/value is invalid or unbounded",
            ));
        }
        Ok(Self { key, value })
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Debug for ObjectTag {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ObjectTag(<redacted>)")
    }
}

impl ExtendedAttribute {
    /// Creates one bounded, lossless extended attribute.
    ///
    /// # Errors
    /// Returns an error for an empty/oversized name or oversized value.
    pub fn new(name: Vec<u8>, value: Vec<u8>) -> Result<Self, ModelValueError> {
        if name.is_empty()
            || name.len() > MAX_MODEL_FIELD_BYTES
            || value.len() > MAX_MODEL_FIELD_BYTES
        {
            return Err(ModelValueError::new(
                "xattr",
                "name/value is empty or exceeds model field limit",
            ));
        }
        Ok(Self { name, value })
    }
    #[must_use]
    pub fn name(&self) -> &[u8] {
        &self.name
    }
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnershipMode {
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
}

/// Explicit externally mapped owner and group for a non-numeric target.
#[derive(Clone, Eq, PartialEq)]
pub struct MappedOwnership {
    owner: String,
    group: String,
    pub mode: u32,
}

impl MappedOwnership {
    /// Creates one bounded mapping result.
    ///
    /// # Errors
    /// Rejects blank, NUL-containing, or unbounded principal values.
    pub fn new(
        owner: impl Into<String>,
        group: impl Into<String>,
        mode: u32,
    ) -> Result<Self, ModelValueError> {
        let owner = owner.into();
        let group = group.into();
        if owner.trim().is_empty()
            || group.trim().is_empty()
            || owner.contains('\0')
            || group.contains('\0')
            || owner.len() > MAX_MODEL_FIELD_BYTES
            || group.len() > MAX_MODEL_FIELD_BYTES
        {
            return Err(ModelValueError::new(
                "principal",
                "is blank, invalid, or unbounded",
            ));
        }
        Ok(Self { owner, group, mode })
    }

    #[must_use]
    pub fn owner(&self) -> &str {
        &self.owner
    }

    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }
}

impl std::fmt::Debug for MappedOwnership {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MappedOwnership")
            .field("owner", &"<redacted>")
            .field("group", &"<redacted>")
            .field("mode", &self.mode)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimestampMetadata {
    pub accessed: Option<StorageTimestamp>,
    pub modified: Option<StorageTimestamp>,
    pub created: Option<StorageTimestamp>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataObservations {
    pub(crate) acl: MetadataObservation<AclMetadata>,
    pub(crate) xattrs: MetadataObservation<Vec<ExtendedAttribute>>,
    pub(crate) tags: MetadataObservation<Vec<ObjectTag>>,
    pub(crate) ownership_mode: MetadataObservation<OwnershipMode>,
    pub(crate) timestamps: MetadataObservation<TimestampMetadata>,
}

impl Default for MetadataObservations {
    fn default() -> Self {
        Self {
            acl: MetadataObservation::NotRequested,
            xattrs: MetadataObservation::NotRequested,
            tags: MetadataObservation::NotRequested,
            ownership_mode: MetadataObservation::NotRequested,
            timestamps: MetadataObservation::NotRequested,
        }
    }
}

impl MetadataObservations {
    pub(crate) fn new(
        acl: MetadataObservation<AclMetadata>,
        xattrs: MetadataObservation<Vec<ExtendedAttribute>>,
        tags: MetadataObservation<Vec<ObjectTag>>,
        ownership_mode: MetadataObservation<OwnershipMode>,
        timestamps: MetadataObservation<TimestampMetadata>,
    ) -> Result<Self, ModelValueError> {
        validate_payload(&acl, &xattrs, &tags)?;
        Ok(Self {
            acl,
            xattrs,
            tags,
            ownership_mode,
            timestamps,
        })
    }
    #[must_use]
    pub const fn acl(&self) -> &MetadataObservation<AclMetadata> {
        &self.acl
    }
    #[must_use]
    pub const fn xattrs(&self) -> &MetadataObservation<Vec<ExtendedAttribute>> {
        &self.xattrs
    }
    #[must_use]
    pub const fn tags(&self) -> &MetadataObservation<Vec<ObjectTag>> {
        &self.tags
    }
    #[must_use]
    pub const fn ownership_mode(&self) -> &MetadataObservation<OwnershipMode> {
        &self.ownership_mode
    }
    #[must_use]
    pub const fn timestamps(&self) -> &MetadataObservation<TimestampMetadata> {
        &self.timestamps
    }
}

fn validate_payload(
    acl: &MetadataObservation<AclMetadata>,
    xattrs: &MetadataObservation<Vec<ExtendedAttribute>>,
    tags: &MetadataObservation<Vec<ObjectTag>>,
) -> Result<(), ModelValueError> {
    let attributes = xattrs.value().map_or(&[][..], Vec::as_slice);
    if attributes.len() > MAX_XATTR_COUNT {
        return Err(ModelValueError::new("xattrs", "too many attributes"));
    }
    let tags = tags.value().map_or(&[][..], Vec::as_slice);
    if tags.len() > MAX_TAG_COUNT {
        return Err(ModelValueError::new("tags", "too many tags"));
    }
    let acl_size = acl.value().map_or(0, |value| {
        value.access.as_ref().map_or(0, Vec::len) + value.default.as_ref().map_or(0, Vec::len)
    });
    let total = attributes.iter().try_fold(acl_size, |total, value| {
        total
            .checked_add(value.name.len())?
            .checked_add(value.value.len())
    });
    let total = total.and_then(|total| {
        tags.iter().try_fold(total, |total, tag| {
            total
                .checked_add(tag.key.len())?
                .checked_add(tag.value.len())
        })
    });
    if total.is_none_or(|total| total > MAX_MODEL_FIELD_BYTES) {
        return Err(ModelValueError::new(
            "metadata",
            "exceeds total payload limit",
        ));
    }
    Ok(())
}

pub(crate) fn encode(metadata: &MetadataObservations, output: &mut Vec<u8>) {
    if let Some(value) = encode_state(&metadata.acl, output) {
        output.push(match value.encoding {
            AclEncoding::Posix => 0,
            AclEncoding::NfsV4 => 1,
            AclEncoding::WindowsSecurityDescriptor => 2,
        });
        encode_optional_bytes(output, value.access.as_deref());
        encode_optional_bytes(output, value.default.as_deref());
    }
    if let Some(values) = encode_state(&metadata.xattrs, output) {
        let Ok(count) = u32::try_from(values.len()) else {
            unreachable!("metadata observation count exceeds snapshot encoding")
        };
        output.extend_from_slice(&count.to_le_bytes());
        for value in values {
            put_bytes(output, &value.name);
            put_bytes(output, &value.value);
        }
    }
    if let Some(values) = encode_state(&metadata.tags, output) {
        let Ok(count) = u32::try_from(values.len()) else {
            unreachable!("metadata observation count exceeds snapshot encoding")
        };
        output.extend_from_slice(&count.to_le_bytes());
        for value in values {
            put_bytes(output, value.key.as_bytes());
            put_bytes(output, value.value.as_bytes());
        }
    }
    if let Some(value) = encode_state(&metadata.ownership_mode, output) {
        for number in [value.uid, value.gid, value.mode] {
            output.extend_from_slice(&number.to_le_bytes());
        }
    }
    if let Some(value) = encode_state(&metadata.timestamps, output) {
        encode_time(output, value.accessed);
        encode_time(output, value.modified);
        encode_time(output, value.created);
    }
}

fn encode_state<'a, T>(value: &'a MetadataObservation<T>, output: &mut Vec<u8>) -> Option<&'a T> {
    match value {
        MetadataObservation::NotRequested => output.push(0),
        MetadataObservation::NotApplicable => output.push(1),
        MetadataObservation::Unsupported => output.push(2),
        MetadataObservation::Failed { class, transience } => {
            output.extend_from_slice(&[3, failure_tag(*class), transience_tag(*transience)]);
        }
        MetadataObservation::Value { value, provenance } => {
            output.extend_from_slice(&[4, provenance_tag(*provenance)]);
            return Some(value);
        }
    }
    None
}

#[derive(Clone, Copy)]
enum State {
    Absent(u8),
    Failed(FailureClass, Transience),
    Value(MetadataProvenance),
}

fn decode_state(cursor: &mut Cursor<'_>) -> Result<State, SnapshotDecodeError> {
    match cursor.byte()? {
        tag @ 0..=2 => Ok(State::Absent(tag)),
        3 => Ok(State::Failed(
            failure_from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?,
            transience_from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?,
        )),
        4 => Ok(State::Value(
            provenance_from_tag(cursor.byte()?).ok_or(SnapshotDecodeError::Malformed)?,
        )),
        _ => Err(SnapshotDecodeError::Malformed),
    }
}

fn finish<T>(
    state: State,
    value: impl FnOnce() -> Result<T, SnapshotDecodeError>,
) -> Result<MetadataObservation<T>, SnapshotDecodeError> {
    match state {
        State::Absent(0) => Ok(MetadataObservation::NotRequested),
        State::Absent(1) => Ok(MetadataObservation::NotApplicable),
        State::Absent(2) => Ok(MetadataObservation::Unsupported),
        State::Failed(class, transience) => Ok(MetadataObservation::Failed { class, transience }),
        State::Value(provenance) => Ok(MetadataObservation::Value {
            value: value()?,
            provenance,
        }),
        State::Absent(_) => Err(SnapshotDecodeError::Malformed),
    }
}

pub(crate) fn decode(cursor: &mut Cursor<'_>) -> Result<MetadataObservations, SnapshotDecodeError> {
    let mut budget = DecodeBudget::new();
    let acl_state = decode_state(cursor)?;
    let acl = finish(acl_state, || decode_acl(cursor, &mut budget))?;
    let xattr_state = decode_state(cursor)?;
    let xattrs = finish(xattr_state, || decode_xattrs(cursor, &mut budget))?;
    let tag_state = decode_state(cursor)?;
    let tags = finish(tag_state, || decode_tags(cursor, &mut budget))?;
    let owner_state = decode_state(cursor)?;
    let ownership = finish(owner_state, || decode_ownership(cursor))?;
    let time_state = decode_state(cursor)?;
    let timestamps = finish(time_state, || decode_timestamps(cursor))?;
    MetadataObservations::new(acl, xattrs, tags, ownership, timestamps)
        .map_err(|_| SnapshotDecodeError::FieldTooLarge)
}

fn decode_tags(
    cursor: &mut Cursor<'_>,
    budget: &mut DecodeBudget,
) -> Result<Vec<ObjectTag>, SnapshotDecodeError> {
    let count = cursor.u32()? as usize;
    if count > MAX_TAG_COUNT {
        return Err(SnapshotDecodeError::FieldTooLarge);
    }
    (0..count)
        .map(|_| {
            let key = String::from_utf8(budgeted_bytes(cursor, budget)?)
                .map_err(|_| SnapshotDecodeError::Malformed)?;
            let value = String::from_utf8(budgeted_bytes(cursor, budget)?)
                .map_err(|_| SnapshotDecodeError::Malformed)?;
            ObjectTag::new(key, value).map_err(|_| SnapshotDecodeError::Malformed)
        })
        .collect()
}

fn decode_acl(
    cursor: &mut Cursor<'_>,
    budget: &mut DecodeBudget,
) -> Result<AclMetadata, SnapshotDecodeError> {
    let encoding = match cursor.byte()? {
        0 => AclEncoding::Posix,
        1 => AclEncoding::NfsV4,
        2 => AclEncoding::WindowsSecurityDescriptor,
        _ => return Err(SnapshotDecodeError::Malformed),
    };
    let access = decode_optional_bytes(cursor, budget)?;
    let default = decode_optional_bytes(cursor, budget)?;
    AclMetadata::new_parts(encoding, access, default).map_err(|_| SnapshotDecodeError::Malformed)
}

fn encode_optional_bytes(output: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            output.push(1);
            put_bytes(output, value);
        }
        None => output.push(0),
    }
}

fn decode_optional_bytes(
    cursor: &mut Cursor<'_>,
    budget: &mut DecodeBudget,
) -> Result<Option<Vec<u8>>, SnapshotDecodeError> {
    match cursor.byte()? {
        0 => Ok(None),
        1 => Ok(Some(budgeted_bytes(cursor, budget)?)),
        _ => Err(SnapshotDecodeError::Malformed),
    }
}

fn decode_xattrs(
    cursor: &mut Cursor<'_>,
    budget: &mut DecodeBudget,
) -> Result<Vec<ExtendedAttribute>, SnapshotDecodeError> {
    let count = cursor.u32()? as usize;
    if count > MAX_XATTR_COUNT {
        return Err(SnapshotDecodeError::FieldTooLarge);
    }
    (0..count)
        .map(|_| {
            ExtendedAttribute::new(
                budgeted_bytes(cursor, budget)?,
                budgeted_bytes(cursor, budget)?,
            )
            .map_err(|_| SnapshotDecodeError::Malformed)
        })
        .collect()
}

struct DecodeBudget {
    remaining: usize,
}

impl DecodeBudget {
    const fn new() -> Self {
        Self {
            remaining: MAX_MODEL_FIELD_BYTES,
        }
    }
    fn consume(&mut self, amount: usize) -> Result<(), SnapshotDecodeError> {
        self.remaining = self
            .remaining
            .checked_sub(amount)
            .ok_or(SnapshotDecodeError::FieldTooLarge)?;
        Ok(())
    }
}

fn budgeted_bytes(
    cursor: &mut Cursor<'_>,
    budget: &mut DecodeBudget,
) -> Result<Vec<u8>, SnapshotDecodeError> {
    let bytes = cursor.bytes()?;
    budget.consume(bytes.len())?;
    Ok(bytes.to_vec())
}

fn decode_ownership(cursor: &mut Cursor<'_>) -> Result<OwnershipMode, SnapshotDecodeError> {
    Ok(OwnershipMode {
        uid: cursor.u32()?,
        gid: cursor.u32()?,
        mode: cursor.u32()?,
    })
}

fn decode_timestamps(cursor: &mut Cursor<'_>) -> Result<TimestampMetadata, SnapshotDecodeError> {
    Ok(TimestampMetadata {
        accessed: decode_time(cursor)?,
        modified: decode_time(cursor)?,
        created: decode_time(cursor)?,
    })
}

const fn provenance_tag(value: MetadataProvenance) -> u8 {
    match value {
        MetadataProvenance::Inline => 0,
        MetadataProvenance::AdditionalCall => 1,
    }
}
const fn provenance_from_tag(tag: u8) -> Option<MetadataProvenance> {
    match tag {
        0 => Some(MetadataProvenance::Inline),
        1 => Some(MetadataProvenance::AdditionalCall),
        _ => None,
    }
}
const fn transience_tag(value: Transience) -> u8 {
    match value {
        Transience::Transient => 0,
        Transience::Permanent => 1,
        Transience::Unknown => 2,
    }
}
const fn transience_from_tag(tag: u8) -> Option<Transience> {
    match tag {
        0 => Some(Transience::Transient),
        1 => Some(Transience::Permanent),
        2 => Some(Transience::Unknown),
        _ => None,
    }
}
const fn failure_tag(value: FailureClass) -> u8 {
    match value {
        FailureClass::Connectivity => 0,
        FailureClass::Authentication => 1,
        FailureClass::PermissionDenied => 2,
        FailureClass::NotFound => 3,
        FailureClass::InvalidInput => 4,
        FailureClass::Unsupported => 5,
        FailureClass::Conflict => 6,
        FailureClass::Capacity => 7,
        FailureClass::Corruption => 8,
        FailureClass::Protocol => 9,
        FailureClass::Cancelled => 10,
        FailureClass::Internal => 11,
    }
}
const fn failure_from_tag(tag: u8) -> Option<FailureClass> {
    match tag {
        0 => Some(FailureClass::Connectivity),
        1 => Some(FailureClass::Authentication),
        2 => Some(FailureClass::PermissionDenied),
        3 => Some(FailureClass::NotFound),
        4 => Some(FailureClass::InvalidInput),
        5 => Some(FailureClass::Unsupported),
        6 => Some(FailureClass::Conflict),
        7 => Some(FailureClass::Capacity),
        8 => Some(FailureClass::Corruption),
        9 => Some(FailureClass::Protocol),
        10 => Some(FailureClass::Cancelled),
        11 => Some(FailureClass::Internal),
        _ => None,
    }
}

#[cfg(test)]
#[path = "metadata_observation_tests.rs"]
mod tests;
