//! Metadata dependencies and instance-specific observation capabilities.

use super::{ObservationMode, ObservationPlan};

/// One independently requested metadata family.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum MetadataFamily {
    Acl = 0,
    Xattrs = 1,
    Tags = 2,
    OwnershipMode = 3,
    Timestamps = 4,
}

impl MetadataFamily {
    /// Families in their stable declaration order.
    pub const ALL: [Self; 5] = [
        Self::Acl,
        Self::Xattrs,
        Self::Tags,
        Self::OwnershipMode,
        Self::Timestamps,
    ];
}

/// Families a filter reads. This declares dependencies without requesting any I/O.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataRequirements(u8);

impl MetadataRequirements {
    /// No metadata dependencies.
    pub const NONE: Self = Self(0);

    /// Adds a family, preserving the existing dependencies.
    #[must_use]
    pub const fn with(self, family: MetadataFamily) -> Self {
        Self(self.0 | (1 << family as u8))
    }

    /// Whether this set contains the family.
    #[must_use]
    pub const fn contains(self, family: MetadataFamily) -> bool {
        self.0 & (1 << family as u8) != 0
    }

    /// Iterates over dependencies in [`MetadataFamily::ALL`] order.
    pub fn iter(self) -> impl Iterator<Item = MetadataFamily> {
        MetadataFamily::ALL
            .into_iter()
            .filter(move |family| self.contains(*family))
    }
}

impl FromIterator<MetadataFamily> for MetadataRequirements {
    fn from_iter<T: IntoIterator<Item = MetadataFamily>>(families: T) -> Self {
        families.into_iter().fold(Self::NONE, Self::with)
    }
}

/// What a connected metadata role can establish without probing individual entries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ObservationSupport {
    /// This instance can observe the family; individual calls may still fail.
    Supported,
    /// This family has no meaning for any entry on this backend.
    NotApplicable,
    /// This instance cannot obtain the family through additional calls.
    Unsupported,
    /// An implementation must determine support while observing an entry.
    #[default]
    Unknown,
}

/// No-I/O facts about the five metadata families of one connected backend.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataCapabilities([ObservationSupport; 5]);

impl MetadataCapabilities {
    /// Records support for one family; all other families retain their values.
    #[must_use]
    pub const fn with(mut self, family: MetadataFamily, support: ObservationSupport) -> Self {
        self.0[family as usize] = support;
        self
    }

    /// Returns support for one family; unspecified families default to `Unknown`.
    #[must_use]
    pub const fn support(self, family: MetadataFamily) -> ObservationSupport {
        self.0[family as usize]
    }
}

impl ObservationPlan {
    /// Returns the requested observation mode for one family.
    #[must_use]
    pub const fn mode(self, family: MetadataFamily) -> ObservationMode {
        match family {
            MetadataFamily::Acl => self.acl(),
            MetadataFamily::Xattrs => self.xattrs(),
            MetadataFamily::Tags => self.tags(),
            MetadataFamily::OwnershipMode => self.ownership_mode(),
            MetadataFamily::Timestamps => self.timestamps(),
        }
    }

    /// Replaces one family's mode, preserving the other requested modes.
    #[must_use]
    pub const fn with(mut self, family: MetadataFamily, mode: ObservationMode) -> Self {
        self = match family {
            MetadataFamily::Acl => self.with_acl(mode),
            MetadataFamily::Xattrs => self.with_xattrs(mode),
            MetadataFamily::Tags => self.with_tags(mode),
            MetadataFamily::OwnershipMode => self.with_ownership_mode(mode),
            MetadataFamily::Timestamps => self.with_timestamps(mode),
        };
        self
    }

    #[doc(hidden)]
    #[must_use]
    pub fn only(self, requirements: MetadataRequirements) -> Self {
        requirements.iter().fold(Self::default(), |plan, family| {
            plan.with(family, self.mode(family))
        })
    }

    #[doc(hidden)]
    #[must_use]
    pub fn permits_io(self) -> bool {
        MetadataFamily::ALL
            .into_iter()
            .any(|family| self.mode(family).permits_io())
    }
}

impl ObservationMode {
    #[doc(hidden)]
    #[must_use]
    pub const fn permits_io(self) -> bool {
        matches!(self, Self::BestEffort | Self::Required)
    }
}
