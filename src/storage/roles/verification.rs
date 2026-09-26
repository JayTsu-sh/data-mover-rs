//! How a destination checked what it wrote (P7 §14.2 N4): the method its read-back uses, and
//! whether the store confirmed the bytes it received. Reported with each transfer's outcome; a
//! weaker method is not a failure — it is recorded with its reason.

/// How read-back verification read the destination.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadBackMethod {
    /// Read-back verification was not performed (`ReadBackVerification::Disabled`).
    NotPerformed,
    /// Every read was bound to what the write made: the private stage before publication, or on
    /// an object store the version the write created or `If-Match` on its `ETag`.
    Pinned,
    /// The store cannot pin a read, so the read is bracketed instead: a `HEAD` anchors the
    /// object's `ETag` and size, every range response must carry that `ETag`, and the content is
    /// compared with the source. A replacement during the read is caught (`Conflict`); one between
    /// the write and the `HEAD` cannot be told from corruption, and is reported as `Corruption`
    /// saying so.
    EtagBracketed {
        /// Why the read could not be pinned.
        reason: UnpinnedReadReason,
    },
}

/// Why a destination's read-back is not pinned.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnpinnedReadReason {
    /// The store answers conditional reads with 501 and keeps no versions (an ONTAP NAS bucket).
    ConditionalReadsUnsupported,
}

/// Whether the store confirmed that the bytes it stored are the bytes sent.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UploadDigest {
    /// Confirmed: a `Content-MD5` the store checked on each request that sent bytes (a
    /// `PutObject`, or each `UploadPart` this attempt sent — an ONTAP NAS bucket too refuses a
    /// wrong one with `BadDigest`, N0), or a reply `ETag` equal to the MD5 computed while
    /// streaming.
    Matched,
    /// Not confirmed; the reason says why.
    Unchecked(UploadDigestGap),
}

/// Why no upload digest was checked.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UploadDigestGap {
    /// The destination has no such digest (a filesystem write).
    NotApplicable,
    /// The reply's `ETag` is not an MD5 of the content (server-side encryption with KMS or a
    /// customer key, or a store that reports another value).
    EtagNotMd5,
    /// A server-side copy: no bytes were sent.
    NativeCopy,
    /// This attempt sent no byte: it resumed every part an earlier attempt sent, whose checks are
    /// not reported. (A partial resume reports what this attempt sent.)
    PriorAttempt,
}
