//! What one S3 bucket can do (P7 §14.2): switches the roles read, set once from the URL's profile
//! (`compat=` and `bucket_type=`) when the protocol is connected. No role branches on a vendor name;
//! each branch reads one switch, so a future bucket type is one more row where the URL is mapped.

/// The switches of one S3 bucket: independent yes / no facts, one per behaviour a role branches on.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct S3BucketCapabilities {
    /// A read can be pinned by the store to what was described: `If-Match` on the `ETag`, or
    /// `?versionId=`. Without it every ranged read is checked after the fact instead: the
    /// response's `ETag`, `Content-Range` total and `Last-Modified` must be the described ones, a
    /// body shorter than announced means the object was replaced mid-read, and a source read ends
    /// with one more `HEAD`.
    pub(crate) conditional_reads: bool,
    /// Stored versions: a source can be read at a version and a key's history listed.
    pub(crate) versions: bool,
    /// Content under a key ending in `/` (P7 F5).
    pub(crate) directory_objects: bool,
    /// A native copy pairs only two storages of the same bucket.
    pub(crate) native_same_bucket_only: bool,
}

impl S3BucketCapabilities {
    /// An object bucket of any S3 profile.
    pub(crate) const OBJECT: Self = Self {
        conditional_reads: true,
        versions: true,
        directory_objects: true,
        native_same_bucket_only: false,
    };

    /// An ONTAP NAS (multiprotocol) bucket, `compat=ontap&bucket_type=nas` (measured on ONTAP
    /// 9.19.1, P7 §14.2 N0): a conditional GET, `CreateMultipartUpload` and `GetBucketVersioning`
    /// answer 501, `ListMultipartUploads` / `ListParts` 403; one `PutObject` is atomic at any size
    /// (5 GiB is a design limit, kept because a failed single PUT starts over); a key ending in
    /// `/` holding content is stored as a file; `CopyObject` between a NAS and another bucket
    /// answers 501.
    pub(crate) const NAS: Self = Self {
        conditional_reads: false,
        versions: false,
        directory_objects: false,
        native_same_bucket_only: true,
    };
}
