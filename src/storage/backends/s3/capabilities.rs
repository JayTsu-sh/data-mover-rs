//! What one S3 bucket can do (P7 §14.2): switches the roles read, set once from the URL's profile
//! (`compat=` and `bucket_type=`) when the protocol is connected. No role branches on a vendor name;
//! each branch reads one switch, so a future bucket type is one more row where the URL is mapped.

/// 5 GiB: the most one `PutObject` or `CopyObject` carries.
pub(crate) const MAX_SINGLE_REQUEST_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// The switches of one S3 bucket: independent yes / no facts, one per behaviour a role branches on.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct S3BucketCapabilities {
    /// A read can be pinned by the store to what was described: `If-Match` on the `ETag`, or
    /// `?versionId=`. Without it every ranged read is checked after the fact instead: the
    /// response's `ETag`, `Content-Range` total and `Last-Modified` must be the described ones, a
    /// body that broke off is a replacement or a lost connection as one `HEAD` after it finds, and
    /// a source read ends with one more `HEAD`.
    pub(crate) conditional_reads: bool,
    /// Multipart uploads (`CreateMultipartUpload` …). Without them no multipart request is ever
    /// sent: an object is one `PutObject`, at most [`Self::max_object_bytes`].
    pub(crate) multipart: bool,
    /// A transfer's progress can be kept at the destination for a later attempt (the `.upload`
    /// pointer beside the key and `ListParts`, ADR-0006). Without it every attempt starts from
    /// zero, and a prepare looks for nothing (no pointer `HEAD`, no `ListMultipartUploads`).
    pub(crate) resumable: bool,
    /// The largest object written here; `None` = no limit of the bucket's own.
    pub(crate) max_object_bytes: Option<u64>,
    /// Keys are file paths: each `/`-separated segment is a file name of at most 255 bytes, and
    /// an empty, `.` or `..` segment is resolved away — the object would land under another key
    /// (N0: `a/../x`, `a/./y`, `a//b` are silently rewritten; a longer segment answers 500).
    pub(crate) file_backed_keys: bool,
    /// A stored object's `ETag` is the MD5 of its content when one `PutObject` wrote it, so a
    /// write whose reply was lost is proven ours by a `HEAD`. Not on an ONTAP NAS bucket, whose
    /// `ETag`s derive from the file's mtime and size (only the `PutObject` reply carries the MD5):
    /// there a prepare looks at the final key once, and an uncertain write is unchanged only when
    /// the key still answers that.
    pub(crate) md5_etags: bool,
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
        multipart: true,
        resumable: true,
        max_object_bytes: None,
        file_backed_keys: false,
        md5_etags: true,
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
        multipart: false,
        resumable: false,
        max_object_bytes: Some(MAX_SINGLE_REQUEST_BYTES),
        file_backed_keys: true,
        md5_etags: false,
        versions: false,
        directory_objects: false,
        native_same_bucket_only: true,
    };
}
