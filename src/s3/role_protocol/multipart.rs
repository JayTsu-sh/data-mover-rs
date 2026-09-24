//! Multipart requests of the role protocol (ADR-0006 C15a): parts carry `Content-MD5`, a
//! completion reports what it wrote, and the uploads in progress on one key can be listed.

use aws_sdk_s3::operation::list_multipart_uploads::ListMultipartUploadsOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use bytes::Bytes;

use super::{
    CompletedPart, ProvideErrorMetadata, S3ProtocolFailure, S3Result, S3Storage, S3WriteFacts,
    s3_role_entry, s3_role_remote_failure, s3_role_transport_failure,
};
use crate::model::FailureClass;

/// Where the next page of a truncated `ListMultipartUploads` starts: (key marker, upload id
/// marker).
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by Direct writes to S3, ADR-0006 C14c")
)]
type UploadsMarker = (Option<String>, Option<String>);

impl S3Storage {
    pub(super) async fn role_upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: Bytes,
        content_md5_base64: &str,
    ) -> S3Result<String> {
        self.client
            .upload_part()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .upload_id(upload_id)
            .part_number(part_number)
            .content_md5(content_md5_base64)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 UploadPart request failed"))?
            .e_tag()
            .map(str::to_string)
            .ok_or_else(|| {
                s3_role_entry(
                    FailureClass::Corruption,
                    "S3 UploadPart response omitted ETag",
                )
            })
    }

    pub(super) async fn role_complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> S3Result<S3WriteFacts> {
        let completed: Vec<CompletedPart> = parts
            .iter()
            .map(|(number, etag)| {
                CompletedPart::builder()
                    .part_number(*number)
                    .e_tag(etag)
                    .build()
            })
            .collect();
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed))
            .build();
        let response = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(self.build_full_key(key))
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(|error| classify_sdk!(error, "S3 CompleteMultipartUpload request failed"))?;
        // The object was written; only the response is malformed.
        let etag = response.e_tag().ok_or_else(|| {
            S3ProtocolFailure::protocol("S3 CompleteMultipartUpload response omitted ETag")
        })?;
        Ok(S3WriteFacts::new(
            etag.to_string(),
            response.version_id().map(str::to_string),
        ))
    }

    /// `ListMultipartUploads` with the key as prefix, keeping only uploads on exactly that key.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by Direct writes to S3, ADR-0006 C14c")
    )]
    pub(super) async fn role_list_uploads(&self, key: &str) -> S3Result<Vec<String>> {
        let full_key = self.build_full_key(key);
        let mut marker: UploadsMarker = (None, None);
        let mut uploads = Vec::new();
        loop {
            let response = self
                .client
                .list_multipart_uploads()
                .bucket(&self.bucket_name)
                .prefix(&full_key)
                .set_key_marker(marker.0.clone())
                .set_upload_id_marker(marker.1.clone())
                .send()
                .await
                .map_err(|error| classify_sdk!(error, "S3 ListMultipartUploads request failed"))?;
            uploads.extend(uploads_on_key(&response, &full_key));
            let Some(next) = next_uploads_marker(&response, &marker)? else {
                return Ok(uploads);
            };
            marker = next;
        }
    }
}

/// The upload ids of `response` whose key is exactly `full_key`.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by Direct writes to S3, ADR-0006 C14c")
)]
fn uploads_on_key(response: &ListMultipartUploadsOutput, full_key: &str) -> Vec<String> {
    response
        .uploads()
        .iter()
        .filter(|upload| upload.key() == Some(full_key))
        .filter_map(|upload| upload.upload_id().map(str::to_string))
        .collect()
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by Direct writes to S3, ADR-0006 C14c")
)]
fn next_uploads_marker(
    response: &ListMultipartUploadsOutput,
    previous: &UploadsMarker,
) -> S3Result<Option<UploadsMarker>> {
    if response.is_truncated() != Some(true) {
        return Ok(None);
    }
    let next = (
        response.next_key_marker().map(str::to_string),
        response.next_upload_id_marker().map(str::to_string),
    );
    if next.0.is_none() || &next == previous {
        return Err(S3ProtocolFailure::protocol(
            "S3 ListMultipartUploads continuation marker did not advance",
        ));
    }
    Ok(Some(next))
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::MultipartUpload;

    use super::*;
    use crate::model::Transience;

    /// A part list the upload does not hold, or out of order, is a permanent `Conflict` of the
    /// entry; a part below the minimum size a permanent `Corruption` — whatever the status.
    #[test]
    fn completion_refusals_concern_the_entry() {
        for (code, class) in [
            ("InvalidPart", FailureClass::Conflict),
            ("InvalidPartOrder", FailureClass::Conflict),
            ("EntityTooSmall", FailureClass::Corruption),
        ] {
            for status in [Some(400), None] {
                assert_eq!(
                    s3_role_remote_failure(status, Some(code), "complete"),
                    S3ProtocolFailure::entry(class, Transience::Permanent, "complete"),
                    "{code} {status:?}"
                );
            }
        }
    }

    fn listing(keys: &[(&str, &str)], truncated: bool) -> ListMultipartUploadsOutput {
        let uploads = keys
            .iter()
            .map(|(key, id)| MultipartUpload::builder().key(*key).upload_id(*id).build())
            .collect();
        let builder = ListMultipartUploadsOutput::builder()
            .set_uploads(Some(uploads))
            .is_truncated(truncated);
        if truncated {
            builder
                .next_key_marker("k")
                .next_upload_id_marker("u")
                .build()
        } else {
            builder.build()
        }
    }

    /// A prefix listing (AWS, Ceph, `StorageGRID`) also returns longer keys; only the exact key's
    /// uploads count.
    #[test]
    fn only_uploads_on_the_exact_key_are_kept() {
        let response = listing(
            &[("p/a", "1"), ("p/a.bak", "2"), ("p/a/b", "3"), ("p/a", "4")],
            false,
        );
        assert_eq!(uploads_on_key(&response, "p/a"), ["1", "4"]);
    }

    #[test]
    fn a_truncated_listing_must_advance() {
        let start = (None, None);
        assert_eq!(next_uploads_marker(&listing(&[], false), &start), Ok(None));
        let next = next_uploads_marker(&listing(&[], true), &start);
        assert_eq!(next, Ok(Some((Some("k".into()), Some("u".into())))));
        let repeated = (Some("k".to_string()), Some("u".to_string()));
        assert!(next_uploads_marker(&listing(&[], true), &repeated).is_err());
    }
}
