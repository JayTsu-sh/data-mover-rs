//! Server-side copy requests (`CopyObject`, `UploadPartCopy`) state their empty body.
//!
//! Both are `PUT`s without a body, for which the SDK (hyper) sends no `Content-Length`. Some
//! servers refuse that with `411 MissingContentLength`: ONTAP 9.19.1 on `CopyObject`, Ceph RGW
//! (DXN) on `UploadPartCopy`. RFC 9110 has a client send `Content-Length: 0` on a `PUT` without
//! a body anyway; `MinIO`, `RustFS`, DXN and ONTAP accept it on both requests (curl, 2026-09-26;
//! AWS was not retested, `StorageGRID` not tested), so every profile
//! sends it on every copy request.

use aws_smithy_runtime_api::client::orchestrator::HttpRequest;

/// Sets `Content-Length: 0` on a copy request; use as `.customize().mutate_request(..)`.
pub(super) fn state_empty_body(request: &mut HttpRequest) {
    request.headers_mut().insert("content-length", "0");
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, PoisonError};

    use aws_credential_types::Credentials;
    use aws_sdk_s3::config::Builder;
    use aws_smithy_runtime_api::client::http::{
        HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use aws_types::region::Region;

    use super::super::{S3Storage, S3UrlOptions};
    use crate::AssertTestValue;
    use crate::storage::backends::s3::S3VersioningAtConnect;

    /// Each request as (method, uri, `Content-Length`, has `x-amz-copy-source`).
    type Seen = Arc<Mutex<Vec<(String, String, Option<String>, bool)>>>;

    /// Answers the requests of a namespace rename (`HEAD`, `CreateMultipartUpload`, `UploadPartCopy`,
    /// `CompleteMultipartUpload`, `CopyObject`) and records each one.
    #[derive(Clone, Debug, Default)]
    struct Recorder {
        seen: Seen,
    }

    impl HttpConnector for Recorder {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let method = request.method().to_string();
            let uri = request.uri().to_string();
            self.seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((
                    method.clone(),
                    uri.clone(),
                    request.headers().get("content-length").map(str::to_string),
                    request.headers().contains_key("x-amz-copy-source"),
                ));
            let body = match (method.as_str(), uri.contains("uploadId")) {
                ("HEAD", _) => "",
                ("GET", _) => "<Tagging><TagSet/></Tagging>",
                ("POST", false) => {
                    "<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>"
                }
                ("POST", true) => {
                    "<CompleteMultipartUploadResult><ETag>&quot;c&quot;</ETag></CompleteMultipartUploadResult>"
                }
                ("PUT", true) => "<CopyPartResult><ETag>&quot;p&quot;</ETag></CopyPartResult>",
                _ => "<CopyObjectResult><ETag>&quot;o&quot;</ETag></CopyObjectResult>",
            };
            let response = StatusCode::try_from(200)
                .map(|status| {
                    let mut response = HttpResponse::new(status, SdkBody::from(body));
                    if method == "HEAD" {
                        response.headers_mut().insert("content-length", "6291456");
                        response.headers_mut().insert("etag", "\"source\"");
                    }
                    response
                })
                .map_err(|error| ConnectorError::other(Box::new(error), None));
            HttpConnectorFuture::ready(response)
        }
    }

    fn storage(connector: Recorder) -> S3Storage {
        let http_client = http_client_fn(move |_settings, _components| {
            SharedHttpConnector::new(connector.clone())
        });
        let config = Builder::new()
            .behavior_version_latest()
            .credentials_provider(Credentials::new("ak", "sk", None, None, "copy-test"))
            .region(Region::new("us-east-1"))
            .endpoint_url("http://copy.test")
            .force_path_style(true)
            .http_client(http_client)
            .build();
        S3Storage {
            read_chunk_bytes: 8 * 1024 * 1024,
            transfer_concurrency: crate::transfer_concurrency::TransferConcurrency::defaults(4, 4),
            options: S3UrlOptions::default(),
            endpoint: "http://copy.test".into(),
            bucket_name: "bucket".into(),
            prefix: None,
            client: aws_sdk_s3::Client::from_conf(config),
            is_bucket_versioned: false,
            versioning_suspended: false,
            versioning_at_connect: S3VersioningAtConnect::Never,
        }
    }

    /// Namespace rename preserves the empty-body protocol workaround on both copy forms.
    #[tokio::test]
    async fn namespace_copies_state_their_empty_body() {
        let recorder = Recorder::default();
        let storage = storage(recorder.clone());
        super::super::role_protocol::rename::rename_with_limits(
            &storage,
            "from",
            "to",
            8 * 1024 * 1024,
            5 * 1024 * 1024,
        )
        .await
        .assert_value("single-copy rename");
        super::super::role_protocol::rename::rename_with_limits(
            &storage,
            "from",
            "to",
            5 * 1024 * 1024,
            5 * 1024 * 1024,
        )
        .await
        .assert_value("multipart rename");
        let seen = recorder
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let copies: Vec<_> = seen.iter().filter(|(.., copy)| *copy).collect();
        // One CopyObject, then two UploadPartCopy parts (6 MiB in 5 MiB parts).
        assert_eq!(copies.len(), 3, "{seen:?}");
        for (method, uri, length, _) in copies {
            assert_eq!(method, "PUT", "{uri}");
            assert_eq!(length.as_deref(), Some("0"), "{method} {uri}");
        }
    }
}
