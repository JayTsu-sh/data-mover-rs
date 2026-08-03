use aws_sdk_s3::config::Builder;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use md5::{Digest, Md5};

/// Register the `StorageGRID` compatibility interceptor.
pub(super) fn configure(builder: Builder) -> Builder {
    builder.interceptor(StorageGridCompatibilityInterceptor)
}

/// Remove the exact `x-id` query parameter from an encoded request URI.
///
/// The common no-match path performs no allocation. A matching URI is scanned
/// once to detect the edit and rebuilt into one preallocated `String`; encoded
/// path and query bytes are copied verbatim.
fn strip_x_id_from_uri(uri: &str) -> Option<String> {
    let query_start = uri.find('?')?;
    let query = &uri[query_start + 1..];
    let is_x_id = |pair: &str| pair == "x-id" || pair.starts_with("x-id=");

    if !query.split('&').any(is_x_id) {
        return None;
    }

    let mut stripped = String::with_capacity(uri.len());
    stripped.push_str(&uri[..query_start]);

    let mut separator = '?';
    for pair in query.split('&') {
        if is_x_id(pair) {
            continue;
        }
        stripped.push(separator);
        stripped.push_str(pair);
        separator = '&';
    }

    Some(stripped)
}

#[derive(Debug)]
struct StorageGridCompatibilityInterceptor;

impl Intercept for StorageGridCompatibilityInterceptor {
    fn name(&self) -> &'static str {
        "StorageGridCompatibilityInterceptor"
    }

    fn modify_before_signing(
        &self,
        context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request_mut();
        if let Some(uri) = strip_x_id_from_uri(request.uri()) {
            request.set_uri(uri)?;
        }
        add_delete_objects_content_md5(request)?;
        Ok(())
    }
}

/// `StorageGRID` 11.5 requires the legacy RFC 1864 checksum for multi-object
/// delete and does not accept the newer `x-amz-checksum-*` headers instead.
/// The SDK-generated XML body is buffered, so hashing borrows it without a
/// body copy. This hook runs before `SigV4` signing so the header is signed.
fn add_delete_objects_content_md5(
    request: &mut aws_smithy_runtime_api::client::orchestrator::HttpRequest,
) -> Result<(), BoxError> {
    let is_delete_objects = request.method() == "POST"
        && request
            .uri()
            .split_once('?')
            .is_some_and(|(_, query)| query.split('&').any(|pair| pair == "delete"));
    if !is_delete_objects || request.headers().contains_key("content-md5") {
        return Ok(());
    }

    let body = request.body().bytes().ok_or_else(|| {
        std::io::Error::other("StorageGRID DeleteObjects request body must be buffered")
    })?;
    let content_md5 = BASE64_STANDARD.encode(Md5::digest(body));
    request
        .headers_mut()
        .try_insert("content-md5", content_md5)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use aws_credential_types::Credentials;
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    use aws_smithy_runtime_api::client::http::{
        HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::result::ConnectorError;
    use aws_smithy_runtime_api::http::StatusCode;
    use aws_smithy_types::body::SdkBody;
    use aws_types::region::Region;

    #[derive(Clone, Debug)]
    struct CapturingConnector {
        uri: Arc<Mutex<Option<String>>>,
    }

    #[derive(Clone, Debug, Default)]
    struct CapturedDeleteRequest {
        uri: String,
        content_md5: Option<String>,
        authorization: Option<String>,
        body: Vec<u8>,
    }

    #[derive(Clone, Debug)]
    struct CapturingDeleteConnector {
        request: Arc<Mutex<Option<CapturedDeleteRequest>>>,
    }

    impl HttpConnector for CapturingConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            *self
                .uri
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(request.uri().to_owned());

            let response = StatusCode::try_from(200)
                .map(|status| {
                    HttpResponse::new(
                        status,
                        SdkBody::from(
                            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Buckets/>
</ListAllMyBucketsResult>"#,
                        ),
                    )
                })
                .map_err(|error| ConnectorError::other(Box::new(error), None));
            HttpConnectorFuture::ready(response)
        }
    }

    impl HttpConnector for CapturingDeleteConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let captured = CapturedDeleteRequest {
                uri: request.uri().to_owned(),
                content_md5: request.headers().get("content-md5").map(str::to_owned),
                authorization: request.headers().get("authorization").map(str::to_owned),
                body: request.body().bytes().unwrap_or_default().to_vec(),
            };
            *self
                .request
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(captured);

            let response = StatusCode::try_from(200)
                .map(|status| {
                    HttpResponse::new(
                        status,
                        SdkBody::from(r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult/>"#),
                    )
                })
                .map_err(|error| ConnectorError::other(Box::new(error), None));
            HttpConnectorFuture::ready(response)
        }
    }

    async fn captured_list_buckets_uri(storagegrid_compatibility: bool) -> String {
        let captured_uri = Arc::new(Mutex::new(None));
        let connector = CapturingConnector {
            uri: Arc::clone(&captured_uri),
        };
        let http_client = http_client_fn(move |_settings, _components| {
            SharedHttpConnector::new(connector.clone())
        });

        let builder = Builder::new()
            .behavior_version_latest()
            .credentials_provider(Credentials::new(
                "access-key",
                "secret-key",
                None,
                None,
                "storagegrid-protocol-test",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url("http://storagegrid.test")
            .force_path_style(true)
            .http_client(http_client);
        let config = if storagegrid_compatibility {
            configure(builder).build()
        } else {
            builder.build()
        };

        let result = aws_sdk_s3::Client::from_conf(config)
            .list_buckets()
            .send()
            .await;
        assert!(
            result.is_ok(),
            "mock S3 response should be accepted: {result:?}"
        );

        captured_uri
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_default()
    }

    async fn captured_delete_objects_request(
        storagegrid_compatibility: bool,
    ) -> CapturedDeleteRequest {
        let captured_request = Arc::new(Mutex::new(None));
        let connector = CapturingDeleteConnector {
            request: Arc::clone(&captured_request),
        };
        let http_client = http_client_fn(move |_settings, _components| {
            SharedHttpConnector::new(connector.clone())
        });
        let builder = Builder::new()
            .behavior_version_latest()
            .credentials_provider(Credentials::new(
                "access-key",
                "secret-key",
                None,
                None,
                "storagegrid-delete-protocol-test",
            ))
            .region(Region::new("us-east-1"))
            .endpoint_url("http://storagegrid.test")
            .force_path_style(true)
            .http_client(http_client);
        let config = if storagegrid_compatibility {
            configure(builder).build()
        } else {
            builder.build()
        };
        let object = ObjectIdentifier::builder()
            .key("prefix/object.txt")
            .build()
            .unwrap();
        let delete = Delete::builder()
            .objects(object)
            .quiet(true)
            .build()
            .unwrap();
        let result = aws_sdk_s3::Client::from_conf(config)
            .delete_objects()
            .bucket("bucket")
            .delete(delete)
            .send()
            .await;
        assert!(
            result.is_ok(),
            "mock delete response should be accepted: {result:?}"
        );

        captured_request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_default()
    }

    #[test]
    fn fast_path_returns_none() {
        assert_eq!(
            strip_x_id_from_uri("https://s3.example/bucket/key?versionId=x-id=value"),
            None
        );
        assert_eq!(
            strip_x_id_from_uri("https://s3.example/bucket/key?x-id-extra=GetObject"),
            None
        );
        assert_eq!(strip_x_id_from_uri("https://s3.example/bucket/key"), None);
    }

    #[test]
    fn preserves_encoded_uri_and_other_parameters() {
        assert_eq!(
            strip_x_id_from_uri(
                "https://s3.example/a%20b/file%2Bname?partNumber=1&x-id=GetObject&versionId=abc"
            ),
            Some("https://s3.example/a%20b/file%2Bname?partNumber=1&versionId=abc".to_string())
        );
        assert_eq!(
            strip_x_id_from_uri("https://s3.example/bucket/key?x-id=GetObject"),
            Some("https://s3.example/bucket/key".to_string())
        );
    }

    #[tokio::test]
    async fn sdk_request_contains_x_id_without_storagegrid_compatibility() {
        let uri = captured_list_buckets_uri(false).await;

        assert!(
            uri.split_once('?')
                .is_some_and(|(_, query)| query.split('&').any(|pair| pair.starts_with("x-id="))),
            "AWS SDK control request should contain x-id: {uri}"
        );
    }

    #[tokio::test]
    async fn storagegrid_compatibility_removes_x_id_from_transmitted_request() {
        let uri = captured_list_buckets_uri(true).await;

        assert!(
            !uri.split_once('?').is_some_and(|(_, query)| {
                query
                    .split('&')
                    .any(|pair| pair == "x-id" || pair.starts_with("x-id="))
            }),
            "StorageGRID request must not contain x-id: {uri}"
        );
    }

    #[tokio::test]
    async fn storagegrid_delete_objects_adds_signed_content_md5() {
        let request = captured_delete_objects_request(true).await;
        let expected = BASE64_STANDARD.encode(Md5::digest(&request.body));

        assert_eq!(request.content_md5.as_deref(), Some(expected.as_str()));
        assert!(!request.uri.contains("x-id="));
        assert!(
            request
                .authorization
                .as_deref()
                .is_some_and(|value| value.contains("content-md5")),
            "Content-MD5 must be covered by SigV4: {:?}",
            request.authorization
        );
    }

    #[tokio::test]
    async fn standard_s3_delete_objects_does_not_add_storagegrid_content_md5() {
        let request = captured_delete_objects_request(false).await;
        assert!(request.content_md5.is_none());
    }
}
