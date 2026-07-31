use aws_sdk_s3::config::Builder;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;

/// Register the `StorageGRID` compatibility interceptor.
pub(super) fn configure(builder: Builder) -> Builder {
    builder.interceptor(StripXIdInterceptor)
}

pub(super) fn compatibility_enabled() -> bool {
    enabled_from_value(std::env::var("AWS_S3_STORAGEGRID_COMPAT").ok().as_deref())
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
struct StripXIdInterceptor;

impl Intercept for StripXIdInterceptor {
    fn name(&self) -> &'static str {
        "StripXIdInterceptor"
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
        Ok(())
    }
}

fn enabled_from_value(value: Option<&str>) -> bool {
    value.is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use aws_credential_types::Credentials;
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

    #[test]
    fn compatibility_is_opt_in() {
        assert!(!enabled_from_value(None));
        assert!(enabled_from_value(Some("1")));
        assert!(enabled_from_value(Some("TRUE")));
        assert!(!enabled_from_value(Some("0")));
        assert!(!enabled_from_value(Some("false")));
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
}
