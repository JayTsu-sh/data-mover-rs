//! Preserve the SDK's resolved transport when disabling retries for shaped GETs.
use std::sync::{Arc, OnceLock};

use aws_sdk_s3::{Client, Config};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::http::SharedHttpClient;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextRef;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;

/// `Client::config()` omits a transport supplied by the SDK's default runtime plugin.
/// Observe that exact transport during metadata preflight instead of constructing another one.
#[derive(Clone, Debug, Default)]
pub(super) struct HttpClientCapture(Arc<OnceLock<SharedHttpClient>>);

impl Intercept for HttpClientCapture {
    fn name(&self) -> &'static str {
        "SourceQosHttpClientCapture"
    }

    fn read_before_transmit(
        &self,
        _: &BeforeTransmitInterceptorContextRef<'_>,
        components: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if self.0.get().is_none()
            && let Some(client) = components.http_client()
        {
            let _ = self.0.set(client);
        }
        Ok(())
    }
}

pub(super) fn capturing_client(config: Config) -> (Client, HttpClientCapture) {
    let capture = HttpClientCapture::default();
    let config = if let Some(client) = config.http_client() {
        let _ = capture.0.set(client);
        config
    } else {
        config.to_builder().interceptor(capture.clone()).build()
    };
    (Client::from_conf(config), capture)
}

pub(super) fn single_attempt_client(client: &Client, capture: &HttpClientCapture) -> Client {
    let mut config = client
        .config()
        .to_builder()
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled());
    if let Some(http) = capture.0.get() {
        config.set_http_client(Some(http.clone()));
    }
    Client::from_conf(config.build())
}
