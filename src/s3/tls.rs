//! The TLS side of an S3 connection: the error that says how to trust an endpoint's certificate,
//! and the client that accepts any certificate (`s3+https://…?tls_verify=false` only).

use std::fmt;
use std::sync::Arc;

use aws_smithy_runtime_api::client::http::{
    HttpConnector as SdkHttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector,
    http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{
    HttpRequest as SdkHttpRequest, HttpResponse as SdkHttpResponse,
};
use aws_smithy_runtime_api::client::result::{ConnectorError, SdkError};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::display::DisplayErrorContext;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::Client as HyperLegacyClient;
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use hyper_util::rt::TokioExecutor;
use rustls::ClientConfig as RustlsClientConfig;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring::default_provider as ring_provider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tracing::warn;

use crate::Result;
use crate::error::StorageError;

/// TLS 证书验证跳过器（自签名 / 私有 CA 证书的端点，只在 URL 写了 `tls_verify=false` 时使用）
#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        // 仅列出安全的签名方案，排除已废弃的 SHA-1 系列
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

/// 跳过 TLS 验证的 SDK HTTP 连接器，包装 hyper-rustls 客户端
#[derive(Clone)]
struct SkipVerifyConnector {
    inner: Arc<HyperLegacyClient<HttpsConnector<HyperHttpConnector>, SdkBody>>,
}

impl fmt::Debug for SkipVerifyConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkipVerifyConnector")
            .finish_non_exhaustive()
    }
}

impl SdkHttpConnector for SkipVerifyConnector {
    fn call(&self, request: SdkHttpRequest) -> HttpConnectorFuture {
        let client = self.inner.clone();
        HttpConnectorFuture::new(async move {
            let req_1x = request
                .try_into_http1x()
                .map_err(|e| ConnectorError::other(Box::new(e) as _, None))?;
            let response = client
                .request(req_1x)
                .await
                .map_err(|e| ConnectorError::io(Box::new(e) as _))?;
            SdkHttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|e| ConnectorError::other(Box::new(e) as _, None))
        })
    }
}

/// 构建跳过 TLS 证书验证的 AWS SDK HTTP 客户端（`s3+https://…?tls_verify=false` 时使用）
pub(super) fn build_skip_verify_http_client() -> Result<SharedHttpClient> {
    // SECURITY NOTE: dangerous() 仅在 URL 显式写了 tls_verify=false 时调用（默认校验证书，系统根证书，
    // 认 SSL_CERT_FILE / SSL_CERT_DIR）。跳过证书验证仅适用于受信任的私有环境（自签名证书部署）。
    // 显式指定 ring：本 crate 的依赖同时打开了 rustls 的 ring 与 aws-lc-rs，进程默认 provider 无法自动选定。
    let tls_config = RustlsClientConfig::builder_with_provider(Arc::new(ring_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| StorageError::ConfigError(format!("TLS client configuration: {error}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();

    let connector = HttpsConnectorBuilder::new()
        .with_tls_config(tls_config)
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    let hyper_client: Arc<HyperLegacyClient<HttpsConnector<HyperHttpConnector>, SdkBody>> =
        Arc::new(HyperLegacyClient::builder(TokioExecutor::new()).build(connector));
    let skip_connector = SkipVerifyConnector {
        inner: hyper_client,
    };

    Ok(http_client_fn(move |_settings, _components| {
        SharedHttpConnector::new(skip_connector.clone())
    }))
}

/// The error for a request whose connection failed on the server's certificate, saying how to
/// trust it; `None` for any other failure (a service error never counts, whatever it says). A
/// `ConfigError`: retrying cannot change the certificate.
pub(super) fn untrusted_certificate<E, R>(
    endpoint: &str,
    error: &SdkError<E, R>,
) -> Option<StorageError> {
    let SdkError::DispatchFailure(failure) = error else {
        return None;
    };
    let detail = DisplayErrorContext(failure.as_connector_error()?).to_string();
    detail.contains("certificate").then(|| {
        StorageError::ConfigError(format!(
            "S3 endpoint {endpoint}: TLS certificate verification failed ({detail}); trust the \
             endpoint's CA through SSL_CERT_FILE / SSL_CERT_DIR, or add tls_verify=false to the \
             s3+https:// URL (the certificate is then not verified)"
        ))
    })
}

/// The one warning of a connection that does not verify the server certificate.
pub(super) fn warn_unverified(context: &str, endpoint: &str) {
    warn!(
        "S3 {context}: {endpoint} with tls_verify=false — the server certificate is not verified; \
         use it only on a trusted network"
    );
}
