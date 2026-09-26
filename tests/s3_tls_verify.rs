//! `s3+https://` verifies the server certificate against the system roots (which honour
//! `SSL_CERT_FILE` / `SSL_CERT_DIR`); `tls_verify=false` accepts any certificate (P7 §14.2).
//!
//! A local TLS endpoint presents `tests/data/tls/server.pem` (`localhost` / `127.0.0.1`), issued by
//! the test CA `tests/data/tls/ca.pem`, which no system trusts. The fixtures were made with
//! `openssl req -x509` (CA, P-256, 100 years) and `openssl x509 -req` (server, serverAuth); the CA's
//! key was discarded.

use std::env;
use std::error::Error;
use std::fs;
use std::process::Command;
use std::sync::Arc;

use data_mover::error::StorageError;
use data_mover::s3::S3Storage;
use data_mover::storage::{BackendConfig, S3BackendConfig, connect_backend};
use rustls::ServerConfig;
use rustls::crypto::ring::default_provider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const CA: &[u8] = include_bytes!("data/tls/ca.pem");
const SERVER_CERT: &[u8] = include_bytes!("data/tls/server.pem");
const SERVER_KEY: &[u8] = include_bytes!("data/tls/server-key.pem");
/// Set in the child process that trusts the test CA through `SSL_CERT_FILE`.
const CHILD: &str = "DATA_MOVER_TLS_TEST_CHILD";

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

/// A local HTTPS endpoint answering every request as an unversioned bucket would.
async fn tls_endpoint() -> TestResult<u16> {
    let certs = CertificateDer::pem_slice_iter(SERVER_CERT).collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_slice(SERVER_KEY)?;
    let config = ServerConfig::builder_with_provider(Arc::new(default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                // A client that refuses the certificate ends the handshake here.
                let Ok(mut stream) = acceptor.accept(stream).await else {
                    return;
                };
                let mut request = Vec::new();
                let mut buf = [0_u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                let body = if request.starts_with(b"HEAD ") {
                    ""
                } else {
                    "<VersioningConfiguration/>"
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    Ok(port)
}

fn url(port: u16, query: &str) -> String {
    format!("s3+https://AKIDTLSTEST:tls/Secret+Key@bucket.127.0.0.1:{port}/prefix{query}")
}

/// The refusal names the certificate and both ways out, and neither credential.
fn assert_untrusted(message: &str) {
    assert!(message.contains("certificate"), "{message}");
    assert!(message.contains("tls_verify=false"), "{message}");
    assert!(message.contains("SSL_CERT_FILE"), "{message}");
    assert!(
        !message.contains("AKIDTLSTEST") && !message.contains("tls/Secret+Key"),
        "{message}"
    );
}

#[tokio::test]
async fn an_untrusted_certificate_is_refused_by_default() -> TestResult {
    if env::var_os(CHILD).is_some() {
        return Ok(());
    }
    let port = tls_endpoint().await?;
    let refused = S3Storage::new(&url(port, ""), None).await;
    let Err(StorageError::ConfigError(message)) = refused else {
        return Err(format!("expected ConfigError, got {:?}", refused.err()).into());
    };
    assert_untrusted(&message);
    let listing = S3Storage::list_buckets(&format!(
        "s3+https://AKIDTLSTEST:tls/Secret+Key@127.0.0.1:{port}"
    ))
    .await;
    let Err(StorageError::ConfigError(message)) = listing else {
        return Err(format!("expected ConfigError, got {listing:?}").into());
    };
    assert_untrusted(&message);

    let role = connect_backend(BackendConfig::S3(S3BackendConfig {
        url: url(port, "?compat=dxn"),
        block_size: None,
        single_put_threshold: None,
    }))
    .await
    .err()
    .ok_or("the role-based connection is refused too")?;
    assert_untrusted(&role.to_string());
    Ok(())
}

#[tokio::test]
async fn tls_verify_false_accepts_any_certificate() -> TestResult {
    let port = tls_endpoint().await?;
    let storage = S3Storage::new(&url(port, "?tls_verify=false"), None).await?;
    storage.check_connectivity().await?;
    connect_backend(BackendConfig::S3(S3BackendConfig {
        url: url(port, "?compat=dxn&tls_verify=false"),
        block_size: None,
        single_put_threshold: None,
    }))
    .await?;
    Ok(())
}

/// A private CA is trusted the way the system trusts one: `SSL_CERT_FILE`. The SDK loads the
/// roots once per process, so a child process runs the check.
#[test]
fn a_ca_named_by_ssl_cert_file_is_trusted() -> TestResult {
    if env::var_os(CHILD).is_some() {
        return Ok(());
    }
    // The binary may run away from the source tree: the CA goes to a file of its own.
    let ca = NamedTempFile::new()?;
    fs::write(ca.path(), CA)?;
    let output = Command::new(env::current_exe()?)
        .args([
            "--exact",
            "trusted_through_ssl_cert_file_child",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("SSL_CERT_FILE", ca.path())
        .env_remove("SSL_CERT_DIR")
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "run by a_ca_named_by_ssl_cert_file_is_trusted with SSL_CERT_FILE set"]
async fn trusted_through_ssl_cert_file_child() -> TestResult {
    if env::var_os(CHILD).is_none() {
        return Ok(());
    }
    let port = tls_endpoint().await?;
    let storage = S3Storage::new(&url(port, ""), None).await?;
    storage.check_connectivity().await?;
    Ok(())
}
