//! The legacy S3 constructor logs what it parsed from the URL but neither credential (R8): not
//! the access key id, not the secret, not even the secret's length.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;

use super::S3Storage;
use crate::AssertTestValue;

const ACCESS_KEY: &str = "AKIDLEAKCHECK4F7Q";
const SECRET_KEY: &str = "sEcReT/leak+check=Zq";

/// Every line the crate's own spans and events wrote.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = Captured;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A local endpoint answering every request (the constructor's `GetBucketVersioning`) with an
/// unversioned bucket, so the constructor runs to the end without a network.
async fn unversioned_endpoint() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assert_value("bind a local endpoint");
    let port = listener.local_addr().assert_value("local address").port();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0_u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                let body = "<VersioningConfiguration/>";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    port
}

#[tokio::test]
async fn the_constructor_logs_no_credential() {
    let port = unversioned_endpoint().await;
    let captured = Captured::default();
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("data_mover=trace"))
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(captured.clone()),
        );
    let guard = tracing::subscriber::set_default(subscriber);
    let url = format!("s3://{ACCESS_KEY}:{SECRET_KEY}@data-mover-leak.127.0.0.1:{port}/prefix");
    let storage = S3Storage::new(&url, None).await;
    drop(guard);
    storage.assert_value("connect to the local endpoint");
    let log = String::from_utf8_lossy(&captured.0.lock().unwrap_or_else(PoisonError::into_inner))
        .into_owned();
    // The capture works: the parsed bucket is logged.
    assert!(log.contains("data-mover-leak"), "{log}");
    assert!(!log.contains(ACCESS_KEY), "access key id logged: {log}");
    assert!(!log.contains(SECRET_KEY), "secret logged: {log}");
    assert!(
        !log.contains(&"*".repeat(SECRET_KEY.len())),
        "secret length logged: {log}"
    );
}
