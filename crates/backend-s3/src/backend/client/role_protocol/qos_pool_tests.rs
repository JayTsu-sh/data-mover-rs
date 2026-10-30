//! Source `QoS` must retain the connection pool already used by metadata and ordinary reads.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use super::super::{S3Compatibility, S3UrlOptions};
use super::S3Storage;
use crate::model::SourceVersion;
use crate::storage::backends::s3::source::S3ReadSource;
use crate::storage::backends::s3::tests::identity;
use crate::storage::{ReadRequest, ReadSource, SourceQosGroup, SourceQosPolicy};

fn serve(
    listener: TcpListener,
    accepted: Arc<AtomicUsize>,
    server_stop: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let connection = tokio::select! {
                () = server_stop.cancelled() => break,
                result = listener.accept() => result,
            };
            let Ok((mut socket, _)) = connection else {
                break;
            };
            accepted.fetch_add(1, Ordering::Relaxed);
            let connection_stop = server_stop.clone();
            tokio::spawn(async move {
                loop {
                    let mut request = Vec::new();
                    let mut buffer = [0; 4096];
                    while !request.windows(4).any(|part| part == b"\r\n\r\n") {
                        let read = tokio::select! {
                            () = connection_stop.cancelled() => return,
                            result = socket.read(&mut buffer) => result,
                        };
                        let Ok(count) = read else { return };
                        if count == 0 {
                            return;
                        }
                        request.extend_from_slice(&buffer[..count]);
                    }
                    let (status, body, length) = if request.starts_with(b"HEAD ") {
                        ("200 OK", "", 1)
                    } else if request.windows(10).any(|part| part == b"versioning") {
                        let body = "<VersioningConfiguration/>";
                        ("200 OK", body, body.len())
                    } else {
                        ("206 Partial Content", "x", 1)
                    };
                    let response = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {length}\r\nETag: \"source\"\r\n\
                         Content-Type: application/xml\r\nConnection: keep-alive\r\n\r\n{body}"
                    );
                    if socket.write_all(response.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    })
}

#[tokio::test]
async fn source_qos_reuses_the_metadata_and_payload_connection_pool()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::clone(&connections);
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let server = serve(listener, accepted, server_stop);
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        // Use the default connector (the regression needs independently created pools),
        // but disable environment proxy discovery for this loopback-only fixture.
        // Production clients continue to use the latest behavior version.
        #[allow(deprecated)]
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::v2025_01_17())
            .credentials_provider(
                aws_credential_types::provider::SharedCredentialsProvider::new(
                    aws_credential_types::Credentials::new(
                        "test-key",
                        "test-secret",
                        None,
                        None,
                        "pool-test",
                    ),
                ),
            )
            .region(aws_types::region::Region::new("us-east-1"))
            .endpoint_url(format!("http://{address}"))
            .build();
        let config = super::super::build_s3_config(&sdk_config, S3Compatibility::default());
        let (client, http_client_capture) = super::super::qos_client::capturing_client(config);
        let storage = S3Storage {
            client,
            http_client_capture,
            single_attempt_client: std::sync::OnceLock::new(),
            transfer_concurrency: crate::transfer_concurrency::TransferConcurrency::defaults(4, 4),
            read_chunk_bytes: 8 * 1024 * 1024,
            options: S3UrlOptions::default(),
            endpoint: format!("http://{address}"),
            bucket_name: "bucket".into(),
            prefix: None,
            is_bucket_versioned: false,
            versioning_suspended: false,
            versioning_at_connect: crate::storage::backends::s3::S3VersioningAtConnect::Never,
        };
        let source = S3ReadSource::new(Arc::new(storage), identity());
        let path = crate::model::StoragePath::new("source")?;
        let described = source.describe(&path).await?;
        let budget = SourceQosGroup::new(SourceQosPolicy::new(
            Some((1 << 40, 1 << 40, Duration::ZERO)),
            8 * 1024 * 1024,
            Some((1_000_000_000, 1_000_000_000, Duration::ZERO)),
        )?)
        .transfer_budget();
        // Exercise the actual caller's HEAD -> regular GET -> HEAD -> shaped GET sequence.
        for qos in [None, Some(budget.clone()), None, Some(budget.clone())] {
            let mut stream = source
                .read(ReadRequest {
                    path: path.clone(),
                    range: Some(0..1),
                    expected_source: Some(described.source_identity.clone()),
                    maximum_chunk_bytes: 8 * 1024 * 1024,
                    read_inflight: 1,
                    read_budget: None,
                    cancel: CancellationToken::new(),
                    source_qos: qos,
                    version: SourceVersion::Current,
                })
                .await?;
            assert_eq!(
                stream.next().await.ok_or("missing payload")??.as_ref(),
                b"x"
            );
            assert!(stream.next().await.is_none());
        }
        assert_eq!(budget.stats().source_read_operations, 2);
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await;
    stop.cancel();
    server.await?;
    result??;
    assert_eq!(
        connections.load(Ordering::Relaxed),
        1,
        "enabling source QoS must not create a second cold HTTP connection pool"
    );
    Ok(())
}
