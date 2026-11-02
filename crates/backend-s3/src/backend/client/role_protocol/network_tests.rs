use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn truncated_http_body_is_connection_loss_but_malformed_xml_is_protocol()
-> Result<(), Box<dyn std::error::Error>> {
    for (length, expected) in [
        (1000, FailureClass::Connectivity),
        (6, FailureClass::Protocol),
    ] {
        let server = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = server.local_addr()?;
        let responder = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await?;
            let mut request = Vec::new();
            let mut byte = [0];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await?;
                request.push(byte[0]);
            }
            socket.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {length}\r\nConnection: close\r\n\r\nbroken"
            ).as_bytes()).await?;
            socket.shutdown().await
        });
        let mut storage = crate::backend::client::tests::make_test_storage(None);
        storage.client = aws_sdk_s3::Client::from_conf(
            storage
                .client
                .config()
                .to_builder()
                .endpoint_url(format!("http://{address}"))
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                .build(),
        );
        let result = storage.role_list_objects_page("", None).await;
        responder.await??;
        assert!(
            matches!(result, Err(S3ProtocolFailure::Session { class, .. })
            if class == expected),
            "{result:?}"
        );
    }
    Ok(())
}
