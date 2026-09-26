//! The HCP REST client reaches the namespace the way its S3 URL does: scheme, host and port.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};

use reqwest::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::{HCPRestClient, HcpTransport};
use crate::AssertTestValue;

pub(crate) const ANNOTATION: &str = "<metaData><processInfo><bpdName>flow</bpdName>\
    <btNo>42</btNo></processInfo><data><uploader>alice</uploader></data></metaData>";

/// The head (request line and headers) of every request a local endpoint received.
pub(crate) type Seen = Arc<Mutex<Vec<String>>>;

/// A local HTTP endpoint answering every request with `status` and `body`.
pub(crate) async fn endpoint(status: &'static str, body: &'static str) -> (SocketAddr, Seen) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .assert_value("bind a local endpoint");
    let address = listener.local_addr().assert_value("local address");
    let seen = Seen::default();
    let record = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let record = record.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0_u8; 4096];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                record
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(String::from_utf8_lossy(&request).into_owned());
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    (address, seen)
}

/// A client for namespace `ns` of `tenant.test:<port>` whose name resolves to `address`.
pub(crate) fn client(address: SocketAddr) -> HCPRestClient {
    HCPRestClient::with_builder(
        "ns",
        &format!("tenant.test:{}", address.port()),
        HcpTransport::Http,
        ("AKHCP", "hcp/Secret+"),
        Client::builder()
            .no_proxy()
            .resolve("ns.tenant.test", address),
    )
    .assert_value("HCP client")
}

#[test]
fn the_rest_url_follows_the_s3_url() {
    let https = HCPRestClient::try_new(
        "ns",
        "tenant.hcp.example:8443",
        HcpTransport::Https {
            verify_certificates: true,
        },
        "a",
        "s",
    )
    .assert_value("https client");
    assert_eq!(https.base_url(), "https://ns.tenant.hcp.example:8443");
    let http = HCPRestClient::try_new("ns", "tenant.hcp.example", HcpTransport::Http, "a", "s")
        .assert_value("http client");
    assert_eq!(http.base_url(), "http://ns.tenant.hcp.example");
}

#[tokio::test]
async fn tags_are_read_from_the_namespace_at_the_urls_port() {
    let (address, seen) = endpoint("200 OK", ANNOTATION).await;
    let tags = client(address)
        .get_tags("/dir/a b.txt")
        .await
        .assert_value("annotation read");
    let pairs: Vec<(&str, &str)> = tags
        .iter()
        .map(|tag| (tag.key.as_str(), tag.value.as_str()))
        .collect();
    assert_eq!(
        pairs,
        [("bpdName", "flow"), ("btNo", "42"), ("uploader", "alice")]
    );
    let seen = seen.lock().unwrap_or_else(PoisonError::into_inner);
    let head = seen
        .first()
        .assert_value("one request")
        .to_ascii_lowercase();
    assert!(
        head.starts_with("get /rest/dir/a%20b.txt?type=custom-metadata&annotation=bpm http/1.1"),
        "{head}"
    );
    assert!(
        head.contains(&format!("host: ns.tenant.test:{}", address.port())),
        "{head}"
    );
    assert!(
        head.contains("cookie: hcp-ns-auth=akhcp:hcp/secret+"),
        "{head}"
    );
}
