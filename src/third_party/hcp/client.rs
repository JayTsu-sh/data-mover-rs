use std::str;
use std::sync::Arc;
use std::time::Duration;

use quick_xml::de::from_str;
use reqwest::cookie::Jar;
use reqwest::{Client, ClientBuilder, Url};

use tracing::debug;

use crate::Tag;
use crate::error::{Result, StorageError};

/// The outcome of one annotation read.
type AnnotationResult = std::result::Result<Option<Vec<Tag>>, HcpAnnotationError>;

/// The largest annotation body read (an annotation is a small XML document).
const MAX_ANNOTATION_BYTES: usize = 1024 * 1024;

/// How the HCP REST client reaches its namespace: as the namespace's S3 URL does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HcpTransport {
    /// `http://`: the `hcp-ns-auth` cookie crosses the network in clear text.
    Http,
    /// `https://`, the server certificate verified unless the S3 URL says `tls_verify=false`.
    Https { verify_certificates: bool },
}

/// How long the HCP REST client waits: the S3 connection's own timeouts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HcpTimeouts {
    /// Establishing a connection.
    pub connect: Duration,
    /// One whole request, reply body included.
    pub request: Duration,
}

/// The HCP REST interface of one namespace, reached like the namespace's S3 URL: the same scheme,
/// host and port (`{scheme}://{namespace}.{host}[:port]`), and certificates verified unless the
/// URL says `tls_verify=false`.
#[derive(Clone, Debug)]
pub struct HCPRestClient {
    client: Client,
    /// `{scheme}://{namespace}.{host}[:port]`.
    base_url: String,
    /// The same, parsed.
    base: Url,
}

/// Why an annotation could not be read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HcpAnnotationError {
    /// No answer: the request or its body was lost.
    Transport,
    /// The namespace answered with this status (not 404, which is no annotation).
    Status(u16),
    /// The annotation is not the expected XML, or larger than any annotation.
    Malformed,
    /// The key has a `.` or `..` segment, which an HTTP path cannot name: the request would read
    /// another object's annotation.
    UnaddressableKey,
}

impl HCPRestClient {
    /// A client for namespace `bucket_name` at `authority` (`host[:port]`); the `hcp-ns-auth`
    /// cookie carries the key pair.
    ///
    /// # Errors
    ///
    /// Returns an error when the base URL is invalid or the HTTP client cannot be built.
    pub fn try_new(
        bucket_name: &str,
        authority: &str,
        transport: HcpTransport,
        timeouts: HcpTimeouts,
        access_key: &str,
        secret_access_key: &str,
    ) -> Result<Self> {
        let accept_invalid_certs = matches!(
            transport,
            HcpTransport::Https {
                verify_certificates: false
            }
        );
        Self::with_builder(
            bucket_name,
            authority,
            transport,
            (access_key, secret_access_key),
            Client::builder()
                .tls_danger_accept_invalid_certs(accept_invalid_certs)
                .connect_timeout(timeouts.connect)
                .timeout(timeouts.request),
        )
    }

    fn with_builder(
        bucket_name: &str,
        authority: &str,
        transport: HcpTransport,
        (access_key, secret_access_key): (&str, &str),
        builder: ClientBuilder,
    ) -> Result<Self> {
        let scheme = match transport {
            HcpTransport::Http => "http",
            HcpTransport::Https { .. } => "https",
        };
        let base_url = format!("{scheme}://{bucket_name}.{authority}");
        let parsed_url: Url = base_url.parse().map_err(|e| {
            StorageError::ConfigError(format!("Invalid HCP base URL '{base_url}': {e}"))
        })?;
        let cookie_jar = Jar::default();
        cookie_jar.add_cookie_str(
            &format!("hcp-ns-auth={access_key}:{secret_access_key}"),
            &parsed_url,
        );
        let client = builder
            .cookie_provider(Arc::new(cookie_jar))
            .build()
            .map_err(|e| {
                StorageError::OperationError(format!("Failed to create HCP HTTP client: {e}"))
            })?;
        Ok(HCPRestClient {
            client,
            base_url,
            base: parsed_url,
        })
    }

    /// The `{scheme}://{namespace}.{host}[:port]` every request goes to.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// The tags of the `BPM` annotation of the object at `key` (of one version when `version` is
    /// given); `None` when the object has no such annotation (404, or an empty reply). A 404 does
    /// not tell a missing annotation from a missing object or version. The key's segments are
    /// percent-encoded.
    pub(crate) async fn annotation_tags(
        &self,
        key: &str,
        version: Option<&str>,
    ) -> AnnotationResult {
        if key.split('/').any(|segment| matches!(segment, "." | "..")) {
            return Err(HcpAnnotationError::UnaddressableKey);
        }
        let mut url = self.base.clone();
        // An http(s) base always has path segments.
        let Ok(mut segments) = url.path_segments_mut() else {
            return Err(HcpAnnotationError::UnaddressableKey);
        };
        segments.pop_if_empty().push("rest").extend(key.split('/'));
        drop(segments);
        url.query_pairs_mut()
            .append_pair("type", "custom-metadata")
            .append_pair("annotation", "BPM");
        if let Some(version) = version {
            url.query_pairs_mut().append_pair("version", version);
        }
        let response = self.client.get(url).send().await.map_err(|error| {
            // The error names the URL, never the cookie.
            debug!("HCP REST annotation request failed: {error}");
            HcpAnnotationError::Transport
        })?;
        match response.status().as_u16() {
            404 | 204 => return Ok(None),
            200..=299 => {}
            status => return Err(HcpAnnotationError::Status(status)),
        }
        if response.content_length().is_some_and(|length| {
            usize::try_from(length).map_or(true, |length| length > MAX_ANNOTATION_BYTES)
        }) {
            return Err(HcpAnnotationError::Malformed);
        }
        let body = response.bytes().await.map_err(|error| {
            debug!("HCP REST annotation body lost: {error}");
            HcpAnnotationError::Transport
        })?;
        if body.len() > MAX_ANNOTATION_BYTES {
            return Err(HcpAnnotationError::Malformed);
        }
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        let text = str::from_utf8(&body).map_err(|_| HcpAnnotationError::Malformed)?;
        MetaData::parse(text)
            .map(Some)
            .ok_or(HcpAnnotationError::Malformed)
    }

    ///
    /// # Errors
    ///
    /// Returns an error when the requested storage operation cannot be completed.
    pub async fn get_tags(&self, path: &str) -> Result<Vec<Tag>> {
        let url = format!(
            "{}/rest{path}?type=custom-metadata&annotation=BPM",
            self.base_url
        );

        let request = self.client.get(&url);

        let response = request.send().await.map_err(|e| {
            StorageError::OperationError(format!("Failed to send request to HCP: {e}"))
        })?;

        if !response.status().is_success() {
            return Err(StorageError::OperationError(format!(
                "HCP request failed with status: {}",
                response.status()
            )));
        }

        let body = response.text().await.map_err(|e| {
            StorageError::OperationError(format!("Failed to read HCP response: {e}"))
        })?;

        let tags = MetaData::parse_tags(&body);
        if tags.is_empty() {
            Err(StorageError::OperationError(
                "Failed to parse HCP XML response".to_string(),
            ))
        } else {
            Ok(tags)
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ProcessInfo {
    #[serde(rename = "bpdName")]
    bpd_name: Option<String>,
    #[serde(rename = "btNo")]
    bt_no: Option<String>,
    #[serde(rename = "createTime")]
    _create_time: Option<String>,
    #[serde(rename = "currentUser")]
    _current_user: Option<String>,
    #[serde(rename = "deptName")]
    _dept_name: Option<String>,
    #[serde(rename = "deptPath")]
    _dept_path: Option<String>,
    #[serde(rename = "employeeNumber")]
    _employee_number: Option<String>,
    #[serde(rename = "orgId")]
    _org_id: Option<String>,
    _title: Option<String>,
    #[serde(rename = "userId")]
    _user_id: Option<String>,
    #[serde(rename = "userName")]
    _user_name: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct Data {
    uploader: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
pub struct MetaData {
    #[serde(rename = "processInfo")]
    process_info: ProcessInfo,
    data: Data,
}

impl MetaData {
    #[must_use]
    pub fn parse_tags(xml: &str) -> Vec<Tag> {
        Self::parse(xml).unwrap_or_default()
    }

    /// The tags of an annotation, `None` when it is not the expected XML.
    fn parse(xml: &str) -> Option<Vec<Tag>> {
        if let Ok(metadata) = from_str::<MetaData>(xml) {
            let mut tags = Vec::new();

            // if let Some(val) = &metadata.process_info.create_time {
            //     tags.push(Tag {
            //         key: "createTime".to_string(),
            //         value: val.clone(),
            //     });
            // }
            if let Some(val) = &metadata.process_info.bpd_name {
                tags.push(Tag {
                    key: "bpdName".to_string(),
                    value: val.clone(),
                });
            }
            // if let Some(val) = &metadata.process_info.current_user {
            //     tags.push(Tag {
            //         key: "currentUser".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.dept_name {
            //     tags.push(Tag {
            //         key: "deptName".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.org_id {
            //     tags.push(Tag {
            //         key: "orgId".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.title {
            //     tags.push(Tag {
            //         key: "title".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.user_id {
            //     tags.push(Tag {
            //         key: "userId".to_string(),
            //         value: val.clone(),
            //     });
            // }
            if let Some(val) = &metadata.process_info.bt_no {
                tags.push(Tag {
                    key: "btNo".to_string(),
                    value: val.clone(),
                });
            }
            // if let Some(val) = &metadata.process_info.user_name {
            //     tags.push(Tag {
            //         key: "userName".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.dept_path {
            //     tags.push(Tag {
            //         key: "deptPath".to_string(),
            //         value: val.clone(),
            //     });
            // }
            // if let Some(val) = &metadata.process_info.employee_number {
            //     tags.push(Tag {
            //         key: "employeeNumber".to_string(),
            //         value: val.clone(),
            //     });
            // }
            if let Some(val) = &metadata.data.uploader {
                tags.push(Tag {
                    key: "uploader".to_string(),
                    value: val.clone(),
                });
            }

            Some(tags)
        } else {
            None
        }
    }
}

#[cfg(test)]
#[path = "client_tests.rs"]
pub(crate) mod tests;
